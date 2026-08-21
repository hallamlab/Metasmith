mod watcher;
mod logger;
mod utils;
mod remote_shell;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::env;
use std::fs::{self, File};
use std::io::{self, Write, BufReader, BufRead};
use std::time::Duration;
use std::thread;
use gethostname::gethostname;
use users::get_current_username;
use nix::unistd::{fork, setsid, ForkResult};
use nix::sys::signal::{signal, SigHandler, SIGCHLD};

#[cfg(target_family = "unix")]
use std::os::unix::fs::symlink;
#[cfg(target_family = "windows")]
use std::os::windows::fs::symlink_dir;

use crate::watcher::{run_watcher, check_status_default_timeout, kill_run, Status, wipe_workspace};
use crate::utils::current_time_millis;
use crate::remote_shell::RemoteShell;

#[derive(Parser, Debug)]
#[command(author, version, about = None, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "PATH")]
    io: Option<std::path::PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
struct ArgsStart {
    #[arg(long, default_value_t = false)]
    local: bool,
    #[arg(long, default_value_t = false)]
    connected: bool,
}

#[derive(Debug, Parser)]
pub struct ArgsBounce {

    pub cmd: String,
}

#[derive(Debug, Parser)]
pub struct ArgsKillRun {

    pub token: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Start(ArgsStart),
    Stop,
    Status,
    Logs,
    Bounce(ArgsBounce),
    KillRun(ArgsKillRun),
}

fn calculate_default_io_path() -> PathBuf {

    let ws_path = std::env::current_exe()
        .map(|path| {

            path.parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| {

                    eprintln!("[ERROR] Executable path has no parent. Falling back to CWD.");
                    std::env::current_dir().unwrap_or_else(|e| {
                        panic!("FATAL: Cannot get CWD for fallback: {}", e)
                    })
                })
        })
        .unwrap_or_else(|e| {

            eprintln!("[ERROR] Failed to get executable path ({}). Falling back to CWD.", e);
            std::env::current_dir().unwrap_or_else(|e| {
                panic!("FATAL: Cannot get CWD for fallback: {}", e)
            })
        });

    let host_name = gethostname()
        .into_string()
        .unwrap_or_else(|_| {
            eprintln!("[ERROR] Failed to get system hostname. Using fallback host 'unknown_host'.");
            "unknown_host".to_string()
        });

    let default_io = ws_path.join(host_name);

    default_io
}

fn setup_workspace(local_link_path: &Path, is_local: bool) -> io::Result<()> {

    let host = gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown_host".to_string());
    let username = get_current_username()
        .and_then(|name| name.into_string().ok())
        .unwrap_or_else(|| "unknown_user".to_string());
    let tmp_dir = env::var("TMPDIR")
        .unwrap_or_else(|_| "/tmp".to_string());

    let workspace_target = PathBuf::from(tmp_dir)
        .join(format!("msm_{}_{}", host, username));

    if local_link_path.is_symlink() {
        println!(" - Unlinking existing symlink at: {}", local_link_path.display());
        fs::remove_file(local_link_path)?;
    }

    if local_link_path.exists() && !is_local {
        println!(" - Removing existing directory at: {}", local_link_path.display());
        fs::remove_dir_all(local_link_path)?;
    }

    if is_local {

        println!(" - Creating local workspace at: {}", local_link_path.display());
        fs::create_dir_all(local_link_path)?;

    } else {

        println!(" - Creating workspace at: {}", workspace_target.display());
        fs::create_dir_all(&workspace_target)?;

        println!(" - Linking workspace to: {}", local_link_path.display());

        #[cfg(target_family = "unix")]
        {
            symlink(&workspace_target, local_link_path)?;
        }
        #[cfg(target_family = "windows")]
        {
            symlink_dir(&workspace_target, local_link_path)?;
        }
        #[cfg(not(any(target_family = "unix", target_family = "windows")))]
        {
            return Err(io::Error::new(io::ErrorKind::Other,
                "Symlink creation not supported/implemented for this OS family."));
        }

    }

    Ok(())
}

fn print_logs(logs_path: &Path) -> io::Result<()> {
    if !logs_path.exists() {

        eprintln!("no logs at [{}]", logs_path.display());
        return Ok(());
    }

    let file = File::open(logs_path)?;

    let reader = BufReader::new(file);

    for line in reader.lines() {

        match line {
            Ok(l) => println!("{}", l),
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn main() {
    let cli = Cli::parse();

    let current_working_directory: PathBuf = env::current_dir()
        .unwrap_or_else(|e| {
            eprintln!("FATAL: Could not determine the Current Working Directory.");
            panic!("Error: {}", e);
        });
    let workspace: PathBuf = cli.io.unwrap_or_else(|| {
        calculate_default_io_path()
    });

    match cli.command {

        Commands::Start(args) => {
            let status = watcher::check_status_default_timeout(&workspace);
            if status.alive {

                println!(
                    "Relay server already running at [{}]",
                    workspace.display()
                );

            } else {
                println!("Starting relay:");
                println!("  - at: [{}]", workspace.display());
                println!("  - cwd: [{}]", current_working_directory.display());

                let _ = setup_workspace(&workspace, args.local);

                // Ignoring SIGCHLD is what reaps each dispatched job: nothing
                // waits on the `sh -c nohup ... &` the watcher spawns, so
                // without this every finished job leaves a zombie. Needed in
                // both modes -- a connected watcher dispatches the same way.
                unsafe {
                    match signal(SIGCHLD, SigHandler::SigIgn) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Failed to set SIGCHLD handler: {}", e);

                        }
                    }
                }

                if args.connected {

                    run_watcher(&workspace, &current_working_directory)
                } else {

                    match unsafe { fork() } {
                        Ok(ForkResult::Parent { child }) => {

                            println!("Watcher daemon PID: {}", child);

                            let check_interval = Duration::from_millis(100);

                            loop {

                                let status = check_status_default_timeout(&workspace);

                                if status.alive {
                                    println!("pid [{}]", status.pid);
                                    println!("success");

                                    std::process::exit(0);
                                }

                                thread::sleep(check_interval);
                            }
                        }

                        Ok(ForkResult::Child) => {

                            // Leave the login shell's process group. Otherwise a
                            // dropped ssh connection SIGHUPs that group, the
                            // shutdown guard runs, and the relay takes down the
                            // bookkeeping for jobs it only half-killed. A daemon
                            // relay stops on `msm_relay stop` and nothing else.
                            if let Err(e) = setsid() {
                                eprintln!("Failed to detach watcher into its own session: {}", e);
                            }

                            run_watcher(&workspace, &current_working_directory)
                        }

                        Err(e) => {

                            eprintln!("Failed to fork process: {}", e);

                            panic!("Fork failed: {}", e);

                        }
                    }
                }
            }
        }
        Commands::Stop => {

            let active_path = workspace.join("active");

            if active_path.exists() {
                match fs::remove_file(&active_path) {
                    Ok(_) => {

                        println!("Signalled relay to stop by removing 'active' file");
                    }
                    Err(e) => {

                        eprintln!(
                            "FATAL: Failed to remove 'active' file at {}: {}",
                            active_path.display(),
                            e
                        );
                    }
                }
            } else {

                println!("Relay not running");
            }

            let start = current_time_millis();
            let timeout_seconds: u64 = 5;
            let timeout_ms = (timeout_seconds as u128) * 1000;
            loop {

                let status: Status = check_status_default_timeout(&workspace);

                if !status.alive {
                    println!("shutdown success");
                    return;
                }

                let now = current_time_millis();

                if now.saturating_sub(start) >= timeout_ms {
                    break;
                }

                thread::sleep(Duration::from_millis(100));
            }

            wipe_workspace(&workspace);

            println!("shutdown enforced");
        }
        Commands::Bounce(args) => {

            let shell_result = RemoteShell::new(&workspace, 3, None);

            match shell_result {
                Ok(shell) => {

                    let key_out = shell.register_on_out(|msg| {

                        println!("{}", msg);

                        let _ = io::stdout().flush();
                    });

                    let key_err = shell.register_on_err(|msg| {

                        eprintln!("{}", msg);

                        let _ = io::stderr().flush();
                    });

                    let _ = shell.exec(&args.cmd, None);

                    shell.remove_on_out(key_out);
                    shell.remove_on_err(key_err);
                    shell.dispose();
                }
                Err(e) => {
                    eprintln!("Failed to initialize RemoteShell for bounce command: {}", e);

                }
            }
        }
        Commands::KillRun(args) => {

            let killed = kill_run(&workspace, &args.token);

            println!("killed [{}] job(s) of run [{}]", killed, args.token);
        }
        Commands::Status => {
            let status = watcher::check_status_default_timeout(&workspace);
            println!("Status:");
            println!("  - alive: {}", status.alive);
            println!("  - PID: {}", status.pid);
            println!("  - active jobs: {}", status.jobs.join(", "));
        }
        Commands::Logs => {

            let logs_path = workspace.join("main.log");

            if let Err(e) = print_logs(&logs_path) {

                eprintln!("Failed to process log file: {}", e);
            }
        }
    }
}
