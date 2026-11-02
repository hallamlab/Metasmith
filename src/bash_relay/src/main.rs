mod watcher;
mod logger;
mod utils;
mod remote_shell;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, BufReader, BufRead};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use std::thread;
use gethostname::gethostname;
use users::{get_current_uid, get_current_username};
use nix::fcntl::FlockArg;
use nix::sys::signal::{kill, signal, SigHandler, Signal, SIGCHLD};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{dup2, fork, setsid, ForkResult, Pid};

#[cfg(target_family = "unix")]
use std::os::unix::fs::symlink;
#[cfg(target_family = "windows")]
use std::os::windows::fs::symlink_dir;

use crate::watcher::{run_watcher, check_status, check_status_default_timeout, kill_run, live_owner, pid_is_relay, wipe_workspace};
use crate::remote_shell::RemoteShell;

/// The engine version this relay ships with, baked in by `dev.sh -b`. A watcher reports it in
/// its status, so a `start` from a newer binary can replace an idle older watcher.
pub const VERSION: &str = match option_env!("MSM_ENGINE_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

// A start that cannot get its watcher answering by then has failed, and says why.
const START_DEADLINE: Duration = Duration::from_secs(30);
const START_LOCK_WAIT: Duration = Duration::from_secs(40);
const STALLED_OWNER_WAIT_S: u64 = 10;
const STOP_WAIT: Duration = Duration::from_secs(5);
const LOG_TAIL_LINES: usize = 20;

#[derive(Parser, Debug)]
#[command(author, version = VERSION, about = None, long_about = None)]
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

// Every agent home on a node has its own workspace: a start that finds another home's
// watcher in a shared one would shut it down along with that home's running jobs.
// FNV-1a rather than std's hasher, whose output Rust does not promise across releases,
// so a relay upgrade still finds the workspace its predecessor wrote.
fn home_token(link: &Path) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in link.as_os_str().to_string_lossy().bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:012x}", h & 0xffff_ffff_ffff)
}

fn workspace_target(link: &Path) -> PathBuf {
    let host = gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown_host".to_string());
    // A static musl binary cannot reach NSS, so on LDAP-backed clusters the name
    // lookup fails for everyone; the uid keeps each user's workspace their own.
    let username = get_current_username()
        .and_then(|name| name.into_string().ok())
        .or_else(|| env::var("USER").ok().filter(|u| !u.is_empty()))
        .unwrap_or_else(|| format!("uid{}", get_current_uid()));
    let tmp_dir = env::var("TMPDIR")
        .unwrap_or_else(|_| "/tmp".to_string());

    PathBuf::from(tmp_dir).join(format!("msm_{}_{}_{}", host, username, home_token(link)))
}

// The lock sits on node-local disk beside the workspace target, never in the agent home:
// flock on Lustre is not reliable, and the race it closes is between starts on one node.
fn lock_path(workspace: &Path, is_local: bool) -> PathBuf {
    let base = if is_local { workspace.to_path_buf() } else { workspace_target(workspace) };
    let mut name = base.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(".lock");
    base.with_file_name(name)
}

#[allow(deprecated)]
fn acquire_lock(path: &Path, wait: Duration) -> io::Result<Option<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    let start = Instant::now();
    loop {
        // The deprecated free function, not nix's Flock guard: the guard unlocks on drop, and a
        // forked watcher dropping its inherited copy would release the parent's lock with it.
        match nix::fcntl::flock(file.as_raw_fd(), FlockArg::LockExclusiveNonblock) {
            Ok(()) => return Ok(Some(file)),
            Err(nix::Error::EWOULDBLOCK) | Err(nix::Error::EINTR) => {}
            Err(e) => return Err(io::Error::from(e)),
        }
        if start.elapsed() >= wait {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn setup_workspace(local_link_path: &Path, is_local: bool) -> io::Result<()> {
    if is_local {
        if local_link_path.is_symlink() {
            fs::remove_file(local_link_path)?;
        }
        println!(" - Creating local workspace at: {}", local_link_path.display());
        fs::create_dir_all(local_link_path)?;
        return Ok(());
    }

    let workspace_target = workspace_target(local_link_path);
    println!(" - Creating workspace at: {}", workspace_target.display());
    fs::create_dir_all(&workspace_target)?;

    if local_link_path.is_symlink() {
        if fs::read_link(local_link_path)? == workspace_target {
            return Ok(());
        }
    } else if local_link_path.exists() {
        println!(" - Removing existing directory at: {}", local_link_path.display());
        fs::remove_dir_all(local_link_path)?;
    }

    println!(" - Linking workspace to: {}", local_link_path.display());
    // Linked beside and renamed over, so no reader ever finds the path missing.
    let mut staged_name = local_link_path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    staged_name.push(format!(".link.{}", std::process::id()));
    let staged = local_link_path.with_file_name(staged_name);
    let _ = fs::remove_file(&staged);

    #[cfg(target_family = "unix")]
    {
        symlink(&workspace_target, &staged)?;
    }
    #[cfg(target_family = "windows")]
    {
        symlink_dir(&workspace_target, &staged)?;
    }
    #[cfg(not(any(target_family = "unix", target_family = "windows")))]
    {
        return Err(io::Error::new(io::ErrorKind::Other,
            "Symlink creation not supported/implemented for this OS family."));
    }

    fs::rename(&staged, local_link_path)
}

fn print_log_tail(workspace: &Path) {
    let logs_path = workspace.join("main.log");
    let lines: Vec<String> = match File::open(&logs_path) {
        Ok(f) => BufReader::new(f).lines().map_while(Result::ok).collect(),
        Err(e) => {
            eprintln!("no watcher log at [{}]: {}", logs_path.display(), e);
            return;
        }
    };
    eprintln!("--- last {} lines of [{}] ---", LOG_TAIL_LINES, logs_path.display());
    for l in &lines[lines.len().saturating_sub(LOG_TAIL_LINES)..] {
        eprintln!("{}", l);
    }
}

fn ignore_sigchld() {
    // Ignoring SIGCHLD is what reaps each dispatched job: nothing waits on the
    // `sh -c nohup ... &` the watcher spawns, so without this every finished job
    // leaves a zombie. Only the watcher sets it; the start parent must still
    // be able to wait on the watcher it forked.
    unsafe {
        if let Err(e) = signal(SIGCHLD, SigHandler::SigIgn) {
            eprintln!("Failed to set SIGCHLD handler: {}", e);
        }
    }
}

// Leave the login shell's session, so a dropped ssh connection cannot SIGHUP the
// watcher, and let go of its stdio: a write to a terminal that has gone kills the
// watcher on the broken pipe.
fn detach() {
    let _ = setsid();
    if let Ok(null) = OpenOptions::new().read(true).write(true).open("/dev/null") {
        for fd in 0..3 {
            let _ = dup2(null.as_raw_fd(), fd);
        }
    }
}

/// Remove `active` and wait for the watcher to leave. With its pid known the wait is on the
/// process itself, since a status probe answers "dead" the moment `active` is gone.
fn stop_watcher(workspace: &Path, owner: Option<i32>, wait: Duration) -> bool {
    let active_path = workspace.join("active");
    if active_path.exists() {
        if let Err(e) = fs::remove_file(&active_path) {
            eprintln!("Failed to remove 'active' file at {}: {}", active_path.display(), e);
        }
    }
    let start = Instant::now();
    loop {
        let gone = match owner {
            Some(pid) => !pid_is_relay(pid),
            None => !check_status(workspace, 1).alive,
        };
        if gone {
            return true;
        }
        if start.elapsed() >= wait {
            return false;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_watcher(workspace: &Path, child: Pid) -> i32 {
    let start = Instant::now();
    loop {
        match waitpid(child, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => {}
            Ok(status) => {
                eprintln!("watcher [{}] exited before it came up: {:?}", child, status);
                print_log_tail(workspace);
                return 1;
            }
            Err(e) => {
                eprintln!("cannot wait on watcher [{}]: {}", child, e);
                print_log_tail(workspace);
                return 1;
            }
        }

        let status = check_status(workspace, 1);
        if status.alive && status.pid == child.as_raw() {
            println!("pid [{}]", status.pid);
            println!("success");
            return 0;
        }

        if start.elapsed() >= START_DEADLINE {
            eprintln!("watcher [{}] did not answer within {}s; killing it", child, START_DEADLINE.as_secs());
            let _ = kill(child, Signal::SIGKILL);
            let _ = waitpid(child, None);
            print_log_tail(workspace);
            return 1;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn start(workspace: &PathBuf, cwd: &PathBuf, args: &ArgsStart) -> i32 {
    let lock_file = lock_path(workspace, args.local);
    let lock = match acquire_lock(&lock_file, START_LOCK_WAIT) {
        Ok(Some(f)) => f,
        Ok(None) => {
            eprintln!("another `msm_relay start` held [{}] for {}s", lock_file.display(), START_LOCK_WAIT.as_secs());
            return 1;
        }
        Err(e) => {
            eprintln!("cannot take the start lock [{}]: {}", lock_file.display(), e);
            return 1;
        }
    };

    let mut status = check_status_default_timeout(workspace);
    if !status.alive {
        if let Some(pid) = live_owner(workspace) {
            status = check_status(workspace, STALLED_OWNER_WAIT_S);
            if !status.alive {
                eprintln!(
                    "watcher [{}] owns [{}] but does not answer; leaving it and its jobs alone. \
                     Stop it with `msm_relay stop` if it is stuck.",
                    pid, workspace.display(),
                );
                return 1;
            }
        }
    }

    if status.alive {
        if status.version == VERSION {
            println!("Relay server already running at [{}]", workspace.display());
            println!("pid [{}]", status.pid);
            return 0;
        }
        if !status.jobs.is_empty() {
            println!(
                "Relay server [{}] at [{}] is version [{}], not [{}]; keeping it while it has {} job(s)",
                status.pid, workspace.display(), status.version, VERSION, status.jobs.len(),
            );
            return 0;
        }
        println!("Replacing idle watcher [{}] version [{}] with [{}]", status.pid, status.version, VERSION);
        if !stop_watcher(workspace, Some(status.pid), STOP_WAIT) {
            eprintln!("watcher [{}] did not stop within {}s", status.pid, STOP_WAIT.as_secs());
            return 1;
        }
    }

    println!("Starting relay:");
    println!("  - at: [{}]", workspace.display());
    println!("  - cwd: [{}]", cwd.display());

    if let Err(e) = setup_workspace(workspace, args.local) {
        eprintln!("failed to set up the relay workspace at [{}]: {}", workspace.display(), e);
        return 1;
    }

    if args.connected {
        ignore_sigchld();
        run_watcher(workspace, cwd, Some(lock));
        return 0;
    }

    match unsafe { fork() } {
        Ok(ForkResult::Parent { child }) => {
            println!("Watcher daemon PID: {}", child);
            wait_for_watcher(workspace, child)
        }
        Ok(ForkResult::Child) => {
            // Closing this copy leaves the parent's lock held until it has seen the watcher answer.
            drop(lock);
            detach();
            ignore_sigchld();
            run_watcher(workspace, cwd, None);
            0
        }
        Err(e) => {
            eprintln!("Failed to fork the watcher: {}", e);
            1
        }
    }
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
            std::process::exit(start(&workspace, &current_working_directory, &args));
        }
        Commands::Stop => {
            let is_local = workspace.exists() && !workspace.is_symlink();
            // Taken to keep a concurrent start from reading a half-stopped workspace, but a stop
            // still proceeds without it: a wedged start must never make a relay unstoppable.
            let _lock = acquire_lock(&lock_path(&workspace, is_local), START_LOCK_WAIT).ok().flatten();
            let owner = live_owner(&workspace);

            if !workspace.join("active").exists() {
                println!("Relay not running");
            }
            if stop_watcher(&workspace, owner, STOP_WAIT) {
                println!("shutdown success");
                return;
            }

            if let Some(pid) = owner.filter(|p| pid_is_relay(*p)) {
                let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
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

                    let code = shell.exec(&args.cmd, None);

                    shell.remove_on_out(key_out);
                    shell.remove_on_err(key_err);
                    shell.dispose();
                    std::process::exit(code);
                }
                Err(e) => {
                    eprintln!("Failed to initialize RemoteShell for bounce command: {}", e);
                    std::process::exit(1);
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
            println!("  - version: {}", status.version);
        }
        Commands::Logs => {

            let logs_path = workspace.join("main.log");

            if let Err(e) = print_logs(&logs_path) {

                eprintln!("Failed to process log file: {}", e);
            }
        }
    }
}
