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
use nix::unistd::{fork, ForkResult};
use nix::sys::signal::{signal, SigHandler, SIGCHLD};
// Assuming the necessary cfg blocks for symlink are available
#[cfg(target_family = "unix")]
use std::os::unix::fs::symlink;
#[cfg(target_family = "windows")]
use std::os::windows::fs::symlink_dir;

use crate::watcher::{run_watcher, check_status_default_timeout, Status, wipe_workspace};
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

// Define the argument structure for the 'Bounce' command
#[derive(Debug, Parser)]
pub struct ArgsBounce {
    /// The command string to be executed remotely via the relay.
    pub cmd: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Start(ArgsStart),
    Stop,
    Status,
    Logs,
    Bounce(ArgsBounce),
}

// fn pause_cli() {
//     // 1. Prompt the user
//     print!("Press Enter to continue...");
    
//     // Ensure the prompt is immediately displayed to the user
//     io::stdout().flush().expect("Failed to flush stdout");

//     // 2. Read the entire line of input
//     let mut buffer = String::new();
//     io::stdin().read_line(&mut buffer)
//         .expect("Failed to read line");

//     // The function returns once the user hits Enter.
// }

// --- Function to calculate the default path ---
fn calculate_default_io_path() -> PathBuf {
    // 1. Get the directory of the executable (WS) OR use CWD as fallback
    let ws_path = std::env::current_exe()
        .map(|path| {
            // Success: Got the executable path. Get its parent directory.
            path.parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| {
                    // Executable path was valid but had no parent (unlikely, but handled)
                    eprintln!("[ERROR] Executable path has no parent. Falling back to CWD.");
                    std::env::current_dir().unwrap_or_else(|e| {
                        panic!("FATAL: Cannot get CWD for fallback: {}", e)
                    })
                })
        })
        .unwrap_or_else(|e| {
            // Error: Failed to get executable path. Fall back to CWD.
            eprintln!("[ERROR] Failed to get executable path ({}). Falling back to CWD.", e);
            std::env::current_dir().unwrap_or_else(|e| {
                panic!("FATAL: Cannot get CWD for fallback: {}", e)
            })
        });

    // 2. Get the hostname
    let host_name = gethostname()
        .into_string() // Convert OsString to String
        .unwrap_or_else(|_| {
            eprintln!("[ERROR] Failed to get system hostname. Using fallback host 'unknown_host'.");
            "unknown_host".to_string()
        });

    // 3. Combine them: WS / host
    let default_io = ws_path.join(host_name);

    default_io
}

fn setup_workspace(local_link_path: &Path, is_local: bool) -> io::Result<()> {
    // --- Step 1: Determine Target and Cleanup Logic ---
    
    // This is the common temporary directory path, regardless of is_local.
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

    // --- Step 2: Clean up the local_link_path (which might be a symlink or a directory) ---

    // 5. if local.is_symlink(): local.unlink()
    if local_link_path.is_symlink() {
        println!(" - Unlinking existing symlink at: {}", local_link_path.display());
        fs::remove_file(local_link_path)?;
    }
    
    // 6. if local.exists(): shutil.rmtree(local)
    // This removes the path if it's a regular directory *or* if it was a dangling symlink target
    // that wasn't previously cleaned up.
    if local_link_path.exists() && !is_local {
        println!(" - Removing existing directory at: {}", local_link_path.display());
        fs::remove_dir_all(local_link_path)?;
    }

    // --- Step 3: Conditional Setup ---

    if is_local {
        // --- A. LOCAL MODE: Create the directory directly at local_link_path ---
        
        println!(" - Creating local workspace at: {}", local_link_path.display());
        fs::create_dir_all(local_link_path)?;
        
    } else {
        // --- B. DEFAULT/REMOTE MODE: Create symlink to the temporary target ---
        
        // 7. Create the target directory
        println!(" - Creating workspace at: {}", workspace_target.display());
        fs::create_dir_all(&workspace_target)?;

        // 8. local.symlink_to(workspace_target)
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
        
        // Final cleanup for the Target (This handles the Python comment about only cleaning 
        // up the *original* folder if not args.local)
        // If we successfully created the symlink, the old local_link_path (if any) is gone. 
        // We only clean up the *target* directory if the job is done, not here.
    }
    
    Ok(())
}

/// Equivalent to the Python _logs function logic.
fn print_logs(logs_path: &Path) -> io::Result<()> {
    if !logs_path.exists() {
        // Equivalent to Log.Error(f"no logs at [{logs_path}]")
        eprintln!("no logs at [{}]", logs_path.display());
        return Ok(()); // Return Ok here to avoid crashing on missing file
    }

    // Open the file
    let file = File::open(logs_path)?;
    
    // Wrap the file in a BufReader for efficient line-by-line reading
    let reader = BufReader::new(file);

    // Iterate over the lines in the file
    for line in reader.lines() {
        // Print the line (equivalent to print(l, end="") in Python)
        // println! automatically adds a newline, so we use print! 
        // and handle the newline manually if needed, but BufRead::lines() 
        // typically strips the newline, so print! with a newline is fine if 
        // you want standard output. If the log file already contains newlines, 
        // we must be careful.
        
        // The common approach for printing stripped lines:
        match line {
            Ok(l) => println!("{}", l), // Prints the stripped line followed by a newline
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

    // Handle the logic based on the subcommand
    match cli.command {
        // We match on the enum variant and capture the specific arguments
        Commands::Start(args) => {
            let status = watcher::check_status_default_timeout(&workspace);
            if status.alive {
                // Python: Log.Warn(f"relay server already running at [{workspace}]")
                println!(
                    "Relay server already running at [{}]", 
                    workspace.display()
                );
                // Python: return (Exits the current function, which might be `main` or another setup function)
                // If the calling function expects an io::Result<()>, we return Ok(()).
            } else {
                println!("Starting relay:");
                println!("  - at: [{}]", workspace.display());
                println!("  - cwd: [{}]", current_working_directory.display());
    
                let _ = setup_workspace(&workspace, args.local);
                
                if args.connected {
                    // 1. Connected Mode (Foreground)
                    // RunWatcher should handle its own errors
                    run_watcher(&workspace, &current_working_directory)
                } else {
                    // 2. Disconnected Mode (Background/Daemon)
    
                    // Set signal handler to ignore SIGCHLD (prevents zombie processes)
                    // Safety: Signal handlers are unsafe because they interact with low-level OS mechanics.
                    unsafe {
                        match signal(SIGCHLD, SigHandler::SigIgn) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Failed to set SIGCHLD handler: {}", e);
                                // Decide if this is a fatal error
                            }
                        }
                    }
    
                    // Fork the process
                    match unsafe { fork() } {
                        Ok(ForkResult::Parent { child }) => {
                            // PARENT PROCESS LOGIC
                            // Parent is responsible for monitoring the child's status briefly,
                            // then exiting to leave the child daemonized.
    
                            println!("Watcher daemon PID: {}", child);
                            
                            // Loop until the watcher confirms it is alive (via CheckStatus)
                            let check_interval = Duration::from_millis(100);
                            
                            // Rust equivalent of the Python parent loop:
                            loop {
                                // CheckStatus needs to be implemented to look at the .pid file, etc.
                                let status = check_status_default_timeout(&workspace); 
    
                                if status.alive {
                                    println!("pid [{}]", status.pid);
                                    println!("success");
                                    
                                    // Exit the parent process cleanly.
                                    // os._exit(0) is best translated as a simple, non-panicking exit.
                                    std::process::exit(0); 
                                }
    
                                // Handle Ctrl+C (KeyboardInterrupt) by just exiting the parent loop
                                // In Rust, handling Ctrl+C requires a dedicated handler on the main thread,
                                // but for this simple loop, we rely on the signal handler.
                                thread::sleep(check_interval);
                            }
                        }
                        
                        Ok(ForkResult::Child) => {
                            // CHILD PROCESS LOGIC
                            // The child runs the watcher and becomes the daemon.
    
                            // ResetGenerator() equivalent: If there are resources that should not be 
                            // shared or inherited from the parent (like a random number generator state), 
                            // they would be reset here. (Often not strictly necessary in Rust unless 
                            // dealing with global state).
    
                            run_watcher(&workspace, &current_working_directory)
                        }
                        
                        Err(e) => {
                            // ERROR HANDLING
                            eprintln!("Failed to fork process: {}", e);
                            // Return or panic, as the desired execution mode failed
                            panic!("Fork failed: {}", e);
                            // return Err(io::Error::new(io::ErrorKind::Other, format!("Fork failed: {}", e)));
                        }
                    }
                }
            }
        }
        Commands::Stop => {
            // 1. active = (workspace/"active")
            let active_path = workspace.join("active");
            // 2. if active.exists(): active.unlink()
            if active_path.exists() {
                match fs::remove_file(&active_path) {
                    Ok(_) => {
                        // 3. Log.Info(f"signalled watcher to stop")
                        println!("Signalled relay to stop by removing 'active' file");
                    }
                    Err(e) => {
                        // If we failed to remove it, report the error
                        eprintln!(
                            "FATAL: Failed to remove 'active' file at {}: {}",
                            active_path.display(),
                            e
                        );
                    }
                }
            } else {
                // Optional: Log that the file was not there, in case it was already stopped.
                println!("Relay not running");
            }

            let start = current_time_millis();
            let timeout_seconds: u64 = 5; // Python equivalent: timeout = 5
            let timeout_ms = (timeout_seconds as u128) * 1000;
            loop {
                // 1. _status = CheckStatus(workspace)
                let status: Status = check_status_default_timeout(&workspace);

                // 2. if not _status.alive:
                if !status.alive {
                    println!("shutdown success");
                    return; // shutdown success
                }

                let now = current_time_millis();
                
                // 3. if now-start >= timeout*1000: break
                if now.saturating_sub(start) >= timeout_ms {
                    break; // Timeout reached
                }

                // Wait a short moment before polling again
                thread::sleep(Duration::from_millis(100));
            }

            // --- Cleanup Enforcement (Outside the loop) ---
            // Wipe(workspace)
            wipe_workspace(&workspace);

            println!("shutdown enforced");
        }
        Commands::Bounce(args) => {

            // 1. Instantiate RemoteShell(args.io)
            let shell_result = RemoteShell::new(&workspace, 3, None); // Use default timeout=3
            
            match shell_result {
                Ok(shell) => {
                    // 2. RegisterOnOut(print) and RegisterOnErr(lambda x: print(x, file=sys.stderr))
                    
                    // Output to stdout
                    let key_out = shell.register_on_out(|msg| {
                        // Use println! which implicitly writes to stdout
                        println!("{}", msg);
                        // Ensure stdout is flushed immediately for real-time output
                        let _ = io::stdout().flush();
                    });
                    
                    // Output to stderr
                    let key_err = shell.register_on_err(|msg| {
                        // Use eprintln! which implicitly writes to stderr
                        eprintln!("{}", msg);
                        // Ensure stderr is flushed immediately
                        let _ = io::stderr().flush();
                    });
                    
                    // 3. Exec(args.cmd, timeout=None)
                    // Exec will block until the job is done or an internal timeout is hit.
                    let _ = shell.exec(&args.cmd, None); 

                    // The output is already printed via the registered callbacks.
                    // We don't need to manually print 'result.out' or 'result.err' here,
                    // but we can check for an exit code if `ShellResult` includes one.
                    
                    // The cleanup guard `_cleanup` is dropped here, calling `shell.dispose()`.
                    shell.remove_on_out(key_out);
                    shell.remove_on_err(key_err);
                    shell.dispose();
                }
                Err(e) => {
                    eprintln!("Failed to initialize RemoteShell for bounce command: {}", e);
                    // Optional: Exit the process with an error code
                    // std::process::exit(1);
                }
            }
        }
        Commands::Status => {
            let status = watcher::check_status_default_timeout(&workspace);
            println!("Status:");
            println!("  - alive: {}", status.alive);
            println!("  - PID: {}", status.pid);
            println!("  - active jobs: {}", status.jobs.join(", ")); // Assuming jobs is Vec<String>
        }
        Commands::Logs => {
            // Equivalent to _logs(Path(args.io)/"main.log")
            let logs_path = workspace.join("main.log");
            
            // Call the Rust function
            if let Err(e) = print_logs(&logs_path) {
                // Handle errors during file opening/reading 
                eprintln!("Failed to process log file: {}", e);
            }
        }
    }
}