mod watcher;
mod logger;

use logger::Logger;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::env;
use std::fs;
use gethostname::gethostname;
use users::get_current_username;
use std::io::{self, Write};

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

#[derive(Subcommand, Debug)]
enum Commands {
    Start(ArgsStart),
}

fn pause_cli() {
    // 1. Prompt the user
    print!("Press Enter to continue...");
    
    // Ensure the prompt is immediately displayed to the user
    io::stdout().flush().expect("Failed to flush stdout");

    // 2. Read the entire line of input
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)
        .expect("Failed to read line");

    // The function returns once the user hits Enter.
}

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

fn setup_temporary_workspace(local_link_path: &Path) {
    // 1. host = socket.gethostname()
    let host = gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown_host".to_string());

    // 2. username = getpass.getuser()
    let username = get_current_username()
        .and_then(|name| name.into_string().ok())
        .unwrap_or_else(|| "unknown_user".to_string());

    // 3. tmp = os.environ.get("TMPDIR", "/tmp")
    let tmp_dir = env::var("TMPDIR")
        .unwrap_or_else(|_| "/tmp".to_string());
    
    // 4. workspace = Path(tmp)/f"msm_{host}_{username}"
    let workspace_target = PathBuf::from(tmp_dir)
        .join(format!("msm_{}_{}", host, username));

    println!("  - using symlinked tmp at [{}]", workspace_target.display());
    
    // 5. if local.is_symlink(): local.unlink()
    if local_link_path.is_symlink() {
        if let Err(e) = fs::remove_file(local_link_path) {
            panic!("Warning: Failed to unlink existing symlink: {}", e);
        } else {
            println!("  - Existing symlink unlinked.");
        }
    }
    
    // 6. if local.exists(): shutil.rmtree(local)
    // Note: This check only runs if it wasn't a symlink that was just unlinked.
    if local_link_path.exists() {
        if let Err(e) = fs::remove_dir_all(local_link_path) {
            panic!("Error: Failed to remove existing directory: {}", e);
        } else {
            println!("  - Existing directory removed.");
        }
    }

    // 7. Create the target directory
    if let Err(e) = fs::create_dir_all(&workspace_target) {
        panic!("Error: Failed to create workspace target directory: {}", e);
    }

    // 8. local.symlink_to(workspace)
    // IMPORTANT: Symlink creation is OS-specific. 
    // This example uses the cross-platform target method.
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::symlink;
        if let Err(e) = symlink(&workspace_target, local_link_path) {
            panic!("Error: Failed to create symlink (UNIX): {}", e);
        }
    }
    #[cfg(target_family = "windows")]
    {
        // Windows requires specifying if the target is a file or a directory
        use std::os::windows::fs::symlink_dir; 
        if let Err(e) = symlink_dir(&workspace_target, local_link_path) {
            panic!("Error: Failed to create symlink (WINDOWS): {}", e);
        }
    }
    #[cfg(not(any(target_family = "unix", target_family = "windows")))]
    {
        panic!("Warning: Symlink creation not supported/implemented for this OS.");
    }
}

fn main() {
    let cli = Cli::parse();

    let current_working_directory: PathBuf = env::current_dir()
        .unwrap_or_else(|e| {
            eprintln!("FATAL: Could not determine the Current Working Directory.");
            panic!("Error: {}", e);
        });
    // Store a boolean flag that captures the state of cli.io before the move.
    let is_io_default = cli.io.is_none(); // Capture the state before consuming cli.io
    let workspace: PathBuf = cli.io.unwrap_or_else(|| {
        calculate_default_io_path()
    });

    // Handle the logic based on the subcommand
    match cli.command {
        // We match on the enum variant and capture the specific arguments
        Commands::Start(args) => {
            println!("Setting up workspace");
            println!("  - at: [{}]", workspace.display());
            println!("  - cwd: [{}]", current_working_directory.display());

            // CONDITION CHECK (Python equivalent: if cli.io was set to default and not args.local)
            // 1. cli.io.is_none(): The user did NOT provide a custom I/O path.
            // 2. !args.local: The --local flag was NOT used (it's false).
            if is_io_default && !args.local {
                setup_temporary_workspace(&workspace);
            }
            
            watcher::run_watcher(&workspace, &current_working_directory)
        }
    }

    pause_cli()
}