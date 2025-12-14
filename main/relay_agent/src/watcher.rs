use std::path::{Path, PathBuf};
use std::fs;
use std::time::Duration;
use std::thread;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use crate::logger::Logger;

// =========================================================================
// RUST EQUIVALENT OF _try_kill_jobs(workspace: Path)
// =========================================================================

/// Attempts to kill processes found in *.pid files within the workspace.
fn try_kill_jobs(workspace: &Path) {
    if let Ok(entries) = fs::read_dir(workspace) {
        for entry in entries.flatten() {
            let path = entry.path();
            
            // 1. if not f.name.endswith(".pid"): continue
            if !path.extension().map_or(false, |ext| ext == "pid") {
                continue;
            }

            // Rust equivalent of try: ... except: pass
            if let Err(e) = (|| -> Result<(), Box<dyn std::error::Error>> {
                // 2. with open(f) as fh: pid = fh.readline().strip()
                let content = fs::read_to_string(&path)?;
                let pid_str = content.trim();

                // 3. pid = int(pid)
                let pid_val: i32 = pid_str.parse()?;
                let pid = Pid::from_raw(pid_val);

                // 4. os.kill(pid, signal.SIGTERM)
                // Use nix::kill for cross-platform signal sending
                kill(pid, Signal::SIGTERM)?;
                
                Logger::info(&format!("  - sent SIGTERM to PID: [{}]", pid_val));
                
                Ok(())
            })() {
                // Equivalent to the Python 'except: pass', but logs the error
                Logger::error(&format!("  - Warning: Failed to kill [{}]", e));
            }
        }
    } else {
        Logger::error(&format!("Warning: Failed to read workspace directory for killing jobs: {}", workspace.display()));
    }
}

// =========================================================================
// RUST EQUIVALENT OF Wipe(workspace: Path)
// =========================================================================

/// Kills tracked processes, removes files, and returns true if 'active' was found.
fn wipe_workspace(workspace: &Path) -> bool {
    // 1. _try_kill_jobs(workspace)
    try_kill_jobs(workspace);

    let mut safe_wait = false;
    
    if let Ok(entries) = fs::read_dir(workspace) {
        for entry in entries.flatten() {
            let path = entry.path();
            
            // Ensure we don't try to delete the workspace directory itself
            if path == workspace { continue; } 

            // 2. if f.name == "main.log": continue
            if path.file_name().map_or(false, |name| name == "main.log") {
                continue;
            }

            // 3. if f.name == "active": safe_wait = True
            if path.file_name().map_or(false, |name| name == "active") {
                safe_wait = true;
            }
            
            // 4. f.unlink() (Delete the file/symlink/empty dir)
            // Use remove_file, as iterdir() likely returns files/symlinks/empty dirs.
            // If we encounter a non-empty directory, it will fail, which is usually fine.
            if path.is_dir() {
                if let Err(e) = fs::remove_dir(&path) {
                    Logger::error(&format!("  - Warning: Could not remove directory {}: {}", path.display(), e));
                    // If it's a non-empty directory, we let the outer logic handle it,
                    // but we can't delete it with remove_dir.
                }
            } else if let Err(e) = fs::remove_file(&path) {
                Logger::error(&format!("  - Warning: Could not remove file {}: {}", path.display(), e));
            }
        }
    } else {
        Logger::error(&format!("Warning: Failed to read workspace directory for file deletion: {}", workspace.display()));
    }

    // 5. return safe_wait
    safe_wait
}

fn wipe(workspace: &PathBuf) {
    Logger::info("Cleaning up previous workspace");
    // Equivalent to: if Wipe(workspace): time.sleep(1)
    if wipe_workspace(&workspace) {
        Logger::info("  - Pausing for 1 second");
        thread::sleep(Duration::from_secs(1)); 
    }
}

// This function is equivalent to Python's RunWatcher class or function
// Note the 'pub' keyword is crucial to make it visible outside this file
pub fn run_watcher(workspace: &PathBuf, cwd: &PathBuf) {
    Logger::init_log_file(&workspace)
        .expect("FATAL: Failed to initialize main log file. Check workspace permissions?");
    wipe(&workspace);

    

    Logger::close_log_file();
}