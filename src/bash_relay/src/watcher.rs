use crate::logger::Logger;
use crate::utils::{generate_id, current_time_millis};

use std::path::{Path, PathBuf};
use std::time::Duration;
use std::thread;
use std::io::{self, Read, Write};
use std::fs::{self, Permissions, File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::process::Command;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde::{Serialize, Deserialize};
use scopeguard::guard;
// =========================================================================
// RUST EQUIVALENT OF @dataclass class Status
// =========================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub alive: bool,
    // pid is an i32 in Rust/nix, matching the Python integer type
    #[serde(default = "default_pid")]
    pub pid: i32,
    #[serde(default)] // Use default for Vec<String> which is an empty vector
    pub jobs: Vec<String>,
}

fn default_pid() -> i32 {
    -1
}

impl Status {
    /// Equivalent to Status.Load(status_file: Path)
    pub fn load(status_file: &Path) -> Option<Self> {
        if !status_file.exists() {
            return None;
        }
        
        // Match on the Result of reading and deserializing
        match fs::File::open(status_file)
            .and_then(|mut f| {
                let mut contents = String::new();
                f.read_to_string(&mut contents)?;
                serde_json::from_str::<Status>(&contents)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            }) 
        {
            Ok(status) => Some(status),
            // Equivalent to Python's blanket 'except: return None'
            Err(e) => {
                eprintln!("Warning: Failed to load status file {}: {}", status_file.display(), e);
                None
            }
        }
    }

    /// Equivalent to Status.Save(self, status_file: Path)
    pub fn save(&self, status_file: &Path) -> io::Result<()> {
        // 1. Serialize the struct to a JSON string
        let json_string = serde_json::to_string_pretty(self)?;
        
        // 2. Write the JSON string to the file
        fs::write(status_file, json_string)?;
        
        Ok(())
    }
}

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
pub fn wipe_workspace(workspace: &Path) -> bool {
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

// 1. Define the constant for macOS targets (including iOS)
#[cfg(any(target_os = "macos", target_os = "ios"))]
const TARGET_SHELL_EXE: &str = "zsh";

// 2. Define the constant for all other targets
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const TARGET_SHELL_EXE: &str = "bash";

// =========================================================================
// RUST EQUIVALENT OF setting up "launcher" and "active" files
// =========================================================================

// Note: This function requires the CWD as a path to include it in the script.
// We'll assume CWD is passed in, matching your `run_watcher` style.
pub fn setup_launcher_script(workspace: &Path, cwd: &Path, active_path: &Path, launcher_path: &Path) -> io::Result<()> {
    Logger::info(&format!(
        "Setting up launcher script at: {}", 
        launcher_path.display()
    ));

    // --- 1. Create the 'active' file (active.touch(0o644)) ---
    
    // Create the file (touch creates it if it doesn't exist)
    File::create(&active_path)?; 
    
    // Set permissions to 0o644 (read/write for owner, read-only for group/other)
    // NOTE: PermissionsExt is Unix-specific. 
    // If targeting Windows, this permission step should be conditional or skipped.
    let permissions_644 = Permissions::from_mode(0o644);
    fs::set_permissions(&active_path, permissions_644)?;
    
    // --- 2. Generate the Script Content (f.write("\n".join(script))) ---
    let script_content: String = [
        "SCRIPT=$1",
        "PIDF=$2",
        "DONEF=$3",
        &format!("cd {}", cwd.display()),
        &format!("{} {}/$SCRIPT &", TARGET_SHELL_EXE, workspace.display()),
        &format!("cd {}", workspace.display()),
        "PID=$!",
        "echo $PID > $PIDF",
        "wait $PID",
        "STATUS=$?",
        "rm $PIDF",
        "rm $SCRIPT",
        "echo $STATUS > $DONEF",
    ].join("\n");

    // --- 3. Write and Set Permissions for launcher.sh ---
    // Open/Create launcher.sh for writing
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true) // Overwrite existing content
            .open(&launcher_path)?;

        // Write the script content
        file.write_all(script_content.as_bytes())?;
    } // File is automatically closed here when `file` goes out of scope

    // Make the launcher script executable (0o755)
    let permissions_755 = Permissions::from_mode(0o755);
    fs::set_permissions(&launcher_path, permissions_755)?;
    Ok(())
}

// =========================================================================
// RUST EQUIVALENT OF dispatch() (local to RunWatcher)
// =========================================================================
/// Dispatches a job by logging its contents, moving the file, and running the launcher
/// in the background using nohup-like behavior.
///
/// Note: This function requires that the necessary log files (OUT, ERR, PID, DONE)
/// are derived from the stem of the input file `start_file`.
pub fn dispatch(start_file: &Path, workspace: &Path, launcher: &Path) -> io::Result<()> {
    
    // 1. Log the script content
    Logger::info(&format!("{}>>>", "-".repeat(25)));
    
    match fs::File::open(start_file) {
        Ok(mut h) => {
            let mut content = String::new();
            h.read_to_string(&mut content)?;
            
            // Log each line (Python's logic removes a single trailing newline if present)
            for l in content.lines() {
                Logger::info(l);
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to read start file {}: {}", start_file.display(), e));
            return Err(e);
        }
    }
    
    Logger::info(&format!("<<<{}", "-".repeat(25)));

    // Ensure the start_file has a stem (for file naming)
    let file_stem = start_file.file_stem()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Start file must have a stem"))?;
    
    let live_name = format!("{}.running", file_stem.to_string_lossy());
    let live_path = workspace.join(&live_name);

    let out_file = format!("{}.out", file_stem.to_string_lossy());
    let err_file = format!("{}.err", file_stem.to_string_lossy());
    let pid_file = format!("{}.pid", file_stem.to_string_lossy());
    let done_file = format!("{}.done", file_stem.to_string_lossy());

    // 2. Perform file operations and execute the command

    // cd {workspace}
    let mut command = Command::new("sh");
    command.current_dir(workspace);

    // mv {f.name} {live}
    // We do this in Rust to avoid complexity in the shell command string
    fs::rename(start_file, &live_path)?;

    // Construct the nohup command line execution string:
    // nohup bash {launcher} {live} {f.stem}.pid {f.stem}.done >{f.stem}.out 2>{f.stem}.err &
    let shell_command = format!(
        "nohup {} {} {} {} {} >{} 2>{} &",
        TARGET_SHELL_EXE,
        launcher.to_string_lossy(),
        live_name,
        pid_file,
        done_file,
        out_file,
        err_file
    );

    // Execute the constructed command using 'sh -c'
    // This allows us to use 'nohup ... &', which is required for background execution
    command.arg("-c").arg(shell_command);

    // Run the command and detach it
    let child = command
        .spawn()
        .map_err(|e| {
            Logger::error(&format!("Failed to spawn background job: {}", e));
            // Attempt to move the file back if spawning failed
            let _ = fs::rename(&live_path, start_file);
            e
        })?;

    // Since 'nohup ... &' is used, the shell process is now detached and running in the background.
    // We simply drop the Child handle, as we don't need to track the shell's PID, 
    // only the job's PID (which the launcher writes to the .pid file).
    std::mem::forget(child);
    
    Ok(())
}

// =========================================================================
// RUST EQUIVALENT OF RunWatcher
// =========================================================================
// Note the 'pub' keyword is crucial to make it visible outside this file
pub fn run_watcher(workspace: &PathBuf, cwd: &PathBuf) {
    Logger::init_log_file(&workspace)
        .expect("FATAL: Failed to initialize main log file");
    Logger::info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    wipe(&workspace);
    let active_path = workspace.join("active");
    let launcher_path = workspace.join("launcher.sh");
    setup_launcher_script(&workspace, &cwd, &active_path, &launcher_path)
        .expect("FATAL: Failed to create launcher script");
    
    // 1. Create a shared Atomic flag
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // 2. Install the Ctrl+C signal handler
    // This closure runs on a separate thread when SIGINT is received.
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    // The variable `_guard` is a ScopeGuard object.
    let workspace_for_guard = workspace.clone();
    let _guard = guard(active_path.clone(), |path| {
        Logger::info("Watcher shutting down");
        // This closure runs when the _guard is dropped (when run_watcher exits)
        if path.exists() {
            if let Err(e) = fs::remove_file(&path) {
                Logger::error(&format!("FATAL: Failed to remove 'active' file on watcher exit: {}", e));
            } else {
                Logger::info("'active' file removed.");
            }
        }
        try_kill_jobs(&workspace_for_guard);
        Logger::info("Watcher has stopped");
        Logger::info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        Logger::flush_and_close_log_file();
    });

    let current_pid = nix::unistd::getpid().as_raw();
    // Equivalent to 'while True:'
    // but we check the Arc for ctrlc
    while running.load(Ordering::SeqCst) {
        let mut active_jobs: Vec<String> = Vec::new();
        let mut status_checks: Vec<PathBuf> = Vec::new();
        let mut found_active = false;

        // Equivalent to 'for f in workspace.iterdir():'
        match fs::read_dir(workspace) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
                    
                    if file_name == active_path.file_name().unwrap().to_string_lossy() {
                        found_active = true;
                    } 
                    else if file_name.ends_with(".start") {
                        let _ = dispatch(&path, &workspace, &launcher_path);
                    } 
                    else if file_name.ends_with(".pid") {
                        // Equivalent to f.stem (file name without extension)
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            active_jobs.push(stem.to_string());
                        }
                    } 
                    else if file_name.ends_with(".check") {
                        status_checks.push(path);
                    }
                }
            },
            Err(e) => {
                Logger::error(&format!("Error reading workspace directory: {}", e));
                break; // Exit loop on directory read failure
            }
        }
        
        // Equivalent to 'if not found:'
        if !found_active {
            Logger::info("'active' file was deleted");
            running.store(false, Ordering::SeqCst); // Ensure the loop terminates
        }

        // Equivalent to 'for f in status_checks:'
        for check_path in status_checks {
            let status_data = Status {
                jobs: active_jobs.clone(), // Clone needed as active_jobs is needed in the next iteration
                alive: true,
                pid: current_pid,
            };
            
            // Equivalent to f.with_suffix(".status")
            let status_path = check_path.with_extension("status"); 
            
            if let Err(e) = status_data.save(&status_path) {
                 Logger::error(&format!("Failed to save status file {}: {}", status_path.display(), e));
            }
            // Remove the .check file after processing, mimicking the Python side-effect
            if let Err(e) = fs::remove_file(&check_path) {
                 Logger::error(&format!("Failed to remove check request file {}: {}", check_path.display(), e));
            }
        }

        // Equivalent to 'time.sleep(0.1)'
        thread::sleep(Duration::from_millis(100));
    }
    // _guard is automatically dropped at the end of the function.
}

// =========================================================================
// RUST EQUIVALENT OF CheckStatus
// =========================================================================

pub fn check_status(workspace: &Path, timeout: u64) -> Status {
    struct CleanupGuard {
        sig_path: PathBuf,
        result_path: PathBuf,
    }

    impl CleanupGuard {
        pub fn new(sig: PathBuf, result: PathBuf) -> Self {
            CleanupGuard { sig_path: sig, result_path: result }
        }
    }

    impl Drop for CleanupGuard {
        fn drop(&mut self) {
            // Equivalent to Python's finally block cleanup
            if self.sig_path.exists() {
                let _ = fs::remove_file(&self.sig_path); // Use '_' to ignore simple errors
            }
            if self.result_path.exists() {
                let _ = fs::remove_file(&self.result_path);
            }
        }
    }

    let active_path = workspace.join("active");
    
    // 1. if not active.exists(): return Status(alive=False)
    if !active_path.exists() {
        return Status { alive: false, pid: -1, jobs: Vec::new() };
    }

    // 2. sig = workspace/f"{GenerateId()}.check"
    let id = generate_id();
    let sig_file_name = format!("{}.check", id);
    let sig = workspace.join(&sig_file_name);
    
    // 3. result = sig.with_suffix(".status")
    let result = sig.with_extension("status"); 
    
    // 4. sig.touch()
    // Create the file. Use fs::File::create() which acts like 'touch'
    if let Err(e) = fs::File::create(&sig) {
         Logger::error(&format!("Failed to create check file {}: {}", sig.display(), e));
         return Status { alive: false, pid: -1, jobs: Vec::new() };
    }

    // Initialize the cleanup guard (equivalent to try/finally)
    let _cleanup = CleanupGuard::new(sig.clone(), result.clone());
    
    // 5. start = CurrentTimeMillis()
    let start_time = current_time_millis();
    let timeout_ms = (timeout as u128) * 1000;
    
    // 6. while CurrentTimeMillis()-start <= timeout*1000:
    loop {
        let elapsed = current_time_millis().saturating_sub(start_time);

        if elapsed > timeout_ms {
            Logger::info(&format!(
                "Status check timed out after {}ms. Returning Status(alive=False).", 
                elapsed
            ));
            // 7. return Status(alive=False)
            return Status { alive: false, pid: -1, jobs: Vec::new() };
        }

        // 8. time.sleep(0.02)
        thread::sleep(Duration::from_millis(20));

        // 9. if not result.exists(): continue
        if !result.exists() {
            continue;
        }

        // 10. status = Status.Load(result)
        // 11. if status is None: continue
        match Status::load(&result) {
            Some(status) => {
                // 12. return status
                return status;
            }
            None => {
                // Status file exists but failed to load/parse, so we continue polling
                continue;
            }
        }
    }
    // Cleanup is guaranteed by the drop of '_cleanup' here
}
pub fn check_status_default_timeout(workspace: &Path) -> Status {
    check_status(&workspace, 2)
}