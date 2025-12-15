use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::Duration;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::borrow::Cow;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use crate::utils::{generate_id, current_time_millis};

/// Finds the indentation of the first non-empty line and removes that much
/// indentation from all lines in the string.
fn remove_leading_indent(s: &str) -> String {
    let lines: Vec<&str> = s.split('\n').collect();
    
    if lines.is_empty() {
        return s.to_string();
    }
    
    let mut indent: usize = 0;
    
    // 1. Find the indentation of the first non-empty line
    for line in &lines {
        if line.is_empty() {
            continue;
        }
        
        // Calculate indentation for this line
        for c in line.chars() {
            if c != ' ' && c != '\t' {
                break;
            }
            indent += 1;
        }
        // Break after checking the first non-empty line
        break;
    }
    
    // If no indentation was found (or all lines were empty), return the original string trimmed
    if indent == 0 {
        return s.trim().to_string();
    }
    
    // 2. Remove the calculated indentation from every line
    let de_indented_lines: Vec<Cow<str>> = lines.iter()
        .map(|line| {
            if line.len() >= indent {
                // Remove indent characters from the start
                Cow::Borrowed(&line[indent..])
            } else {
                // If a line is shorter than the indent (e.g., an empty line),
                // it might become empty, but we must keep it to maintain line count.
                Cow::Borrowed(*line)
            }
        })
        .collect();
        
    // 3. Join, strip, and selectively re-add trailing newline
    
    // Combine lines into a single string
    let mut cleaned = de_indented_lines.join("\n");
    
    // Get the length of the string before the final cleanup, 
    // to check the state of the last line.
    let last_line_after_de_indentation = lines.last()
        .map(|l| {
            if l.len() >= indent {
                &l[indent..]
            } else {
                l
            }
        })
        .unwrap_or("");


    // Apply .strip() equivalent
    cleaned = cleaned.trim().to_string();
    
    // If lines[-1][indent:] == "": cleaned += "\n"
    // This checks if the last line, after de-indenting, was empty or only whitespace
    // (the Python `split` doesn't remove trailing newlines, so an empty last element means 
    // the original string ended with a newline).
    if last_line_after_de_indentation.is_empty() {
        // The last line was effectively empty after cleanup, so add a trailing newline
        cleaned.push('\n');
    }
    
    cleaned
}

// --- Job Structure (Job and ShellResult) ---
// Equivalent to Python's job management object
struct Job {
    // key: String,
    out_log: PathBuf,
    err_log: PathBuf,
    done_path: PathBuf,
    
    // Equivalent to Python's j.out_i and j.err_i (file pointers)
    out_index: u64,
    err_index: u64,
    
    // Note: The Python class doesn't store the .compile/.start paths, but we will.
    start_path: PathBuf, 
}

fn get_pid_from_file(pid_file: &Path) -> Result<i32, String> {
    if !pid_file.exists() {
        return Err(format!("PID file not found: {}", pid_file.display()));
    }
    
    let contents = fs::read_to_string(pid_file)
        .map_err(|e| format!("Failed to read PID file: {}", e))?;

    let pid_str = contents.trim();
    pid_str.parse::<i32>()
        .map_err(|_| format!("Invalid PID format in file: {}", pid_str))
}

impl Job {
    fn new(key: String, base_path: &Path) -> Self {
        let compile_path = base_path.join(format!("{}.compile", key));
        Job {
            // key,
            out_log: compile_path.with_extension("out"),
            err_log: compile_path.with_extension("err"),
            done_path: compile_path.with_extension("done"),
            start_path: compile_path.with_extension("start"),
            out_index: 0,
            err_index: 0,
        }
    }

    /// Equivalent to Python's SignalStop method.
    /// Sends a SIGINT signal to the process specified in the .pid file.
    pub fn signal_stop(&self) {
        let pid_file = self.out_log.with_extension("pid");

        // if pidf.exists():
        if !pid_file.exists() {
            return;
        }

        // try: ... except ProcessLookupError: pass
        if let Ok(pid) = get_pid_from_file(&pid_file) {
            let nix_pid = Pid::from_raw(pid);
            
            // os.kill(pid, signal.SIGINT)
            match kill(nix_pid, Signal::SIGINT) {
                Ok(_) => {
                    println!("SignalStop: Sent SIGINT to PID {}", pid);
                }
                Err(nix::Error::ESRCH) => {
                    // Handles the case where the process ID no longer exists (ProcessLookupError)
                    println!("PID {} not found during SIGTERM (ProcessLookupError).", pid);
                }
                Err(e) => {
                    // Handle all other types of errors
                    println!("Failed to send SIGTERM to PID {}: {}", pid, e);
                }
            }
        } else {
            // Error logged by get_pid_from_file, or we handle it here if it failed to parse.
            eprintln!("SignalStop: Could not retrieve valid PID from {}", pid_file.display());
        }
    }
    
    /// Equivalent to Python's Dispose method for cleanup and termination.
    /// Returns the exit code (default 1 if not found/error).
    pub fn dispose(&self, timeout: f64) -> i32 {
        let one_tenth_sec = Duration::from_millis(100);
        
        // 1. Resolve file paths
        let pid_file = self.out_log.with_extension("pid");
        let done_file = self.out_log.with_extension("done");
        
        // 2. Wait for the done file (for _ in range(int(timeout*10)))
        let max_iterations = (timeout * 10.0).round() as u64;

        for _ in 0..max_iterations {
            if done_file.exists() {
                break;
            }
            sleep(one_tenth_sec);
        }
        
        let mut exit_code: i32 = 1;
        
        if !done_file.exists() {
            // 3. DONE file not found: Try to kill the process (enforcement)
            if let Ok(pid) = get_pid_from_file(&pid_file) {
                // os.kill(pid, signal.SIGTERM)
                let nix_pid = Pid::from_raw(pid);
                
                match kill(nix_pid, Signal::SIGTERM) {
                    Ok(_) => {
                        println!("Successfully sent SIGTERM to PID {}", pid);
                        sleep(Duration::from_millis(500)); // sleep(0.5)
                    }
                    Err(nix::Error::ESRCH) => {
                        // ProcessLookupError: PID doesn't exist, ignore
                        println!("PID {} not found during SIGTERM (ProcessLookupError).", pid);
                    }
                    Err(e) => {
                        eprintln!("Failed to send SIGTERM to PID {}: {}", pid, e);
                    }
                }
            }
        } else {
            // 4. DONE file exists: Read the exit code
            // with open(donef) as f: code = f.readline().strip()
            match fs::read_to_string(&done_file) {
                Ok(contents) => {
                    let code_str = contents.lines().next().unwrap_or("1").trim();
                    // try/except block to parse int
                    match code_str.parse::<i32>() {
                        Ok(c) => exit_code = c,
                        Err(_) => {
                            eprintln!("Invalid exit code in .done file: '{}'. Defaulting to 1.", code_str);
                            exit_code = 1;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read .done file {}: {}", done_file.display(), e);
                    exit_code = 1;
                }
            }
        }

        // 5. Cleanup files
        let mut files_to_delete = vec![
            &self.out_log,
            &self.err_log,
        ];
        
        if done_file.exists() {
            // Only clean up the control files if the job finished cleanly
            files_to_delete.push(&done_file);
            files_to_delete.push(&pid_file);
        }
        
        // p.unlink(missing_ok=True)
        for p in files_to_delete {
            if p.exists() {
                if let Err(e) = fs::remove_file(p) {
                    eprintln!("Failed to delete job file {}: {}", p.display(), e);
                }
            }
        }

        exit_code
    }
}

// /// Equivalent to Python's ShellResult
// pub struct ShellResult {
//     pub out: Vec<String>,
//     pub err: Vec<String>,
// }

// --- The RemoteShell Structure ---

// Define the type for the callback functions
type LogCallback = Box<dyn Fn(String) + Send + Sync + 'static>;
/// A unique identifier for a registered callback.
type CallbackId = usize; 
// Static counter to generate unique IDs
static NEXT_CALLBACK_ID: AtomicUsize = AtomicUsize::new(1);
// Define the core type for a callback trait object
// Type alias for the map we are operating on for cleaner function signature
type CallbackMap = Arc<Mutex<HashMap<CallbackId, LogCallback>>>;
// Implement the Python context manager traits

pub struct RemoteShell {
    _watcher_path: PathBuf,
    _timeout: Duration,
    _setup_commands: Vec<String>,
    
    // Thread-safe storage for callbacks (since they might be called from a polling thread)
    _out_callbacks: CallbackMap,
    _err_callbacks: CallbackMap,
    
    // Thread-safe storage for active jobs
    _active_jobs: Arc<Mutex<HashMap<String, Job>>>,
}

impl RemoteShell {
    pub fn new(watcher_path: &Path, timeout: u64, setup_commands: Option<Vec<String>>) -> io::Result<Self> {
        if !watcher_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("Watcher path does not exist: [{}]", watcher_path.display())));
        }
        
        Ok(RemoteShell {
            _watcher_path: watcher_path.to_owned(),
            _timeout: Duration::from_secs(timeout),
            _setup_commands: setup_commands.unwrap_or_default(),
            _out_callbacks: Arc::new(Mutex::new(HashMap::new())),
            _err_callbacks: Arc::new(Mutex::new(HashMap::new())),
            _active_jobs: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    
    // Implementing RAII equivalent to __enter__ and __exit__
    // The idiomatic way is to handle cleanup in a 'dispose' method called at the end.
    
    // Equivalent to Dispose()
    pub fn dispose(&self) {
        // Lock jobs and signal/dispose all running jobs
        let active_jobs = self._active_jobs.lock().unwrap();
        
        for j in active_jobs.values() {
            j.signal_stop();
        }
        // Need to wait for watcher to clean up, but for simplicity:
        let timeout_seconds = self._timeout.as_secs_f64();
        for j in active_jobs.values() {
            j.dispose(timeout_seconds);
        }
    }
    
    // Helper to allow this struct to be used in a block where cleanup is guaranteed
    // In Rust, this often means creating a wrapper or using it directly in the scope
    // where its drop should occur.
    
    // Helper to generate a new unique ID
    fn generate_callback_id() -> CallbackId {
        NEXT_CALLBACK_ID.fetch_add(1, Ordering::SeqCst)
    }

    pub fn register_on_out<F>(&self, callback: F) -> CallbackId
    where F: Fn(String) + Send + Sync + 'static {
        let id = Self::generate_callback_id();
        self._out_callbacks.lock().unwrap().insert(id, Box::new(callback));
        id
    }
    
    pub fn register_on_err<F>(&self, callback: F) -> CallbackId
    where F: Fn(String) + Send + Sync + 'static {
        let id = Self::generate_callback_id();
        self._err_callbacks.lock().unwrap().insert(id, Box::new(callback));
        id
    }
    
    /// Private helper to handle the thread-safe removal logic for any callback map.
    fn remove_callback(&self, callbacks: &CallbackMap, id: CallbackId, map_name: &str) -> bool {
        // Attempt to lock the Mutex
        let mut map_guard = match callbacks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Log the poisoning error to stderr
                eprintln!("Warning: {} Mutex was poisoned during removal.", map_name);
                // Recover the inner data if poisoned
                poisoned.into_inner()
            }
        };

        // Call .remove() on the HashMap held by the MutexGuard
        map_guard.remove(&id).is_some()
    }

    /// Removes a registered output callback by its ID.
    /// Returns true if the callback was found and removed.
    pub fn remove_on_out(&self, id: CallbackId) -> bool {
        self.remove_callback(&self._out_callbacks, id, "_out_callbacks")
    }

    /// Removes a registered error callback by its ID.
    /// Returns true if the callback was found and removed.
    pub fn remove_on_err(&self, id: CallbackId) -> bool {
        self.remove_callback(&self._err_callbacks, id, "_err_callbacks")
    }
    
    // Equivalent to ExecAsync
    pub fn exec_async(&self, cmd: &str) -> String {
        let script = remove_leading_indent(cmd);
        let k = generate_id();
        let job = Job::new(k.clone(), &self._watcher_path);
        
        // Capture the target path *before* the move
        let job_start_path = job.start_path.clone(); // <--- Clone the path
        let job_compile_path = job.start_path.with_extension("compile");

        let result = (|| -> io::Result<()> {
            // 1. Write the job script (.compile)
            let mut f = File::create(&job_compile_path)?;
            
            for line in &self._setup_commands {
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")?;
            }
            f.write_all(script.as_bytes())?;
            f.write_all(b"\n")?; 
            
            // 2. Rename the compile file to the start file (.start)
            // Use the cloned path variable, job_start_path
            fs::rename(&job_compile_path, &job_start_path)?; // Atomic action
            
            // 3. Store active job *after* the rename operation has used its fields
            self._active_jobs.lock().unwrap().insert(k.clone(), job);
            
            Ok(())
        })();

        if let Err(e) = result {
            // Handle error during file creation/rename
            eprintln!("Error during ExecAsync setup: {}", e);
            // In a production system, you might want to remove the job from active_jobs here
            return String::new(); // Return empty key on error
        }
        
        k
    }

    // Equivalent to AwaitDone
    pub fn await_done(&self, timeout: Option<Duration>, key_filter: Option<&str>) {
        let start = current_time_millis();
        let mut dt = Duration::from_millis(100);
        const MAX_DT: Duration = Duration::from_millis(500);

        // Helper function equivalent to check_log
        let check_log = |log_path: &Path, start_index: &mut u64, callbacks: &CallbackMap| -> io::Result<()> {
            if !log_path.exists() { return Ok(()); }
            
            // Open file for reading
            let mut file = OpenOptions::new().read(true).open(log_path)?;
            
            // Seek to the last read position
            file.seek(SeekFrom::Start(*start_index))?;

            // Read lines from the current position
            let mut reader = BufReader::new(file);
            let mut lines = String::new();
            
            // Read all remaining data
            if reader.read_to_string(&mut lines)? == 0 { return Ok(()); }
            
            let mut current_offset = *start_index;
            let mut buffer = String::new();
            
            // Re-read lines to ensure we only send complete, newline-terminated lines
            for line in lines.lines() {
                buffer.clear();
                buffer.push_str(line);
                buffer.push('\n'); // Add newline back for length calculation
                
                // Check if this line was a complete line ending in '\n' in the file
                // The BufReader + lines() iterator simplifies this significantly in Rust.
                // We assume lines() strips the newline.
                
                let line_len = buffer.len() as u64; // Length including the stripped newline
                
                // Update file pointer for the next iteration
                // We rely on the total bytes read to update the index precisely
                // The Python logic is tricky due to f.seek(start) and readlines().
                // Rust needs to track bytes more carefully.

                // A simpler, more reliable Rust approach:
                if let Some(map_guard) = callbacks.lock().ok() {
                    // Iterate over the values of the HashMap
                    for cb in map_guard.values() {
                        // println!(">{} | {}", start_index, line.to_string());
                        cb(line.to_string());
                    }
                }
                current_offset += line_len;
            }
            
            *start_index = current_offset;
            Ok(())
        };

        loop {
            let mut finished_keys = Vec::new();
            
            // Lock active jobs for iteration and modification
            let mut active_jobs = self._active_jobs.lock().unwrap();
            
            // Use collect and then iterate to satisfy the borrow checker when removing keys
            let keys: Vec<String> = active_jobs.keys().cloned().collect();
            
            for k in keys {
                if let Some(j) = active_jobs.get_mut(&k) {
                    // Check logs and update indexes
                    let _ = check_log(&j.out_log, &mut j.out_index, &self._out_callbacks);
                    let _ = check_log(&j.err_log, &mut j.err_index, &self._err_callbacks);
                    
                    // Check for done file
                    if j.done_path.exists() {
                        let timeout_seconds = self._timeout.as_secs_f64();
                        j.dispose(timeout_seconds);
                        finished_keys.push(k.clone());
                    }
                }
            }

            // Remove finished jobs
            for k in finished_keys {
                active_jobs.remove(&k);
            }
            
            // Check exit condition
            let is_done = match key_filter {
                Some(k) => !active_jobs.contains_key(k),
                None => active_jobs.is_empty(),
            };

            if is_done { break; }

            let now = current_time_millis();
            if let Some(t) = timeout {
                if now.saturating_sub(start) > t.as_millis() { break; }
            }

            // Sleep and backoff
            thread::sleep(dt);
            dt = dt.saturating_add(Duration::from_millis(100)).min(MAX_DT);
        }
    }

    // Equivalent to Exec
    pub fn exec(&self, cmd: &str, timeout: Option<Duration>) {
        // let out_arc: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        // let err_arc: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        
        // let out_clone = out_arc.clone();
        // let err_clone = err_arc.clone();

        // // Define local capture callbacks
        // let _on_out = move |msg: String| { out_clone.lock().unwrap().push(msg); };
        // let _on_err = move |msg: String| { err_clone.lock().unwrap().push(msg); };
        
        // let mut out_id: Option<CallbackId> = None;
        // let mut err_id: Option<CallbackId> = None;
        
        // if history {
        //     // Store the returned IDs
        //     let key_out = self.register_on_out(_on_out);
        //     let key_err = self.register_on_err(_on_err);
        //     out_id = Some(key_out);
        //     err_id = Some(key_err);
        // }

        let key = self.exec_async(cmd);
        // Wait for job completion
        self.await_done(timeout, Some(&key));
        
        // Final cleanup
        if let Some(mut active_jobs) = self._active_jobs.lock().ok() {
            if let Some(job) = active_jobs.remove(&key) {
                let timeout_seconds = self._timeout.as_secs_f64();
                job.dispose(timeout_seconds);
            }
        }
        
        // if history {
        //     // Use the stored IDs to remove the specific callbacks
        //     if let Some(id) = out_id {
        //         self.remove_on_out(id);
        //     }
        //     if let Some(id) = err_id {
        //         self.remove_on_err(id);
        //     }
        // }
        
        // ShellResult {
        //     out: Arc::try_unwrap(out_arc).unwrap_or_default().into_inner().unwrap_or_default(),
        //     err: Arc::try_unwrap(err_arc).unwrap_or_default().into_inner().unwrap_or_default(),
        // }
    }
}