use std::fs::{self, OpenOptions, File};
use std::io::{self, Write};
use std::path::Path;
use chrono::Local;
use lazy_static::lazy_static;
use std::sync::Mutex;

// --- Global Log File Handle ---
// This safely holds the log file handle, allowing multiple threads to write to it.
lazy_static! {
    static ref LOG_FILE: Mutex<Option<File>> = Mutex::new(None);
}

// =========================================================================
// RUST EQUIVALENT OF Log CLASS
// =========================================================================

/// Initializes the log file and provides simple logging functions.
pub struct Logger;

impl Logger {
    /// Equivalent to Log::AddLogFile for the main log file.
    /// Creates the log file at workspace/main.log if it doesn't exist.
    pub fn init_log_file(workspace: &Path) -> io::Result<()> {
        let log_path = workspace.join("main.log");
        
        // 1. Create the parent directory if it doesn't exist
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // 2. Open the file in append mode, creating it if necessary
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)?;

        // 3. Store the file handle globally
        let mut log_file_guard = LOG_FILE.lock().unwrap();
        *log_file_guard = Some(file);

        Ok(())
    }

    // Equivalent to Log::Error
    pub fn error(message: &str) {
        Self::log_to_file("E", message);
    }

    // Equivalent to Log::Info
    pub fn info(message: &str) {
        Self::log_to_file(" ", message);
    }
    
    // Private method to handle the actual timestamped file writing
    fn log_to_file(level: &str, message: &str) {
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        
        let log_line = format!("[{}] [{}] {}\n", timestamp, level, message);

        // Lock the file handle and write the line
        let mut log_file_guard = LOG_FILE.lock().unwrap();
        if let Some(file) = log_file_guard.as_mut() {
            if let Err(e) = file.write_all(log_line.as_bytes()) {
                // If writing fails, we can only report it to stderr
                eprintln!("FATAL: Failed to write to log file: {}", e);
            }
        }
    }

    // Equivalent to Log::RemoveLogFile (closing the file)
    pub fn close_log_file() {
        let mut log_file_guard = LOG_FILE.lock().unwrap();
        *log_file_guard = None; // Drop the file handle, causing it to close
    }
}