use std::fs::{self, OpenOptions, File};
use std::io::{self, Write};
use std::path::Path;
use chrono::Local;
use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    static ref LOG_FILE: Mutex<Option<File>> = Mutex::new(None);
}

pub struct Logger;

impl Logger {

    pub fn init_log_file(workspace: &Path) -> io::Result<()> {
        let log_path = workspace.join("main.log");

        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)?;

        let mut log_file_guard = LOG_FILE.lock().unwrap();
        *log_file_guard = Some(file);

        Ok(())
    }

    pub fn error(message: &str) {
        Self::log_to_file("E", message);
    }

    pub fn info(message: &str) {
        Self::log_to_file(" ", message);
    }

    fn log_to_file(level: &str, message: &str) {
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();

        let log_line = format!("{} {}| {}\n", timestamp, level, message);

        let mut log_file_guard = LOG_FILE.lock().unwrap();
        if let Some(file) = log_file_guard.as_mut() {
            if let Err(e) = file.write_all(log_line.as_bytes()) {

                eprintln!("FATAL: Failed to write to log file: {}", e);
            }
        }
    }

    pub fn flush_and_close_log_file() {

        let mut log_file_guard = match LOG_FILE.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {

                eprintln!("Warning: LOG_FILE Mutex was poisoned. Proceeding with cleanup.");
                poisoned.into_inner()
            }
        };

        if let Some(file) = log_file_guard.as_mut() {

            if let Err(e) = file.flush() {

                eprintln!("FATAL: Failed to flush log file before closing: {}", e);
            }
        }

        *log_file_guard = None;
    }
}
