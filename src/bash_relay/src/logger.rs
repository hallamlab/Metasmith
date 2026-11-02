use std::fs::{self, OpenOptions, File};
use std::io::{self, Write};
use std::path::Path;
use chrono::Local;
use lazy_static::lazy_static;
use std::sync::{Mutex, MutexGuard};

lazy_static! {
    static ref LOG_FILE: Mutex<Option<File>> = Mutex::new(None);
}

// A daemon watcher's stdio is /dev/null, and before that it was a terminal that may have gone:
// a failed log write has nowhere to be reported, and must never panic the watcher.
fn log_file() -> MutexGuard<'static, Option<File>> {
    LOG_FILE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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

        *log_file() = Some(file);

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

        if let Some(file) = log_file().as_mut() {
            let _ = file.write_all(log_line.as_bytes());
        }
    }

    pub fn flush_and_close_log_file() {
        let mut guard = log_file();
        if let Some(file) = guard.as_mut() {
            let _ = file.flush();
        }
        *guard = None;
    }
}
