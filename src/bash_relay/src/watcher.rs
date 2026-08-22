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
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use serde::{Serialize, Deserialize};
use scopeguard::guard;

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub alive: bool,

    #[serde(default = "default_pid")]
    pub pid: i32,
    #[serde(default)]
    pub jobs: Vec<String>,
}

fn default_pid() -> i32 {
    -1
}

impl Status {

    pub fn load(status_file: &Path) -> Option<Self> {
        if !status_file.exists() {
            return None;
        }

        match fs::File::open(status_file)
            .and_then(|mut f| {
                let mut contents = String::new();
                f.read_to_string(&mut contents)?;
                serde_json::from_str::<Status>(&contents)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            })
        {
            Ok(status) => Some(status),

            Err(e) => {
                eprintln!("Warning: Failed to load status file {}: {}", status_file.display(), e);
                None
            }
        }
    }

    pub fn save(&self, status_file: &Path) -> io::Result<()> {

        let json_string = serde_json::to_string_pretty(self)?;

        fs::write(status_file, json_string)?;

        Ok(())
    }
}

// A job's `.pid` holds a process GROUP id, not a bare pid: launcher.sh runs the
// job under `set -m`, so the recorded number leads a group containing the tool
// the job actually started. Signalling the number alone stops the job's shell
// and orphans that tool -- which is the leak this whole protocol exists to
// close. Everything below therefore signals the group.
const JOB_KILL_GRACE_MS: u128 = 5_000;

fn read_pgid(pid_file: &Path) -> Option<i32> {
    let raw = fs::read_to_string(pid_file).ok()?;
    let pgid: i32 = raw.trim().parse().ok()?;
    // 0 and negatives address the caller's own group or "everything": a
    // truncated or half-written .pid must never be read as either.
    if pgid > 1 { Some(pgid) } else { None }
}

fn group_alive(pgid: i32) -> bool {
    // EPERM means the group exists and is someone else's -- alive, and not ours
    // to stop. Only ESRCH means gone.
    !matches!(killpg(Pid::from_raw(pgid), None), Err(nix::Error::ESRCH))
}

fn kill_job_group(pgid: i32) {
    let pid = Pid::from_raw(pgid);

    match killpg(pid, Signal::SIGTERM) {
        Ok(_) => Logger::info(&format!("  - sent SIGTERM to group [{}]", pgid)),
        Err(nix::Error::ESRCH) => return,
        Err(e) => {
            Logger::error(&format!("  - Warning: Failed to SIGTERM group [{}]: {}", pgid, e));
            return;
        }
    }

    let start = current_time_millis();
    while group_alive(pgid) {
        if current_time_millis().saturating_sub(start) >= JOB_KILL_GRACE_MS { break; }
        thread::sleep(Duration::from_millis(100));
    }

    if group_alive(pgid) {
        match killpg(pid, Signal::SIGKILL) {
            Ok(_) => Logger::info(&format!("  - sent SIGKILL to group [{}]", pgid)),
            Err(nix::Error::ESRCH) => {}
            Err(e) => Logger::error(&format!("  - Warning: Failed to SIGKILL group [{}]: {}", pgid, e)),
        }
    }
}

fn job_pid_files(workspace: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    match fs::read_dir(workspace) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "pid") {
                    out.push(path);
                }
            }
        }
        Err(e) => Logger::error(&format!(
            "Warning: Failed to read workspace directory {}: {}", workspace.display(), e,
        )),
    }
    out
}

fn try_kill_jobs(workspace: &Path) {
    for path in job_pid_files(workspace) {
        match read_pgid(&path) {
            Some(pgid) => kill_job_group(pgid),
            None => Logger::error(&format!(
                "  - Warning: no usable pgid in [{}]", path.display(),
            )),
        }
    }
}

/// Kill every job tagged with `token` in its `.run` file, leaving other runs'
/// jobs alone. This is what scopes a cancel to one run in a workspace that is
/// shared by every run on the host.
pub fn kill_run(workspace: &Path, token: &str) -> usize {
    let mut killed = 0;
    for path in job_pid_files(workspace) {
        let run_file = path.with_extension("run");
        let tagged = fs::read_to_string(&run_file)
            .map_or(false, |c| c.trim() == token);
        if !tagged { continue; }
        if let Some(pgid) = read_pgid(&path) {
            Logger::info(&format!("Killing job [{}] of run [{}]", path.display(), token));
            kill_job_group(pgid);
            killed += 1;
        }
    }
    killed
}

pub fn wipe_workspace(workspace: &Path) -> bool {

    try_kill_jobs(workspace);

    // Whatever try_kill_jobs could not kill is a genuine survivor. Its records
    // are the only trace of it, so they outlive the wipe -- deleting them is
    // how a half-failed shutdown used to erase the evidence of what it left
    // running.
    let survivors: Vec<String> = job_pid_files(workspace).into_iter()
        .filter(|p| read_pgid(p).map_or(false, group_alive))
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .collect();
    if !survivors.is_empty() {
        Logger::error(&format!(
            "  - Warning: [{}] job(s) survived shutdown, keeping their records: {}",
            survivors.len(), survivors.join(", "),
        ));
    }

    let mut safe_wait = false;

    if let Ok(entries) = fs::read_dir(workspace) {
        for entry in entries.flatten() {
            let path = entry.path();

            if path == workspace { continue; }

            if path.file_name().map_or(false, |name| name == "main.log") {
                continue;
            }

            if path.file_stem().map_or(false, |stem| {
                survivors.iter().any(|s| s.as_str() == stem.to_string_lossy())
            }) {
                continue;
            }

            if path.file_name().map_or(false, |name| name == "active") {
                safe_wait = true;
            }

            if path.is_dir() {
                if let Err(e) = fs::remove_dir(&path) {
                    Logger::error(&format!("  - Warning: Could not remove directory {}: {}", path.display(), e));

                }
            } else if let Err(e) = fs::remove_file(&path) {
                Logger::error(&format!("  - Warning: Could not remove file {}: {}", path.display(), e));
            }
        }
    } else {
        Logger::error(&format!("Warning: Failed to read workspace directory for file deletion: {}", workspace.display()));
    }

    safe_wait
}

fn wipe(workspace: &PathBuf) {
    Logger::info("Cleaning up previous workspace");

    if wipe_workspace(&workspace) {
        Logger::info("  - Pausing for 1 second");
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
const TARGET_SHELL_EXE: &str = "zsh";

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const TARGET_SHELL_EXE: &str = "bash";

pub fn setup_launcher_script(workspace: &Path, cwd: &Path, active_path: &Path, launcher_path: &Path) -> io::Result<()> {
    Logger::info(&format!(
        "Setting up launcher script at: {}",
        launcher_path.display()
    ));

    File::create(&active_path)?;

    let permissions_644 = Permissions::from_mode(0o644);
    fs::set_permissions(&active_path, permissions_644)?;

    let script_content: String = [
        "SCRIPT=$1",
        "PIDF=$2",
        "DONEF=$3",
        &format!("cd {}", cwd.display()),
        // `set -m` gives the job its own process group, so the $PID recorded
        // below is a pgid and a stop reaches the tool the job started, not
        // just the shell that started it.
        "set -m",
        &format!("{} {}/$SCRIPT &", TARGET_SHELL_EXE, workspace.display()),
        "set +m",
        &format!("cd {}", workspace.display()),
        "PID=$!",
        "echo $PID > $PIDF",
        "wait $PID",
        "STATUS=$?",
        "rm $PIDF",
        "rm $SCRIPT",
        "echo $STATUS > $DONEF",
    ].join("\n");

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&launcher_path)?;

        file.write_all(script_content.as_bytes())?;
    }

    let permissions_755 = Permissions::from_mode(0o755);
    fs::set_permissions(&launcher_path, permissions_755)?;
    Ok(())
}

pub fn dispatch(start_file: &Path, workspace: &Path, launcher: &Path) -> io::Result<()> {

    Logger::info(&format!("{}>>>", "-".repeat(25)));

    match fs::File::open(start_file) {
        Ok(mut h) => {
            let mut content = String::new();
            h.read_to_string(&mut content)?;

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

    let file_stem = start_file.file_stem()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Start file must have a stem"))?;

    let live_name = format!("{}.running", file_stem.to_string_lossy());
    let live_path = workspace.join(&live_name);

    let out_file = format!("{}.out", file_stem.to_string_lossy());
    let err_file = format!("{}.err", file_stem.to_string_lossy());
    let pid_file = format!("{}.pid", file_stem.to_string_lossy());
    let done_file = format!("{}.done", file_stem.to_string_lossy());

    let mut command = Command::new("sh");
    command.current_dir(workspace);

    fs::rename(start_file, &live_path)?;

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

    command.arg("-c").arg(shell_command);

    let child = command
        .spawn()
        .map_err(|e| {
            Logger::error(&format!("Failed to spawn background job: {}", e));

            let _ = fs::rename(&live_path, start_file);
            e
        })?;

    std::mem::forget(child);

    Ok(())
}

pub fn run_watcher(workspace: &PathBuf, cwd: &PathBuf) {
    Logger::init_log_file(&workspace)
        .expect("FATAL: Failed to initialize main log file");
    Logger::info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    wipe(&workspace);
    let active_path = workspace.join("active");
    let launcher_path = workspace.join("launcher.sh");
    setup_launcher_script(&workspace, &cwd, &active_path, &launcher_path)
        .expect("FATAL: Failed to create launcher script");

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let workspace_for_guard = workspace.clone();
    let _guard = guard(active_path.clone(), |path| {
        Logger::info("Watcher shutting down");

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

    while running.load(Ordering::SeqCst) {
        let mut active_jobs: Vec<String> = Vec::new();
        let mut status_checks: Vec<PathBuf> = Vec::new();
        let mut found_active = false;

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

                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            let run = fs::read_to_string(path.with_extension("run"))
                                .ok()
                                .map(|c| c.trim().to_string())
                                .filter(|c| !c.is_empty());
                            active_jobs.push(match run {
                                Some(token) => format!("{} run={}", stem, token),
                                None => stem.to_string(),
                            });
                        }
                    }
                    else if file_name.ends_with(".check") {
                        status_checks.push(path);
                    }
                }
            },
            Err(e) => {
                Logger::error(&format!("Error reading workspace directory: {}", e));
                break;
            }
        }

        if !found_active {
            Logger::info("'active' file was deleted");
            running.store(false, Ordering::SeqCst);
        }

        for check_path in status_checks {
            let status_data = Status {
                jobs: active_jobs.clone(),
                alive: true,
                pid: current_pid,
            };

            let status_path = check_path.with_extension("status");

            if let Err(e) = status_data.save(&status_path) {
                 Logger::error(&format!("Failed to save status file {}: {}", status_path.display(), e));
            }

            if let Err(e) = fs::remove_file(&check_path) {
                 Logger::error(&format!("Failed to remove check request file {}: {}", check_path.display(), e));
            }
        }

        thread::sleep(Duration::from_millis(100));
    }

}

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

            if self.sig_path.exists() {
                let _ = fs::remove_file(&self.sig_path);
            }
            if self.result_path.exists() {
                let _ = fs::remove_file(&self.result_path);
            }
        }
    }

    let active_path = workspace.join("active");

    if !active_path.exists() {
        return Status { alive: false, pid: -1, jobs: Vec::new() };
    }

    let id = generate_id();
    let sig_file_name = format!("{}.check", id);
    let sig = workspace.join(&sig_file_name);

    let result = sig.with_extension("status");

    if let Err(e) = fs::File::create(&sig) {
         Logger::error(&format!("Failed to create check file {}: {}", sig.display(), e));
         return Status { alive: false, pid: -1, jobs: Vec::new() };
    }

    let _cleanup = CleanupGuard::new(sig.clone(), result.clone());

    let start_time = current_time_millis();
    let timeout_ms = (timeout as u128) * 1000;

    loop {
        let elapsed = current_time_millis().saturating_sub(start_time);

        if elapsed > timeout_ms {
            Logger::info(&format!(
                "Status check timed out after {}ms. Returning Status(alive=False).",
                elapsed
            ));

            return Status { alive: false, pid: -1, jobs: Vec::new() };
        }

        thread::sleep(Duration::from_millis(20));

        if !result.exists() {
            continue;
        }

        match Status::load(&result) {
            Some(status) => {

                return status;
            }
            None => {

                continue;
            }
        }
    }

}
pub fn check_status_default_timeout(workspace: &Path) -> Status {
    check_status(&workspace, 2)
}
