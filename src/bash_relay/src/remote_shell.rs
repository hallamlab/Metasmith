use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::borrow::Cow;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;

use crate::utils::generate_id;
use crate::watcher::{read_owner, pid_is_relay, Owner};

// How long a job may sit undispatched, or orphaned by a dead watcher, before the caller is
// told it will never run. The watcher polls every 100 ms, so either means it is gone or stuck.
const PICKUP_DEADLINE: Duration = Duration::from_secs(15);
const ORPHAN_GRACE: Duration = Duration::from_secs(5);

fn remove_leading_indent(s: &str) -> String {
    let lines: Vec<&str> = s.split('\n').collect();

    if lines.is_empty() {
        return s.to_string();
    }

    let mut indent: usize = 0;

    for line in &lines {
        if line.is_empty() {
            continue;
        }

        for c in line.chars() {
            if c != ' ' && c != '\t' {
                break;
            }
            indent += 1;
        }

        break;
    }

    if indent == 0 {
        return s.trim().to_string();
    }

    let de_indented_lines: Vec<Cow<str>> = lines.iter()
        .map(|line| {
            if line.len() >= indent {

                Cow::Borrowed(&line[indent..])
            } else {

                Cow::Borrowed(*line)
            }
        })
        .collect();

    let mut cleaned = de_indented_lines.join("\n");

    let last_line_after_de_indentation = lines.last()
        .map(|l| {
            if l.len() >= indent {
                &l[indent..]
            } else {
                l
            }
        })
        .unwrap_or("");

    cleaned = cleaned.trim().to_string();

    if last_line_after_de_indentation.is_empty() {

        cleaned.push('\n');
    }

    cleaned
}

struct Job {

    out_log: PathBuf,
    err_log: PathBuf,
    done_path: PathBuf,

    out_index: u64,
    err_index: u64,

    start_path: PathBuf,
    pid_path: PathBuf,
    submitted: Instant,
    orphaned_since: Option<Instant>,
}

// The `.pid` file holds a process GROUP id: launcher.sh runs the job under
// `set -m`, so the recorded number leads a group containing the tool the job
// started. Signalling the number alone stops the job's shell and orphans that
// tool, which is the leak this protocol exists to close.
const GROUP_KILL_GRACE: Duration = Duration::from_secs(5);

fn get_pgid_from_file(pid_file: &Path) -> Result<i32, String> {
    if !pid_file.exists() {
        return Err(format!("PID file not found: {}", pid_file.display()));
    }

    let contents = fs::read_to_string(pid_file)
        .map_err(|e| format!("Failed to read PID file: {}", e))?;

    let pid_str = contents.trim();
    let pgid = pid_str.parse::<i32>()
        .map_err(|_| format!("Invalid PID format in file: {}", pid_str))?;

    // 0 and negatives address the caller's own group or every process it may
    // signal: a truncated .pid must never be read as either.
    if pgid > 1 { Ok(pgid) } else { Err(format!("Refusing to signal group {}", pgid)) }
}

fn group_alive(pgid: Pid) -> bool {
    // EPERM means the group exists and is someone else's -- alive, and not ours
    // to stop. Only ESRCH means gone.
    !matches!(killpg(pgid, None), Err(nix::Error::ESRCH))
}

fn kill_group_escalating(pgid_val: i32) {
    let pgid = Pid::from_raw(pgid_val);

    match killpg(pgid, Signal::SIGTERM) {
        Ok(_) => println!("Sent SIGTERM to group {}", pgid_val),
        Err(nix::Error::ESRCH) => {
            println!("Group {} not found during SIGTERM.", pgid_val);
            return;
        }
        Err(e) => {
            eprintln!("Failed to send SIGTERM to group {}: {}", pgid_val, e);
            return;
        }
    }

    let start = Instant::now();
    while group_alive(pgid) && start.elapsed() < GROUP_KILL_GRACE {
        sleep(Duration::from_millis(100));
    }

    if group_alive(pgid) {
        match killpg(pgid, Signal::SIGKILL) {
            Ok(_) => println!("Sent SIGKILL to group {}", pgid_val),
            Err(e) => eprintln!("Failed to send SIGKILL to group {}: {}", pgid_val, e),
        }
    }
}

impl Job {
    fn new(key: String, base_path: &Path) -> Self {
        let compile_path = base_path.join(format!("{}.compile", key));
        Job {

            out_log: compile_path.with_extension("out"),
            err_log: compile_path.with_extension("err"),
            done_path: compile_path.with_extension("done"),
            start_path: compile_path.with_extension("start"),
            pid_path: compile_path.with_extension("pid"),
            out_index: 0,
            err_index: 0,
            submitted: Instant::now(),
            orphaned_since: None,
        }
    }

    // Why this job can no longer finish, if it cannot: never picked up, or picked up by a
    // watcher that has since died and took the job's process group with it.
    fn stranded(&self, workspace: &Path) -> Option<String> {
        if self.done_path.exists() {
            return None;
        }
        let watcher_alive = match read_owner(workspace) {
            Owner::Missing => false,
            Owner::Legacy => true,
            Owner::Pid(pid) => pid_is_relay(pid),
        };
        if self.start_path.exists() {
            if !watcher_alive {
                return Some("no live watcher to pick it up".to_string());
            }
            if self.submitted.elapsed() >= PICKUP_DEADLINE {
                return Some(format!("the watcher did not pick it up within {}s", PICKUP_DEADLINE.as_secs()));
            }
            return None;
        }
        let job_alive = get_pgid_from_file(&self.pid_path)
            .map_or(false, |pgid| group_alive(Pid::from_raw(pgid)));
        if watcher_alive || job_alive {
            return None;
        }
        Some("its watcher died and the job is gone".to_string())
    }

    pub fn signal_stop(&self) {
        let pid_file = self.out_log.with_extension("pid");

        if !pid_file.exists() {
            return;
        }

        if let Ok(pgid) = get_pgid_from_file(&pid_file) {
            let nix_pgid = Pid::from_raw(pgid);

            match killpg(nix_pgid, Signal::SIGINT) {
                Ok(_) => {
                    println!("SignalStop: Sent SIGINT to group {}", pgid);
                }
                Err(nix::Error::ESRCH) => {

                    println!("Group {} not found during SIGINT (ProcessLookupError).", pgid);
                }
                Err(e) => {

                    println!("Failed to send SIGINT to group {}: {}", pgid, e);
                }
            }
        } else {

            eprintln!("SignalStop: Could not retrieve valid PID from {}", pid_file.display());
        }
    }

    pub fn dispose(&self, timeout: f64) -> i32 {
        let one_tenth_sec = Duration::from_millis(100);

        let pid_file = self.out_log.with_extension("pid");
        let done_file = self.out_log.with_extension("done");

        let max_iterations = (timeout * 10.0).round() as u64;

        for _ in 0..max_iterations {
            if done_file.exists() {
                break;
            }
            sleep(one_tenth_sec);
        }

        let mut exit_code: i32 = 1;

        if !done_file.exists() {

            if let Ok(pgid) = get_pgid_from_file(&pid_file) {
                kill_group_escalating(pgid);
            }
        } else {

            match fs::read_to_string(&done_file) {
                Ok(contents) => {
                    let code_str = contents.lines().next().unwrap_or("1").trim();

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

        let run_file = self.out_log.with_extension("run");

        let mut files_to_delete = vec![
            &self.out_log,
            &self.err_log,
        ];

        if done_file.exists() {

            files_to_delete.push(&done_file);
            files_to_delete.push(&pid_file);
            files_to_delete.push(&run_file);
        }

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

type LogCallback = Box<dyn Fn(String) + Send + Sync + 'static>;

type CallbackId = usize;

static NEXT_CALLBACK_ID: AtomicUsize = AtomicUsize::new(1);

type CallbackMap = Arc<Mutex<HashMap<CallbackId, LogCallback>>>;

pub struct RemoteShell {
    _watcher_path: PathBuf,
    _timeout: Duration,
    _setup_commands: Vec<String>,

    _out_callbacks: CallbackMap,
    _err_callbacks: CallbackMap,

    _active_jobs: Arc<Mutex<HashMap<String, Job>>>,
    _exit_codes: Arc<Mutex<HashMap<String, i32>>>,
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
            _exit_codes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn dispose(&self) {

        let active_jobs = self._active_jobs.lock().unwrap();

        for j in active_jobs.values() {
            j.signal_stop();
        }

        let timeout_seconds = self._timeout.as_secs_f64();
        for j in active_jobs.values() {
            j.dispose(timeout_seconds);
        }
    }

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

    fn remove_callback(&self, callbacks: &CallbackMap, id: CallbackId, map_name: &str) -> bool {

        let mut map_guard = match callbacks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {

                eprintln!("Warning: {} Mutex was poisoned during removal.", map_name);

                poisoned.into_inner()
            }
        };

        map_guard.remove(&id).is_some()
    }

    pub fn remove_on_out(&self, id: CallbackId) -> bool {
        self.remove_callback(&self._out_callbacks, id, "_out_callbacks")
    }

    pub fn remove_on_err(&self, id: CallbackId) -> bool {
        self.remove_callback(&self._err_callbacks, id, "_err_callbacks")
    }

    pub fn exec_async(&self, cmd: &str) -> String {
        let script = remove_leading_indent(cmd);
        let k = generate_id();
        let job = Job::new(k.clone(), &self._watcher_path);

        let job_start_path = job.start_path.clone();
        let job_compile_path = job.start_path.with_extension("compile");

        let result = (|| -> io::Result<()> {

            let mut f = File::create(&job_compile_path)?;

            for line in &self._setup_commands {
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")?;
            }
            f.write_all(script.as_bytes())?;
            f.write_all(b"\n")?;

            // Which run this job belongs to, written before the rename below
            // so the watcher never sees a dispatchable job without it: the run
            // token is how a cancel reaches one run's jobs in a workspace
            // shared by every run on the host.
            if let Ok(token) = std::env::var("METASMITH_RUN") {
                if !token.trim().is_empty() {
                    fs::write(
                        job_compile_path.with_extension("run"),
                        format!("{}\n", token.trim()),
                    )?;
                }
            }

            // Written under `.compile` and renamed: the watcher dispatches any
            // `.start` file it sees, so a script it can observe half-written
            // would be run half-written.
            fs::rename(&job_compile_path, &job_start_path)?;

            self._active_jobs.lock().unwrap().insert(k.clone(), job);

            Ok(())
        })();

        if let Err(e) = result {

            eprintln!("Error during ExecAsync setup: {}", e);

            return String::new();
        }

        k
    }

    pub fn await_done(&self, timeout: Option<Duration>, key_filter: Option<&str>) {
        let start = Instant::now();
        let mut dt = Duration::from_millis(100);
        const MAX_DT: Duration = Duration::from_millis(500);

        let check_log = |log_path: &Path, start_index: &mut u64, callbacks: &CallbackMap| -> io::Result<()> {
            if !log_path.exists() { return Ok(()); }

            let mut file = OpenOptions::new().read(true).open(log_path)?;

            file.seek(SeekFrom::Start(*start_index))?;

            let mut reader = BufReader::new(file);
            let mut lines = String::new();

            if reader.read_to_string(&mut lines)? == 0 { return Ok(()); }

            let mut current_offset = *start_index;
            let mut buffer = String::new();

            for line in lines.lines() {
                buffer.clear();
                buffer.push_str(line);
                buffer.push('\n');

                let line_len = buffer.len() as u64;

                if let Some(map_guard) = callbacks.lock().ok() {

                    for cb in map_guard.values() {

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

            let mut active_jobs = self._active_jobs.lock().unwrap();

            let keys: Vec<String> = active_jobs.keys().cloned().collect();

            for k in keys {
                if let Some(j) = active_jobs.get_mut(&k) {

                    let _ = check_log(&j.out_log, &mut j.out_index, &self._out_callbacks);
                    let _ = check_log(&j.err_log, &mut j.err_index, &self._err_callbacks);

                    if j.done_path.exists() {
                        let timeout_seconds = self._timeout.as_secs_f64();
                        let code = j.dispose(timeout_seconds);
                        self._exit_codes.lock().unwrap_or_else(|p| p.into_inner()).insert(k.clone(), code);
                        finished_keys.push(k.clone());
                        continue;
                    }

                    match j.stranded(&self._watcher_path) {
                        None => j.orphaned_since = None,
                        Some(why) => {
                            let since = *j.orphaned_since.get_or_insert_with(Instant::now);
                            if since.elapsed() < ORPHAN_GRACE {
                                continue;
                            }
                            // A start file that vanishes under us was just dispatched, not stranded.
                            let withdrawn = !j.start_path.exists() || fs::remove_file(&j.start_path).is_ok();
                            if withdrawn && !j.done_path.exists() {
                                eprintln!("relay job [{}] will never finish: {}", k, why);
                                j.dispose(0.0);
                                self._exit_codes.lock().unwrap_or_else(|p| p.into_inner()).insert(k.clone(), 1);
                                finished_keys.push(k.clone());
                            }
                        }
                    }
                }
            }

            for k in finished_keys {
                active_jobs.remove(&k);
            }

            let is_done = match key_filter {
                Some(k) => !active_jobs.contains_key(k),
                None => active_jobs.is_empty(),
            };

            if is_done { break; }

            if let Some(t) = timeout {
                if start.elapsed() > t { break; }
            }

            thread::sleep(dt);
            dt = dt.saturating_add(Duration::from_millis(100)).min(MAX_DT);
        }
    }

    /// Run `cmd` through the watcher and return its exit code: 1 when it timed out, could
    /// not be submitted, or was stranded by a missing watcher.
    pub fn exec(&self, cmd: &str, timeout: Option<Duration>) -> i32 {

        let key = self.exec_async(cmd);
        if key.is_empty() {
            return 1;
        }

        self.await_done(timeout, Some(&key));

        if let Some(job) = self._active_jobs.lock().unwrap_or_else(|p| p.into_inner()).remove(&key) {
            let timeout_seconds = self._timeout.as_secs_f64();
            return job.dispose(timeout_seconds);
        }
        self._exit_codes.lock().unwrap_or_else(|p| p.into_inner()).remove(&key).unwrap_or(1)
    }
}
