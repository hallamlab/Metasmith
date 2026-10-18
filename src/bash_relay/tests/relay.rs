use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

const RELAY: &str = env!("CARGO_BIN_EXE_msm_relay");
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Sandbox {
    io: PathBuf,
    tmpdir: PathBuf,
}

impl Sandbox {
    fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("{}_{}_{}", name, std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let tmpdir = root.join("tmp");
        let home = root.join("home");
        fs::create_dir_all(&tmpdir).unwrap();
        fs::create_dir_all(&home).unwrap();
        Sandbox { io: home.join("host"), tmpdir }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(RELAY)
            .arg("--io").arg(&self.io)
            .args(args)
            .env("TMPDIR", &self.tmpdir)
            .output()
            .unwrap()
    }

    fn owner(&self) -> Option<i32> {
        fs::read_to_string(self.io.join("active")).ok()?.split_whitespace().next()?.parse().ok()
    }

    fn watchers(&self) -> usize {
        let needle = self.io.to_string_lossy().into_owned();
        fs::read_dir("/proc").unwrap().flatten()
            .filter(|e| e.file_name().to_string_lossy().chars().all(|c| c.is_ascii_digit()))
            .filter_map(|e| fs::read(e.path().join("cmdline")).ok())
            .map(|raw| String::from_utf8_lossy(&raw).replace('\0', " "))
            .filter(|cmd| cmd.contains(&needle) && cmd.contains(" start"))
            .count()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if let Some(pid) = self.owner() {
            let _ = kill(Pid::from_raw(pid), Signal::SIGCONT);
        }
        let _ = self.run(&["stop"]);
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

#[test]
fn concurrent_starts_leave_one_watcher() {
    let sb = Sandbox::new("race");
    let handles: Vec<_> = (0..8).map(|_| {
        let io = sb.io.clone();
        let tmpdir = sb.tmpdir.clone();
        thread::spawn(move || {
            Command::new(RELAY).arg("--io").arg(&io).arg("start")
                .env("TMPDIR", &tmpdir).output().unwrap()
        })
    }).collect();
    for h in handles {
        let out = h.join().unwrap();
        assert!(out.status.success(), "start failed: {}", String::from_utf8_lossy(&out.stderr));
    }

    let status = stdout(&sb.run(&["status"]));
    let owner = sb.owner().expect("active names no watcher");
    assert!(status.contains("alive: true"), "{}", status);
    assert!(status.contains(&format!("PID: {}", owner)), "{}", status);
    assert_eq!(sb.watchers(), 1);
}

#[test]
fn start_reports_a_failed_setup() {
    if users::get_current_uid() == 0 { return; }
    let sb = Sandbox::new("setup");
    let parent = sb.io.parent().unwrap();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o555)).unwrap();

    let t = Instant::now();
    let out = sb.run(&["start"]);
    fs::set_permissions(parent, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(!out.status.success());
    assert!(t.elapsed() < Duration::from_secs(5));
    assert!(String::from_utf8_lossy(&out.stderr).contains("failed to set up"));
}

#[test]
fn start_reports_a_watcher_that_dies() {
    if users::get_current_uid() == 0 { return; }
    let sb = Sandbox::new("dies");
    let user = Command::new(RELAY).arg("--io").arg(&sb.io).arg("start")
        .env("TMPDIR", &sb.tmpdir).output().unwrap();
    assert!(user.status.success());
    let _ = sb.run(&["stop"]);
    let target = fs::read_link(&sb.io).unwrap();
    fs::remove_file(target.join("main.log")).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o555)).unwrap();

    let t = Instant::now();
    let out = sb.run(&["start"]);
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(!out.status.success());
    assert!(t.elapsed() < Duration::from_secs(10), "took {:?}", t.elapsed());
    assert!(String::from_utf8_lossy(&out.stderr).contains("exited before it came up"));
}

#[test]
fn start_leaves_a_stalled_watcher_alone() {
    let sb = Sandbox::new("stalled");
    assert!(sb.run(&["start"]).status.success());
    let owner = sb.owner().unwrap();
    kill(Pid::from_raw(owner), Signal::SIGSTOP).unwrap();

    let out = sb.run(&["start"]);

    assert!(!out.status.success());
    assert_eq!(sb.owner(), Some(owner));
    kill(Pid::from_raw(owner), Signal::SIGCONT).unwrap();
    assert!(stdout(&sb.run(&["status"])).contains(&format!("PID: {}", owner)));
}

#[test]
fn bounce_returns_the_jobs_exit_code() {
    let sb = Sandbox::new("bounce");
    assert!(sb.run(&["start"]).status.success());

    let ok = sb.run(&["bounce", "echo hello"]);
    assert_eq!(ok.status.code(), Some(0));
    assert!(stdout(&ok).contains("hello"));

    assert_eq!(sb.run(&["bounce", "exit 3"]).status.code(), Some(3));
}

#[test]
fn bounce_gives_up_on_a_stalled_watcher() {
    let sb = Sandbox::new("bounce_stalled");
    assert!(sb.run(&["start"]).status.success());
    let owner = sb.owner().unwrap();
    kill(Pid::from_raw(owner), Signal::SIGSTOP).unwrap();

    let t = Instant::now();
    let out = sb.run(&["bounce", "echo never"]);
    kill(Pid::from_raw(owner), Signal::SIGCONT).unwrap();

    assert!(!out.status.success());
    assert!(t.elapsed() < Duration::from_secs(30), "took {:?}", t.elapsed());
    assert!(!stdout(&out).contains("never"));
}

#[test]
fn bounce_gives_up_without_a_watcher() {
    let sb = Sandbox::new("bounce_none");
    assert!(sb.run(&["start"]).status.success());
    assert!(sb.run(&["stop"]).status.success());

    let t = Instant::now();
    let out = sb.run(&["bounce", "echo never"]);

    assert!(!out.status.success());
    assert!(t.elapsed() < Duration::from_secs(15), "took {:?}", t.elapsed());
}

#[test]
fn two_homes_on_one_node_keep_their_own_watchers() {
    let a = Sandbox::new("home_a");
    let b = Sandbox { io: a.io.parent().unwrap().parent().unwrap().join("home_b").join("host"), tmpdir: a.tmpdir.clone() };
    fs::create_dir_all(b.io.parent().unwrap()).unwrap();

    assert!(a.run(&["start"]).status.success());
    let first = a.owner().expect("home a has no watcher");
    assert!(b.run(&["start"]).status.success());

    assert_ne!(fs::read_link(&a.io).unwrap(), fs::read_link(&b.io).unwrap());
    assert_eq!(a.owner(), Some(first), "starting home b replaced home a's watcher");
    assert!(stdout(&a.run(&["status"])).contains("alive: true"));
    assert!(stdout(&b.run(&["status"])).contains("alive: true"));
}
