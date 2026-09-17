//! Parent-process watchdog.
//!
//! When the broker is spawned by a supervisor (the ChopFlow macOS app), it
//! receives the supervisor's PID via `--parent-pid`. A force-quit of the
//! supervisor would otherwise orphan the broker — the supervisor's own
//! shutdown hook never runs — so the broker polls its parent's liveness and
//! self-terminates when the parent disappears. Unix `kill(pid, 0)` is the
//! existence probe: it sends no signal, it only checks the process exists.

use std::time::Duration;

use tracing::info;

/// Whether `pid` refers to a live process. Signal 0 is the standard
/// existence check: no signal is delivered, only permission/existence errors
/// are returned.
#[cfg(unix)]
pub fn parent_alive(pid: u32) -> bool {
    // SAFETY: `kill` with signal 0 never delivers a signal and is safe to
    // call on any PID; it merely reports existence/permission.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
pub fn parent_alive(_pid: u32) -> bool {
    // Windows is not a target of the desktop app in v1; a stub that never
    // triggers keeps the watchdog a no-op there instead of killing brokers.
    true
}

/// Poll `parent_pid` every `poll` until the parent dies, then call `on_dead`.
/// The callback is invoked exactly once, from the polling task.
pub async fn run_watchdog<F>(parent_pid: u32, poll: Duration, on_dead: F)
where
    F: FnOnce(),
{
    info!(
        "watchdog: supervising parent pid {} (poll every {:?})",
        parent_pid, poll
    );
    loop {
        if !parent_alive(parent_pid) {
            info!("watchdog: parent {} is gone — shutting down", parent_pid);
            on_dead();
            return;
        }
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn spawn_and_reaped_short_lived() -> u32 {
        // Spawn a `sleep` that dies on its own, then `wait()` it so the pid
        // is reaped — without the wait the process lingers as a zombie, and
        // `kill(pid, 0)` reports zombies as alive, which would hang a
        // watchdog waiting on them. Once reaped the pid is truly gone.
        let mut child = std::process::Command::new("sleep")
            .arg("0.05")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        child.wait().expect("wait for child");
        pid
    }

    #[cfg(unix)]
    #[test]
    fn parent_alive_true_for_live_process() {
        // Our own pid is live for the duration of the test.
        assert!(parent_alive(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn parent_alive_false_for_dead_process() {
        let pid = spawn_and_reaped_short_lived();
        assert!(!parent_alive(pid));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watchdog_fires_when_parent_dies() {
        // A reaped-away parent must trip the watchdog on the first poll.
        let pid = spawn_and_reaped_short_lived();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fired_clone = fired.clone();
        run_watchdog(pid, Duration::from_millis(20), move || {
            fired_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watchdog_does_not_fire_while_parent_lives() {
        // A long-lived "parent" (our own PID works: this test process is
        // alive) — run the watchdog briefly and assert it did NOT fire.
        let me = std::process::id();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fired_clone = fired.clone();
        tokio::select! {
            _ = run_watchdog(me, Duration::from_millis(20), move || {
                fired_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }) => {}
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        assert!(!fired.load(std::sync::atomic::Ordering::SeqCst));
    }
}
