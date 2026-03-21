use rustler::{Env, LocalPid, OwnedEnv, Resource, ResourceArc, Term};
use rustler::env::SavedTerm;
use std::sync::Mutex;

/// A NIF resource that sends a pre-built message to a local PID when
/// the Erlang VM garbage-collects the reference.
///
/// Uses OwnedEnv to store the message, then raw enif_send in Drop
/// because Rustler's send_and_clear panics on managed threads (GC runs
/// on the scheduler thread).
pub struct GcSentinelRef {
    inner: Mutex<Option<SentinelInner>>,
}

struct SentinelInner {
    owned_env: OwnedEnv,
    pid: LocalPid,
    msg: SavedTerm,
}

unsafe impl Send for SentinelInner {}
unsafe impl Sync for SentinelInner {}

#[rustler::resource_impl]
impl Resource for GcSentinelRef {}

impl Drop for GcSentinelRef {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(inner) = guard.take() {
                // We can't use OwnedEnv::send_and_clear here because Drop runs
                // on a managed BEAM scheduler thread. Instead, spawn an OS thread
                // to do the send, which is unmanaged.
                std::thread::spawn(move || {
                    let pid = inner.pid;
                    let msg = inner.msg;
                    let mut owned_env = inner.owned_env;
                    let _ = owned_env.send_and_clear(&pid, |env| {
                        msg.load(env)
                    });
                });
            }
        }
    }
}

/// Create a new GC sentinel that will send `msg` to `pid` when collected.
#[rustler::nif]
#[allow(unused_variables)]
fn gc_sentinel_new<'a>(env: Env<'a>, pid: LocalPid, msg: Term<'a>) -> ResourceArc<GcSentinelRef> {
    let owned_env = OwnedEnv::new();
    let saved_msg = owned_env.save(msg);

    ResourceArc::new(GcSentinelRef {
        inner: Mutex::new(Some(SentinelInner {
            owned_env,
            pid,
            msg: saved_msg,
        })),
    })
}

/// Check if a GC sentinel reference is still alive (not yet collected).
#[rustler::nif]
fn gc_sentinel_alive(sentinel: ResourceArc<GcSentinelRef>) -> bool {
    if let Ok(guard) = sentinel.inner.lock() {
        guard.is_some()
    } else {
        false
    }
}
