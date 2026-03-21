use rustler::env::SavedTerm;
use rustler::{Env, LocalPid, OwnedEnv, Resource, ResourceArc, Term};
use std::sync::Mutex;

/// A NIF resource that sends a pre-built message to a local PID when
/// the Erlang VM garbage-collects the reference.
///
/// This is the core mechanism for distributed GC: when a remote node
/// is done with a resource, the GC sentinel fires a notification to
/// the local LocalGC process, which forwards it to the Holder on the
/// origin node.
///
/// The message is saved in an OwnedEnv at creation time and sent via
/// `send_and_clear` in the Drop impl — this works from the GC thread.
pub struct GcSentinelRef {
    inner: Mutex<Option<SentinelInner>>,
}

struct SentinelInner {
    owned_env: OwnedEnv,
    pid: LocalPid,
    msg: SavedTerm,
}

// SavedTerm is Send, OwnedEnv is Send, LocalPid is Send
unsafe impl Send for SentinelInner {}
unsafe impl Sync for SentinelInner {}

#[rustler::resource_impl]
impl Resource for GcSentinelRef {}

impl Drop for GcSentinelRef {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(inner) = guard.take() {
                // Send the pre-saved message to the target PID.
                // This works from the GC thread because OwnedEnv
                // owns its own environment.
                let pid = inner.pid;
                let msg = inner.msg;
                let mut owned_env = inner.owned_env;
                let _ = owned_env.send_and_clear(&pid, |env| {
                    msg.load(env)
                });
            }
        }
    }
}

/// Create a new GC sentinel that will send `msg` to `pid` when collected.
///
/// The message is deep-copied into an OwnedEnv so it survives independently
/// of the caller's environment.
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
