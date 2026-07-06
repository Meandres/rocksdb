// Application-side M:N green-thread scheduler that provides ricochet's
// submit/park/unpark hooks so a UPF page-fault can be resolved as a
// non-blocking, interruptible userspace task: the faulting green thread yields
// its carrier, a resolver thread performs the (blocking) fill, and the green is
// resumed on its home carrier once the page is present. The ricochet library
// (api.cpp) is untouched; it only calls the function pointers below.
#pragma once

#include <cstddef>

#include "api.hpp"  // ricochet::FaultTask, ricochet::region_handle_fault

namespace upf {

// One-time setup. n_carriers carrier slots are indexed 0..n_carriers-1 (one per
// db_bench worker thread / pinned CPU). n_resolvers resolver threads drain the
// fill queue. green_stack_bytes sizes each (mlock'd) green stack; 0 keeps the
// default. Call once, on one thread, after the UFFD->UPF switch but BEFORE any
// _stui (i.e. while UIF=0 on every carrier).
void sched_init(int n_carriers, int n_resolvers, size_t green_stack_bytes);

// Spawn the resolver threads. Resolvers never register for UINTR / never _stui,
// so they may block on condition variables freely.
void sched_start_resolvers();

// Bind the calling thread to carrier slot `id`. Call from the db_bench worker
// thread that will host greens `id`.
void carrier_bind(int id);

// Create a green thread homed on carrier `home` that runs body(arg). Call from
// the home carrier before carrier_run().
void green_create(int home, void (*body)(void*), void* arg);

// Run the calling (bound) carrier's scheduler loop until all of its greens
// finish. Returns when live_greens reaches 0 (or on shutdown).
void carrier_run();

// Signal shutdown, wake and join resolver threads, free carriers. Call once,
// after every carrier_run() has returned.
void sched_shutdown();

// True iff the calling context is a green thread currently running on a carrier
// (used by sched_submit to fall back to inline fill for non-green faults).
bool is_current_green();

// ricochet Handlers hooks (function-pointer compatible with submit/park/unpark).
void* sched_submit(ricochet::FaultTask* task, void* ctx);
void sched_park(void* token, void* ctx);
void sched_unpark(void* token, void* ctx);

// The per-fault fill a resolver performs. Defaults to ricochet::region_handle_fault
// (which itself calls sched_unpark when task->token is set). Overridable for unit
// testing the scheduler without real UPF delivery; call before sched_start_resolvers().
using resolve_fn = void (*)(ricochet::FaultTask* task);
void sched_set_resolver(resolve_fn fn);

}  // namespace upf
