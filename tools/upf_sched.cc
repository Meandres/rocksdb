#include "upf_sched.h"

#include <sys/mman.h>
#include <ucontext.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include <x86intrin.h>  // _clui / _stui (available under -muintr)

namespace upf {
namespace {

// Mask/unmask user interrupts around the one futex wait a carrier performs
// while idle. Safe because no green runs on an idle carrier, so no UPF can
// originate there; a fill completion arrives as an unpark from a resolver
// (UIF=0), which signals the carrier's condition variable. No-op unless the TU
// was compiled -muintr.
inline void clui() {
#ifdef __UINTR__
  _clui();
#endif
}
inline void stui() {
#ifdef __UINTR__
  _stui();
#endif
}

enum class GState { RUNNABLE, RUNNING, PARKED, DONE };

struct Green {
  ucontext_t ctx;
  void* stack = nullptr;
  size_t stack_size = 0;
  int home = -1;
  GState state = GState::RUNNABLE;
  void (*body)(void*) = nullptr;
  void* arg = nullptr;
  // Private copy of the in-flight fault. ricochet's FaultTask is a per-OS-thread
  // thread-local shared by all greens on this carrier, so submit must copy it
  // here before a sibling green's fault overwrites the thread-local.
  ricochet::FaultTask task;
};

struct Carrier {
  ucontext_t sched_ctx;
  std::deque<Green*> local_rq;  // runnable greens homed here (MPSC: resolvers push)
  std::mutex rq_mu;
  std::condition_variable rq_cv;
  int live_greens = 0;  // greens homed here not yet DONE
  int id = -1;
};

Carrier* g_carriers = nullptr;
int g_n_carriers = 0;
int g_n_resolvers = 0;
size_t g_green_stack = 512 * 1024;

std::deque<ricochet::FaultTask*> g_resolve_q;  // MPMC fill queue
std::mutex g_resolve_mu;
std::condition_variable g_resolve_cv;
std::atomic<bool> g_shutdown{false};
std::vector<std::thread> g_resolvers;

// Per-fault fill; overridable for tests. Default forwards to ricochet.
resolve_fn g_resolve_fn = &ricochet::region_handle_fault;

thread_local Carrier* tl_carrier = nullptr;
thread_local Green* tl_current_green = nullptr;

// First (and every) entry point of a green's stack.
void green_entry() {
  Green* self = tl_current_green;
  self->body(self->arg);
  self->state = GState::DONE;
  // Hand control back to the home carrier; this context is never resumed.
  swapcontext(&self->ctx, &g_carriers[self->home].sched_ctx);
}

// Resolver worker: dequeue a fault, perform the blocking fill. region_handle_fault
// calls sched_unpark(task->token) at its end, re-queuing the green.
void resolver_run() {
  for (;;) {
    ricochet::FaultTask* t = nullptr;
    {
      std::unique_lock<std::mutex> lk(g_resolve_mu);
      g_resolve_cv.wait(
          lk, [] { return !g_resolve_q.empty() || g_shutdown.load(); });
      if (g_resolve_q.empty()) {
        if (g_shutdown.load()) return;
        continue;
      }
      t = g_resolve_q.front();
      g_resolve_q.pop_front();
    }
    g_resolve_fn(t);
  }
}

}  // namespace

void sched_init(int n_carriers, int n_resolvers, size_t green_stack_bytes) {
  g_n_carriers = n_carriers;
  g_n_resolvers = n_resolvers;
  if (green_stack_bytes) g_green_stack = green_stack_bytes;
  g_shutdown.store(false);
  g_carriers = new Carrier[n_carriers];
  for (int i = 0; i < n_carriers; i++) g_carriers[i].id = i;
}

void sched_start_resolvers() {
  for (int i = 0; i < g_n_resolvers; i++) g_resolvers.emplace_back(resolver_run);
}

void sched_set_resolver(resolve_fn fn) { g_resolve_fn = fn; }

void carrier_bind(int id) { tl_carrier = &g_carriers[id]; }

void green_create(int home, void (*body)(void*), void* arg) {
  Green* g = new Green();
  g->home = home;
  g->body = body;
  g->arg = arg;
  g->stack_size = g_green_stack;
  g->stack = mmap(nullptr, g->stack_size, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK, -1, 0);
  if (g->stack == MAP_FAILED) {
    perror("upf_sched: green stack mmap");
    abort();
  }
  // Pin the green stack: the UPF trampoline + fill call chain descend into it
  // during delivery and must not fault (mirrors ricochet prefault_handler_stack).
  if (mlock(g->stack, g->stack_size) != 0) {
    perror("upf_sched: green stack mlock");
    abort();
  }
  getcontext(&g->ctx);
  g->ctx.uc_stack.ss_sp = g->stack;
  g->ctx.uc_stack.ss_size = g->stack_size;
  g->ctx.uc_link = nullptr;
  makecontext(&g->ctx, green_entry, 0);

  Carrier& c = g_carriers[home];
  std::lock_guard<std::mutex> lk(c.rq_mu);
  c.local_rq.push_back(g);
  c.live_greens++;
}

void carrier_run() {
  Carrier* c = tl_carrier;
  for (;;) {
    Green* g = nullptr;
    {
      std::unique_lock<std::mutex> lk(c->rq_mu);
      while (c->local_rq.empty() && c->live_greens > 0 && !g_shutdown.load()) {
        clui();
        c->rq_cv.wait(lk);
        stui();
      }
      if (g_shutdown.load() || (c->local_rq.empty() && c->live_greens == 0)) break;
      g = c->local_rq.front();
      c->local_rq.pop_front();
    }
    g->state = GState::RUNNING;
    tl_current_green = g;
    swapcontext(&c->sched_ctx, &g->ctx);
    tl_current_green = nullptr;
    if (g->state == GState::DONE) {
      munlock(g->stack, g->stack_size);
      munmap(g->stack, g->stack_size);
      {
        std::lock_guard<std::mutex> lk(c->rq_mu);
        c->live_greens--;
      }
      delete g;
    }
    // else: parked -> a resolver will re-enqueue it via sched_unpark.
  }
}

void sched_shutdown() {
  g_shutdown.store(true);
  g_resolve_cv.notify_all();
  for (auto& t : g_resolvers)
    if (t.joinable()) t.join();
  g_resolvers.clear();
  for (int i = 0; i < g_n_carriers; i++) g_carriers[i].rq_cv.notify_all();
  delete[] g_carriers;
  g_carriers = nullptr;
}

bool is_current_green() { return tl_current_green != nullptr; }

void* sched_submit(ricochet::FaultTask* task, void* /*ctx*/) {
  Green* g = tl_current_green;
  if (g == nullptr) {
    // Non-green fault (background/compaction thread, or warmup): resolve inline,
    // blocking, exactly as OS-thread mode does. task->token stays null, so the
    // fill will not call unpark.
    g_resolve_fn(task);
    return nullptr;
  }
  g->state = GState::PARKED;
  // Copy the fault out of the shared thread-local into per-green storage, then
  // publish the token, before enqueuing: a resolver may dequeue and start the
  // fill the instant it is queued.
  g->task = *task;
  g->task.token = g;
  {
    std::lock_guard<std::mutex> lk(g_resolve_mu);
    g_resolve_q.push_back(&g->task);
  }
  g_resolve_cv.notify_one();
  return g;
}

void sched_park(void* token, void* /*ctx*/) {
  if (token == nullptr) return;  // inline path already resolved the fault
  Green* g = static_cast<Green*>(token);
  swapcontext(&g->ctx, &g_carriers[g->home].sched_ctx);
  // Resumed here (still on the green's stack) after unpark + carrier re-entry;
  // returns into the trampoline, which retries the now-present faulting load.
}

void sched_unpark(void* token, void* /*ctx*/) {
  Green* g = static_cast<Green*>(token);
  Carrier& c = g_carriers[g->home];  // resume on the HOME carrier only
  {
    std::lock_guard<std::mutex> lk(c.rq_mu);
    g->state = GState::RUNNABLE;
    c.local_rq.push_back(g);
  }
  c.rq_cv.notify_one();
}

}  // namespace upf
