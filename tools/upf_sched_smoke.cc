// Native unit test for the upf green-thread scheduler (no real UPF delivery).
// It drives the submit/park/unpark contract with a mock resolver, verifying
// that greens interleave over simulated faults, resume correctly (stack state
// survives a yield), and that the non-green inline-fallback path works.
//
// Build (native, no ricochet lib needed — a stub provides the default symbol):
//   g++ -std=c++17 -g -fsanitize=address -I../../ricochet_lib \
//       upf_sched.cc upf_sched_smoke.cc -pthread -o upf_sched_smoke
//   ./upf_sched_smoke
#include <sys/resource.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include "upf_sched.h"

// Stub for the default resolver symbol; never called (we override it below).
namespace ricochet {
void region_handle_fault(FaultTask*) { std::abort(); }
}  // namespace ricochet

static std::atomic<long> g_fills{0};
static std::atomic<long> g_ops{0};
static ricochet::RicochetRegion g_dummy_region;  // contents unused by the mock

// Mock fill: simulate a little I/O latency, then resume the parked green.
static void mock_resolve(ricochet::FaultTask* t) {
  g_fills.fetch_add(1, std::memory_order_relaxed);
  for (volatile int i = 0; i < 2000; i++) {
  }
  if (t->token) upf::sched_unpark(t->token, nullptr);
}

// Mimic ricochet_trampoline_body at a fault point.
static inline void fault(size_t offset) {
  ricochet::FaultTask task{};
  task.region = &g_dummy_region;
  task.offset = offset;
  task.token = nullptr;
  task.token = upf::sched_submit(&task, nullptr);
  upf::sched_park(task.token, nullptr);
}

struct GArg {
  int carrier;
  int gid;
  int iters;
};

static void green_body(void* p) {
  GArg* a = static_cast<GArg*>(p);
  char scratch[4096];
  for (int i = 0; i < a->iters; i++) {
    unsigned char mark = static_cast<unsigned char>((a->gid + i) & 0xff);
    std::memset(scratch, mark, sizeof(scratch));
    fault(static_cast<size_t>((a->carrier * 100000 + a->gid * 1000 + i)) * 4096);
    // Stack state (and this local frame) must survive the yield.
    if (static_cast<unsigned char>(scratch[0]) != mark ||
        static_cast<unsigned char>(scratch[4095]) != mark) {
      std::fprintf(stderr, "FAIL: stack corrupted across yield\n");
      std::abort();
    }
    g_ops.fetch_add(1, std::memory_order_relaxed);
  }
  delete a;
}

static void carrier_thread(int cid, int greens, int iters) {
  upf::carrier_bind(cid);
  for (int i = 0; i < greens; i++)
    upf::green_create(cid, green_body, new GArg{cid, i, iters});
  upf::carrier_run();
}

int main() {
  // Green stacks are mlock'd; raise the soft limit as far as the hard limit allows.
  struct rlimit rl;
  if (getrlimit(RLIMIT_MEMLOCK, &rl) == 0) {
    rl.rlim_cur = rl.rlim_max;
    setrlimit(RLIMIT_MEMLOCK, &rl);
  }

  const int NC = 2, NR = 2, G = 4, IT = 50;
  upf::sched_init(NC, NR, 128 * 1024);
  upf::sched_set_resolver(mock_resolve);
  upf::sched_start_resolvers();

  // Inline-fallback path: a non-green thread faults -> resolved inline, no token.
  fault(999 * 4096);
  if (g_fills.load() != 1) {
    std::fprintf(stderr, "FAIL: inline fallback did not run\n");
    return 1;
  }

  std::vector<std::thread> carriers;
  for (int c = 0; c < NC; c++) carriers.emplace_back(carrier_thread, c, G, IT);
  for (auto& t : carriers) t.join();

  upf::sched_shutdown();

  long expect_ops = static_cast<long>(NC) * G * IT;
  long expect_fills = expect_ops + 1;  // +1 for the inline fault
  std::printf("ops=%ld (expect %ld) fills=%ld (expect %ld)\n", g_ops.load(),
              expect_ops, g_fills.load(), expect_fills);
  if (g_ops.load() != expect_ops || g_fills.load() != expect_fills) {
    std::printf("FAIL\n");
    return 1;
  }
  std::printf("PASS\n");
  return 0;
}
