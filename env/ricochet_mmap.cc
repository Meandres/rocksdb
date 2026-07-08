#include "ricochet_mmap.h"
#include "replacement.hpp"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstddef>

namespace ROCKSDB_NAMESPACE {

void ricochet_fill(void* buf, size_t offset, void* ctx) {
  auto* c = static_cast<RicochetFileCtx*>(ctx);
  pread(c->fd, buf, 4096, static_cast<off_t>(offset));
}

// Green-thread scheduler hooks (definitions; see ricochet_mmap.h).
ricochet::submit_fn g_ric_submit = nullptr;
ricochet::park_fn g_ric_park = nullptr;
ricochet::unpark_fn g_ric_unpark = nullptr;

// ---------------------------------------------------------------------------
// RicochetMmapManager
// ---------------------------------------------------------------------------

static RicochetMmapManager* g_instance = nullptr;

RicochetMmapManager* RicochetMmapManager::Get() { return g_instance; }

void RicochetMmapManager::Init(int ncpus, size_t max_cache_pages) {
  ricochet::cache_init(max_cache_pages);
  if (ncpus <= 0)
    ncpus = (int)sysconf(_SC_NPROCESSORS_ONLN);

  ricochet::handler_pool_init(ncpus);

  g_instance = new RicochetMmapManager();
}

void RicochetMmapManager::AddRegion(ricochet::RicochetRegion* r) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_.push_back(r);
}

void RicochetMmapManager::RemoveRegion(ricochet::RicochetRegion* r) {
  std::lock_guard<std::mutex> lk(mu_);
  regions_.erase(std::remove(regions_.begin(), regions_.end(), r),
                 regions_.end());
}

void RicochetMmapManager::SwitchToUPF() {
#ifdef ROCKSDB_GEM5
  ricochet::stop_handler_pool();
  if (ricochet::region_register_thread() < 0) {
    perror("SwitchToUPF: region_register_thread failed");
    abort();
  }
#endif
  // Reset counters so stats cover the measured (UPF) phase only.
  ricochet::reset_precise_stats();
}

void RicochetMmapManager::EnableUINTR() {
#ifdef ROCKSDB_GEM5
  ricochet::region_enable_uintr();
#endif
}

}  // namespace ROCKSDB_NAMESPACE

// ---------------------------------------------------------------------------
// C API
// ---------------------------------------------------------------------------

void rocksdb_ricochet_init(int ncpus, size_t max_cache_pages) {
  ROCKSDB_NAMESPACE::RicochetMmapManager::Init(ncpus, max_cache_pages);
}

void rocksdb_ricochet_switch_upf() {
  auto* mgr = ROCKSDB_NAMESPACE::RicochetMmapManager::Get();
  if (mgr) mgr->SwitchToUPF();
}

void rocksdb_ricochet_enable_uintr() {
  auto* mgr = ROCKSDB_NAMESPACE::RicochetMmapManager::Get();
  if (mgr) mgr->EnableUINTR();
}

void rocksdb_ricochet_set_precise(int enabled) {
  ricochet::precise_timing = (enabled != 0);
}

void rocksdb_ricochet_set_sched_hooks(void* submit, void* park, void* unpark) {
  ROCKSDB_NAMESPACE::g_ric_submit =
      reinterpret_cast<ricochet::submit_fn>(submit);
  ROCKSDB_NAMESPACE::g_ric_park = reinterpret_cast<ricochet::park_fn>(park);
  ROCKSDB_NAMESPACE::g_ric_unpark =
      reinterpret_cast<ricochet::unpark_fn>(unpark);
}

void rocksdb_ricochet_print_stats() {
  // Single source of truth: the library formats ricochet_stats/ricochet_timing.
  fputs(ricochet::precise_stats_report().c_str(), stdout);
}

void rocksdb_ricochet_pin_range(const void* addr, size_t len) {
  if (!ROCKSDB_NAMESPACE::RicochetMmapManager::Get()) return;
  if (!ricochet::region_lookup(reinterpret_cast<uintptr_t>(addr))) return;
  ricochet::region_pin_range(const_cast<void*>(addr), len);
}
