#include "ricochet_mmap.h"
#include "replacement.hpp"

#include <cinttypes>
#include <cstdio>
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

void ricochet_evict(size_t offset, void* ctx) {
  auto* c = static_cast<RicochetFileCtx*>(ctx);
  madvise(static_cast<char*>(c->base) + offset, 4096, MADV_DONTNEED);
}

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
  ricochet::global_cache().evictedPageCount.store(0, std::memory_order_relaxed);
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

void rocksdb_ricochet_print_stats() {
  ricochet::PageCache& c = ricochet::global_cache();
  uint64_t faults = c.upfFaultCount.load(std::memory_order_relaxed);
  uint64_t evicts = c.evictedPageCount.load(std::memory_order_relaxed);
  printf("ricochet_stats: upf_faults=%" PRIu64 " evicted_pages=%" PRIu64 "\n",
         faults, evicts);
}
