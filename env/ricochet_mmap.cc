#include "ricochet_mmap.h"

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

void RicochetMmapManager::Init(int handlers_per_file,
                                size_t max_cache_pages) {
  ricochet::cache_init(max_cache_pages);
  g_instance = new RicochetMmapManager();
  g_instance->handlers_per_file_ = handlers_per_file;
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
  // Snapshot the region list without holding the lock during region_switch
  // (which may block joining UFFD threads).
  std::vector<ricochet::RicochetRegion*> snapshot;
  {
    std::lock_guard<std::mutex> lk(mu_);
    snapshot = regions_;
  }
  for (auto* r : snapshot) {
    ricochet::region_switch(r);
  }
#endif
}

}  // namespace ROCKSDB_NAMESPACE

// ---------------------------------------------------------------------------
// C API
// ---------------------------------------------------------------------------

void rocksdb_ricochet_init(int handlers_per_file, size_t max_cache_pages) {
  ROCKSDB_NAMESPACE::RicochetMmapManager::Init(handlers_per_file,
                                                max_cache_pages);
}

void rocksdb_ricochet_switch_upf() {
  auto* mgr = ROCKSDB_NAMESPACE::RicochetMmapManager::Get();
  if (mgr) mgr->SwitchToUPF();
}
