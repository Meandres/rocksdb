#pragma once

#include <cstddef>
#include <mutex>
#include <vector>

#include "api.hpp"

namespace ROCKSDB_NAMESPACE {

// Per-file context owned by each ricochet-backed PosixMmapReadableFile.
struct RicochetFileCtx {
  int fd;     // file descriptor kept open for pread
  void* base; // region->addr, set after region_init, used by evict handler
};

// Fill handler: pread one 4 KiB page from the backing file.
void ricochet_fill(void* buf, size_t offset, void* ctx);

// Evict handler: release the physical page from the anonymous mapping.
void ricochet_evict(size_t offset, void* ctx);

// Singleton that tracks all open ricochet regions.  Constructed once via
// Init(); Get() returns nullptr until then so callers can check cheaply.
class RicochetMmapManager {
 public:
  static RicochetMmapManager* Get();
  // Call once before DB::Open().  handlers_per_file UFFD handler threads are
  // spawned per SST file.  max_cache_pages=0 uses the PHYS_MEM_MB env var.
  static void Init(int handlers_per_file, size_t max_cache_pages);

  void AddRegion(ricochet::RicochetRegion* r);
  void RemoveRegion(ricochet::RicochetRegion* r);

  // Call from every reader thread before the measurement loop.
  // Stops UFFD threads (once per region) and enables UPF on this thread.
  void SwitchToUPF();

  int handlers_per_file() const { return handlers_per_file_; }

 private:
  int handlers_per_file_;
  std::mutex mu_;
  std::vector<ricochet::RicochetRegion*> regions_;
};

}  // namespace ROCKSDB_NAMESPACE

// C linkage so benchmark drivers written in C can call these.
extern "C" {
void rocksdb_ricochet_init(int handlers_per_file, size_t max_cache_pages);
void rocksdb_ricochet_switch_upf();
void rocksdb_ricochet_print_stats();
}
