#pragma once

#include <atomic>
#include <cstddef>
#include <mutex>
#include <vector>

#include "api.hpp"

namespace ROCKSDB_NAMESPACE {

// Per-file context owned by each ricochet-backed PosixMmapReadableFile.
struct RicochetFileCtx {
  int fd;     // file descriptor kept open for pread
  void* base; // region->addr, set after region_init
};

// Fill handler: pread one 4 KiB page from the backing file.
// Eviction uses the library's default batched MADV_DONTNEED path.
void ricochet_fill(void* buf, size_t offset, void* ctx);

// Singleton that tracks all open ricochet regions.  Constructed once via
// Init(); Get() returns nullptr until then so callers can check cheaply.
class RicochetMmapManager {
 public:
  static RicochetMmapManager* Get();
  // Call once before DB::Open().  ncpus handler threads are started globally.
  // max_cache_pages=0 uses the PHYS_MEM_MB env var.
  static void Init(int ncpus, size_t max_cache_pages);

  void AddRegion(ricochet::RicochetRegion* r);
  void RemoveRegion(ricochet::RicochetRegion* r);

  // Call from every reader thread at the UFFD→UPF switch point.
  // Stops all handler pool threads (once, globally) then registers the UINTR
  // handler.  Does NOT enable UINTR reception (_stui); call EnableUINTR() after
  // any barrier so the futex wait is not disrupted by a pending interrupt.
  void SwitchToUPF();

  // Enable UINTR reception on the calling thread.  Call after SwitchToUPF()
  // and after any pthread_barrier_wait that must not be disturbed by UINTRs.
  void EnableUINTR();

 private:
  std::mutex mu_;
  std::vector<ricochet::RicochetRegion*> regions_;
};

}  // namespace ROCKSDB_NAMESPACE

// C linkage so benchmark drivers written in C can call these.
extern "C" {
void rocksdb_ricochet_init(int ncpus, size_t max_cache_pages);
void rocksdb_ricochet_switch_upf();
void rocksdb_ricochet_enable_uintr();
void rocksdb_ricochet_print_stats();
}
