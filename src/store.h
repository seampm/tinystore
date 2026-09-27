#pragma once

#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace tinystore {

struct Entry {
  std::string value;
  uint64_t ver = 0;          // last-writer-wins version stamp
  int64_t expire_at_ms = 0;  // monotonic ms, 0 = no expiry
  bool tombstone = false;
};

// Thread-safe sharded hash map. Writes carry a version stamp; a write is
// applied only if its version is newer than the stored one (last-writer-wins),
// which keeps replicas convergent without coordination.
class Store {
 public:
  explicit Store(int shards = 64);

  // Returns true if the write was applied (ver newer than stored).
  bool set(const std::string& key, const std::string& value, uint64_t ver,
           uint32_t ttl_sec);
  // Delete = tombstone write, so deletes replicate and win over stale data.
  bool del(const std::string& key, uint64_t ver);
  // Returns false when missing, expired, or tombstoned.
  bool get(const std::string& key, std::string& value_out);

  size_t approx_size() const;  // live (non-tombstone, non-expired) keys

  // Visit every entry, including tombstones and expired ones.
  // fn(key, value, ver, ttl_remaining_sec, tombstone)
  template <typename F>
  void scan(F&& fn) const {
    int64_t now = now_ms();
    for (const auto& s : shards_) {
      std::shared_lock<std::shared_mutex> lk(s.mu);
      for (const auto& [k, e] : s.map) {
        uint32_t ttl_left = 0;
        if (e.expire_at_ms > 0)
          ttl_left = e.expire_at_ms > now
                         ? static_cast<uint32_t>((e.expire_at_ms - now) / 1000)
                         : 0;
        fn(k, e.value, e.ver, ttl_left, e.tombstone);
      }
    }
  }

  void purge_expired();  // drop expired entries and old tombstones

 private:
  struct Shard {
    mutable std::shared_mutex mu;
    std::unordered_map<std::string, Entry> map;
  };
  static int64_t now_ms();

  std::vector<Shard> shards_;
  size_t shard_for(const std::string& key) const;
};

}  // namespace tinystore
