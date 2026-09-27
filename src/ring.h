#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace tinystore {

// 64-bit FNV-1a: stable across runs and platforms (std::hash is not),
// followed by a murmur3-style avalanche so sequential keys still spread
// uniformly around the ring.
inline uint64_t fnv1a64(const std::string& s) {
  uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) {
    h ^= c;
    h *= 1099511628211ull;
  }
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdull;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ull;
  h ^= h >> 33;
  return h;
}

// Consistent hash ring with virtual nodes. Keys map to the first node
// clockwise from hash(key); adding/removing a node only moves ~1/N keys.
class HashRing {
 public:
  void set_nodes(const std::vector<uint32_t>& node_ids, int vnodes = 160) {
    ring_.clear();
    for (uint32_t id : node_ids)
      for (int i = 0; i < vnodes; ++i)
        ring_.push_back({fnv1a64("node:" + std::to_string(id) + "#" + std::to_string(i)), id});
    std::sort(ring_.begin(), ring_.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
  }

  bool empty() const { return ring_.empty(); }

  // First node clockwise from hash(key).
  uint32_t owner(const std::string& key) const {
    uint64_t h = fnv1a64("key:" + key);
    auto it = std::lower_bound(
        ring_.begin(), ring_.end(), h,
        [](const std::pair<uint64_t, uint32_t>& p, uint64_t v) { return p.first < v; });
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
  }

  // R distinct nodes starting at the owner, walking clockwise.
  std::vector<uint32_t> replica_set(const std::string& key, int r) const {
    std::vector<uint32_t> out;
    if (ring_.empty() || r <= 0) return out;
    uint64_t h = fnv1a64("key:" + key);
    size_t idx = std::lower_bound(
                     ring_.begin(), ring_.end(), h,
                     [](const std::pair<uint64_t, uint32_t>& p, uint64_t v) {
                       return p.first < v;
                     }) -
                 ring_.begin();
    if (idx == ring_.size()) idx = 0;
    for (size_t i = 0; i < ring_.size() && (int)out.size() < r; ++i) {
      uint32_t id = ring_[(idx + i) % ring_.size()].second;
      if (std::find(out.begin(), out.end(), id) == out.end()) out.push_back(id);
    }
    return out;
  }

 private:
  std::vector<std::pair<uint64_t, uint32_t>> ring_;
};

}  // namespace tinystore
