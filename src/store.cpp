#include "store.h"

#include <mutex>

#include "protocol.h"  // now_millis_mono

#include <functional>

namespace tinystore {

int64_t Store::now_ms() { return now_millis_mono(); }

Store::Store(int shards) : shards_(shards > 0 ? shards : 1) {}

size_t Store::shard_for(const std::string& key) const {
  return std::hash<std::string>{}(key) % shards_.size();
}

bool Store::set(const std::string& key, const std::string& value, uint64_t ver,
                uint32_t ttl_sec) {
  Shard& s = shards_[shard_for(key)];
  std::unique_lock<std::shared_mutex> lk(s.mu);
  Entry& e = s.map[key];  // default-constructs ver=0 on first touch
  if (ver <= e.ver && e.ver != 0) return false;
  // A resurrected live write clears any tombstone.
  e.value = value;
  e.ver = ver;
  e.tombstone = false;
  e.expire_at_ms = ttl_sec ? now_ms() + int64_t(ttl_sec) * 1000 : 0;
  return true;
}

bool Store::del(const std::string& key, uint64_t ver) {
  Shard& s = shards_[shard_for(key)];
  std::unique_lock<std::shared_mutex> lk(s.mu);
  Entry& e = s.map[key];
  if (ver <= e.ver && e.ver != 0) return false;
  e.value.clear();
  e.ver = ver;
  e.tombstone = true;
  e.expire_at_ms = now_ms() + 60'000;  // tombstone retention
  return true;
}

bool Store::get(const std::string& key, std::string& value_out) {
  Shard& s = shards_[shard_for(key)];
  std::shared_lock<std::shared_mutex> lk(s.mu);
  auto it = s.map.find(key);
  if (it == s.map.end()) return false;
  const Entry& e = it->second;
  if (e.tombstone) return false;
  if (e.expire_at_ms > 0 && e.expire_at_ms <= now_ms()) return false;
  value_out = e.value;
  return true;
}

size_t Store::approx_size() const {
  size_t n = 0;
  int64_t now = now_ms();
  for (const auto& s : shards_) {
    std::shared_lock<std::shared_mutex> lk(s.mu);
    for (const auto& [k, e] : s.map) {
      if (e.tombstone) continue;
      if (e.expire_at_ms > 0 && e.expire_at_ms <= now) continue;
      ++n;
    }
  }
  return n;
}

void Store::purge_expired() {
  int64_t now = now_ms();
  for (auto& s : shards_) {
    std::unique_lock<std::shared_mutex> lk(s.mu);
    for (auto it = s.map.begin(); it != s.map.end();) {
      const Entry& e = it->second;
      bool gone = (e.expire_at_ms > 0 && e.expire_at_ms <= now);
      if (gone)
        it = s.map.erase(it);
      else
        ++it;
    }
  }
}

}  // namespace tinystore
