#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ring.h"
#include "server.h"
#include "store.h"

namespace tinystore {

struct NodeInfo {
  uint32_t id;
  std::string host;
  int port;
};

// Background per-peer replication sender. The owner of a write applies it
// locally, acks the client immediately, and enqueues the write here;
// at-least-once delivery (version stamps make redelivery idempotent).
// Asynchronous replication keeps request workers from ever blocking on the
// network for replication, which is what makes the cross-node wait graph
// acyclic: a worker only ever waits on a remote worker doing fast local
// work. If a peer is declared dead its queue is dropped -- a rejoining node
// heals via SYNC instead.
class ReplSender {
 public:
  explicit ReplSender(std::function<bool(const Request&)> send)
      : send_(std::move(send)), running_(true), th_([this] { loop(); }) {}
  ~ReplSender() { stop(); }
  ReplSender(const ReplSender&) = delete;
  ReplSender& operator=(const ReplSender&) = delete;

  void enqueue(Request r) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (q_.size() >= 65536) q_.pop_front();  // bounded
      q_.push_back(std::move(r));
    }
    cv_.notify_one();
  }
  void drop() {  // peer declared dead: stop queueing for it
    std::lock_guard<std::mutex> lk(mu_);
    q_.clear();
  }
  void stop() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!running_) return;
      running_ = false;
      q_.clear();
    }
    cv_.notify_all();
    if (th_.joinable()) th_.join();
  }

 private:
  void loop() {
    for (;;) {
      Request r;
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return !running_ || !q_.empty(); });
        if (!running_ && q_.empty()) return;
        r = std::move(q_.front());
        q_.pop_front();
      }
      if (!send_(r)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::lock_guard<std::mutex> lk(mu_);
        if (!running_) return;
        q_.push_front(std::move(r));
      }
    }
  }
  std::function<bool(const Request&)> send_;
  std::deque<Request> q_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool running_;
  std::thread th_;
};

// A cluster node: owns a Store, routes requests with a consistent hash ring,
// replicates writes to R-1 successors, and detects failures with heartbeats.
//
// Failure model (documented, not hidden): with replication factor R the
// cluster keeps serving through R-1 concurrent node failures, because every
// key lives on R distinct nodes and a dead owner is simply skipped by the
// ring. Two owners can briefly disagree during a membership change; writes
// carry (timestamp, node) version stamps so replicas converge
// last-writer-wins.
class Cluster : public Server::Handler {
 public:
  Cluster(uint32_t self_id, std::vector<NodeInfo> nodes, int repl_factor,
          int hb_interval_ms = 500, int dead_timeout_ms = 2000,
          int rpc_timeout_ms = 500);

  bool start(int port, int workers = 8);
  void stop();

  std::vector<Response> on_request(const Request& req) override;

  uint64_t requests_served() const {
    return server_ ? server_->requests_served() : 0;
  }
  size_t keys() const { return store_.approx_size(); }

 private:
  uint64_t make_ver() const;
  std::vector<uint32_t> live_ids() const;  // self + recently-heartbeated peers
  const NodeInfo* find_node(uint32_t id) const;

  // Blocking RPC to a peer. Updates liveness on success.
  bool rpc(const NodeInfo& peer, const Request& req, Response& out,
           int timeout_ms) const;
  // Enqueue an owner write for async replication; returns # of replicas
  // the write was queued for.
  int replicate(const Request& repl);
  void apply_repl(const Request& req);

  // Persistent pooled connections to peers (connect-per-request would drown
  // in TIME_WAIT at any real throughput).
  int pool_take(const NodeInfo& peer, int timeout_ms) const;  // -1 on failure
  void pool_give(uint32_t peer_id, int fd) const;
  static constexpr int MAX_POOL_PER_PEER = 8;
  mutable std::mutex pool_mu_;
  mutable std::unordered_map<uint32_t, std::vector<int>> pool_;

  // Ring helpers (ring cached; rebuilt only when the live set changes).
  void refresh_ring(std::vector<uint32_t> live) const;
  uint32_t ring_owner(const std::string& key) const;  // UINT32_MAX if none
  std::vector<uint32_t> ring_replicas(const std::string& key, int r) const;

  void heartbeat_loop();
  void sync_from_peers();  // pull our key range after (re)start
  void sweeper_loop();

  uint32_t self_id_;
  std::vector<NodeInfo> nodes_;
  int repl_factor_;
  int hb_interval_ms_;
  int dead_timeout_ms_;
  int rpc_timeout_ms_;

  Store store_;
  std::unique_ptr<Server> server_;

  // Per-peer async replication senders, created on demand.
  mutable std::mutex senders_mu_;
  mutable std::unordered_map<uint32_t, std::unique_ptr<ReplSender>> senders_;
  ReplSender* sender_for(uint32_t peer_id) const;

  mutable std::mutex live_mu_;
  mutable std::unordered_map<uint32_t, int64_t> last_seen_ms_;  // node id -> mono ms

  // Cached ring; rebuilt only when the live set changes.
  mutable std::mutex ring_mu_;
  mutable std::vector<uint32_t> ring_live_;
  mutable HashRing ring_;

  std::atomic<bool> running_{false};
  std::thread hb_thread_;
  std::thread sweep_thread_;
  int64_t start_ms_ = 0;
};

}  // namespace tinystore
