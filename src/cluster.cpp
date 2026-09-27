#include "cluster.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>

namespace tinystore {

static Request with_forwarded(const Request& r) {
  Request c = r;
  c.flags |= FLAG_FORWARDED;
  return c;
}

Cluster::Cluster(uint32_t self_id, std::vector<NodeInfo> nodes, int repl_factor,
                 int hb_interval_ms, int dead_timeout_ms, int rpc_timeout_ms)
    : self_id_(self_id),
      nodes_(std::move(nodes)),
      repl_factor_(repl_factor > 0 ? repl_factor : 1),
      hb_interval_ms_(hb_interval_ms),
      dead_timeout_ms_(dead_timeout_ms),
      rpc_timeout_ms_(rpc_timeout_ms) {
  int64_t now = now_millis_mono();
  std::lock_guard<std::mutex> lk(live_mu_);
  last_seen_ms_[self_id_] = now;
}

uint64_t Cluster::make_ver() const {
  return (now_micros() << 10) | (self_id_ & 0x3ff);
}

const NodeInfo* Cluster::find_node(uint32_t id) const {
  for (auto& n : nodes_)
    if (n.id == id) return &n;
  return nullptr;
}

std::vector<uint32_t> Cluster::live_ids() const {
  int64_t now = now_millis_mono();
  std::lock_guard<std::mutex> lk(live_mu_);
  std::vector<uint32_t> out;
  for (auto& n : nodes_) {
    auto it = last_seen_ms_.find(n.id);
    if (it != last_seen_ms_.end() && now - it->second < dead_timeout_ms_)
      out.push_back(n.id);
  }
  // Self is always live.
  if (std::find(out.begin(), out.end(), self_id_) == out.end())
    out.push_back(self_id_);
  return out;
}

// Dial a peer with a connect timeout. Returns the fd or -1.
static int dial_peer(const NodeInfo& peer, int timeout_ms);

bool Cluster::rpc(const NodeInfo& peer, const Request& req, Response& out,
                  int timeout_ms) const {
  // Up to two attempts: a pooled fd may have gone stale (peer restarted).
  for (int attempt = 0; attempt < 2; ++attempt) {
    int fd = (attempt == 0) ? pool_take(peer, timeout_ms)
                            : dial_peer(peer, timeout_ms);
    if (fd < 0) return false;
    bool ok = write_all(fd, encode_request(req)) && read_response(fd, out);
    if (ok) {
      pool_give(peer.id, fd);
      std::lock_guard<std::mutex> lk(live_mu_);
      last_seen_ms_[peer.id] = now_millis_mono();
      return true;
    }
    ::close(fd);  // stale or broken; next attempt dials fresh
  }
  return false;
}

// Dial a peer with a connect timeout. Returns the fd or -1.
static int dial_peer(const NodeInfo& peer, int timeout_ms) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  int fl = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, fl | O_NONBLOCK);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(peer.port));
  if (::inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return -1;
  }
  int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS) {
    ::close(fd);
    return -1;
  }
  fd_set wf;
  FD_ZERO(&wf);
  FD_SET(fd, &wf);
  timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  if (::select(fd + 1, nullptr, &wf, nullptr, &tv) <= 0) {
    ::close(fd);
    return -1;
  }
  int err = 0;
  socklen_t elen = sizeof(err);
  ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
  if (err != 0) {
    ::close(fd);
    return -1;
  }
  fcntl(fd, F_SETFL, fl);  // blocking again
  timeval rtv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &rtv, sizeof(rtv));
  int flag = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  return fd;
}

int Cluster::pool_take(const NodeInfo& peer, int timeout_ms) const {
  int fd = -1;
  {
    std::lock_guard<std::mutex> lk(pool_mu_);
    auto it = pool_.find(peer.id);
    if (it != pool_.end() && !it->second.empty()) {
      fd = it->second.back();
      it->second.pop_back();
    }
  }
  if (fd >= 0) {
    // A pooled fd carries the timeout of whatever rpc dialed it; refresh it
    // for this caller so a stale value can't leak across uses.
    timeval rtv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &rtv, sizeof(rtv));
    return fd;
  }
  return dial_peer(peer, timeout_ms);
}

void Cluster::pool_give(uint32_t peer_id, int fd) const {
  std::lock_guard<std::mutex> lk(pool_mu_);
  auto& v = pool_[peer_id];
  if ((int)v.size() < MAX_POOL_PER_PEER)
    v.push_back(fd);
  else
    ::close(fd);
}

// Asynchronous replication: the owner applies locally and acks immediately;
// a background per-peer sender delivers at-least-once (idempotent via
// version stamps). Returns the number of replicas the write was queued for.
int Cluster::replicate(const Request& repl) {
  auto targets = ring_replicas(repl.key, repl_factor_);
  int queued = 0;
  for (uint32_t id : targets) {
    if (id == self_id_) continue;
    if (ReplSender* s = sender_for(id)) {
      s->enqueue(repl);  // already translated to REPL_SET/REPL_DEL by caller
      ++queued;
    }
  }
  return queued;
}

ReplSender* Cluster::sender_for(uint32_t peer_id) const {
  std::lock_guard<std::mutex> lk(senders_mu_);
  auto it = senders_.find(peer_id);
  if (it != senders_.end()) return it->second.get();
  if (const NodeInfo* peer = find_node(peer_id)) {
    NodeInfo node = *peer;
    auto s = std::make_unique<ReplSender>([this, node](const Request& r) {
      Response rr;
      return rpc(node, r, rr, rpc_timeout_ms_) && rr.status == Status::OK;
    });
    ReplSender* p = s.get();
    senders_[peer_id] = std::move(s);
    return p;
  }
  return nullptr;
}

void Cluster::refresh_ring(std::vector<uint32_t> live) const {
  std::sort(live.begin(), live.end());
  std::lock_guard<std::mutex> lk(ring_mu_);
  if (live != ring_live_) {
    ring_live_ = live;
    ring_.set_nodes(live);
  }
}

uint32_t Cluster::ring_owner(const std::string& key) const {
  refresh_ring(live_ids());
  std::lock_guard<std::mutex> lk(ring_mu_);
  if (ring_.empty()) return UINT32_MAX;
  return ring_.owner(key);
}

std::vector<uint32_t> Cluster::ring_replicas(const std::string& key,
                                             int r) const {
  refresh_ring(live_ids());
  std::lock_guard<std::mutex> lk(ring_mu_);
  return ring_.replica_set(key, r);
}

void Cluster::apply_repl(const Request& req) {
  if (req.op == Op::REPL_SET)
    store_.set(req.key, req.value, req.ver, req.ttl_sec);
  else if (req.op == Op::REPL_DEL)
    store_.del(req.key, req.ver);
}

std::vector<Response> Cluster::on_request(const Request& req) {
  std::vector<Response> out;
  switch (req.op) {
    case Op::PING: {
      // Heartbeat: key carries the sender's node id.
      try {
        uint32_t sender = static_cast<uint32_t>(std::stoul(req.key));
        std::lock_guard<std::mutex> lk(live_mu_);
        last_seen_ms_[sender] = now_millis_mono();
      } catch (...) {
      }
      out.push_back(Response{Status::OK, "PONG", 0});
      return out;
    }
    case Op::STATS: {
      std::ostringstream ss;
      auto live = live_ids();
      ss << "keys: " << store_.approx_size() << "\n";
      ss << "uptime_sec: " << (now_millis_mono() - start_ms_) / 1000 << "\n";
      ss << "requests: " << requests_served() << "\n";
      ss << "peers: " << nodes_.size() << "\n";
      ss << "live: " << live.size() << "\n";
      ss << "repl_factor: " << repl_factor_ << "\n";
      ss << "node_id: " << self_id_ << "\n";
      out.push_back(Response{Status::OK, ss.str(), 0});
      return out;
    }
    case Op::REPL_SET:
    case Op::REPL_DEL: {
      apply_repl(req);
      out.push_back(Response{Status::OK, "", 0});
      return out;
    }
    case Op::SYNC: {
      // A (re)joining node pulls every key it should hold.
      uint32_t joiner = 0;
      try {
        joiner = static_cast<uint32_t>(std::stoul(req.key));
      } catch (...) {
        out.push_back(Response{Status::ERROR, "bad node id", 0});
        return out;
      }
      HashRing ring;
      std::vector<uint32_t> all;
      for (auto& n : nodes_) all.push_back(n.id);
      ring.set_nodes(all);
      store_.scan([&](const std::string& k, const std::string& v, uint64_t ver,
                      uint32_t ttl_left, bool tomb) {
        auto rs = ring.replica_set(k, repl_factor_);
        if (std::find(rs.begin(), rs.end(), joiner) == rs.end()) return;
        Request inner;
        inner.op = tomb ? Op::REPL_DEL : Op::REPL_SET;
        inner.key = k;
        inner.value = v;
        inner.ver = ver;
        inner.ttl_sec = ttl_left;
        out.push_back(Response{Status::MORE, std::string(), 0});
        out.back().value.assign(
            [&] {
              auto b = encode_request(inner);
              return std::string(b.begin(), b.end());
            }());
      });
      out.push_back(Response{Status::OK, "", 0});
      return out;
    }
    case Op::GET:
    case Op::SET:
    case Op::DEL: {
      uint32_t owner = ring_owner(req.key);
      if (owner == UINT32_MAX) {
        out.push_back(Response{Status::ERROR, "no live nodes", 0});
        return out;
      }
      if (owner != self_id_) {
        if (req.flags & FLAG_FORWARDED) {
          out.push_back(Response{Status::ERROR, "routing disagreement", 0});
          return out;
        }
        const NodeInfo* peer = find_node(owner);
        Response fr;
        if (peer && rpc(*peer, with_forwarded(req), fr, rpc_timeout_ms_ * 4))
          out.push_back(fr);
        else
          out.push_back(Response{Status::ERROR, "owner unreachable", 0});
        return out;
      }
      if (req.op == Op::GET) {
        std::string v;
        if (store_.get(req.key, v))
          out.push_back(Response{Status::OK, v, 0});
        else
          out.push_back(Response{Status::NOT_FOUND, "", 0});
        return out;
      }
      // Owner write: apply locally, then replicate.
      Request repl = req;
      repl.ver = make_ver();
      repl.op = (req.op == Op::SET) ? Op::REPL_SET : Op::REPL_DEL;
      repl.flags = 0;
      apply_repl(repl);
      int acks = replicate(repl);
      out.push_back(Response{Status::OK, "", static_cast<uint64_t>(acks)});
      return out;
    }
  }
  out.push_back(Response{Status::ERROR, "unknown op", 0});
  return out;
}

void Cluster::heartbeat_loop() {
  Request ping;
  ping.op = Op::PING;
  ping.key = std::to_string(self_id_);
  while (running_) {
    for (auto& n : nodes_) {
      if (n.id == self_id_) continue;
      Response r;
      rpc(n, ping, r, std::min(1000, dead_timeout_ms_ / 2));
    }
    // Peers with no successful rpc inside the dead timeout are considered
    // dead: drop their queued replication (a rejoining node heals via SYNC).
    auto live = live_ids();
    {
      std::lock_guard<std::mutex> lk(senders_mu_);
      for (auto& [id, s] : senders_)
        if (std::find(live.begin(), live.end(), id) == live.end()) s->drop();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(hb_interval_ms_));
  }
}

void Cluster::sweeper_loop() {
  while (running_) {
    for (int i = 0; i < 25 && running_; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    store_.purge_expired();
  }
}

void Cluster::sync_from_peers() {
  // Give the server a moment to come up, then pull our range from each peer.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  Request sync;
  sync.op = Op::SYNC;
  sync.key = std::to_string(self_id_);
  for (auto& n : nodes_) {
    if (n.id == self_id_) continue;
    int fd = -1;
    // Open one connection and stream all MORE frames.
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) continue;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(n.port));
    if (::inet_pton(AF_INET, n.host.c_str(), &addr.sin_addr) != 1) {
      ::close(fd);
      continue;
    }
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    bool conn_ok = false;
    if (rc == 0 || errno == EINPROGRESS) {
      fd_set wf;
      FD_ZERO(&wf);
      FD_SET(fd, &wf);
      timeval tv{10, 0};
      if (::select(fd + 1, nullptr, &wf, nullptr, &tv) > 0) {
        int err = 0;
        socklen_t elen = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
        conn_ok = (err == 0);
      }
    }
    if (!conn_ok) {
      ::close(fd);
      continue;
    }
    fcntl(fd, F_SETFL, fl);
    timeval rtv{30, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &rtv, sizeof(rtv));
    if (!write_all(fd, encode_request(sync))) {
      ::close(fd);
      continue;
    }
    RequestParser parser;
    for (;;) {
      Response r;
      if (!read_response(fd, r)) break;
      if (r.status == Status::OK) break;
      if (r.status != Status::MORE) break;
      auto frames = parser.feed(r.value.data(), r.value.size());
      for (auto& f : frames) apply_repl(f);
      if (parser.error()) break;
    }
    ::close(fd);
  }
}

bool Cluster::start(int port, int workers) {
  start_ms_ = now_millis_mono();
  server_ = std::make_unique<Server>(port, this, workers);
  if (!server_->start()) return false;
  running_ = true;
  hb_thread_ = std::thread(&Cluster::heartbeat_loop, this);
  sweep_thread_ = std::thread(&Cluster::sweeper_loop, this);
  // Pull our key range in the background so startup stays fast.
  std::thread(&Cluster::sync_from_peers, this).detach();
  return true;
}

void Cluster::stop() {
  if (server_) server_->stop();  // close listen socket first: frees the port
  running_ = false;
  if (hb_thread_.joinable()) hb_thread_.join();
  if (sweep_thread_.joinable()) sweep_thread_.join();
  {
    // Stop replication senders before tearing down the pool they send on.
    std::lock_guard<std::mutex> lk(senders_mu_);
    for (auto& [id, s] : senders_) s->stop();
  }
  {
    std::lock_guard<std::mutex> lk(pool_mu_);
    for (auto& [id, v] : pool_)
      for (int fd : v) ::close(fd);
    pool_.clear();
  }
}

}  // namespace tinystore
