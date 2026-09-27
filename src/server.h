#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "protocol.h"

namespace tinystore {

// Single-threaded epoll reactor for all socket I/O, plus a worker thread pool
// for request handling. The reactor never blocks: workers do the compute (and
// any blocking inter-node RPC), then hand encoded responses back to the
// reactor through a queue + eventfd.
class Server {
 public:
  class Handler {
   public:
    virtual ~Handler() = default;
    // May return multiple responses (SYNC streams MORE frames, then a final OK).
    virtual std::vector<Response> on_request(const Request& req) = 0;
  };

  Server(int port, Handler* handler, int workers = 8);
  ~Server();

  bool start();  // bind + spawn threads; false on bind failure
  void stop();

  uint64_t requests_served() const { return requests_served_.load(); }
  int port() const { return port_; }

 private:
  struct Conn;
  struct Task {
    std::shared_ptr<Conn> conn;
    Request req;
  };

  void loop_thread();
  void worker_thread(int wi);
  void try_flush(const std::shared_ptr<Conn>& conn);
  void close_conn(int fd);
  void notify_flush(const std::shared_ptr<Conn>& conn);

  static int set_nonblocking(int fd);

  int port_;
  Handler* handler_;
  int num_workers_;

  int listen_fd_ = -1;
  int epoll_fd_ = -1;
  int wake_fd_ = -1;  // eventfd: workers -> reactor

  std::atomic<bool> running_{false};
  std::thread loop_;
  std::vector<std::thread> workers_;

  std::mutex conns_mu_;
  std::unordered_map<int, std::shared_ptr<Conn>> conns_;

  // Per-worker queues with connection affinity: every request from one
  // connection is processed by the same worker, so pipelined responses go
  // back in the order the requests arrived. (A shared queue would let two
  // workers answer one connection's requests out of order.)
  struct WorkerQueue {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<Task> q;
  };
  std::vector<std::unique_ptr<WorkerQueue>> queues_;

  std::mutex flush_mu_;
  std::vector<std::shared_ptr<Conn>> flush_pending_;

  std::atomic<uint64_t> requests_served_{0};
};

}  // namespace tinystore
