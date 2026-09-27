#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace tinystore {

struct Server::Conn {
  int fd = -1;
  std::vector<char> read_buf;
  RequestParser parser;
  std::vector<char> write_buf;
  size_t write_off = 0;
  std::mutex m;
  std::atomic<bool> closed{false};
  bool out_armed = false;  // EPOLLOUT currently armed (loop thread only)
};

Server::Server(int port, Handler* handler, int workers)
    : port_(port), handler_(handler), num_workers_(workers > 0 ? workers : 1) {}

Server::~Server() { stop(); }

int Server::set_nonblocking(int fd) {
  int f = fcntl(fd, F_GETFL, 0);
  return fcntl(fd, F_SETFL, f | O_NONBLOCK);
}

bool Server::start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) return false;
  int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  set_nonblocking(listen_fd_);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port_));
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    return false;
  if (::listen(listen_fd_, 512) < 0) return false;

  epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) return false;
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) return false;

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd_;
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = wake_fd_;
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev);

  running_ = true;
  loop_ = std::thread(&Server::loop_thread, this);
  queues_.reserve(num_workers_);
  for (int i = 0; i < num_workers_; ++i) {
    queues_.push_back(std::make_unique<WorkerQueue>());
    workers_.emplace_back(&Server::worker_thread, this, i);
  }
  return true;
}

void Server::stop() {
  bool was = running_.exchange(false);
  if (!was) return;
  // Wake loop + workers so they can exit.
  uint64_t one = 1;
  ::write(wake_fd_, &one, sizeof(one));
  for (auto& q : queues_) q->cv.notify_all();
  if (loop_.joinable()) loop_.join();
  for (auto& t : workers_)
    if (t.joinable()) t.join();
  {
    std::lock_guard<std::mutex> lk(conns_mu_);
    for (auto& [fd, c] : conns_) {
      c->closed = true;
      ::close(fd);
    }
    conns_.clear();
  }
  if (listen_fd_ >= 0) ::close(listen_fd_);
  if (epoll_fd_ >= 0) ::close(epoll_fd_);
  if (wake_fd_ >= 0) ::close(wake_fd_);
  listen_fd_ = epoll_fd_ = wake_fd_ = -1;
}

void Server::close_conn(int fd) {
  std::shared_ptr<Conn> c;
  {
    std::lock_guard<std::mutex> lk(conns_mu_);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    c = it->second;
    conns_.erase(it);
  }
  c->closed = true;
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  ::close(fd);
}

void Server::notify_flush(const std::shared_ptr<Conn>& conn) {
  {
    std::lock_guard<std::mutex> lk(flush_mu_);
    flush_pending_.push_back(conn);
  }
  uint64_t one = 1;
  ::write(wake_fd_, &one, sizeof(one));  // coalesced by eventfd counter
}

// Loop thread only.
void Server::try_flush(const std::shared_ptr<Conn>& conn) {
  // Stale entry (fd closed and possibly reused)?
  {
    std::lock_guard<std::mutex> lk(conns_mu_);
    auto it = conns_.find(conn->fd);
    if (it == conns_.end() || it->second.get() != conn.get()) return;
  }
  bool hard_error = false;
  {
    std::lock_guard<std::mutex> lk(conn->m);
    if (conn->closed) return;
    while (conn->write_off < conn->write_buf.size()) {
      ssize_t w = ::send(conn->fd, conn->write_buf.data() + conn->write_off,
                         conn->write_buf.size() - conn->write_off, MSG_NOSIGNAL);
      if (w < 0) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          if (!conn->out_armed) {
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
            ev.data.fd = conn->fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn->fd, &ev);
            conn->out_armed = true;
          }
          return;
        }
        hard_error = true;  // connection reset etc.
        break;
      }
      if (w == 0) return;
      conn->write_off += static_cast<size_t>(w);
    }
    if (!hard_error) {
      conn->write_buf.clear();
      conn->write_off = 0;
      if (conn->out_armed) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = conn->fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn->fd, &ev);
        conn->out_armed = false;
      }
    }
  }
  if (hard_error) close_conn(conn->fd);
}

void Server::loop_thread() {
  constexpr int MAXE = 128;
  epoll_event events[MAXE];
  while (running_) {
    int n = ::epoll_wait(epoll_fd_, events, MAXE, 100);
    for (int i = 0; i < n; ++i) {
      int fd = events[i].data.fd;
      uint32_t ev = events[i].events;
      if (fd == listen_fd_) {
        for (;;) {
          int cfd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (cfd < 0) break;  // EAGAIN: drained
          int flag = 1;
          ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
          auto conn = std::make_shared<Conn>();
          conn->fd = cfd;
          conn->read_buf.reserve(4096);
          {
            std::lock_guard<std::mutex> lk(conns_mu_);
            conns_[cfd] = conn;
          }
          epoll_event cev{};
          cev.events = EPOLLIN | EPOLLET;
          cev.data.fd = cfd;
          ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cfd, &cev);
        }
        continue;
      }
      if (fd == wake_fd_) {
        uint64_t cnt = 0;
        while (::read(wake_fd_, &cnt, sizeof(cnt)) > 0) {
        }
        std::vector<std::shared_ptr<Conn>> pending;
        {
          std::lock_guard<std::mutex> lk(flush_mu_);
          pending.swap(flush_pending_);
        }
        for (auto& c : pending) try_flush(c);
        continue;
      }
      if (ev & (EPOLLERR | EPOLLHUP)) {
        close_conn(fd);
        continue;
      }
      std::shared_ptr<Conn> conn;
      {
        std::lock_guard<std::mutex> lk(conns_mu_);
        auto it = conns_.find(fd);
        if (it == conns_.end()) continue;
        conn = it->second;
      }
      if (ev & EPOLLOUT) try_flush(conn);
      if (ev & EPOLLIN) {
        bool dead = false;
        for (;;) {
          char tmp[8192];
          ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
          if (r < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) dead = true;
            break;
          }
          if (r == 0) {
            dead = true;
            break;
          }
          auto reqs = conn->parser.feed(tmp, static_cast<size_t>(r));
          if (conn->parser.error()) {
            dead = true;
            break;
          }
          for (auto& rq : reqs) {
            // Connection affinity: one connection's requests always land on
            // the same worker, keeping pipelined responses ordered.
            size_t wi = static_cast<size_t>(fd) % queues_.size();
            auto& wq = queues_[wi];
            {
              std::lock_guard<std::mutex> lk(wq->mu);
              wq->q.push_back(Task{conn, std::move(rq)});
            }
            wq->cv.notify_one();
          }
        }
        if (dead) close_conn(fd);
      }
    }
  }
}

void Server::worker_thread(int wi) {
  WorkerQueue& wq = *queues_[wi];
  for (;;) {
    Task task;
    {
      std::unique_lock<std::mutex> lk(wq.mu);
      wq.cv.wait(lk, [&] { return !running_ || !wq.q.empty(); });
      if (!running_ && wq.q.empty()) return;
      task = std::move(wq.q.front());
      wq.q.pop_front();
    }
    auto responses = handler_->on_request(task.req);
    requests_served_.fetch_add(1);
    bool appended = false;
    {
      std::lock_guard<std::mutex> lk(task.conn->m);
      if (!task.conn->closed) {
        for (auto& r : responses) {
          auto bytes = encode_response(r);
          task.conn->write_buf.insert(task.conn->write_buf.end(), bytes.begin(),
                                      bytes.end());
        }
        appended = true;
      }
    }
    if (appended) notify_flush(task.conn);
  }
}

}  // namespace tinystore
