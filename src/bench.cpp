// tinystore-bench: throughput + latency benchmark.
//
//   tinystore-bench --node 127.0.0.1:7000 --threads 8 --conns 2 \
//       --requests 200000 --pipeline 16 --get-ratio 0.9
//
// Each thread pipelines requests over its connections and records per-op
// latency (send -> full response). Reports throughput and p50/p99/max.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "protocol.h"

using namespace tinystore;
using clk = std::chrono::steady_clock;

static std::string flag(int argc, char** argv, const char* name,
                        const std::string& dflt) {
  for (int i = 1; i + 1 < argc; ++i)
    if (std::string(argv[i]) == name) return argv[i + 1];
  return dflt;
}

static int dial(const std::string& host, int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return -1;
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return -1;
  }
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return fd;
}

struct WorkerCfg {
  std::string host;
  int port;
  int conns;
  long requests;
  int pipeline;
  double get_ratio;
  int keyspace;
  int value_size;
};

static void worker(const WorkerCfg& cfg, std::vector<double>& lat_out,
                   std::atomic<long>& done, std::atomic<bool>& failed) {
  std::mt19937_64 rng(std::random_device{}() ^
                      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  std::uniform_int_distribution<int> key_dist(0, cfg.keyspace - 1);
  std::uniform_real_distribution<double> op_dist(0.0, 1.0);

  std::vector<int> fds;
  for (int i = 0; i < cfg.conns; ++i) {
    int fd = dial(cfg.host, cfg.port);
    if (fd < 0) {
      failed = true;
      return;
    }
    fds.push_back(fd);
  }

  std::string value(cfg.value_size, 'v');
  for (int i = 0; i < cfg.value_size; ++i)
    value[i] = 'a' + (rng() % 26);

  lat_out.reserve(cfg.requests);
  std::vector<clk::time_point> sent(cfg.pipeline);
  std::vector<Request> batch(cfg.pipeline);
  int conn_rr = 0;

  long remaining = cfg.requests;
  while (remaining > 0 && !failed) {
    int d = (int)std::min<long>(cfg.pipeline, remaining);
    int fd = fds[conn_rr++ % fds.size()];
    for (int i = 0; i < d; ++i) {
      Request r;
      char kb[32];
      std::snprintf(kb, sizeof(kb), "key%08d", key_dist(rng));
      r.key = kb;
      if (op_dist(rng) < cfg.get_ratio) {
        r.op = Op::GET;
      } else {
        r.op = Op::SET;
        r.value = value;
      }
      batch[i] = r;
      sent[i] = clk::now();
      if (!write_all(fd, encode_request(r))) {
        failed = true;
        break;
      }
    }
    if (failed) break;
    for (int i = 0; i < d; ++i) {
      Response resp;
      if (!read_response(fd, resp)) {
        failed = true;
        break;
      }
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                    clk::now() - sent[i])
                    .count();
      lat_out.push_back(us / 1000.0);
    }
    remaining -= d;
    done += d;
  }
  for (int fd : fds) ::close(fd);
}

int main(int argc, char** argv) {
  WorkerCfg cfg;
  std::string node = flag(argc, argv, "--node", "127.0.0.1:7000");
  size_t colon = node.rfind(':');
  cfg.host = node.substr(0, colon);
  cfg.port = std::stoi(node.substr(colon + 1));
  int threads = std::stoi(flag(argc, argv, "--threads", "8"));
  cfg.conns = std::stoi(flag(argc, argv, "--conns", "2"));
  cfg.requests = std::stol(flag(argc, argv, "--requests", "200000"));
  cfg.pipeline = std::stoi(flag(argc, argv, "--pipeline", "16"));
  cfg.get_ratio = std::stod(flag(argc, argv, "--get-ratio", "0.9"));
  cfg.keyspace = std::stoi(flag(argc, argv, "--keyspace", "100000"));
  cfg.value_size = std::stoi(flag(argc, argv, "--value-size", "64"));

  // Warm up: populate the keyspace so GETs usually hit.
  {
    int fd = dial(cfg.host, cfg.port);
    if (fd < 0) {
      std::fprintf(stderr, "error: cannot connect to %s\n", node.c_str());
      return 1;
    }
    std::string value(cfg.value_size, 'w');
    for (int i = 0; i < cfg.keyspace; i += 10) {
      Request r;
      char kb[32];
      std::snprintf(kb, sizeof(kb), "key%08d", i);
      r.op = Op::SET;
      r.key = kb;
      r.value = value;
      if (!write_all(fd, encode_request(r))) break;
      Response resp;
      if (!read_response(fd, resp)) break;
    }
    ::close(fd);
    std::printf("warmup done (%d keys)\n", cfg.keyspace / 10);
    std::fflush(stdout);
  }

  std::vector<std::thread> th;
  std::vector<std::vector<double>> lat(threads);
  std::atomic<long> done{0};
  std::atomic<bool> failed{false};
  auto t0 = clk::now();
  for (int i = 0; i < threads; ++i)
    th.emplace_back(worker, std::cref(cfg), std::ref(lat[i]), std::ref(done),
                    std::ref(failed));
  for (auto& t : th) t.join();
  auto t1 = clk::now();

  if (failed) {
    std::fprintf(stderr, "error: connection failed during benchmark\n");
    return 1;
  }
  std::vector<double> all;
  for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
  std::sort(all.begin(), all.end());
  double secs =
      std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() /
      1e6;
  auto pct = [&](double p) { return all[(size_t)(p * (all.size() - 1))]; };
  std::printf(
      "ops: %zu  threads: %d  conns/thread: %d  pipeline: %d  get_ratio: "
      "%.2f\n",
      all.size(), threads, cfg.conns, cfg.pipeline, cfg.get_ratio);
  std::printf("throughput: %.0f ops/sec\n", all.size() / secs);
  std::printf("latency ms:  p50 %.3f  p99 %.3f  max %.3f\n", pct(0.50),
              pct(0.99), all.back());
  return 0;
}
