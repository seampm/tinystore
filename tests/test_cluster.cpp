// Integration tests: real 3-node cluster as forked processes.
// Exercises routing, replication, failover on kill -9, and rejoin via SYNC.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "../src/protocol.h"
#include "../src/ring.h"

using namespace tinystore;

static int failures = 0;
static int checks = 0;
#define CHECK(cond)                                              \
  do {                                                           \
    ++checks;                                                    \
    if (!(cond)) {                                               \
      ++failures;                                                \
      std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
    }                                                            \
  } while (0)

static const char* SERVER_BIN = "build/tinystore-server";
static int BASE_PORT = 17210;

static std::vector<pid_t> children;

static void kill_all() {
  for (pid_t p : children) {
    ::kill(p, SIGKILL);
    ::waitpid(p, nullptr, 0);
  }
  children.clear();
}

static pid_t start_node(int id) {
  pid_t p = ::fork();
  if (p == 0) {
    char id_s[16], hb_s[16], dead_s[16];
    std::snprintf(id_s, sizeof(id_s), "%d", id);
    std::snprintf(hb_s, sizeof(hb_s), "%d", 100);
    std::snprintf(dead_s, sizeof(dead_s), "%d", 400);
    std::string peers;
    for (int i = 0; i < 3; ++i) {
      if (i) peers += ",";
      peers += "127.0.0.1:" + std::to_string(BASE_PORT + i);
    }
    ::execl(SERVER_BIN, SERVER_BIN, "--id", id_s, "--peers", peers.c_str(),
            "--repl", "2", "--hb-ms", hb_s, "--dead-ms", dead_s, "--workers",
            "4", (char*)nullptr);
    ::_exit(127);
  }
  children.push_back(p);
  return p;
}

static int dial(int port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  timeval tv{2, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  if (::connect(fd, (sockaddr*)&addr, sizeof(addr))) {
    ::close(fd);
    return -1;
  }
  return fd;
}

static bool rpc(int port, const Request& req, Response& out) {
  int fd = dial(port);
  if (fd < 0) return false;
  bool ok = write_all(fd, encode_request(req)) && read_response(fd, out);
  ::close(fd);
  return ok;
}

static bool rpc_retry(int port, const Request& req, Response& out,
                      int tries = 50) {
  for (int i = 0; i < tries; ++i) {
    if (rpc(port, req, out)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

static int stat_live(int port) {
  Request r;
  r.op = Op::STATS;
  Response resp;
  if (!rpc_retry(port, r, resp)) return -1;
  for (const char* p = resp.value.c_str(); *p; ++p)
    if (!std::strncmp(p, "live: ", 6)) return std::atoi(p + 6);
  return -1;
}

static bool wait_live(int port, int want, int timeout_ms = 8000) {
  auto t0 = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - t0)
             .count() < timeout_ms) {
    if (stat_live(port) == want) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

static bool set_key(int port, const std::string& k, const std::string& v) {
  Request r;
  r.op = Op::SET;
  r.key = k;
  r.value = v;
  Response resp;
  return rpc_retry(port, r, resp) && resp.status == Status::OK;
}

static bool get_key(int port, const std::string& k, std::string& v) {
  Request r;
  r.op = Op::GET;
  r.key = k;
  Response resp;
  if (!rpc_retry(port, r, resp)) return false;
  if (resp.status != Status::OK) return false;
  v = resp.value;
  return true;
}

static bool wait_repl(const std::string& k, const std::string& v, int want,
                      int timeout_ms = 8000) {
  // Replication is asynchronous: poll until `want` nodes hold the key.
  auto t0 = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - t0)
             .count() < timeout_ms) {
    int have = 0;
    for (int i = 0; i < 3; ++i) {
      std::string gv;
      if (get_key(BASE_PORT + i, k, gv) && gv == v) ++have;
    }
    if (have >= want) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

static bool wait_gone(const std::string& k, int timeout_ms = 8000) {
  // Poll until no live node returns the key (async tombstone propagation).
  auto t0 = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - t0)
             .count() < timeout_ms) {
    bool any = false;
    for (int i = 0; i < 3; ++i) {
      std::string gv;
      if (get_key(BASE_PORT + i, k, gv)) {
        any = true;
        break;
      }
    }
    if (!any) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

static uint32_t key_owner(const std::string& k,
                          const std::vector<uint32_t>& live) {
  HashRing ring;
  ring.set_nodes(live);
  return ring.owner(k);
}

int main() {
  std::atexit(kill_all);

  pid_t pids[3];
  for (int i = 0; i < 3; ++i) pids[i] = start_node(i);
  for (int i = 0; i < 3; ++i) CHECK(wait_live(BASE_PORT + i, 3));

  // 1. Write through node 0, read through node 2 (routing/forwarding).
  for (int i = 0; i < 100; ++i)
    CHECK(set_key(BASE_PORT + 0, "k" + std::to_string(i),
                  "v" + std::to_string(i)));
  for (int i = 0; i < 100; ++i) {
    std::string v;
    CHECK(get_key(BASE_PORT + 2, "k" + std::to_string(i), v) &&
          v == "v" + std::to_string(i));
  }

  // 2. Kill -9 the owner of a key; reads must survive via replicas.
  std::string fk = "failover-key";
  CHECK(set_key(BASE_PORT + 1, fk, "important"));
  CHECK(wait_repl(fk, "important", 2));  // owner + replica, before the kill
  uint32_t owner = key_owner(fk, {0, 1, 2});
  ::kill(pids[owner], SIGKILL);
  ::waitpid(pids[owner], nullptr, 0);
  // remove from children so atexit doesn't double-wait
  children.erase(std::remove(children.begin(), children.end(), pids[owner]),
                 children.end());
  int surv[2], ns = 0;
  for (int i = 0; i < 3; ++i)
    if (i != (int)owner) surv[ns++] = i;
  int s0 = surv[0], s1 = surv[1];
  CHECK(wait_live(BASE_PORT + s0, 2));
  CHECK(wait_live(BASE_PORT + s1, 2));
  {
    std::string v;
    CHECK(get_key(BASE_PORT + s0, fk, v) && v == "important");
  }

  // 3. Writes while a node is dead, then restart it: SYNC must heal it.
  for (int i = 0; i < 20; ++i)
    CHECK(set_key(BASE_PORT + s0, "w" + std::to_string(i), "x"));
  pids[owner] = start_node(owner);
  for (int i = 0; i < 3; ++i) CHECK(wait_live(BASE_PORT + i, 3));
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // sync
  for (int i = 0; i < 20; ++i) {
    std::string v;
    CHECK(get_key(BASE_PORT + owner, "w" + std::to_string(i), v) && v == "x");
  }
  {
    std::string v;
    CHECK(get_key(BASE_PORT + owner, fk, v) && v == "important");
  }

  // 4. Delete propagates to all replicas.
  {
    Request r;
    r.op = Op::DEL;
    r.key = "k7";
    Response resp;
    CHECK(rpc_retry(BASE_PORT + s1, r, resp) && resp.status == Status::OK);
    CHECK(wait_gone("k7"));
    std::string v;
    CHECK(!get_key(BASE_PORT + owner, "k7", v));
  }

  kill_all();
  std::printf("%d checks, %d failures\n", checks, failures);
  return failures ? 1 : 0;
}
