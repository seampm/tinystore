// tinystore-server: one cluster node.
//
//   tinystore-server --id 0 \
//       --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
//       --repl 2
//
// The node binds to peers[id]. All nodes in the cluster share the same
// --peers list; --id selects this node's identity.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "cluster.h"

using namespace tinystore;

static std::string flag(int argc, char** argv, const char* name,
                        const std::string& dflt) {
  for (int i = 1; i + 1 < argc; ++i)
    if (std::string(argv[i]) == name) return argv[i + 1];
  return dflt;
}

static std::vector<NodeInfo> parse_peers(const std::string& s) {
  std::vector<NodeInfo> out;
  size_t pos = 0;
  uint32_t id = 0;
  while (pos <= s.size()) {
    size_t comma = s.find(',', pos);
    std::string tok =
        s.substr(pos, comma == std::string::npos ? comma : comma - pos);
    size_t colon = tok.rfind(':');
    if (colon == std::string::npos) break;
    out.push_back(NodeInfo{id++, tok.substr(0, colon),
                           std::stoi(tok.substr(colon + 1))});
    if (comma == std::string::npos) break;
    pos = comma + 1;
  }
  return out;
}

static std::atomic<bool> g_stop{false};

int main(int argc, char** argv) {
  if (flag(argc, argv, "--peers", "").empty()) {
    std::fprintf(stderr,
                 "usage: tinystore-server --id N --peers h1:p1,h2:p2,... "
                 "[--repl 2] [--hb-ms 500] [--dead-ms 2000] [--workers 8]\n");
    return 1;
  }
  uint32_t id = static_cast<uint32_t>(std::stoul(flag(argc, argv, "--id", "0")));
  auto peers = parse_peers(flag(argc, argv, "--peers", ""));
  int repl = std::stoi(flag(argc, argv, "--repl", "2"));
  int hb = std::stoi(flag(argc, argv, "--hb-ms", "500"));
  int dead = std::stoi(flag(argc, argv, "--dead-ms", "2000"));
  int workers = std::stoi(flag(argc, argv, "--workers", "8"));

  if (id >= peers.size()) {
    std::fprintf(stderr, "error: --id %u out of range for %zu peers\n", id,
                 peers.size());
    return 1;
  }

  std::signal(SIGPIPE, SIG_IGN);
  Cluster c(id, peers, repl, hb, dead);
  if (!c.start(peers[id].port, workers)) {
    std::fprintf(stderr, "error: failed to bind port %d\n", peers[id].port);
    return 1;
  }
  std::printf("tinystore node %u listening on port %d (%zu peers, repl=%d)\n",
              id, peers[id].port, peers.size(), repl);
  std::fflush(stdout);

  std::signal(SIGTERM, [](int) { g_stop = true; });
  std::signal(SIGINT, [](int) { g_stop = true; });
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(200));
  c.stop();
  return 0;
}
