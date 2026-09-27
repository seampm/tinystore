// tinystore-cli: blocking client for a tinystore cluster.
//
//   tinystore-cli --node 127.0.0.1:7000 set mykey myvalue
//   tinystore-cli --node 127.0.0.1:7000 get mykey
//   tinystore-cli --node 127.0.0.1:7000 del mykey
//   tinystore-cli --node 127.0.0.1:7000 stats
//   tinystore-cli --node 127.0.0.1:7000 ping
//
// Any node accepts any key; requests are routed to the owning node.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "protocol.h"

using namespace tinystore;

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
  int flag = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  return fd;
}

int main(int argc, char** argv) {
  std::string node = "127.0.0.1:7000";
  for (int i = 1; i + 1 < argc; ++i)
    if (std::string(argv[i]) == "--node") node = argv[i + 1];

  int cmd_idx = -1;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "set" || a == "get" || a == "del" || a == "stats" || a == "ping") {
      cmd_idx = i;
      break;
    }
  }
  if (cmd_idx < 0) {
    std::fprintf(stderr,
                 "usage: tinystore-cli [--node host:port] "
                 "set <k> <v> [ttl] | get <k> | del <k> | stats | ping\n");
    return 1;
  }

  size_t colon = node.rfind(':');
  std::string host = node.substr(0, colon);
  int port = std::stoi(node.substr(colon + 1));
  int fd = dial(host, port);
  if (fd < 0) {
    std::fprintf(stderr, "error: cannot connect to %s\n", node.c_str());
    return 1;
  }

  Request req;
  std::string cmd = argv[cmd_idx];
  if (cmd == "set") {
    if (cmd_idx + 2 >= argc) {
      std::fprintf(stderr, "usage: set <key> <value> [ttl_sec]\n");
      return 1;
    }
    req.op = Op::SET;
    req.key = argv[cmd_idx + 1];
    req.value = argv[cmd_idx + 2];
    if (cmd_idx + 3 < argc) req.ttl_sec = std::stoul(argv[cmd_idx + 3]);
  } else if (cmd == "get") {
    req.op = Op::GET;
    req.key = argv[cmd_idx + 1];
  } else if (cmd == "del") {
    req.op = Op::DEL;
    req.key = argv[cmd_idx + 1];
  } else if (cmd == "stats") {
    req.op = Op::STATS;
  } else {
    req.op = Op::PING;
  }

  if (!write_all(fd, encode_request(req))) {
    std::fprintf(stderr, "error: write failed\n");
    return 1;
  }
  Response resp;
  if (!read_response(fd, resp)) {
    std::fprintf(stderr, "error: no response\n");
    return 1;
  }
  ::close(fd);

  switch (resp.status) {
    case Status::OK:
      if (cmd == "set")
        std::printf("OK (replicating to %llu nodes)\n",
                    (unsigned long long)resp.extra + 1);
      else
        std::printf("%s\n", resp.value.c_str());
      return 0;
    case Status::NOT_FOUND:
      std::printf("(nil)\n");
      return 0;
    case Status::ERROR:
      std::fprintf(stderr, "error: %s\n", resp.value.c_str());
      return 1;
    default:
      std::fprintf(stderr, "error: unexpected response\n");
      return 1;
  }
}
