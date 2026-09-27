#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace tinystore {

// Operations. REPL_* and SYNC are inter-node; the server accepts them from
// any connection (trusted network), which keeps the design simple.
enum class Op : uint8_t {
  GET = 1,
  SET = 2,
  DEL = 3,
  PING = 4,
  REPL_SET = 5,
  REPL_DEL = 6,
  STATS = 7,
  SYNC = 8,
};

enum class Status : uint8_t {
  OK = 0,
  NOT_FOUND = 1,
  ERROR = 2,
  MORE = 3,  // more frames follow (SYNC streaming)
};

struct Request {
  Op op = Op::PING;
  uint8_t flags = 0;  // bit 0: forwarded (do not route again)
  std::string key;
  std::string value;
  uint64_t ver = 0;      // SET/REPL_SET version stamp
  uint32_t ttl_sec = 0;  // SET time-to-live, 0 = none
};

struct Response {
  Status status = Status::OK;
  std::string value;   // payload for GET / STATS / ERROR message
  uint64_t extra = 0;  // op-specific: e.g. replicas that acked a write
};

constexpr uint8_t REQ_MAGIC = 0x54;   // 'T'
constexpr uint8_t RESP_MAGIC = 0x52;  // 'R'
constexpr size_t REQ_HEADER_LEN = 23;
constexpr size_t RESP_HEADER_LEN = 14;
constexpr size_t MAX_KEY_LEN = 256;
constexpr size_t MAX_VAL_LEN = 1 << 20;  // 1 MiB

constexpr uint8_t FLAG_FORWARDED = 0x01;

std::vector<char> encode_request(const Request& r);
std::vector<char> encode_response(const Response& r);

// Incremental parser for the request stream. Feed arbitrary byte chunks;
// complete requests are returned, the remainder is buffered.
class RequestParser {
 public:
  std::vector<Request> feed(const char* data, size_t n);
  bool error() const { return error_; }

 private:
  std::vector<char> buf_;
  bool error_ = false;
};

// Blocking helpers (used by CLI, bench, inter-node RPC).
bool write_all(int fd, const char* data, size_t n);
inline bool write_all(int fd, const std::vector<char>& v) {
  return write_all(fd, v.data(), v.size());
}
// Reads exactly one response frame. Returns false on EOF/error.
bool read_response(int fd, Response& out);

uint64_t now_micros();      // wall clock, microseconds since epoch
int64_t now_millis_mono();  // monotonic clock, milliseconds

}  // namespace tinystore
