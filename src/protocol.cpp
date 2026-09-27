#include "protocol.h"

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>

namespace tinystore {

static void put_u32be(std::vector<char>& v, uint32_t x) {
  v.push_back(static_cast<char>((x >> 24) & 0xff));
  v.push_back(static_cast<char>((x >> 16) & 0xff));
  v.push_back(static_cast<char>((x >> 8) & 0xff));
  v.push_back(static_cast<char>(x & 0xff));
}

static void put_u64be(std::vector<char>& v, uint64_t x) {
  for (int i = 7; i >= 0; --i) v.push_back(static_cast<char>((x >> (i * 8)) & 0xff));
}

static uint32_t get_u32be(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  return (uint32_t(u[0]) << 24) | (uint32_t(u[1]) << 16) | (uint32_t(u[2]) << 8) |
         uint32_t(u[3]);
}

static uint64_t get_u64be(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  uint64_t x = 0;
  for (int i = 0; i < 8; ++i) x = (x << 8) | uint64_t(u[i]);
  return x;
}

std::vector<char> encode_request(const Request& r) {
  std::vector<char> v;
  v.reserve(REQ_HEADER_LEN + r.key.size() + r.value.size());
  v.push_back(static_cast<char>(REQ_MAGIC));
  v.push_back(static_cast<char>(r.op));
  v.push_back(static_cast<char>(r.flags));
  put_u32be(v, static_cast<uint32_t>(r.key.size()));
  put_u32be(v, static_cast<uint32_t>(r.value.size()));
  put_u64be(v, r.ver);
  put_u32be(v, r.ttl_sec);
  v.insert(v.end(), r.key.begin(), r.key.end());
  v.insert(v.end(), r.value.begin(), r.value.end());
  return v;
}

std::vector<char> encode_response(const Response& r) {
  std::vector<char> v;
  v.reserve(RESP_HEADER_LEN + r.value.size());
  v.push_back(static_cast<char>(RESP_MAGIC));
  v.push_back(static_cast<char>(r.status));
  put_u32be(v, static_cast<uint32_t>(r.value.size()));
  put_u64be(v, r.extra);
  v.insert(v.end(), r.value.begin(), r.value.end());
  return v;
}

static bool valid_op(uint8_t b) {
  return b >= 1 && b <= 8;
}

std::vector<Request> RequestParser::feed(const char* data, size_t n) {
  std::vector<Request> out;
  if (error_) return out;
  buf_.insert(buf_.end(), data, data + n);
  for (;;) {
    if (buf_.size() < REQ_HEADER_LEN) return out;
    const char* h = buf_.data();
    if (static_cast<uint8_t>(h[0]) != REQ_MAGIC || !valid_op(static_cast<uint8_t>(h[1]))) {
      error_ = true;
      return out;
    }
    uint32_t klen = get_u32be(h + 3);
    uint32_t vlen = get_u32be(h + 7);
    if (klen > MAX_KEY_LEN || vlen > MAX_VAL_LEN) {
      error_ = true;
      return out;
    }
    size_t total = REQ_HEADER_LEN + klen + vlen;
    if (buf_.size() < total) return out;
    Request r;
    r.op = static_cast<Op>(static_cast<uint8_t>(h[1]));
    r.flags = static_cast<uint8_t>(h[2]);
    r.ver = get_u64be(h + 11);
    r.ttl_sec = get_u32be(h + 19);
    r.key.assign(h + REQ_HEADER_LEN, klen);
    r.value.assign(h + REQ_HEADER_LEN + klen, vlen);
    out.push_back(std::move(r));
    buf_.erase(buf_.begin(), buf_.begin() + total);
  }
}

bool write_all(int fd, const char* data, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::send(fd, data + off, n - off, MSG_NOSIGNAL);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (w == 0) return false;
    off += static_cast<size_t>(w);
  }
  return true;
}

static bool read_all(int fd, char* data, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t r = ::recv(fd, data + off, n - off, 0);
    if (r < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (r == 0) return false;  // EOF
    off += static_cast<size_t>(r);
  }
  return true;
}

bool read_response(int fd, Response& out) {
  char h[RESP_HEADER_LEN];
  if (!read_all(fd, h, RESP_HEADER_LEN)) return false;
  if (static_cast<uint8_t>(h[0]) != RESP_MAGIC) return false;
  uint32_t vlen = get_u32be(h + 2);
  if (vlen > MAX_VAL_LEN + 1024) return false;
  out.status = static_cast<Status>(static_cast<uint8_t>(h[1]));
  out.extra = get_u64be(h + 6);
  out.value.resize(vlen);
  if (vlen && !read_all(fd, out.value.data(), vlen)) return false;
  return true;
}

uint64_t now_micros() {
  using namespace std::chrono;
  return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

int64_t now_millis_mono() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

}  // namespace tinystore
