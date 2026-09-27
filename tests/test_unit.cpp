// Unit tests: protocol framing, store semantics, hash ring properties.
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "../src/protocol.h"
#include "../src/ring.h"
#include "../src/store.h"

using namespace tinystore;

static int failures = 0;
static int checks = 0;
#define CHECK(cond)                                                        \
  do {                                                                     \
    ++checks;                                                              \
    if (!(cond)) {                                                         \
      ++failures;                                                          \
      std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);           \
    }                                                                      \
  } while (0)

static void test_protocol_roundtrip() {
  Request r;
  r.op = Op::SET;
  r.key = "hello";
  r.value = "world";
  r.ver = 123456789ULL;
  r.ttl_sec = 60;
  r.flags = FLAG_FORWARDED;
  auto bytes = encode_request(r);
  RequestParser p;
  auto out = p.feed(bytes.data(), bytes.size());
  CHECK(!p.error());
  CHECK(out.size() == 1);
  CHECK(out[0].op == Op::SET);
  CHECK(out[0].key == "hello");
  CHECK(out[0].value == "world");
  CHECK(out[0].ver == 123456789ULL);
  CHECK(out[0].ttl_sec == 60);
  CHECK(out[0].flags == FLAG_FORWARDED);
}

static void test_protocol_partial_and_pipelined() {
  Request a, b;
  a.op = Op::GET;
  a.key = "k1";
  b.op = Op::DEL;
  b.key = "k2";
  auto ba = encode_request(a), bb = encode_request(b);
  std::vector<char> both;
  both.insert(both.end(), ba.begin(), ba.end());
  both.insert(both.end(), bb.begin(), bb.end());
  // Feed one byte at a time; frames must still parse exactly at boundaries.
  RequestParser p;
  std::vector<Request> got;
  for (size_t i = 0; i < both.size(); ++i) {
    auto r = p.feed(&both[i], 1);
    got.insert(got.end(), r.begin(), r.end());
  }
  CHECK(!p.error());
  CHECK(got.size() == 2);
  CHECK(got[0].key == "k1" && got[0].op == Op::GET);
  CHECK(got[1].key == "k2" && got[1].op == Op::DEL);

  // Garbage magic -> error.
  RequestParser q;
  char bad[REQ_HEADER_LEN] = {0};
  q.feed(bad, sizeof(bad));
  CHECK(q.error());

  // Oversize key -> error.
  Request big;
  big.op = Op::SET;
  big.key = std::string(MAX_KEY_LEN + 1, 'x');
  auto bb2 = encode_request(big);
  RequestParser q2;
  q2.feed(bb2.data(), bb2.size());
  CHECK(q2.error());
}

static void test_response_roundtrip() {
  Response r{Status::OK, "payload", 42};
  auto b = encode_response(r);
  // Decode via a socketpair instead of exposing internals.
  int sv[2];
  CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
  CHECK(write_all(sv[0], b));
  Response out;
  CHECK(read_response(sv[1], out));
  CHECK(out.status == Status::OK);
  CHECK(out.value == "payload");
  CHECK(out.extra == 42);
  ::close(sv[0]);
  ::close(sv[1]);
}

static void test_store_basics() {
  Store s;
  std::string v;
  CHECK(!s.get("k", v));
  CHECK(s.set("k", "v1", 10, 0));
  CHECK(s.get("k", v) && v == "v1");
  // Stale version loses.
  CHECK(!s.set("k", "v2", 5, 0));
  CHECK(s.get("k", v) && v == "v1");
  CHECK(s.set("k", "v2", 11, 0));
  CHECK(s.get("k", v) && v == "v2");
  // Delete is a tombstone: get misses, stale resurrect fails.
  CHECK(s.del("k", 12));
  CHECK(!s.get("k", v));
  CHECK(!s.set("k", "v3", 11, 0));
  CHECK(s.set("k", "v3", 13, 0));
  CHECK(s.get("k", v) && v == "v3");
  CHECK(s.approx_size() == 1);
}

static void test_store_ttl() {
  Store s;
  std::string v;
  CHECK(s.set("t", "x", 1, 1));  // 1 second
  CHECK(s.get("t", v));
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  CHECK(!s.get("t", v));
  s.purge_expired();
  CHECK(s.approx_size() == 0);
}

static void test_store_concurrent() {
  Store s(64);
  const int T = 8, N = 2000;
  std::vector<std::thread> th;
  std::atomic<int> errs{0};
  for (int t = 0; t < T; ++t) {
    th.emplace_back([&, t] {
      for (int i = 0; i < N; ++i) {
        char kb[64];
        std::snprintf(kb, sizeof(kb), "t%d-k%d", t, i);
        uint64_t ver = (uint64_t)(i + 1);
        if (!s.set(kb, "v", ver, 0)) errs++;
        std::string v;
        if (!s.get(kb, v) || v != "v") errs++;
      }
    });
  }
  for (auto& t : th) t.join();
  CHECK(errs == 0);
  CHECK(s.approx_size() == (size_t)T * N);
}

static void test_ring_distribution() {
  HashRing ring;
  ring.set_nodes({0, 1, 2, 3, 4});
  std::vector<int> cnt(5, 0);
  const int K = 100000;
  for (int i = 0; i < K; ++i) cnt[ring.owner("key" + std::to_string(i))]++;
  for (int c : cnt) {
    double frac = (double)c / K;
    CHECK(frac > 0.15 && frac < 0.25);  // ~20% each, loosely
  }
  // Replica sets hold R distinct nodes including the owner.
  auto rs = ring.replica_set("somekey", 3);
  CHECK(rs.size() == 3);
  CHECK(rs[0] == ring.owner("somekey"));
  CHECK(rs[0] != rs[1] && rs[1] != rs[2] && rs[0] != rs[2]);
}

static void test_ring_stability() {
  HashRing before, after;
  before.set_nodes({0, 1, 2, 3, 4});
  after.set_nodes({0, 1, 2, 3});  // node 4 removed
  const int K = 20000;
  int moved = 0;
  for (int i = 0; i < K; ++i) {
    std::string k = "k" + std::to_string(i);
    if (before.owner(k) != after.owner(k)) ++moved;
  }
  double frac = (double)moved / K;
  // Only keys owned by node 4 (~1/5) should move.
  CHECK(frac > 0.15 && frac < 0.25);
}

int main() {
  test_protocol_roundtrip();
  test_protocol_partial_and_pipelined();
  test_response_roundtrip();
  test_store_basics();
  test_store_ttl();
  test_store_concurrent();
  test_ring_distribution();
  test_ring_stability();
  std::printf("%d checks, %d failures\n", checks, failures);
  return failures ? 1 : 0;
}
