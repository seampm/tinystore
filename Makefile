CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread
LIB_SRC = src/protocol.cpp src/store.cpp src/server.cpp src/cluster.cpp

all: build/tinystore-server build/tinystore-cli build/tinystore-bench

build:
	mkdir -p build

build/tinystore-server: build src/main.cpp $(LIB_SRC)
	$(CXX) $(CXXFLAGS) -o $@ src/main.cpp $(LIB_SRC)

build/tinystore-cli: build src/cli.cpp src/protocol.cpp
	$(CXX) $(CXXFLAGS) -o $@ src/cli.cpp src/protocol.cpp

build/tinystore-bench: build src/bench.cpp src/protocol.cpp
	$(CXX) $(CXXFLAGS) -o $@ src/bench.cpp src/protocol.cpp

build/test_unit: build tests/test_unit.cpp src/protocol.cpp src/store.cpp src/protocol.h src/store.h src/ring.h
	$(CXX) $(CXXFLAGS) -g -o $@ tests/test_unit.cpp src/protocol.cpp src/store.cpp

build/test_cluster: build tests/test_cluster.cpp $(LIB_SRC) src/server.h src/cluster.h src/protocol.h src/store.h src/ring.h
	$(CXX) $(CXXFLAGS) -g -o $@ tests/test_cluster.cpp $(LIB_SRC)

build/tinystore-server: build src/main.cpp $(LIB_SRC) src/server.h src/cluster.h src/protocol.h src/store.h src/ring.h

test: build/test_unit build/test_cluster build/tinystore-server
	./build/test_unit
	./build/test_cluster

clean:
	rm -rf build

.PHONY: all test clean
