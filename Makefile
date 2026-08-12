# mmapq Makefile
#
# Targets:
#   make            build static library (default)
#   make demo       build examples/demo
#   make test       run test suite (normal)
#   make test-asan  run test suite (AddressSanitizer)
#   make test-tsan  run test suite (ThreadSanitizer)
#   make test-multi run multiprocess tests
#   make clean      remove build artefacts

CC      = gcc
CFLAGS  = -std=gnu11 -O3 -march=native -flto -DNDEBUG -Wall -Wextra -Wno-unused-parameter -D_GNU_SOURCE -I include -I src
LDFLAGS = -pthread -lm

SRCS    = $(wildcard src/*.c)
OBJS    = $(SRCS:.c=.o)
LIB     = libmmapq.a

.PHONY: all demo bench echo_server batch_demo batch_bench batch_echo_server bench-suite batch-bench-suite interop test test-asan test-tsan test-multi install clean

all: $(LIB) demo bench echo_server batch_demo batch_bench batch_echo_server

$(LIB): $(OBJS)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

demo: $(LIB)
	$(CC) $(CFLAGS) examples/demo.c -L. -lmmapq $(LDFLAGS) -o demo
	@echo "built: demo"

batch_demo: $(LIB)
	$(CC) $(CFLAGS) examples/batch_demo.c -L. -lmmapq $(LDFLAGS) -o batch_demo
	@echo "built: batch_demo"

bench: $(LIB)
	$(CC) $(CFLAGS) examples/latency_bench.c -L. -lmmapq $(LDFLAGS) -o latency_bench
	@echo "built: latency_bench"

echo_server: $(LIB)
	$(CC) $(CFLAGS) examples/echo_server.c -L. -lmmapq $(LDFLAGS) -o echo_server
	@echo "built: echo_server"

batch_bench: $(LIB)
	$(CC) $(CFLAGS) examples/batch_latency_bench.c -L. -lmmapq $(LDFLAGS) -o batch_latency_bench
	@echo "built: batch_latency_bench"

batch_echo_server: $(LIB)
	$(CC) $(CFLAGS) examples/batch_echo_server.c -L. -lmmapq $(LDFLAGS) -o batch_echo_server
	@echo "built: batch_echo_server"

bench-suite: bench echo_server
	@echo "Running single-message queue benchmark suite..."
	@bash scripts/bench.sh

batch-bench-suite: batch_bench batch_echo_server
	@echo "Running batch queue benchmark suite..."
	@bash scripts/batch_bench.sh

test: $(LIB)
	@bash scripts/test.sh

test-asan:
	@bash scripts/test.sh --asan

test-tsan:
	@bash scripts/test.sh --tsan

test-multi: $(LIB)
	@bash test/multiprocess/test_multiprocess.sh

print-cflags-release:
	@echo $(CFLAGS)

# Ensure scripts are executable after checkout
scripts/test.sh test/multiprocess/test_multiprocess.sh:
	chmod +x $@

install: $(LIB)
	install -d $(DESTDIR)$(PREFIX)/include/mmapq
	install -m 644 $(addprefix include/,$(shell cat include/PUBLIC_HEADERS)) $(DESTDIR)$(PREFIX)/include/mmapq/
	install -d $(DESTDIR)$(PREFIX)/lib
	install -m 644 $(LIB) $(DESTDIR)$(PREFIX)/lib/

clean:
	rm -f $(OBJS) $(LIB) demo latency_bench echo_server batch_demo batch_latency_bench batch_echo_server
