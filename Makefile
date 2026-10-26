# mmapq Makefile
#
# Targets:
#   make            build static library (default)
#   make demo       build examples/demo
#   make test       run test suite (normal)
#   make test-asan  run test suite (AddressSanitizer)
#   make test-tsan  run test suite (ThreadSanitizer)
#   make test-multi run multiprocess tests
#   make mmapq-ids  build the operator tool tools/mmapq-ids (not part of all)
#   make mmapq-ids-test  run the tool's own unit/CLI suite (python3)
#   make clean      remove build artefacts

PREFIX    ?= /usr/local
LIBDIR    ?= $(PREFIX)/lib
INCLUDEDIR ?= $(PREFIX)/include
DATADIR   ?= $(PREFIX)/share
NATIVE    ?= 0
LTO       ?= 0

CC      = gcc
NATIVE_FLAGS = $(if $(filter 1,$(NATIVE)),-march=native,)
LTO_FLAGS    = $(if $(filter 1,$(LTO)),-flto,)
CFLAGS  = -std=gnu11 -O3 $(NATIVE_FLAGS) $(LTO_FLAGS) -DNDEBUG -Wall -Wextra \
          -Wno-unused-parameter -Wmissing-prototypes -Wstrict-prototypes \
          -Wnull-dereference -D_GNU_SOURCE -I include -I src
LDFLAGS = -pthread -lm

SRCS    = $(wildcard src/*.c)
OBJS    = $(SRCS:.c=.o)
LIB     = libmmapq.a

.PHONY: all demo bench echo_server batch_demo batch_bench batch_echo_server bench-suite batch-bench-suite interop test test-asan test-tsan test-multi mmapq-ids mmapq-ids-test install clean

all: $(LIB) demo bench echo_server batch_demo batch_bench batch_echo_server

$(LIB): $(OBJS)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

demo: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/demo.c -L. -lmmapq $(LDFLAGS) -o demo
	@echo "built: demo"

batch_demo: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/batch_demo.c -L. -lmmapq $(LDFLAGS) -o batch_demo
	@echo "built: batch_demo"

bench: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/latency_bench.c -L. -lmmapq $(LDFLAGS) -o latency_bench
	@echo "built: latency_bench"

echo_server: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/echo_server.c -L. -lmmapq $(LDFLAGS) -o echo_server
	@echo "built: echo_server"

batch_bench: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/batch_latency_bench.c -L. -lmmapq $(LDFLAGS) -o batch_latency_bench
	@echo "built: batch_latency_bench"

batch_echo_server: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/batch_echo_server.c -L. -lmmapq $(LDFLAGS) -o batch_echo_server
	@echo "built: batch_echo_server"

# DEMO-01: the end-to-end demo lives next to its driver; not part of the
# default build or test.sh (spec: audit/DEMO-01-spec.md v3).
mktsim: $(LIB)
	$(CC) $(CFLAGS) -Wno-missing-prototypes examples/mktsim/mktsim.c -L. -lmmapq $(LDFLAGS) -o examples/mktsim/mktsim
	@echo "built: examples/mktsim/mktsim"

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

# Operator tool: separate build, no link to libmmapq (see tools/mmapq-ids).
mmapq-ids:
	$(MAKE) -C tools/mmapq-ids

mmapq-ids-test:
	$(MAKE) -C tools/mmapq-ids test

print-cflags-release:
	@echo $(CFLAGS)

# Ensure scripts are executable after checkout
scripts/test.sh test/multiprocess/test_multiprocess.sh:
	chmod +x $@

install: $(LIB)
	install -d $(DESTDIR)$(INCLUDEDIR)/mmapq
	install -m 644 $(addprefix include/,$(shell cat include/PUBLIC_HEADERS)) $(DESTDIR)$(INCLUDEDIR)/mmapq/
	install -d $(DESTDIR)$(LIBDIR)
	install -m 644 $(LIB) $(DESTDIR)$(LIBDIR)/
	install -d $(DESTDIR)$(DATADIR)/doc/mmapq
	install -m 644 doc/reference/wire-format.md $(DESTDIR)$(DATADIR)/doc/mmapq/

clean:
	rm -f $(OBJS) $(LIB) demo latency_bench echo_server batch_demo batch_latency_bench batch_echo_server
	rm -rf tools/mmapq-ids/build
