CC ?= cc
AR ?= ar
PYTHON ?= python3
MMAPQ_ROOT ?= ../..
BUILD ?= build/release
CFLAGS ?= -O3 -DNDEBUG
# The tool includes mmapq's internal/appender_id_pool.h for the pool layout
# (compile-time constants only — no mmapq object code is linked).
CPPFLAGS += -D_GNU_SOURCE -Iinclude -I$(MMAPQ_ROOT)/include -I$(MMAPQ_ROOT)/src
WARN = -std=c11 -Wall -Wextra
MODULE = src/mmapq_appender_id_admin.c
HEADER = include/mmapq_appender_id_admin.h
WRAPS = -Wl,--wrap=msync,--wrap=mmap,--wrap=fstat,--wrap=fstatat,--wrap=readdir,--wrap=fdopendir,--wrap=munmap,--wrap=close

.PHONY: all test check clean
all: $(BUILD)/mmapq-ids $(BUILD)/libmmapq_appender_id_admin.a $(BUILD)/libmmapq_appender_id_admin.so

$(BUILD):
	mkdir -p $@

$(BUILD)/admin.o: $(MODULE) $(HEADER) Makefile | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARN) -fPIC -c $< -o $@

$(BUILD)/libmmapq_appender_id_admin.a: $(BUILD)/admin.o
	$(AR) rcs $@ $<

$(BUILD)/libmmapq_appender_id_admin.so: $(BUILD)/admin.o
	$(CC) $(CFLAGS) $(LDFLAGS) -shared $< -o $@

$(BUILD)/mmapq-ids: src/mmapq_ids.c $(BUILD)/libmmapq_appender_id_admin.a $(HEADER) Makefile
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARN) $< $(BUILD)/libmmapq_appender_id_admin.a $(LDFLAGS) -o $@

$(BUILD)/test_admin.so: $(MODULE) $(HEADER) tests/faults.c Makefile | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARN) -shared -fPIC $(MODULE) tests/faults.c $(WRAPS) $(LDFLAGS) -o $@

check: all $(BUILD)/test_admin.so
	ADMIN_BUILD=$(abspath $(BUILD)) $(PYTHON) -m unittest discover -s tests -p test_admin.py -v

test:
	$(MAKE) -B BUILD=build/test-debug CFLAGS='-O0 -g -Werror' check
	$(MAKE) -B BUILD=build/test-release CFLAGS='-O3 -DNDEBUG -Werror' check

clean:
	rm -rf build
