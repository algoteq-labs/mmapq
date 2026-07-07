cmake_minimum_required(VERSION 3.16)
project(mmapq
    VERSION 0.1.0
    DESCRIPTION "Low-latency shared memory primitives in C"
    LANGUAGES C
)

# ── Standards and global flags ────────────────────────────────────────────
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_EXTENSIONS ON)   # gnu11 — needed for POSIX/Linux extensions
set(CMAKE_C_STANDARD_REQUIRED ON)

# ── Default build type ────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo
        CACHE STRING "Build type: Debug | Release | RelWithDebInfo | ASAN | UBSAN" FORCE)
endif()

# ── Compiler flags ────────────────────────────────────────────────────────
set(MMAP_COMMON_FLAGS
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter     # state machine ops take ctx they don't always use
    -fno-omit-frame-pointer   # better stack traces
)

set(MMAP_DEBUG_FLAGS   ${MMAP_COMMON_FLAGS} -O0 -g3)
set(MMAP_RELEASE_FLAGS ${MMAP_COMMON_FLAGS} -O3 -DNDEBUG -march=native)
set(MMAP_RELDBG_FLAGS  ${MMAP_COMMON_FLAGS} -O2 -g -DNDEBUG)
set(MMAP_ASAN_FLAGS    ${MMAP_DEBUG_FLAGS}  -fsanitize=address,undefined -fsanitize-recover=all)
set(MMAP_UBSAN_FLAGS   ${MMAP_DEBUG_FLAGS}  -fsanitize=undefined)

# ── Options ───────────────────────────────────────────────────────────────
option(MMAP_BUILD_TESTS    "Build test suite"         ON)
option(MMAP_ENABLE_ASAN    "Enable AddressSanitizer"  OFF)
option(MMAP_ENABLE_UBSAN   "Enable UBSanitizer"       OFF)
option(MMAP_ENABLE_TSAN    "Enable ThreadSanitizer"   OFF)

# ── Platform guard ────────────────────────────────────────────────────────
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    message(FATAL_ERROR "mmapq supports Linux and macOS only")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(-D_GNU_SOURCE)
endif()

# ── Subdirectories ────────────────────────────────────────────────────────
add_subdirectory(src)

if(MMAP_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

# ── Examples / Benchmarks ────────────────────────────────────────────────
add_executable(demo examples/demo.c)
target_link_libraries(demo PRIVATE mmapq)

add_executable(latency_bench examples/latency_bench.c)
target_link_libraries(latency_bench PRIVATE mmapq)

add_executable(echo_server examples/echo_server.c)
target_link_libraries(echo_server PRIVATE mmapq)

# ── Summary ───────────────────────────────────────────────────────────────
message(STATUS "")
message(STATUS "mmapq ${PROJECT_VERSION}")
message(STATUS "  Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "  Tests      : ${MMAP_BUILD_TESTS}")
message(STATUS "  ASAN       : ${MMAP_ENABLE_ASAN}")
message(STATUS "  UBSAN      : ${MMAP_ENABLE_UBSAN}")
message(STATUS "  TSAN       : ${MMAP_ENABLE_TSAN}")
message(STATUS "")
