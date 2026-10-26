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
    -Wmissing-prototypes
    -Wstrict-prototypes
    -Wnull-dereference
    -fno-omit-frame-pointer   # better stack traces
)

set(MMAP_DEBUG_FLAGS   ${MMAP_COMMON_FLAGS} -O0 -g3)
option(MMAP_NATIVE_ARCH "Tune for the build host" OFF)
if(MMAP_NATIVE_ARCH)
    set(MMAP_RELEASE_FLAGS ${MMAP_COMMON_FLAGS} -O3 -DNDEBUG -march=native)
else()
    set(MMAP_RELEASE_FLAGS ${MMAP_COMMON_FLAGS} -O3 -DNDEBUG)
endif()
set(MMAP_RELDBG_FLAGS  ${MMAP_COMMON_FLAGS} -O2 -g -DNDEBUG)
# Sanitizer flags are set via MMAP_ENABLE_ASAN / MMAP_ENABLE_UBSAN options
# in src/CMakeLists.txt — the old MMAP_ASAN_FLAGS / MMAP_UBSAN_FLAGS
# variables were defined here but never referenced.

# ── Options ───────────────────────────────────────────────────────────────
option(MMAP_ENABLE_ASAN    "Enable AddressSanitizer"  OFF)
option(MMAP_ENABLE_UBSAN   "Enable UBSanitizer"       OFF)
option(MMAP_ENABLE_TSAN    "Enable ThreadSanitizer"   OFF)

# ── Platform guard ────────────────────────────────────────────────────────
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "mmapq supports Linux only")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(-D_GNU_SOURCE)
endif()

# ── Subdirectories ────────────────────────────────────────────────────────
# Tests are built and run exclusively by scripts/test.sh (both build
# configurations plus per-test fault-injection special-casing); there is
# no CMake test target by design — see README "Running the tests".
add_subdirectory(src)

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
message(STATUS "  ASAN       : ${MMAP_ENABLE_ASAN}")
message(STATUS "  UBSAN      : ${MMAP_ENABLE_UBSAN}")
message(STATUS "  TSAN       : ${MMAP_ENABLE_TSAN}")
message(STATUS "")
