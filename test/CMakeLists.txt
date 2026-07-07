# ── Unity test framework ──────────────────────────────────────────────────
add_library(unity STATIC unity.c)
target_include_directories(unity PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# ── Test helper macro ─────────────────────────────────────────────────────
# Each test file becomes its own executable + CTest entry.
# ASAN/UBSAN flags are inherited from the mmapq library target.
macro(mmap_test name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} PRIVATE mmapq unity)
    target_compile_options(${name} PRIVATE
        $<$<CONFIG:Debug>:        ${MMAP_DEBUG_FLAGS}>
        $<$<CONFIG:Release>:      ${MMAP_RELEASE_FLAGS}>
        $<$<CONFIG:RelWithDebInfo>:${MMAP_RELDBG_FLAGS}>
    )
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES
        TIMEOUT 30
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
endmacro()

# ── Test executables ──────────────────────────────────────────────────────
mmap_test(test_file_initialiser)
mmap_test(test_file_mapper_single_ro)
mmap_test(test_file_mapper_single_rw)
mmap_test(test_file_mapper_rolled)
mmap_test(test_region_sync)
mmap_test(test_region_async)
mmap_test(test_region_ring)
mmap_test(test_region_ring_accessor)
mmap_test(test_fixed_size_region)
mmap_test(test_runtime)
# Queue layer
mmap_test(test_header_codec)
mmap_test(test_appender_id_pool)
mmap_test(test_queue_meta)
mmap_test(test_payload_walk)
mmap_test(test_indexed_queue)
mmap_test(test_queue_transfer)
