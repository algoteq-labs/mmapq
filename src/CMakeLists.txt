# ── mmapq static library ────────────────────────────────────────────────

add_library(mmapq STATIC
    file_initialiser.c
    file_mapper_single_ro.c
    file_mapper_single_rw.c
    file_mapper_rolled.c
    region_sync.c
    region_async.c
    region_ring.c
    region_ring_accessor.c
    fixed_size_region.c
    runtime.c
    platform.c
    # Queue layer
    queue_meta.c
    appender_meta.c
    appender_id_pool.c
    queue_region_accessor.c
    header_reader.c
    payload_walk.c
    walk_classify.c
    payload_iterator.c
    committed_ranges.c
    offset_set.c
    uncommitted_payload_iterator.c
    indexed_appender.c
    indexed_poller.c
    indexed_reader.c
    indexed_queue.c
    mmapq_strerror.c
    queue_latency_probe.c
    # Archiver layer
    queue_files.c
    indexed_archiver.c
    # Batch queue layer
    batch_payload_iterator.c
    batch_queue_latency_probe.c
    uncommitted_batch_iterator.c
    indexed_batch_appender.c
    indexed_batch_poller.c
    indexed_batch_reader.c
    indexed_batch_archiver.c
    indexed_batch_queue.c
    queue_transfer.c
    batch_queue_transfer.c
)

target_include_directories(mmapq
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        # Internal headers live under src/ and are referenced as
        # internal/<name>.h; needed at build time (sources, examples, tests)
        # but NOT exported to consumers (src/ is not installed).
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
)

target_link_libraries(mmapq
    PUBLIC pthread m
)

# _GNU_SOURCE is required to build mmapq (Phase 12i); propagate it to
# consumers so find_package(mmapq) links can include mmap_platform.h.
target_compile_definitions(mmapq PUBLIC _GNU_SOURCE)

# Apply build-type-specific flags
target_compile_options(mmapq PRIVATE
    $<$<CONFIG:Debug>:        ${MMAP_DEBUG_FLAGS}>
    $<$<CONFIG:Release>:      ${MMAP_RELEASE_FLAGS}>
    $<$<CONFIG:RelWithDebInfo>:${MMAP_RELDBG_FLAGS}>
)

# Sanitizer support
if(MMAP_ENABLE_ASAN)
    target_compile_options(mmapq PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options   (mmapq PUBLIC -fsanitize=address,undefined)
endif()

if(MMAP_ENABLE_UBSAN AND NOT MMAP_ENABLE_ASAN)
    target_compile_options(mmapq PUBLIC -fsanitize=undefined)
    target_link_options   (mmapq PUBLIC -fsanitize=undefined)
endif()

if(MMAP_ENABLE_TSAN)
    target_compile_options(mmapq PUBLIC -fsanitize=thread)
    target_link_options   (mmapq PUBLIC -fsanitize=thread)
endif()

# Install targets
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS mmapq
    EXPORT mmapqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mmapq
    FILES_MATCHING PATTERN "*.h"
)
install(FILES ${PROJECT_SOURCE_DIR}/doc/reference/wire-format.md
    DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/mmapq
)

# pkg-config
configure_file(${PROJECT_SOURCE_DIR}/mmapq.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/mmapq.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mmapq.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

# CMake package config so downstream projects can find_package(mmapq)
configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/cmake/mmapqConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/mmapqConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mmapq
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/mmapqConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(EXPORT mmapqTargets
    FILE mmapqTargets.cmake
    NAMESPACE mmapq::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mmapq
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/mmapqConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/mmapqConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/mmapq
)
