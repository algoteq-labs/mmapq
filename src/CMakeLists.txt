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
    # Queue layer
    queue_meta.c
    appender_meta.c
    appender_id_pool.c
    queue_region_accessor.c
    header_reader.c
    payload_walk.c
    payload_iterator.c
    uncommitted_payload_iterator.c
    indexed_appender.c
    indexed_poller.c
    indexed_reader.c
    indexed_queue.c
    queue_latency_probe.c
    # Archiver layer
    queue_files.c
    indexed_archiver.c
    # Batch queue layer
    batch_payload_iterator.c
    batch_queue_latency_probe.c
    indexed_batch_appender.c
    indexed_batch_poller.c
    indexed_batch_reader.c
    indexed_batch_archiver.c
    indexed_batch_queue.c
    queue_transfer.c
    batch_queue_transfer.c
)

target_include_directories(mmapq
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(mmapq
    PUBLIC pthread m
)

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
install(TARGETS mmapq
    ARCHIVE DESTINATION lib
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION include/mmapq
)
install(FILES ${PROJECT_SOURCE_DIR}/doc/wire-format.md
    DESTINATION share/doc/mmapq
)
