# ── mmap-c static library ────────────────────────────────────────────────

add_library(mmap-c STATIC
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
)

target_include_directories(mmap-c
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(mmap-c
    PUBLIC pthread m
)

# Apply build-type-specific flags
target_compile_options(mmap-c PRIVATE
    $<$<CONFIG:Debug>:        ${MMAP_DEBUG_FLAGS}>
    $<$<CONFIG:Release>:      ${MMAP_RELEASE_FLAGS}>
    $<$<CONFIG:RelWithDebInfo>:${MMAP_RELDBG_FLAGS}>
)

# Sanitizer support
if(MMAP_ENABLE_ASAN)
    target_compile_options(mmap-c PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options   (mmap-c PUBLIC -fsanitize=address,undefined)
endif()

if(MMAP_ENABLE_UBSAN AND NOT MMAP_ENABLE_ASAN)
    target_compile_options(mmap-c PUBLIC -fsanitize=undefined)
    target_link_options   (mmap-c PUBLIC -fsanitize=undefined)
endif()

if(MMAP_ENABLE_TSAN)
    target_compile_options(mmap-c PUBLIC -fsanitize=thread)
    target_link_options   (mmap-c PUBLIC -fsanitize=thread)
endif()

# Install targets
install(TARGETS mmap-c
    ARCHIVE DESTINATION lib
)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
    DESTINATION include/mmap-c
)
