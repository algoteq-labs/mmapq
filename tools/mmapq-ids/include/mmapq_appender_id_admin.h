#ifndef MMAPQ_APPENDER_ID_ADMIN_H
#define MMAPQ_APPENDER_ID_ADMIN_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MMAPQ_APPENDER_ID_ADMIN_COUNT 256

typedef struct {
    int taken;
    int has_meta;
    uint64_t payload_files;
    uint64_t payload_bytes;
    int has_mtime;
    time_t latest_mtime;
} MmapqAppenderIdFacts;

/*
 * Inspect an existing 64-byte appender pool and associated lane files.
 * Works independently of the queue meta file, for normal and batch queues.
 * Results are observations, not a coherent snapshot or proof of ownership.
 * A missing/old mtime is not evidence that a writer is dead.
 *
 * directory: existing base directory
 * queue:     one directory component; not empty, ".", "..", or a path
 * facts:     output array of MMAPQ_APPENDER_ID_ADMIN_COUNT entries
 * Returns: 0 on success, otherwise a positive errno value. EINVAL indicates
 *          invalid arguments or malformed files. Output is zeroed on error.
 *          Files disappearing during inspection cause an error, not a partial
 *          successful report. Canonical lane names must refer to regular files;
 *          symlinks are refused. No files are created or changed.
 */
int mmapq_appender_id_admin_inspect(const char *directory, const char *queue,
                                  MmapqAppenderIdFacts *facts);

/*
 * Clear one acquired bit using CAS, preserving every other bit and byte.
 * This does NOT verify ownership, process liveness, or whether an ID is orphaned.
 * The operator must ensure no appender uses the ID and prevent acquisition or
 * restart during reclamation. Prefer stopped producers with restarts disabled.
 * Clearing an active ID can allow two writers into one lane and corrupt it.
 * CAS protects neighboring bits; it does not establish this precondition.
 * Do not truncate, replace, or modify the pool's layout during either operation.
 *
 * directory: existing base directory
 * queue:     existing queue directory component, as for inspect
 * id:        appender ID in [0, 255]
 * released:  required output, initially set to 0; becomes 1 immediately after
 *            a successful CAS and stays 1 even if writeback or cleanup fails
 * Returns: 0 on success (including an already-free ID), positive errno on error.
 *          A changed bit is immediately visible through MAP_SHARED. Success
 *          after a change also requires msync(MS_SYNC); this is not a general
 *          queue durability guarantee. On error, inspect released before taking
 *          further action: do not blindly retry a release that already changed
 *          the bit. An already-free bit causes no writeback.
 *
 * All operations are independent and use no global mutable state. Neither
 * operation links to mmapq or changes its on-disk format.
 */
int mmapq_appender_id_admin_release(const char *directory, const char *queue,
                                  unsigned int id, int *released);

#ifdef __cplusplus
}
#endif
#endif
