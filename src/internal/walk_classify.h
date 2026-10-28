#ifndef WALK_CLASSIFY_H
#define WALK_CLASSIFY_H

#include <stdint.h>

/**
 * CAND-58 (spec v2.2 §4.2): the one shared classification rule for every
 * result that ends a writer walk/scan now that all such walks map read-only
 * and size-checked (the walk accessor, §4.1). A missing file is ambiguous —
 * never created or already reclaimed; the RO mapper never creates
 * (file_mapper_single_ro.c: stat + O_RDONLY only), so a map miss reaches the
 * walk as FILE_NOT_FOUND / BEYOND_FILE_SIZE (or a zero/unwritten header read
 * on the header side) and the site's classification resolves it:
 *
 *   position <  the re-read floor  → OVERTAKEN: the walk's snapshot is stale
 *                                    (the archive line / start_index moved
 *                                    past the probe); restart the walk inside
 *                                    the library from fresh state (§4.4) —
 *                                    never surfaced to the caller.
 *   position >= floor, past everything known written → EOS (the CAND-51e
 *                                    semantics, preserved exactly).
 *   position >= floor at a known-written position  → ECORRUPT.
 *
 * The floor is re-read AFTER the failure (the syscall pair unlink → failed
 * open orders the archiver's floor store before the walker's load across
 * processes, §5): the archive line (appender_meta_archive_offset_sc, seq_cst)
 * for payload, queue_meta_start_index (acquire) for headers — never
 * start_offset, which a commit min can lower (L(iv)).
 *
 * Soundness: reclaim deletes only below a bound <= the line (header files
 * only wholly below a published start_index), so a probe at position >= the
 * re-read floor cannot have been library-reclaimed — which is why
 * ">= floor" is EOS-or-ECORRUPT, never a silent retry.
 */

/**
 * MMAPQI_WALK_MAP_MISS — internal walk outcome, never an MMAPQ_* code and
 * never surfaced past the walk's caller: the probe's wrap missed the file
 * (not found / beyond size). The walk leaves the failing position in
 * probing_offset. The site's classification (mmapqi_walk_classify) decides
 * EOS vs restart vs corruption. INT32_MIN collides with no MMAPQ_* code and
 * with no valid message length (<= region_size).
 */
#define MMAPQI_WALK_MAP_MISS  ((int32_t)INT32_MIN)

typedef enum {
    MMAPQI_WALK_EOS       = 0,   /* past everything known written        */
    MMAPQI_WALK_OVERTAKEN = 1,   /* below the re-read floor — restart    */
    MMAPQI_WALK_CORRUPT   = 2    /* at/above the floor, known-written    */
} MmapqiWalkClass;

/**
 * mmapqi_walk_classify — classify a walk-ending failure.
 *
 * position:          the probed position (payload offset or header index)
 * known_written_end: positions strictly below it were written (header
 *                    evidence or the frozen end of stream); INT64_MIN when
 *                    nothing is known written (the header scan: a NULL read
 *                    at/above the floor is simply the end of the headers)
 * floor:             re-reads the site's monotone floor AFTER the failure
 * floor_ctx:         context for floor
 * floor_out:         receives the re-read floor (the restart's record for
 *                    the no-progress detector); may be NULL
 */
MmapqiWalkClass mmapqi_walk_classify(
    int64_t  position,
    int64_t  known_written_end,
    int64_t (*floor)(void *ctx),
    void    *floor_ctx,
    int64_t *floor_out);

/**
 * mmapqi_walk_restart_permitted — the restart guard (v2.2): termination is
 * by progress, not by a bare cap. Every restart begins at or above the floor
 * that classified the failure (§4.4), the floors are monotone, and the walk's
 * range is bounded, so restarts terminate. As defence: each restart records
 * the floor that classified it (per floor DOMAIN — the header side's
 * start_index and the payload side's line are different units and must not
 * be compared), and a failure whose classifying floor is <= the previous one
 * in the same domain is a bug: MMAPQ_ECORRUPT with a distinct log line.
 * Legitimate repeated overtakes under heavy archiving never trip this. The
 * absolute cap (1024) is a last resort — same ECORRUPT, a different log line.
 *
 * classified_floor:   the floor that classified the current failure
 * domain_last_floor:  the domain's recorded floor (initialise to INT64_MIN)
 * restart_count:      the operation's restart count (initialise to 0)
 *
 * Returns 0 to proceed with the restart, MMAPQ_ECORRUPT to fail.
 */
#define MMAPQI_WALK_RESTART_CAP 1024

int mmapqi_walk_restart_permitted(int64_t  classified_floor,
                                  int64_t *domain_last_floor,
                                  int     *restart_count);

/**
 * MMAPQI_WALK_RESTART — the internal restart signal: a walk/scan helper
 * returns it up to its restart loop when a failure classifies OVERTAKEN (the
 * classifying floor is carried on the iterator's or appender's
 * walk_ovt_floor). Distinct from every MMAPQ_* code and from each helper's
 * success/exhaustion values (0/1 are taken by the existing protocols);
 * never leaves the library.
 */
#define MMAPQI_WALK_RESTART 2

/* The no-progress detector's floor domains — the header side's start_index
 * and the payload side's archive line are different units and must never be
 * compared, so each restart loop keeps one slot per domain. */
#define MMAPQI_WALK_DOM_HEADER   0   /* the queue start_index */
#define MMAPQI_WALK_DOM_PAYLOAD  1   /* the lane's archive line */

#endif /* WALK_CLASSIFY_H */
