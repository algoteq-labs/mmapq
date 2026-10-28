#include "internal/walk_classify.h"
#include "internal/test_hooks.h"
#include "mmap_platform.h"

#include <stdio.h>

/* ── The shared classification rule (CAND-58 spec v2.2 §4.2) ─────────────── */

MmapqiWalkClass mmapqi_walk_classify(
    int64_t  position,
    int64_t  known_written_end,
    int64_t (*floor)(void *ctx),
    void    *floor_ctx,
    int64_t *floor_out)
{
    /* Re-read the monotone floor AFTER the failure — the cross-process
     * ordering argument (spec §5) requires the load to follow the failed
     * open. */
    int64_t f = floor(floor_ctx);
    if (floor_out != NULL) *floor_out = f;

    if (position < f)                 return MMAPQI_WALK_OVERTAKEN;
    if (position < known_written_end) return MMAPQI_WALK_CORRUPT;
    return MMAPQI_WALK_EOS;
}

/* ── The restart guard (v2.2: no-progress detector + last-resort cap) ─────── */

int mmapqi_walk_restart_permitted(int64_t  classified_floor,
                                  int64_t *domain_last_floor,
                                  int     *restart_count)
{
    if (classified_floor <= *domain_last_floor) {
        /* No progress: a restart re-failed at a floor that never advanced —
         * the restart-floor rule is broken or the file vanished without the
         * floor moving and the clamp was defeated. A bug either way. */
        fprintf(stderr,
                "mmapq: CAND-58 walk restart made no progress "
                "(classifying floor %ld <= previous %ld) — treating as corruption\n",
                (long)classified_floor, (long)*domain_last_floor);
        return MMAPQ_ECORRUPT;
    }
    if (*restart_count >= MMAPQI_WALK_RESTART_CAP) {
        fprintf(stderr,
                "mmapq: CAND-58 walk restart cap (%d) exceeded "
                "after %d restarts — treating as corruption\n",
                MMAPQI_WALK_RESTART_CAP, *restart_count);
        return MMAPQ_ECORRUPT;
    }
    *domain_last_floor = classified_floor;
    (*restart_count)++;
    C58_WALK_RESTART_NOTE();
    return 0;
}

/* ── CAND-58 test hooks (never in release: MMAPQ_TEST_HOOKS only) ────────── */

#ifdef MMAPQ_TEST_HOOKS

#include <stddef.h>
#include <stdatomic.h>

static void (*scan_span_pre_walk_fn)(int64_t, int64_t, int64_t);
static void (*scan_header_pre_read_fn)(int64_t);
static void (*use_phase_step_fn)(int64_t);
static void (*join_walk_step_fn)(int64_t);

void mmapq_test_hook_set_uncommitted_scan_span_pre_walk(
    void (*fn)(int64_t, int64_t, int64_t)) { scan_span_pre_walk_fn = fn; }
void mmapq_test_hook_set_uncommitted_scan_header_pre_read(
    void (*fn)(int64_t)) { scan_header_pre_read_fn = fn; }
void mmapq_test_hook_set_uncommitted_use_phase_step(
    void (*fn)(int64_t)) { use_phase_step_fn = fn; }
void mmapq_test_hook_set_join_walk_step(
    void (*fn)(int64_t)) { join_walk_step_fn = fn; }

void mmapqi_c58_hook_scan_span_pre_walk(int64_t off, int64_t ss, int64_t line)
{
    if (scan_span_pre_walk_fn != NULL) scan_span_pre_walk_fn(off, ss, line);
}
void mmapqi_c58_hook_scan_header_pre_read(int64_t idx)
{
    if (scan_header_pre_read_fn != NULL) scan_header_pre_read_fn(idx);
}
void mmapqi_c58_hook_use_phase_step(int64_t off)
{
    if (use_phase_step_fn != NULL) use_phase_step_fn(off);
}
void mmapqi_c58_hook_join_walk_step(int64_t off)
{
    if (join_walk_step_fn != NULL) join_walk_step_fn(off);
}

/* The retry pins' observability: a process-global count of walk restarts.
 * Serial test binaries read the delta around a pin's walk. */
static _Atomic int walk_restart_count;
int mmapq_test_hook_walk_restart_count(void)
{
    return atomic_load_explicit(&walk_restart_count, memory_order_acquire);
}
void mmapqi_c58_walk_restart_note(void)
{
    atomic_fetch_add_explicit(&walk_restart_count, 1, memory_order_acq_rel);
}

#endif /* MMAPQ_TEST_HOOKS */
