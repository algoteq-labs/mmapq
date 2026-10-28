#ifndef TEST_HOOKS_H
#define TEST_HOOKS_H

#include <stdint.h>

/**
 * Test-only hooks (CAND-30 pins P9/P15). Compiled only with
 * -DMMAPQ_TEST_HOOKS — never in release builds or the shipped library
 * (acceptance verifies with nm that no hook symbol is in the release
 * archive). Each setter installs a process-global callback; NULL disarms.
 *
 * commit_pre: invoked in mmapq_indexed_appender_commit's slow path after
 *   the start_offset pre-check passes and before the header CAS — lets a
 *   pin hold a commit there while an archive runs (P9).
 * commit_min_pre: invoked in the slow path's re-check lowering branch,
 *   immediately before appender_meta_start_offset_min — lets a pin hold
 *   the commit between the archiver's snapshot and its publish (CAND-37).
 * commit_post_cas: invoked in the slow path immediately after the header
 *   CAS and before the seq_cst fence + re-check — the CAND-35 crash
 *   point; lets a pin kill -9 the process at the literal window.
 * batch_commit_pre_publish: invoked in commit_batch after the §3.5
 *   overtaken pre-check passes and before the publish CAS — lets a pin
 *   park a batch commit there while an archive runs (CAND-51b). Parking
 *   must be here, not post-CAS: post-CAS the archiver's live pass already
 *   sees the header, and the pin would be vacuous.
 * archiver_live_pass / batch twin: invoked on each iteration of the
 *   archiver's live-header pass; a nonzero return arms the wrap-level
 *   injection for that read (P15).
 * archiver_publish_pre: invoked once per archive_forward_variable call
 *   immediately before the publish loop — lets a pin park the archiver
 *   between its start_offset snapshot and the publish (CAND-37).
 * reclaim_pre_unlink / batch twin: invoked once per reclaim_files call
 *   immediately before the unlink loop, while archive.lock SH is held —
 *   lets a pin park a reclaim mid-hold (CAND-36).
 * archiver_listing_scan / batch twin: invoked on each iteration of the
 *   listing's live-header scan (CAND-35); a nonzero return arms the
 *   wrap-level injection for that read — same discipline as
 *   archiver_live_pass.
 * header_wrap_inject: substitutes the wrap result inside both
 *   mmapqi_header_read and mmapqi_header_read_checked while armed — so an
 *   injected timeout collapses to end-of-stream through the plain read but
 *   reports MMAPQ_ETIMEDOUT through the checked read (this is what makes
 *   the plain-read mutation observable).
 * set_page_size: overrides the runtime page size seen by the CAND-38
 *   geometry checks (region_size % page_size == 0) — the only way to pin
 *   16/64 KB-page host behaviour on a 4 KB-page host (CAND-38 P4).
 *   0 disarms.
 * set_appender_walk_result: arms ONE appender-join walk site (D8) —
 *   0 = walk_payload_to_end, 1 = the committed-entry walk, 2 = the tail
 *   walk; while armed, that site behaves as if its walk returned rc.
 *   -1 disarms. appender_walk_fired returns how many times the armed site
 *   fired since the last setter call — the liveness assertion against
 *   vacuous pin geometries.
 * batch_recover_walk_step: invoked in recover_open_batch's walk loop
 *   after each walked message (argument = count walked so far) — lets a
 *   pin kill -9 the process mid-join-completion, before the count store
 *   (CAND-51e P8: crash-during-recovery; also what makes the early
 *   count-store mutation observable).
 * batch_commit_post_cas / batch_commit_min_pre (CAND-54): the
 *   commit_post_cas / commit_min_pre pins' batch twins — invoked in
 *   commit_batch immediately after the publish CAS and immediately before
 *   the re-check's lowering min. They exist so the batch hole pins can
 *   drive the same CAS→archive→min interleaving as the single-message
 *   pins (pre-CAND-54 parking pre-publish was the only option, which
 *   leaves the file's deletion blocked by the parked commit's intent
 *   post-fix and cannot reach the hole).
 * archiver_listing_pre_intent / batch twin (CAND-54): invoked in
 *   archivable_files_locked per lane, between the start_offset load and
 *   the intent load — lets the hole pins drive the model's intra-lister
 *   interleaving (re-list reads start_offset before the commit's min and
 *   the intent after its clear), which is the only way the archived file
 *   is legitimately deletable while a commit min is pending.
 */
#ifdef MMAPQ_TEST_HOOKS
void mmapq_test_hook_set_commit_pre(void (*fn)(void));
void mmapq_test_hook_set_commit_min_pre(void (*fn)(void));
void mmapq_test_hook_set_commit_post_cas(void (*fn)(void));
void mmapq_test_hook_set_batch_commit_pre_publish(void (*fn)(void));
void mmapq_test_hook_set_batch_commit_post_cas(void (*fn)(void));
void mmapq_test_hook_set_batch_commit_min_pre(void (*fn)(void));
void mmapq_test_hook_set_batch_recover_walk_step(void (*fn)(int walked));
void mmapq_test_hook_set_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_set_batch_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_set_archiver_publish_pre(void (*fn)(void));
void mmapq_test_hook_set_reclaim_pre_unlink(void (*fn)(void));
void mmapq_test_hook_set_batch_reclaim_pre_unlink(void (*fn)(void));
void mmapq_test_hook_set_archiver_listing_scan(int (*fn)(void));
void mmapq_test_hook_set_batch_archiver_listing_scan(int (*fn)(void));
void mmapq_test_hook_set_archiver_listing_pre_intent(void (*fn)(void));
void mmapq_test_hook_set_batch_archiver_listing_pre_intent(void (*fn)(void));
void mmapq_test_hook_header_wrap_inject(int wrap_result);
void mmapq_test_hook_set_page_size(long page_size);
void mmapq_test_hook_set_appender_walk_result(int site, int32_t rc);
int  mmapq_test_hook_appender_walk_fired(void);

/* CAND-58 walk pins (audit/CAND-58-spec.md v2.2 §6). The four setters
 * install process-global callbacks (NULL disarms); the counter getter backs
 * the retry pins' restart-count assertions. State lives in walk_classify.c;
 * the fire entry points are shared across the single-message and batch call
 * sites (four TUs).
 *
 * uncommitted_scan_span_pre_walk (P1a/P1b/P2): invoked in the uncommitted
 *   factory's committed-span processing — single-message: both span sites in
 *   scan_committed_ranges, AFTER the span's inner init and BEFORE the advance
 *   (the init↔advance seam); batch: before the span measurement — with
 *   (span_start, stream_start, archive_line).
 * uncommitted_scan_header_pre_read (P6): invoked in the factory's header
 *   scan loop, immediately before the checked header read, with the index.
 * uncommitted_use_phase_step (P5): invoked at the top of each use-phase
 *   advance/next call, with the probing offset (batch: the gap position).
 * join_walk_step (P4/P4b): invoked at each join payload-walk step —
 *   walk_payload_to_end's loop, the step-3 first and tail walks, and the
 *   batch twins (skip_to_end_of_stream's loop, recover_open_batch's walk
 *   loop) — with the probing offset.
 * walk_restart_count: the process-global walk-restart counter (the retry
 *   pins assert a >= 1 delta; a vacuously-green first pass fails). */
void mmapq_test_hook_set_uncommitted_scan_span_pre_walk(
    void (*fn)(int64_t header_offset, int64_t stream_start, int64_t archive_line));
void mmapq_test_hook_set_uncommitted_scan_header_pre_read(
    void (*fn)(int64_t header_index));
void mmapq_test_hook_set_uncommitted_use_phase_step(
    void (*fn)(int64_t next_probing_offset));
void mmapq_test_hook_set_join_walk_step(void (*fn)(int64_t probing_offset));
int  mmapq_test_hook_walk_restart_count(void);
void mmapqi_c58_hook_scan_span_pre_walk(int64_t, int64_t, int64_t);
void mmapqi_c58_hook_scan_header_pre_read(int64_t);
void mmapqi_c58_hook_use_phase_step(int64_t);
void mmapqi_c58_hook_join_walk_step(int64_t);
void mmapqi_c58_walk_restart_note(void);
#define C58_HOOK_SCAN_SPAN_PRE_WALK(off, ss, line) \
    mmapqi_c58_hook_scan_span_pre_walk((off), (ss), (line))
#define C58_HOOK_SCAN_HEADER_PRE_READ(idx) \
    mmapqi_c58_hook_scan_header_pre_read((idx))
#define C58_HOOK_USE_PHASE_STEP(off) \
    mmapqi_c58_hook_use_phase_step((off))
#define C58_HOOK_JOIN_WALK_STEP(off) \
    mmapqi_c58_hook_join_walk_step((off))
#define C58_WALK_RESTART_NOTE() mmapqi_c58_walk_restart_note()
#else
#define C58_HOOK_SCAN_SPAN_PRE_WALK(off, ss, line) ((void)0)
#define C58_HOOK_SCAN_HEADER_PRE_READ(idx)         ((void)0)
#define C58_HOOK_USE_PHASE_STEP(off)               ((void)0)
#define C58_HOOK_JOIN_WALK_STEP(off)               ((void)0)
#define C58_WALK_RESTART_NOTE()                    ((void)0)
#endif

#endif /* TEST_HOOKS_H */
