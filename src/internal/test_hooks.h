#ifndef TEST_HOOKS_H
#define TEST_HOOKS_H

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
 */
#ifdef MMAPQ_TEST_HOOKS
void mmapq_test_hook_set_commit_pre(void (*fn)(void));
void mmapq_test_hook_set_commit_min_pre(void (*fn)(void));
void mmapq_test_hook_set_commit_post_cas(void (*fn)(void));
void mmapq_test_hook_set_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_set_batch_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_set_archiver_publish_pre(void (*fn)(void));
void mmapq_test_hook_set_reclaim_pre_unlink(void (*fn)(void));
void mmapq_test_hook_set_batch_reclaim_pre_unlink(void (*fn)(void));
void mmapq_test_hook_set_archiver_listing_scan(int (*fn)(void));
void mmapq_test_hook_set_batch_archiver_listing_scan(int (*fn)(void));
void mmapq_test_hook_header_wrap_inject(int wrap_result);
void mmapq_test_hook_set_page_size(long page_size);
#endif

#endif /* TEST_HOOKS_H */
