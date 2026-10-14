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
 * archiver_live_pass / batch twin: invoked on each iteration of the
 *   archiver's live-header pass; a nonzero return arms the wrap-level
 *   injection for that read (P15).
 * header_wrap_inject: substitutes the wrap result inside both
 *   mmapqi_header_read and mmapqi_header_read_checked while armed — so an
 *   injected timeout collapses to end-of-stream through the plain read but
 *   reports MMAPQ_ETIMEDOUT through the checked read (this is what makes
 *   the plain-read mutation observable).
 */
#ifdef MMAPQ_TEST_HOOKS
void mmapq_test_hook_set_commit_pre(void (*fn)(void));
void mmapq_test_hook_set_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_set_batch_archiver_live_pass(int (*fn)(void));
void mmapq_test_hook_header_wrap_inject(int wrap_result);
#endif

#endif /* TEST_HOOKS_H */
