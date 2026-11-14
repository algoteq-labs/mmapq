/* POLL-ARCH-01 pins: a failed read after start_index has moved is ARCHIVED,
 * not ERROR/EOS.
 *
 * The archive race: the pollers/readers check start_index BEFORE reading an
 * entry; the archiver may then advance start_index past the cursor and
 * reclaim the entry's header/payload files, so the read fails. Today that
 * failure surfaces as ERROR+ETIMEDOUT (payload path — the first soak's
 * failure), CORRUPT+ECORRUPT (batch payload path), or END_OF_STREAM
 * (missing header file). The fix re-reads start_index on the failure path
 * only: moved past the cursor -> the ARCHIVED outcome; otherwise today's
 * result and last_error exactly.
 *
 * Driver: the pre_read_seam hook fires at each site's pre-read check, after
 * the check passes and before the read; the pin callback performs the
 * archiver's own reclaim effect (archive_up_to_index + unlink) inside that
 * window. Nothing else is faked — the failed wrap is the library's own.
 *
 * Pin cells (expected results are the POST-FIX contract; at the base tree
 * every race pin fails with the base result noted per cell, every control
 * passes):
 *   payload race, per site: single poller ARCHIVED (base ERROR+ETIMEDOUT),
 *     single reader ENTRY_ARCHIVED (base ENTRY_ERROR+ETIMEDOUT), batch
 *     poller ARCHIVED (base CORRUPT+ECORRUPT), batch reader
 *     BATCH_ENTRY_ARCHIVED (base BATCH_ENTRY_ERROR+ECORRUPT).
 *   batch poller MID-BATCH race (the post-handler re-check): a batch
 *     spanning two payload files; the handler delivers the first message,
 *     then archives past the batch index and unlinks the file holding the
 *     later messages (the handler IS the window — no production hook),
 *     and the next advance fails on the iterator's own error channel.
 *     ARCHIVED post-fix; with the re-check disabled, CORRUPT+ECORRUPT
 *     (the unclassified mid-batch wrap failure). Control: the same
 *     mid-batch failure WITHOUT a move stays CORRUPT+ECORRUPT.
 *   header file reclaimed + moved, per site: ARCHIVED outcomes (base
 *     END_OF_STREAM — the missing-header collapse this card fixes).
 *   header mapping timeout + moved (header_wrap_inject, extended to the
 *     sequential read per §6.2 option (a)): ARCHIVED outcomes (base
 *     ERROR/ENTRY_ERROR + ETIMEDOUT). The archiver disarms the inject
 *     around its own reads, so the seam callback re-arms it AFTER the
 *     archive call.
 *   entry_status (the unchecked-read collapse): an injected header mapping
 *     timeout must surface ENTRY_ERROR + last_error ETIMEDOUT (base:
 *     END_OF_STREAM with last_error 0 — the enum-comment contradiction);
 *     a missing header file without a move stays END_OF_STREAM; and the
 *     re-check branch itself is driven fail-first through entry_status's
 *     own seam (owner ruling R-b): seam advances start_index + injects
 *     MAP_TIMED_OUT (base END_OF_STREAM, fixed ARCHIVED) and seam advances
 *     + unlinks the header file (base END_OF_STREAM, fixed ARCHIVED), for
 *     both readers. An index below a moved start_index before the call
 *     stays ARCHIVED via the pre-read check (regression pin).
 *
 * Controls (unchanged by the fix, must pass at the base): a missing header
 *   file WITHOUT a move -> END_OF_STREAM; a payload failure WITHOUT a move
 *   -> ERROR+ETIMEDOUT (single; CORRUPT+ECORRUPT since 0.2.1 E3(a)) /
 *   CORRUPT+ECORRUPT (batch); an
 *   idle unwritten slot (successful wrap) -> END_OF_STREAM with no
 *   re-check; a non-archivable queue -> today's outcome (its start_index
 *   never moves — the archivable gate is an optimisation, not a semantic
 *   requirement); capacity/length corruption -> CORRUPT / ENTRY_ERROR +
 *   ECORRUPT (a successful wrap, not a reclaimed file).
 *
 * Suite note: entry_status's seam (owner ruling R-b) fires after its own
 * pre-read start_index check, so the re-check branch — start_index moving
 * between the check and the read — is driven directly, like the four read
 * paths.
 *
 * Hook-only suite (TEST-01): without MMAPQ_TEST_HOOKS this file runs zero
 * tests by design and is SKIPped by the release loop.
 */

#include "indexed_queue.h"
#include "indexed_batch_queue.h"
#include "indexed_archiver.h"
#include "indexed_batch_archiver.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>

#ifdef MMAPQ_TEST_HOOKS

static const char    *DIR     = "/tmp";
static const int32_t  REGION  = 1 << 20;
static const int      RING    = 4;
static const int64_t  MAXFILE = (int64_t)REGION * 64;
static const uint64_t TIMEOUT = 30000000000ULL;

#define MSG_LEN 64
#define N_ENTRIES 3   /* committed indices 0..2; every race pin targets 0 */

static char msg[MSG_LEN];
static MmapqRuntime *rt = NULL;

/* ── queue/dir helpers ── */

static void rm_queue(const char *name)
{
    char cmd[512];
    snprintf(cmd, sizeof cmd, "rm -rf %s/%s", DIR, name);
    int rc = system(cmd); (void)rc;
    char d[512];
    snprintf(d, sizeof d, "%s/%s", DIR, name);
    TEST_ASSERT_EQUAL_INT(0, mkdir(d, 0775));
}

static void qpath(const char *name, const char *file, char *out, size_t n)
{
    snprintf(out, n, "%s/%s/%s", DIR, name, file);
}

/* Archivable single-message queue with committed entries at 0..N_ENTRIES-1. */
static void mk_single(const char *name)
{
    rm_queue(name);
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, name, REGION, RING, 1, MAXFILE, 0, TIMEOUT,
        1 /* archivable */, rt, rt, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int i = 0; i < N_ENTRIES; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, msg, MSG_LEN));
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

/* Archivable batch queue with one committed batch (index 0, 3 messages). */
static void mk_batch(const char *name)
{
    rm_queue(name);
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, name, REGION, RING, 1, MAXFILE, 0, TIMEOUT,
        1 /* archivable */, rt, rt, &q));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    /* append returns the batch's message count before this message */
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "m0", 2));
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(b, "m1", 2));
    TEST_ASSERT_EQUAL_INT(2, mmapq_mutable_batch_append(b, "m2", 2));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
}

/* Capture entry 0's payload file path (index 0 -> file 0) via a throwaway
 * read, before any hook is armed. The reader path carries the seam but the
 * hook is NULL here, so nothing fires. */
static void capture_payload_single(const char *name, char *out, size_t n)
{
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, name, RING, TIMEOUT, rt, &r));
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    snprintf(out, n, "%s/%s/payload_%u_%lld", DIR, name,
             (unsigned)ctx.appender_id,
             (long long)(ctx.msg_offset / MAXFILE));
    mmapq_indexed_reader_close(r);
}

static void capture_payload_batch(const char *name, char *out, size_t n)
{
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, name, RING, TIMEOUT, rt, &r));
    MmapqBatchReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, 0, &ctx));
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    snprintf(out, n, "%s/%s/payload_%u_%lld", DIR, name,
             (unsigned)ctx.appender_id,
             (long long)(ctx.batch_offset / MAXFILE));
    mmapq_indexed_batch_reader_close(r);
}

/* ── the seam callback: the archiver's reclaim effect, in the window ── */

static int  g_fired;           /* liveness: the seam fired in this pin */
static int  g_advance;         /* archive_up_to_index(index + 1) */
static int  g_unlink_payload;
static int  g_unlink_header;   /* index 0 lives in header_0 */
static int  g_rearm_inject;    /* re-arm MAP_TIMED_OUT after the archive */
static int  g_arch_rc;
static int  g_unlink_rc;
static int  g_batch_queue;
static char g_qname[64];
static char g_payload_path[512];

/* No TEST_ASSERT in here: it runs inside library code (an assert would
 * longjmp out with the archiver's lock potentially held). Outcomes land in
 * the globals and are asserted after the call under test returns. */
static void seam_hook(int64_t index)
{
    if (g_fired) return;
    g_fired = 1;
    if (g_advance) {
        if (g_batch_queue) {
            MmapqIndexedBatchArchiver *ba = NULL;
            g_arch_rc = mmapq_indexed_batch_archiver_create(DIR, g_qname, RING, rt, &ba);
            if (g_arch_rc == 0) {
                g_arch_rc = mmapq_indexed_batch_archiver_archive_up_to_index(ba, index + 1);
                mmapq_indexed_batch_archiver_close(ba);
            }
        } else {
            MmapqIndexedArchiver *a = NULL;
            g_arch_rc = mmapq_indexed_archiver_create(DIR, g_qname, RING, rt, &a);
            if (g_arch_rc == 0) {
                g_arch_rc = mmapq_indexed_archiver_archive_up_to_index(a, index + 1);
                mmapq_indexed_archiver_close(a);
            }
        }
    }
    if (g_unlink_payload) g_unlink_rc = unlink(g_payload_path);
    if (g_unlink_header) {
        char p[512];
        qpath(g_qname, "header_0", p, sizeof p);
        g_unlink_rc = unlink(p);
    }
    /* The archivers disarm header_wrap_inject after every internal read,
     * so a pin that both archives and injects re-arms it here, after the
     * archiver call — the read under test then sees the injection. */
    if (g_rearm_inject) mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
}

static void seam_reset(void)
{
    g_fired = 0;
    g_advance = 0;
    g_unlink_payload = 0;
    g_unlink_header = 0;
    g_rearm_inject = 0;
    g_arch_rc = 0;
    g_unlink_rc = 0;
}

/* Liveness + reclaim assertions shared by every race pin. */
static void assert_race_moved(void)
{
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_arch_rc);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
}

static MmapqNextMove on_msg(int64_t i, uint8_t a, const void *d, int32_t l, void *c)
{
    (void)i; (void)a; (void)d; (void)l; (void)c;
    return MMAPQ_NEXT_ADVANCE;
}

static MmapqNextMove on_batch(int64_t i, MmapqBatchReadingContext *ctx, void *c)
{
    (void)i; (void)ctx; (void)c;
    return MMAPQ_NEXT_ADVANCE;
}

/* ── payload-race pins (the soak's path): reclaim the payload file ── */

static void pin_poller_payload_race(void)
{
    const char *qn = "pa_ppr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    capture_payload_single(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_advance = 1;
    g_unlink_payload = 1;

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_POLL_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_poller_close(p);
}

static void pin_reader_payload_race(void)
{
    const char *qn = "pa_rpr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    capture_payload_single(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_advance = 1;
    g_unlink_payload = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_ENTRY_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_reader_close(r);
}

static void pin_batch_poller_payload_race(void)
{
    const char *qn = "pa_bpr";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    capture_payload_batch(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_advance = 1;
    g_unlink_payload = 1;

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, on_batch, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_POLL_CORRUPT + MMAPQ_ECORRUPT (a
     * reclaimed batch file maps to ECORRUPT outside walk mode today) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_poller_current_batch_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_batch_poller_close(p);
}

static void pin_batch_reader_payload_race(void)
{
    const char *qn = "pa_brr";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    capture_payload_batch(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_advance = 1;
    g_unlink_payload = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_BATCH_ENTRY_ERROR + MMAPQ_ECORRUPT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_batch_reader_close(r);
}

/* ── batch poller MID-BATCH race: the handler-driven window ────────────────
 *
 * The post-handler re-check (it->last_error != 0) is the only POLL-ARCH-01
 * gate no pre-read seam can reach: the failure happens DURING the
 * handler's iteration, after read_message_count and the header read both
 * succeeded. The handler itself is the test's window — no production hook
 * needed: it delivers the first message, then performs the archiver's own
 * reclaim effect (archive past the batch index + unlink the payload file
 * holding the LATER messages), then advances again — the iterator's own
 * wrap fails and sets it->last_error, and the poller classifies it after
 * the handler returns.
 *
 * Geometry: max_file_size == region_size, so payload files roll per
 * region; message A nearly fills file 0, message B cannot fit the tail
 * and lands in payload_0_1 — the batch spans two payload files, so the
 * mid-batch failure is a real cold wrap of a reclaimed file. The poller
 * is created with a NULL runtime (sync ring: no premap — a sync ring
 * maps strictly on demand, region_ring_accessor.c forces
 * regions_to_map_ahead to 0), so file 1 is provably unmapped before the
 * handler unlinks it and the next wrap deterministically fails. */

#define BIG_LEN ((int32_t)((1 << 20) - 76))   /* count(4)+framing(4)+A end at REGION-4; B (6 bytes) cannot fit the 4-byte tail */
static char big_msg[BIG_LEN];

static void mk_batch_spanning(const char *name)
{
    rm_queue(name);
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, name, REGION, RING, 1, REGION /* one region per payload file */,
        0, TIMEOUT, 1 /* archivable */, rt, rt, &q));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, big_msg, BIG_LEN)); /* file 0 */
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(b, "m1", 2));          /* file 1 */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
}

struct midbatch_cfg {
    int archive;     /* 1 = advance start_index past the batch index */
    int delivered;   /* messages delivered before the failure */
    int arch_rc;
    int unlink_rc;
};

/* Runs inside mmapq_indexed_batch_poller_poll — no TEST_ASSERT here (an
 * assert would longjmp out with the archiver's lock potentially held);
 * outcomes land in the cfg and are asserted after the poll returns. */
static MmapqNextMove midbatch_handler(int64_t i, MmapqBatchReadingContext *ctx, void *c)
{
    struct midbatch_cfg *cfg = (struct midbatch_cfg *)c;
    /* message A: payload file 0, delivered while the file exists */
    if (mmapq_batch_iterator_advance(ctx->iterator)) {
        cfg->delivered++;
        if (cfg->archive) {
            MmapqIndexedBatchArchiver *ba = NULL;
            cfg->arch_rc = mmapq_indexed_batch_archiver_create(DIR, g_qname, RING, rt, &ba);
            if (cfg->arch_rc == 0) {
                cfg->arch_rc = mmapq_indexed_batch_archiver_archive_up_to_index(ba, i + 1);
                mmapq_indexed_batch_archiver_close(ba);
            }
        }
        char p[512];
        qpath(g_qname, "payload_0_1", p, sizeof p);
        cfg->unlink_rc = unlink(p);   /* the file holding message B */
        /* message B: cold wrap of the reclaimed file — the iterator's own
         * error channel (it->last_error) is set here, mid-batch */
        mmapq_batch_iterator_advance(ctx->iterator);
    }
    return MMAPQ_NEXT_ADVANCE;
}

static void pin_batch_poller_midbatch_race(void)
{
    const char *qn = "pa_mbr";
    mk_batch_spanning(qn);
    strcpy(g_qname, qn);
    seam_reset();
    struct midbatch_cfg cfg = { .archive = 1 };

    /* NULL runtime: the sync ring has no premap, so payload file 1 is
     * unmapped until the handler's second advance — the failure is a
     * deterministic cold wrap after the unlink. */
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, NULL, &p));
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, midbatch_handler, &cfg);

    /* post-fix contract; with the post-handler re-check disabled (the
     * mutation) the base answers MMAPQ_POLL_CORRUPT + MMAPQ_ECORRUPT —
     * the mid-batch wrap failure, unclassified */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_poller_current_batch_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_start_index(p) > 0);
    TEST_ASSERT_EQUAL_INT(1, cfg.delivered);   /* A delivered; B failed mid-batch */
    TEST_ASSERT_EQUAL_INT(0, cfg.arch_rc);
    TEST_ASSERT_EQUAL_INT(0, cfg.unlink_rc);
    mmapq_indexed_batch_poller_close(p);
}

static void ctrl_midbatch_no_move(void)
{
    const char *qn = "pa_mbc";
    mk_batch_spanning(qn);
    strcpy(g_qname, qn);
    seam_reset();
    struct midbatch_cfg cfg = { .archive = 0 };   /* unlink only, NO move */

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, NULL, &p));
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, midbatch_handler, &cfg);

    /* both eras: a mid-batch failure without a move keeps today's
     * CORRUPT + ECORRUPT (outside walk mode a missing file maps to
     * ECORRUPT — the BATCH-MAP-MISS-CODE conflation, out of scope) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_start_index(p));
    TEST_ASSERT_EQUAL_INT(1, cfg.delivered);
    TEST_ASSERT_EQUAL_INT(0, cfg.unlink_rc);
    mmapq_indexed_batch_poller_close(p);
}

/* ── header-reclaimed pins: reclaim the header file (the EOS collapse) ── */

static void pin_poller_header_reclaimed(void)
{
    const char *qn = "pa_phr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_POLL_END_OF_STREAM (a missing header
     * file collapses to EOS today — the defect this cell pins) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_poller_close(p);
}

static void pin_batch_poller_header_reclaimed(void)
{
    const char *qn = "pa_bph";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, on_batch, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_POLL_END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_poller_current_batch_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_batch_poller_close(p);
}

static void pin_reader_header_reclaimed(void)
{
    const char *qn = "pa_rhr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_ENTRY_END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_reader_close(r);
}

static void pin_batch_reader_header_reclaimed(void)
{
    const char *qn = "pa_brh";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: MMAPQ_BATCH_ENTRY_END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_batch_reader_close(r);
}

/* ── header-mapping-timeout pins (inject; seam re-arms after archive) ── */

static void pin_poller_header_timeout(void)
{
    const char *qn = "pa_pht";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: MMAPQ_POLL_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_poller_close(p);
}

static void pin_batch_poller_header_timeout(void)
{
    const char *qn = "pa_bpt";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, on_batch, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: MMAPQ_POLL_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_poller_current_batch_index(p));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_start_index(p) > 0);
    assert_race_moved();
    mmapq_indexed_batch_poller_close(p);
}

static void pin_reader_header_timeout(void)
{
    const char *qn = "pa_rht";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: MMAPQ_ENTRY_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_reader_close(r);
}

static void pin_batch_reader_header_timeout(void)
{
    const char *qn = "pa_brt";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: MMAPQ_BATCH_ENTRY_ERROR + MMAPQ_ETIMEDOUT */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_batch_reader_close(r);
}

/* ── entry_status pins (the unchecked-read collapse) ── */

static void pin_entry_status_timeout_single(void)
{
    const char *qn = "pa_est";
    mk_single(qn);
    seam_reset();

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    /* No seam, no archive: a pure header mapping timeout, start_index not
     * moved. Base: the unchecked read collapses the timeout to
     * END_OF_STREAM with last_error 0 — the enum-comment contradiction. */
    mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
    MmapqEntryStatus st = mmapq_indexed_reader_entry_status(r, 0);
    mmapq_test_hook_header_wrap_inject(0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, st);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);
}

static void pin_entry_status_timeout_batch(void)
{
    const char *qn = "pa_esb";
    mk_batch(qn);
    seam_reset();

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
    MmapqBatchEntryStatus st = mmapq_indexed_batch_reader_entry_status(r, 0);
    mmapq_test_hook_header_wrap_inject(0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, st);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapq_indexed_batch_reader_last_error(r));
    mmapq_indexed_batch_reader_close(r);
}

/* ── entry_status re-check pins (ruling R-b: entry_status has its own
 * seam; the race is driven in-call, fail-first at the base) ── */

static void pin_entry_status_race_timeout_single(void)
{
    const char *qn = "pa_rst";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqEntryStatus st = mmapq_indexed_reader_entry_status(r, 0);
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: END_OF_STREAM (the unchecked read collapses
     * the injected timeout) — start_index moved past 0 in the window */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_reader_close(r);
}

static void pin_entry_status_race_timeout_batch(void)
{
    const char *qn = "pa_rsb";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_rearm_inject = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchEntryStatus st = mmapq_indexed_batch_reader_entry_status(r, 0);
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);

    /* post-fix contract; base: END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_batch_reader_close(r);
}

static void pin_entry_status_race_missing_single(void)
{
    const char *qn = "pa_rmh";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqEntryStatus st = mmapq_indexed_reader_entry_status(r, 0);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: END_OF_STREAM (a missing header file
     * collapses to EOS through the unchecked read) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_reader_close(r);
}

static void pin_entry_status_race_missing_batch(void)
{
    const char *qn = "pa_rmb";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    g_payload_path[0] = 0;
    seam_reset();
    g_advance = 1;
    g_unlink_header = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchEntryStatus st = mmapq_indexed_batch_reader_entry_status(r, 0);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* post-fix contract; base: END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    assert_race_moved();
    mmapq_indexed_batch_reader_close(r);
}

static void pin_entry_status_missing_single(void)
{
    const char *qn = "pa_esm";
    mk_single(qn);
    seam_reset();
    char p[512];
    qpath(qn, "header_0", p, sizeof p);
    TEST_ASSERT_EQUAL_INT(0, unlink(p));   /* missing header, NO move */

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    MmapqEntryStatus st = mmapq_indexed_reader_entry_status(r, 0);

    /* both eras: a missing header file without a move is END_OF_STREAM */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);
}

static void pin_entry_status_missing_batch(void)
{
    const char *qn = "pa_ebm";
    mk_batch(qn);
    seam_reset();
    char p[512];
    qpath(qn, "header_0", p, sizeof p);
    TEST_ASSERT_EQUAL_INT(0, unlink(p));

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    MmapqBatchEntryStatus st = mmapq_indexed_batch_reader_entry_status(r, 0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM, st);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    mmapq_indexed_batch_reader_close(r);
}

static void pin_entry_status_archived_single(void)
{
    const char *qn = "pa_esa";
    mk_single(qn);
    seam_reset();
    MmapqIndexedArchiver *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, qn, RING, rt, &a));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(a, 1));
    mmapq_indexed_archiver_close(a);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    /* Outcome pin: both eras answer ARCHIVED (base via the pre-read
     * check). entry_status has no seam of its own — see the suite note. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ARCHIVED, mmapq_indexed_reader_entry_status(r, 0));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_start_index(r) > 0);
    mmapq_indexed_reader_close(r);
}

static void pin_entry_status_archived_batch(void)
{
    const char *qn = "pa_eba";
    mk_batch(qn);
    seam_reset();
    MmapqIndexedBatchArchiver *ba = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_archiver_create(DIR, qn, RING, rt, &ba));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_archiver_archive_up_to_index(ba, 1));
    mmapq_indexed_batch_archiver_close(ba);

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ARCHIVED, mmapq_indexed_batch_reader_entry_status(r, 0));
    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_start_index(r) > 0);
    mmapq_indexed_batch_reader_close(r);
}

/* ── controls (must pass at the base and stay passing after the fix) ── */

static void ctrl_missing_header_no_move_poller(void)
{
    const char *qn = "pa_cmh";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_unlink_header = 1;   /* missing header file, NO start_index move */

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_start_index(p));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_poller_close(p);
}

static void ctrl_missing_header_no_move_reader(void)
{
    const char *qn = "pa_cmr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    g_payload_path[0] = 0;
    seam_reset();
    g_unlink_header = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_start_index(r));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_reader_close(r);
}

static void ctrl_payload_no_move_poller(void)
{
    const char *qn = "pa_cpp";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    capture_payload_single(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_unlink_payload = 1;   /* payload failure, NO start_index move */

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* 0.2.1 E3(a): a payload missing under a committed header with no
     * start_index move is corruption (was POLL_ERROR + ETIMEDOUT) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_start_index(p));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_poller_close(p);
}

static void ctrl_payload_no_move_reader(void)
{
    const char *qn = "pa_cpr";
    mk_single(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    capture_payload_single(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_unlink_payload = 1;

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* 0.2.1 E3(a): corruption (was ENTRY_ERROR + ETIMEDOUT) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_start_index(r));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_reader_close(r);
}

static void ctrl_payload_no_move_batch_poller(void)
{
    const char *qn = "pa_cbp";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    capture_payload_batch(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_unlink_payload = 1;

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, on_batch, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* a reclaimed batch file without a move stays CORRUPT (the wrap-level
     * conflation is out of scope — the re-check must not lose it either) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_batch_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_start_index(p));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_batch_poller_close(p);
}

static void ctrl_payload_no_move_batch_reader(void)
{
    const char *qn = "pa_cbr";
    mk_batch(qn);
    strcpy(g_qname, qn);
    g_batch_queue = 1;
    capture_payload_batch(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_unlink_payload = 1;

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqBatchReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_start_index(r));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_batch_reader_close(r);
}

static void ctrl_idle_unwritten_poller(void)
{
    const char *qn = "pa_cip";
    mk_single(qn);
    seam_reset();

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_end(p));
    TEST_ASSERT_EQUAL_INT64(N_ENTRIES, mmapq_indexed_poller_current_index(p));

    /* hot idle path: a successful wrap of an unwritten slot — EOS, no
     * re-check, both eras */
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, pr);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT64(N_ENTRIES, mmapq_indexed_poller_current_index(p));
    TEST_ASSERT_EQUAL_INT(0, g_fired);
    mmapq_indexed_poller_close(p);
}

static void ctrl_idle_unwritten_reader(void)
{
    const char *qn = "pa_cir";
    mk_single(qn);
    seam_reset();

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, N_ENTRIES, &ctx));

    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, g_fired);
    mmapq_indexed_reader_close(r);
}

static void ctrl_nonarchivable_poller(void)
{
    const char *qn = "pa_cnp";
    rm_queue(qn);
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, qn, REGION, RING, 1, MAXFILE, 0, TIMEOUT,
        0 /* NOT archivable */, rt, rt, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, msg, MSG_LEN));
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    seam_reset();

    char pay[512];
    capture_payload_single(qn, pay, sizeof pay);
    TEST_ASSERT_EQUAL_INT(0, unlink(pay));   /* payload gone, start_index fixed */

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &p));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, NULL);
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* a non-archivable queue's start_index never moves — the miss is
     * corruption (0.2.1 E3(a); was POLL_ERROR + ETIMEDOUT); the seam never
     * fires (it lives inside the archivable gate) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_poller_last_error(p));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_start_index(p));
    TEST_ASSERT_EQUAL_INT(0, g_fired);
    mmapq_indexed_poller_close(p);
}

static void ctrl_nonarchivable_reader(void)
{
    const char *qn = "pa_cnr";
    rm_queue(qn);
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, qn, REGION, RING, 1, MAXFILE, 0, TIMEOUT,
        0 /* NOT archivable */, rt, rt, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, msg, MSG_LEN));
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    strcpy(g_qname, qn);
    g_batch_queue = 0;
    capture_payload_single(qn, g_payload_path, sizeof g_payload_path);
    seam_reset();
    g_unlink_payload = 1;   /* reader seam fires; NO move is possible */

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    mmapq_test_hook_set_pre_read_seam(seam_hook);
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
    mmapq_test_hook_set_pre_read_seam(NULL);

    /* the reader's re-check is unconditional but reads a fixed value —
     * the miss is corruption (0.2.1 E3(a); was ETIMEDOUT) */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_start_index(r));
    TEST_ASSERT_EQUAL_INT(1, g_fired);
    TEST_ASSERT_EQUAL_INT(0, g_unlink_rc);
    mmapq_indexed_reader_close(r);
}

/* Plant a committed entry whose payload length is corrupt (a successful
 * wrap, then a length the capacity check rejects) — true corruption, not
 * a reclaimed file: the capacity/length branches get no re-check. */
static void ctrl_corruption_poller(void)
{
    const char *qn = "pa_ccp";
    mk_single(qn);
    seam_reset();

    char p[512];
    int64_t pos = 0;
    {
        MmapqIndexedReader *r = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
        MmapqReadingContext ctx;
        TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
        TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
        pos = ctx.msg_offset;
        mmapq_indexed_reader_close(r);
    }
    qpath(qn, "payload_0_0", p, sizeof p);
    FILE *f = fopen(p, "r+b");
    TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQUAL_INT(0, fseek(f, (long)pos, SEEK_SET));
    int32_t bad = -1;
    TEST_ASSERT_EQUAL_INT((int)sizeof bad, (int)fwrite(&bad, 1, sizeof bad, f));
    fclose(f);

    MmapqIndexedPoller *pl = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_create(DIR, qn, RING, TIMEOUT, rt, &pl));
    MmapqPollResult pr = mmapq_indexed_poller_poll(pl, on_msg, NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_poller_last_error(pl));
    TEST_ASSERT_EQUAL_INT(0, g_fired);
    mmapq_indexed_poller_close(pl);
}

static void ctrl_corruption_reader(void)
{
    const char *qn = "pa_ccr";
    mk_single(qn);
    seam_reset();

    char p[512];
    int64_t pos = 0;
    {
        MmapqIndexedReader *r = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
        MmapqReadingContext ctx;
        TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));
        TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
        pos = ctx.msg_offset;
        mmapq_indexed_reader_close(r);
    }
    qpath(qn, "payload_0_0", p, sizeof p);
    FILE *f = fopen(p, "r+b");
    TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQUAL_INT(0, fseek(f, (long)pos, SEEK_SET));
    int32_t bad = -1;
    TEST_ASSERT_EQUAL_INT((int)sizeof bad, (int)fwrite(&bad, 1, sizeof bad, f));
    fclose(f);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, qn, RING, TIMEOUT, rt, &r));
    MmapqReadingContext ctx;
    TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, 0, &ctx));

    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(0, g_fired);
    mmapq_indexed_reader_close(r);
}

#endif /* MMAPQ_TEST_HOOKS */

void setUp(void)
{
#ifdef MMAPQ_TEST_HOOKS
    memset(msg, 'x', MSG_LEN);
    memset(big_msg, 'x', sizeof big_msg);
    if (rt == NULL) {
        int rc = mmapq_runtime_region_mapper(&rt);
        TEST_ASSERT_EQUAL_INT(0, rc);
    }
#endif
}

void tearDown(void)
{
#ifdef MMAPQ_TEST_HOOKS
    /* never leak an armed hook into a later test */
    mmapq_test_hook_set_pre_read_seam(NULL);
    mmapq_test_hook_header_wrap_inject(0);
#endif
}

int main(void)
{
    UNITY_BEGIN();
#ifdef MMAPQ_TEST_HOOKS
    /* race pins — fail-first at the base */
    RUN_TEST(pin_poller_payload_race);
    RUN_TEST(pin_reader_payload_race);
    RUN_TEST(pin_batch_poller_payload_race);
    RUN_TEST(pin_batch_reader_payload_race);
    RUN_TEST(pin_batch_poller_midbatch_race);
    RUN_TEST(pin_poller_header_reclaimed);
    RUN_TEST(pin_batch_poller_header_reclaimed);
    RUN_TEST(pin_reader_header_reclaimed);
    RUN_TEST(pin_batch_reader_header_reclaimed);
    RUN_TEST(pin_poller_header_timeout);
    RUN_TEST(pin_batch_poller_header_timeout);
    RUN_TEST(pin_reader_header_timeout);
    RUN_TEST(pin_batch_reader_header_timeout);
    RUN_TEST(pin_entry_status_timeout_single);
    RUN_TEST(pin_entry_status_timeout_batch);
    RUN_TEST(pin_entry_status_race_timeout_single);
    RUN_TEST(pin_entry_status_race_timeout_batch);
    RUN_TEST(pin_entry_status_race_missing_single);
    RUN_TEST(pin_entry_status_race_missing_batch);
    RUN_TEST(pin_entry_status_missing_single);
    RUN_TEST(pin_entry_status_missing_batch);
    RUN_TEST(pin_entry_status_archived_single);
    RUN_TEST(pin_entry_status_archived_batch);
    /* controls — pass at the base, unchanged after the fix */
    RUN_TEST(ctrl_missing_header_no_move_poller);
    RUN_TEST(ctrl_missing_header_no_move_reader);
    RUN_TEST(ctrl_payload_no_move_poller);
    RUN_TEST(ctrl_payload_no_move_reader);
    RUN_TEST(ctrl_payload_no_move_batch_poller);
    RUN_TEST(ctrl_payload_no_move_batch_reader);
    RUN_TEST(ctrl_midbatch_no_move);
    RUN_TEST(ctrl_idle_unwritten_poller);
    RUN_TEST(ctrl_idle_unwritten_reader);
    RUN_TEST(ctrl_nonarchivable_poller);
    RUN_TEST(ctrl_nonarchivable_reader);
    RUN_TEST(ctrl_corruption_poller);
    RUN_TEST(ctrl_corruption_reader);
#endif
    return UNITY_END();
}
