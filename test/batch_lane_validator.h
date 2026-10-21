#ifndef BATCH_LANE_VALIDATOR_H
#define BATCH_LANE_VALIDATOR_H

/*
 * batch_lane_validator.h — the batch lane validator (CAND-49 §7.9).
 *
 * TEST-ONLY (owner-mandated): header-only, static inline, never in src/ —
 * zero runtime cost. Walks one appender's lane and asserts the no-holes
 * invariant. Reads the boundaries from the ON-DISK meta files itself (a pin
 * can't hide a wrong boundary). Plain pread throughout — no mapping
 * lifecycle, no library state.
 *
 * Checks:
 *  1. Walk from start_offset: every position is a batch (count > 0 with
 *     exactly count well-formed messages, short/long padding rules), a
 *     < 4-byte region-skip tail immediately preceding a batch header, or a
 *     §3.8 boundary padding marker (negative, exact-extent) skipping to the
 *     next region; the walk ends EXACTLY at write_position; no count-0
 *     record before the end; no leftover bytes.
 *  2. Tail-zero: every byte from write_position to the end of the last
 *     existing payload file of the lane is zero — the forward-only
 *     invariant (§3.7's (a)–(c)). Kills every "positioned too early" bug.
 *     CAND-51e: a rejoin at an exact file boundary creates the next
 *     payload file (the RW walk's probe + the eager-scaffolding wrap —
 *     documented warm-up behaviour), so later files may exist iff they are
 *     ENTIRELY zero; the zero scan covers them. File existence is mapping
 *     policy, not content; the teeth (content past EOS) are unchanged.
 *  3. Header cross-check: every header entry of this appender (scanned from
 *     the queue's on-disk start index) points at a batch start encountered
 *     in the walk.
 *  4. Header uniqueness: each batch start is referenced by at most one
 *     header entry of this appender (the C counterpart of the model's
 *     NoDoubleCommit — catches double commit).
 *
 * Every CAND-49 batch pin calls assert_batch_lane() at the end, including
 * after each rejoin.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "unity.h"
#include "internal/header_codec.h"
#include "internal/queue_meta.h"
#include "internal/appender_meta.h"

/* The lane walk is a test-side re-implementation of the framing rules;
 * the pins' message counts are tiny. */

typedef struct {
    char     dir_queue[512];   /* "<dir>/<queue>" */
    uint8_t  appender_id;
    int32_t  region_size;
    int64_t  max_file_size;
    int64_t  queue_start_index;
    int64_t  start_offset;     /* on-disk appender_meta start_offset */
    /* collected during the walk */
    int64_t  batch_starts[4096];
    int      batch_count;
} BatchLane;

static inline int64_t blv_read_at(const char *path, int64_t file_off,
                                  void *buf, size_t len)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    ssize_t got = pread(fd, buf, len, (off_t)file_off);
    close(fd);
    return got == (ssize_t)len ? 0 : -1;
}

static inline void blv_payload_path(BatchLane *L, int64_t pos, char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/payload_%d_%lld", L->dir_queue, L->appender_id,
             (long long)(pos / L->max_file_size));
}

/* Read the 4-byte int32 at a payload position. Returns 0 or -1 (no file). */
static inline int blv_read32(BatchLane *L, int64_t pos, int32_t *out)
{
    char path[600];
    blv_payload_path(L, pos, path, sizeof(path));
    return blv_read_at(path, pos % L->max_file_size, out, sizeof(int32_t));
}

static inline void blv_fail(BatchLane *L, int64_t pos, const char *what)
{
    char msg[256];
    snprintf(msg, sizeof(msg),
             "batch lane validator: %s (appender %u at %lld)",
             what, L->appender_id, (long long)pos);
    TEST_FAIL_MESSAGE(msg);
}

static inline void assert_batch_lane(
    const char *dir, const char *queue, uint8_t appender_id,
    int64_t write_position)
{
    BatchLane L;
    memset(&L, 0, sizeof L);
    int dq = snprintf(L.dir_queue, sizeof L.dir_queue, "%s/%s", dir, queue);
    if (dq < 0 || dq >= (int)sizeof L.dir_queue)
        TEST_FAIL_MESSAGE("batch lane validator: dir/queue too long");
    L.appender_id = appender_id;

    /* On-disk queue meta */
    char path[600];
    snprintf(path, sizeof path, "%s/meta", L.dir_queue);
    TEST_ASSERT_EQUAL_INT(0,
        blv_read_at(path, QUEUE_META_REGION_SIZE_OFFSET, &L.region_size, 4));
    TEST_ASSERT_EQUAL_INT(0,
        blv_read_at(path, QUEUE_META_MAX_FILE_SIZE_OFFSET, &L.max_file_size, 8));
    TEST_ASSERT_EQUAL_INT(0,
        blv_read_at(path, QUEUE_META_START_INDEX_OFFSET, &L.queue_start_index, 8));

    /* On-disk appender meta — the boundary is read from disk, never from
     * the appender under test. */
    snprintf(path, sizeof path, "%s/appender_meta_%u", L.dir_queue, appender_id);
    TEST_ASSERT_EQUAL_INT(0,
        blv_read_at(path, APPENDER_META_START_OFFSET_OFFSET, &L.start_offset, 8));

    /* ── Check 1: the walk ─────────────────────────────────────────────── */
    int64_t pos = L.start_offset;
    while (pos < write_position) {
        int64_t cap = L.region_size - (pos % L.region_size);
        if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
            /* CAND-51e unified tail rule: <= 4 bytes left is padding —
             * skip; a batch count slot never occupies a region tail. */
            pos += cap;
            continue;
        }
        int32_t count;
        TEST_ASSERT_EQUAL_INT(0, blv_read32(&L, pos, &count));
        if (count == 0)
            blv_fail(&L, pos, "count-0 record before the end of the lane");
        if (count < 0) {
            /* §3.8 boundary padding marker: legal iff exact-extent */
            int64_t skip = -(int64_t)count + MMAPQ_BATCH_MSG_COUNT_SIZE;
            if (skip != cap)
                blv_fail(&L, pos, "boundary padding marker with non-exact extent");
            pos += cap;
            continue;
        }
        /* A batch: record its start, walk exactly count messages */
        if (L.batch_count >= (int)(sizeof L.batch_starts / sizeof L.batch_starts[0]))
            TEST_FAIL_MESSAGE("batch lane validator: too many batches");
        L.batch_starts[L.batch_count++] = pos;
        int64_t m = pos + MMAPQ_BATCH_MSG_COUNT_SIZE;
        for (int32_t i = 0; i < count; i++) {
            int64_t mcap = L.region_size - (m % L.region_size);
            if (mcap <= PAYLOAD_LENGTH_SIZE) { m += mcap; i--; continue; }  /* short padding: no length field is written — not a message (CAND-51e; the writer skips cap <= 4 silently, so the walk must not consume a count slot) */
            int32_t len;
            TEST_ASSERT_EQUAL_INT(0, blv_read32(&L, m, &len));
            if (len == 0)
                blv_fail(&L, m, "zero length inside a batch");
            if (len < 0) {
                /* message-level long padding: exact-extent skip */
                int64_t skip = -(int64_t)len + PAYLOAD_LENGTH_SIZE;
                if (skip != mcap)
                    blv_fail(&L, m, "long padding with non-exact extent");
                m += mcap;
                i--;   /* padding is not a message */
                continue;
            }
            if ((int64_t)len > mcap - PAYLOAD_LENGTH_SIZE)
                blv_fail(&L, m, "message length does not fit the region");
            m += PAYLOAD_LENGTH_SIZE + len;
        }
        pos = m;
    }
    if (pos != write_position)
        blv_fail(&L, pos, "lane walk does not end exactly at the write position");

    /* ── Check 2: tail-zero ────────────────────────────────────────────── */
    {
        int64_t last = pos;   /* == write_position */
        char zpath[600];
        blv_payload_path(&L, last, zpath, sizeof zpath);
        struct stat st;
        if (stat(zpath, &st) == 0) {
            /* zero bytes from write_position to the end of the LAST existing
             * payload file of the lane; later files (a rejoin at an exact
             * boundary creates the next one — see the header comment) must
             * be entirely zero */
            for (int64_t fi = last / L.max_file_size + 1; ; fi++) {
                char p2[600];
                snprintf(p2, sizeof p2, "%s/payload_%d_%lld", L.dir_queue,
                         appender_id, (long long)fi);
                if (stat(p2, &st) != 0) break;
                int lfd = open(p2, O_RDONLY);
                TEST_ASSERT_GREATER_THAN_INT(0, lfd);
                char lbuf[4096];
                ssize_t lgot; int64_t loff = 0;
                while ((lgot = pread(lfd, lbuf, sizeof lbuf, (off_t)loff)) > 0) {
                    for (ssize_t i = 0; i < lgot; i++)
                        if (lbuf[i] != 0) {
                            close(lfd);
                            blv_fail(&L, fi * L.max_file_size + loff + i,
                                     "non-zero byte in a payload file past the write position's file");
                        }
                    loff += lgot;
                }
                close(lfd);
            }
            /* scan the current file from write_position % max_file_size */
            int fd = open(zpath, O_RDONLY);
            TEST_ASSERT_GREATER_THAN_INT(0, fd);
            int64_t off = last % L.max_file_size;
            char buf[4096];
            ssize_t got;
            while ((got = pread(fd, buf, sizeof buf, (off_t)off)) > 0) {
                for (ssize_t i = 0; i < got; i++)
                    if (buf[i] != 0) {
                        close(fd);
                        blv_fail(&L, last / L.max_file_size * L.max_file_size + off + i,
                                 "non-zero byte past the write position");
                    }
                off += got;
            }
            close(fd);
        }
    }

    /* ── Checks 3+4: header cross-check + uniqueness ───────────────────── */
    {
        int64_t referenced[4096]; int nref = 0;
        for (int64_t i = L.queue_start_index; ; i++) {
            int64_t hpos = header_position(i);
            char hpath[600];
            snprintf(hpath, sizeof hpath, "%s/header_%lld", L.dir_queue,
                     (long long)(hpos / L.max_file_size));
            uint64_t hdr;
            if (blv_read_at(hpath, hpos % L.max_file_size, &hdr, 8) != 0)
                break;   /* no more header files */
            if (hdr == HEADER_NULL) break;
            if (header_appender_id(hdr) != appender_id) continue;
            int64_t p = header_payload_position(hdr);
            int found = 0;
            for (int k = 0; k < L.batch_count; k++)
                if (L.batch_starts[k] == p) { found = 1; break; }
            if (!found)
                blv_fail(&L, p, "header entry does not point at a walked batch start");
            for (int k = 0; k < nref; k++)
                if (referenced[k] == p)
                    blv_fail(&L, p, "batch start referenced by two header entries (double commit)");
            referenced[nref++] = p;
        }
    }
}

#endif /* BATCH_LANE_VALIDATOR_H */
