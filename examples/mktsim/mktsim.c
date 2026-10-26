/*
 * mktsim.c — agentic market simulation over one shared mmapq log.
 *
 * Every trader is an independent process. Traders publish INTENTS only
 * (NEW limit order, CANCEL). Every process embeds the same deterministic
 * matching engine and replays the log; fills are never published — each
 * process derives them. The log index is time priority; the order id of a
 * NEW is the log index it was committed at.
 *
 * Every intent carries a claim: "when I wrote this I had applied k messages,\n * my input hash was H and my derived-event digest was D". Every trader checks
 * every other trader's claims as it replays (online), and verify mode replays
 * the whole log at the end (offline). A claim mismatch means two processes
 * saw different histories or computed different markets.
 *
 * Modes:
 *   mktsim trader  <dir> <queue> <trader_id> <incarnation> <stopfile> [sync|async] [appender_id|-1] [batch 0|1]
 *   mktsim gateway <dir> <queue> <gateway_id> <incarnation> <stopfile> [sync|async] [appender_id|-1]
 *   mktsim verify  <dir> <queue> [trader_log ...]
 *
 * Batch lane (F4): with batch=1 every trader also drives a batch appender on
 * the queue "market_batch" in the same directory and mirrors its intent stream
 * there. The MAX_LIVE path (cancel oldest to make room for a NEW) commits the
 * pair [NEW, CANCEL] as one atomic batch; every other intent commits as a
 * singleton batch. Batch payloads reuse the MsgHdr wire format and the
 * (k,H,D) claim scheme, evaluated over the batch lane's own message count by
 * a second embedded engine per process.
 *
 * Gateway (F5): a gateway process appends to the single-message queue in
 * arrival order (append_uncommitted) and commits in sequence order (commit):
 * a fixed permutation delays every 7th packet by 2 arrival positions, so
 * payload offsets are non-monotone in commit order. Packets are MsgHdr
 * T_GATEWAY records carrying gateway id, incarnation, seq and the packet's
 * own payload offset. On rejoin the gateway walks its lane's uncommitted
 * iterator, asserts the walk yields exactly the appended-but-uncommitted set
 * its own gap log recorded at death, and applies the torn-tail recipe
 * (the highest-offset record is discarded and re-appended).
 *
 * appender_id >= 0 uses that fixed id (the supervisor's assignment; on a
 * restart the supervisor has reclaimed it with mmapq-ids --unset after reaping
 * the previous incarnation). MMAPQ_EIDCONFLICT is a failure (exit 2).
 * appender_id -1 takes a pool-assigned id.
 *
 * Exit codes (trader): 0 clean stop; 2 queue/appender error; 3 divergence or
 * invalid message detected online; 4 poll error/corrupt.
 * Exit codes (verify): 0 all checks pass; 1 any check failed.
 *
 * Engine rules (the determinism contract):
 *   - prices and quantities are positive int32 ticks/lots; anything else is
 *     rejected (REJECT event, no book change)
 *   - an incoming NEW matches the best opposite order while prices cross;
 *     best = best price, ties broken by lowest order id (earliest index)
 *   - fills execute at the resting order's price
 *   - self-trade is allowed (explicit rule, counted)
 *   - a remainder rests with order id = its own log index
 *   - CANCEL removes the order only if it exists and the sender owns it;
 *     otherwise CANCEL_REJECT (no book change)
 *   - no wall clock, no floating point, no randomness inside the engine
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "indexed_batch_queue.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <errno.h>
#include <signal.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Small regions on purpose: this is the geometry where the C-08 concurrent
 * append defect reproduced (64 KiB regions, 256 KiB files). */
#define REGION_SIZE   (64 * 1024)
#define RING_SIZE     4
#define MAX_FILE_SIZE ((int64_t)REGION_SIZE * 4)
#define TIMEOUT_NS    5000000000ULL

#define MAGIC        0x534B544Du   /* "MTKS" */
#define T_NEW        1
#define T_CANCEL     2
#define T_GATEWAY    3
#define MAX_TRADERS  64
#define MAX_PAD      1500
#define MAX_LIVE     6             /* a trader cancels its oldest order beyond this */
#define ENGINE_SEED  0x6D6D617071ULL
#define BATCH_QNAME  "market_batch"
#define MAX_BATCH_MSGS 16          /* the demo never builds batches beyond pairs */

/* ── wire format (little-endian, packed by construction) ───────────────── */

typedef struct {
    uint32_t magic;
    uint16_t type;
    uint16_t trader;
    uint32_t incarnation;
    uint32_t pad_len;
    uint64_t seq;            /* per trader, contiguous across incarnations */
    int64_t  seen_count;     /* messages applied when this was written */
    uint64_t seen_input;     /* input hash after seen_count messages */
    uint64_t seen_digest;    /* engine digest after seen_count messages */
    int32_t  side;           /* NEW: +1 buy, -1 sell */
    int32_t  price;          /* NEW */
    int32_t  qty;            /* NEW */
    int32_t  reserved;
    int64_t  cancel_id;      /* CANCEL: order id (log index of its NEW) */
} MsgHdr;                    /* 72 bytes; then pad_len bytes; then u64 crc */

_Static_assert(sizeof(MsgHdr) == 72, "MsgHdr layout");

static uint64_t mix64(uint64_t x)
{
    x ^= x >> 30; x *= 0xBF58476D1CE4E5B9ULL;
    x ^= x >> 27; x *= 0x94D049BB133111EBULL;
    x ^= x >> 31;
    return x;
}

static uint64_t fnv64(const void *p, size_t n)
{
    const uint8_t *b = p;
    uint64_t h = 0xCBF29CE484222325ULL;
    for (size_t i = 0; i < n; i++) { h ^= b[i]; h *= 0x100000001B3ULL; }
    return h;
}

static uint8_t pad_byte(uint16_t trader, uint64_t seq, uint32_t i)
{
    return (uint8_t)mix64(((uint64_t)trader << 48) ^ (seq << 16) ^ i);
}

/* ── engine ─────────────────────────────────────────────────────────────── */

typedef struct { int64_t id; int32_t owner, side, price, qty; } Order;

typedef struct {
    Order   *orders; int n, cap;
    int64_t  applied;                 /* messages applied */
    uint64_t input_hash;              /* chain over (index, raw bytes) */
    uint64_t digest;                  /* chain over derived events */
    int64_t  position[MAX_TRADERS];   /* lots */
    int64_t  cash[MAX_TRADERS];       /* ticks*lots, sign: buyer pays */
    int64_t  trades, volume, self_trades, rejects, cancel_rejects;
    int32_t  last_trade;
    /* per-index history for claim checks */
    uint64_t *hist_input, *hist_digest; int64_t hist_cap;
    /* per-trader replay facts */
    int64_t  next_seq[MAX_TRADERS];
    uint32_t last_inc[MAX_TRADERS];
    int      tiebreak_bug;            /* test hook: planted divergence */
} Engine;

static void engine_init(Engine *e)
{
    memset(e, 0, sizeof *e);
    e->input_hash = ENGINE_SEED;
    e->digest = ENGINE_SEED ^ 1;
    e->last_trade = 1000;
    const char *bug = getenv("MKTSIM_TIEBREAK_BUG");
    e->tiebreak_bug = bug && atoi(bug) != 0;
}

static void engine_free(Engine *e)
{
    free(e->orders); free(e->hist_input); free(e->hist_digest);
}

static void ev(Engine *e, uint64_t a, uint64_t b, uint64_t c, uint64_t d)
{
    e->digest = mix64(e->digest ^ mix64(a ^ mix64(b ^ mix64(c ^ mix64(d)))));
}

static void hist_push(Engine *e)
{
    if (e->applied >= e->hist_cap) {
        int64_t nc = e->hist_cap ? e->hist_cap * 2 : 65536;
        e->hist_input  = realloc(e->hist_input,  (size_t)nc * 8);
        e->hist_digest = realloc(e->hist_digest, (size_t)nc * 8);
        if (!e->hist_input || !e->hist_digest) { perror("realloc"); exit(5); }
        e->hist_cap = nc;
    }
    e->hist_input[e->applied]  = e->input_hash;
    e->hist_digest[e->applied] = e->digest;
}

/* state after `count` messages */
static void hist_at(const Engine *e, int64_t count, uint64_t *in, uint64_t *dg)
{
    if (count == 0) { *in = ENGINE_SEED; *dg = ENGINE_SEED ^ 1; return; }
    *in = e->hist_input[count - 1];
    *dg = e->hist_digest[count - 1];
}

static int best_index(const Engine *e, int side)
{
    int b = -1;
    for (int i = 0; i < e->n; i++) {
        const Order *o = &e->orders[i];
        if (o->side != side) continue;
        if (b < 0) { b = i; continue; }
        const Order *x = &e->orders[b];
        int better = side > 0 ? o->price > x->price : o->price < x->price;
        int tie_earlier = o->price == x->price &&
            (e->tiebreak_bug ? o->id > x->id : o->id < x->id);
        if (better || tie_earlier) b = i;
    }
    return b;
}

static void remove_at(Engine *e, int i)
{
    /* order-preserving removal keeps iteration deterministic */
    memmove(&e->orders[i], &e->orders[i + 1], (size_t)(e->n - i - 1) * sizeof(Order));
    e->n--;
}

/* Validate framing of one message. Returns NULL on success, reason otherwise. */
static const char *validate(const void *data, int32_t len, MsgHdr *h)
{
    if (len < (int32_t)(sizeof(MsgHdr) + 8)) return "short";
    memcpy(h, data, sizeof *h);
    if (h->magic != MAGIC) return "magic";
    if (h->pad_len > MAX_PAD) return "pad_len";
    if ((int64_t)len != (int64_t)sizeof(MsgHdr) + h->pad_len + 8) return "length";
    uint64_t crc; memcpy(&crc, (const uint8_t *)data + len - 8, 8);
    if (crc != fnv64(data, (size_t)len - 8)) return "crc";
    const uint8_t *pad = (const uint8_t *)data + sizeof(MsgHdr);
    for (uint32_t i = 0; i < h->pad_len; i++)
        if (pad[i] != pad_byte(h->trader, h->seq, i)) return "pad";
    if (h->trader >= MAX_TRADERS) return "trader";
    if (h->type != T_NEW && h->type != T_CANCEL && h->type != T_GATEWAY) return "type";
    return NULL;
}

/* Apply message at `index`. The caller has validated it. */
static void engine_apply(Engine *e, int64_t index, const void *data, int32_t len, const MsgHdr *h)
{
    e->input_hash = mix64(e->input_hash ^ mix64((uint64_t)index) ^ fnv64(data, (size_t)len));

    if (h->type == T_NEW) {
        if ((h->side != 1 && h->side != -1) || h->price <= 0 || h->qty <= 0) {
            e->rejects++; ev(e, 1, (uint64_t)index, 0, 0);
        } else {
            int32_t remaining = h->qty;
            for (;;) {
                if (remaining == 0) break;
                int b = best_index(e, -h->side);
                if (b < 0) break;
                Order *m = &e->orders[b];
                int crosses = h->side > 0 ? h->price >= m->price : h->price <= m->price;
                if (!crosses) break;
                int32_t q = remaining < m->qty ? remaining : m->qty;
                int32_t px = m->price;
                int buyer  = h->side > 0 ? h->trader : m->owner;
                int seller = h->side > 0 ? m->owner : h->trader;
                e->position[buyer]  += q; e->cash[buyer]  -= (int64_t)q * px;
                e->position[seller] -= q; e->cash[seller] += (int64_t)q * px;
                if (buyer == seller) e->self_trades++;
                e->trades++; e->volume += q; e->last_trade = px;
                ev(e, 2, (uint64_t)index, (uint64_t)m->id, ((uint64_t)(uint32_t)px << 32) | (uint32_t)q);
                remaining -= q; m->qty -= q;
                if (m->qty == 0) remove_at(e, b);
            }
            if (remaining > 0) {
                if (e->n == e->cap) {
                    e->cap = e->cap ? e->cap * 2 : 256;
                    e->orders = realloc(e->orders, (size_t)e->cap * sizeof(Order));
                    if (!e->orders) { perror("realloc"); exit(5); }
                }
                e->orders[e->n++] = (Order){ index, h->trader, h->side, h->price, remaining };
                ev(e, 3, (uint64_t)index, (uint64_t)(uint32_t)h->price, (uint64_t)(uint32_t)remaining);
            }
        }
    } else if (h->type == T_CANCEL) {
        int found = -1;
        for (int i = 0; i < e->n; i++) if (e->orders[i].id == h->cancel_id) { found = i; break; }
        if (found >= 0 && e->orders[found].owner == h->trader) {
            ev(e, 4, (uint64_t)index, (uint64_t)h->cancel_id, (uint64_t)(uint32_t)e->orders[found].qty);
            remove_at(e, found);
        } else {
            e->cancel_rejects++; ev(e, 5, (uint64_t)index, (uint64_t)h->cancel_id, 0);
        }
    } else {
        /* T_GATEWAY: no book effect. The digest derivation is payload-only
         * (never the offset field), so every replayer agrees. */
        ev(e, 6, h->seq, (uint64_t)h->trader, 0);
    }
    hist_push(e);        /* slot `applied` = state after applied+1 messages */
    e->applied++;
}

/* ── shared replay: poll → validate → claim check → apply ──────────────── */

typedef struct {
    Engine *e;
    const char *who;
    int fatal;                 /* set on any check failure */
    int64_t claims_checked;
    uint16_t self;             /* trader id of this process, or 0xFFFF */
    int strict_seq;            /* verify mode: also check gateway lane seq continuity */
    /* gateway lane tracking (verify only; zeroed otherwise) */
    int64_t  gw_msgs;
    int      gw_seen[MAX_TRADERS];
    uint64_t gw_next[MAX_TRADERS];
    uint64_t gw_last_seq[MAX_TRADERS];
    int64_t  gw_last_off[MAX_TRADERS];
    int      gw_inv_set;       /* first offset inversion in commit order */
    int      gw_inv_gw;
    uint64_t gw_inv_prev_seq, gw_inv_seq;
    int64_t  gw_inv_prev_off, gw_inv_off;
} Replay;

static MmapqNextMove on_msg(int64_t index, uint8_t appender_id, const void *data, int32_t len, void *ctx)
{
    (void)appender_id;
    Replay *r = ctx; Engine *e = r->e;
    if (index != e->applied) {
        fprintf(stderr, "%s: GAP expected index %lld got %lld\n", r->who, (long long)e->applied, (long long)index);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    MsgHdr h; const char *bad = validate(data, len, &h);
    if (bad) {
        fprintf(stderr, "%s: INVALID message at %lld (%s), len=%d\n", r->who, (long long)index, bad, len);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    if (h.type == T_GATEWAY) {
        /* gateway packet: no claim, no book effect; the digest derivation is
         * payload-only (engine_apply), so every replayer agrees. Verify also
         * checks the lane's seq continuity and records offset inversions. */
        if (r->strict_seq) {
            int g = h.trader;
            if (h.seq != r->gw_next[g]) {
                fprintf(stderr, "%s: GATEWAY SEQ gw %u at %lld: expected %llu got %llu (gap or inversion)\n",
                        r->who, h.trader, (long long)index,
                        (unsigned long long)r->gw_next[g], (unsigned long long)h.seq);
                r->fatal = 1; return MMAPQ_NEXT_STOP;
            }
            if (r->gw_seen[g] && !r->gw_inv_set && h.cancel_id < r->gw_last_off[g]) {
                r->gw_inv_set = 1; r->gw_inv_gw = g;
                r->gw_inv_prev_seq = r->gw_last_seq[g]; r->gw_inv_prev_off = r->gw_last_off[g];
                r->gw_inv_seq = h.seq; r->gw_inv_off = h.cancel_id;
            }
            r->gw_seen[g] = 1; r->gw_next[g] = h.seq + 1;
            r->gw_last_seq[g] = h.seq; r->gw_last_off[g] = h.cancel_id;
            r->gw_msgs++;
        }
        engine_apply(e, index, data, len, &h);
        return MMAPQ_NEXT_ADVANCE;
    }
    /* claim: the writer's state after seen_count messages */
    if (h.seen_count < 0 || h.seen_count > index) {
        fprintf(stderr, "%s: CAUSALITY at %lld: seen_count %lld\n", r->who, (long long)index, (long long)h.seen_count);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    uint64_t in, dg; hist_at(e, h.seen_count, &in, &dg);
    if (in != h.seen_input || dg != h.seen_digest) {
        fprintf(stderr, "%s: DIVERGENCE at %lld: trader %u inc %u claims state@%lld input=%016llx digest=%016llx, "
                        "mine input=%016llx digest=%016llx\n",
                r->who, (long long)index, h.trader, h.incarnation, (long long)h.seen_count,
                (unsigned long long)h.seen_input, (unsigned long long)h.seen_digest,
                (unsigned long long)in, (unsigned long long)dg);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    r->claims_checked++;
    /* per-trader sequence continuity across incarnations */
    if (h.seq != (uint64_t)e->next_seq[h.trader]) {
        fprintf(stderr, "%s: SEQ trader %u at %lld: expected %lld got %llu (inc %u)\n", r->who, h.trader,
                (long long)index, (long long)e->next_seq[h.trader], (unsigned long long)h.seq, h.incarnation);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    if (h.incarnation < e->last_inc[h.trader]) {
        fprintf(stderr, "%s: INCARNATION went backwards for trader %u at %lld\n", r->who, h.trader, (long long)index);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    e->next_seq[h.trader] = (int64_t)h.seq + 1;
    e->last_inc[h.trader] = h.incarnation;
    engine_apply(e, index, data, len, &h);
    return MMAPQ_NEXT_ADVANCE;
}

/* Poll until end of stream. Returns 0, or 3 on check failure, 4 on poll error. */
static int drain(MmapqIndexedPoller *p, Replay *r, int64_t *n_out)
{
    int64_t n = 0;
    for (;;) {
        MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, r);
        if (r->fatal) return 3;
        if (pr == MMAPQ_POLL_ADVANCED) { n++; continue; }
        if (pr == MMAPQ_POLL_END_OF_STREAM) break;
        fprintf(stderr, "%s: poll returned %d (last_error %d) at %lld\n", r->who, (int)pr,
                mmapq_indexed_poller_last_error(p), (long long)r->e->applied);
        return 4;
    }
    if (n_out) *n_out = n;
    return 0;
}

/* ── batch lane replay: poll → per-message validate → claim check → apply ── */

typedef struct {
    Engine *e;
    const char *who;
    int fatal;
    int64_t claims_checked;
    int64_t batches;           /* batches applied; also the expected next index */
    int reverse;               /* MKTSIM_BATCH_REORDER: planted defect */
    int partial;               /* MKTSIM_BATCH_PARTIAL_READ: planted defect */
} BReplay;

/* One batch per poll; framing/CRC/claims are validated per message, batch
 * indices must be contiguous, and per-trader batch seqs must be continuous
 * across incarnations — all over the batch lane's own message count. */
static MmapqNextMove on_batch(int64_t bi, MmapqBatchReadingContext *c, void *ctx)
{
    BReplay *r = ctx; Engine *e = r->e;
    if (bi != r->batches) {
        fprintf(stderr, "%s: BATCH GAP expected batch %lld got %lld\n", r->who,
                (long long)r->batches, (long long)bi);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    const void *d[MAX_BATCH_MSGS]; int32_t l[MAX_BATCH_MSGS]; int n = 0;
    for (;;) {
        int rc = mmapq_batch_iterator_next(c->iterator, &d[n], &l[n]);
        if (rc == 0) break;
        if (rc < 0) {
            fprintf(stderr, "%s: batch %lld iterator error %d\n", r->who, (long long)bi, rc);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        if (++n == MAX_BATCH_MSGS) {
            fprintf(stderr, "%s: batch %lld has %d+ messages\n", r->who, (long long)bi, n);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
    }
    if (n != c->message_count) {
        fprintf(stderr, "%s: batch %lld declares %d messages, iterated %d\n",
                r->who, (long long)bi, c->message_count, n);
        r->fatal = 1; return MMAPQ_NEXT_STOP;
    }
    /* framing, claim and sequence checks run in committed order */
    MsgHdr hh[MAX_BATCH_MSGS];
    for (int i = 0; i < n; i++) {
        const char *bad = validate(d[i], l[i], &hh[i]);
        if (bad) {
            fprintf(stderr, "%s: INVALID batch message at batch %lld msg %d (%s), len=%d\n",
                    r->who, (long long)bi, i, bad, l[i]);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        if (hh[i].type == T_GATEWAY) continue;
        if (hh[i].seen_count < 0 || hh[i].seen_count > e->applied) {
            fprintf(stderr, "%s: CAUSALITY at batch %lld msg %d: seen_count %lld\n",
                    r->who, (long long)bi, i, (long long)hh[i].seen_count);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        uint64_t in, dg; hist_at(e, hh[i].seen_count, &in, &dg);
        if (in != hh[i].seen_input || dg != hh[i].seen_digest) {
            fprintf(stderr, "%s: DIVERGENCE at batch %lld msg %d: trader %u inc %u claims state@%lld "
                            "input=%016llx digest=%016llx, mine input=%016llx digest=%016llx\n",
                    r->who, (long long)bi, i, hh[i].trader, hh[i].incarnation, (long long)hh[i].seen_count,
                    (unsigned long long)hh[i].seen_input, (unsigned long long)hh[i].seen_digest,
                    (unsigned long long)in, (unsigned long long)dg);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        r->claims_checked++;
        if (hh[i].seq != (uint64_t)e->next_seq[hh[i].trader]) {
            fprintf(stderr, "%s: BATCH SEQ trader %u at batch %lld: expected %lld got %llu (inc %u)\n",
                    r->who, hh[i].trader, (long long)bi,
                    (long long)e->next_seq[hh[i].trader], (unsigned long long)hh[i].seq, hh[i].incarnation);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        if (hh[i].incarnation < e->last_inc[hh[i].trader]) {
            fprintf(stderr, "%s: INCARNATION went backwards for trader %u at batch %lld\n",
                    r->who, hh[i].trader, (long long)bi);
            r->fatal = 1; return MMAPQ_NEXT_STOP;
        }
        e->next_seq[hh[i].trader] = (int64_t)hh[i].seq + 1;
        e->last_inc[hh[i].trader] = hh[i].incarnation;
    }
    /* engine application, with the two demo-side batch-discipline plants:
     * MKTSIM_BATCH_REORDER applies each batch's messages in reverse order;
     * MKTSIM_BATCH_PARTIAL_READ applies only the first message of a
     * multi-message batch. Both diverge the batch-lane digest. */
    int upto = (r->partial && n >= 2) ? 1 : n;
    for (int k = 0; k < upto; k++) {
        int i = r->reverse ? n - 1 - k : k;
        engine_apply(e, e->applied, d[i], l[i], &hh[i]);
    }
    r->batches++;
    return MMAPQ_NEXT_ADVANCE;
}

/* Poll the batch lane until end of stream. Same return discipline as drain. */
static int drain_batch(MmapqIndexedBatchPoller *p, BReplay *r, int64_t *n_out)
{
    int64_t n = 0;
    for (;;) {
        BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, on_batch, r);
        if (r->fatal) return 3;
        if (pr == MMAPQ_POLL_ADVANCED) { n++; continue; }
        if (pr == MMAPQ_POLL_END_OF_STREAM) break;
        fprintf(stderr, "%s: batch poll returned %d (last_error %d) at batch %lld\n", r->who, (int)pr,
                mmapq_indexed_batch_poller_last_error(p), (long long)r->batches);
        return 4;
    }
    if (n_out) *n_out = n;
    return 0;
}

/* ── trader ─────────────────────────────────────────────────────────────── */

static uint64_t rng_state;
static uint64_t rnd(void) { rng_state += 0x9E3779B97F4A7C15ULL; return mix64(rng_state); }
static int32_t rnd_range(int32_t lo, int32_t hi) { return lo + (int32_t)(rnd() % (uint64_t)(hi - lo + 1)); }

static int32_t build_msg(uint8_t *buf, Engine *e, uint16_t trader, uint32_t inc,
                         uint64_t seq, int type, int32_t side, int32_t price, int32_t qty, int64_t cancel_id)
{
    MsgHdr h = {0};
    h.magic = MAGIC; h.type = (uint16_t)type; h.trader = trader; h.incarnation = inc;
    /* mostly small, occasionally large: exercises region-end padding at 64 KiB */
    h.pad_len = (rnd() % 8 == 0) ? (uint32_t)rnd_range(200, MAX_PAD) : (uint32_t)rnd_range(0, 40);
    h.seq = seq; h.seen_count = e->applied;
    hist_at(e, e->applied, &h.seen_input, &h.seen_digest);
    h.side = side; h.price = price; h.qty = qty; h.cancel_id = cancel_id;
    memcpy(buf, &h, sizeof h);
    for (uint32_t i = 0; i < h.pad_len; i++) buf[sizeof h + i] = pad_byte(trader, seq, i);
    size_t body = sizeof h + h.pad_len;
    uint64_t crc = fnv64(buf, body); memcpy(buf + body, &crc, 8);
    return (int32_t)(body + 8);
}

static int publish(MmapqIndexedAppender *a, Engine *e, uint16_t trader, uint32_t inc,
                   uint64_t seq, int type, int32_t side, int32_t price, int32_t qty, int64_t cancel_id)
{
    uint8_t buf[sizeof(MsgHdr) + MAX_PAD + 8];
    int32_t len = build_msg(buf, e, trader, inc, seq, type, side, price, qty, cancel_id);
    int64_t idx = mmapq_indexed_appender_append(a, buf, len);
    if (idx < 0) { fprintf(stderr, "trader %u: append failed rc=%lld\n", trader, (long long)idx); return 2; }
    return 0;
}

/* Mirror one intent (nmsg == 1) or the MAX_LIVE [NEW, CANCEL] pair
 * (nmsg == 2) onto the batch lane as one atomic batch. Batch messages carry
 * per-trader batch seqs (bseq, bseq+1) and claims over the batch engine. */
static int publish_batch(MmapqIndexedBatchAppender *ba, Engine *be, uint16_t trader, uint32_t inc,
                         uint64_t bseq, int nmsg,
                         int type1, int32_t side1, int32_t price1, int32_t qty1, int64_t cancel_id1,
                         int type2, int32_t side2, int32_t price2, int32_t qty2, int64_t cancel_id2)
{
    uint8_t buf1[sizeof(MsgHdr) + MAX_PAD + 8], buf2[sizeof(MsgHdr) + MAX_PAD + 8];
    int32_t len1 = build_msg(buf1, be, trader, inc, bseq, type1, side1, price1, qty1, cancel_id1);
    int32_t len2 = 0;
    if (nmsg == 2) len2 = build_msg(buf2, be, trader, inc, bseq + 1, type2, side2, price2, qty2, cancel_id2);
    MmapqMutableBatch *b = NULL;
    int rc = mmapq_indexed_batch_appender_start_batch_checked(ba, &b);
    if (rc) { fprintf(stderr, "trader %u: batch start rc=%d\n", trader, rc); return 2; }
    if (mmapq_mutable_batch_append(b, buf1, len1) < 0 ||
        (nmsg == 2 && mmapq_mutable_batch_append(b, buf2, len2) < 0)) {
        fprintf(stderr, "trader %u: batch append failed\n", trader);
        mmapq_mutable_batch_discard(b);
        return 2;
    }
    int64_t idx = mmapq_mutable_batch_complete(b);
    if (idx < 0) { fprintf(stderr, "trader %u: batch complete rc=%lld\n", trader, (long long)idx); return 2; }
    return 0;
}

static int run_trader(const char *dir, const char *qname, int trader, uint32_t inc,
                      const char *stopfile, int async, int want_id, int batch_on)
{
    char who[64]; snprintf(who, sizeof who, "trader %d/%u", trader, inc);
    rng_state = mix64(((uint64_t)trader << 32) ^ inc ^ (uint64_t)getpid());

    MmapqRuntime *rt = NULL;
    if (async && mmapq_runtime_region_mapper(&rt) != 0) { fprintf(stderr, "%s: runtime\n", who); return 2; }
    MmapqIndexedQueue *q = NULL;
    int rc = mmapq_indexed_queue_create(dir, qname, REGION_SIZE, RING_SIZE, 1, MAX_FILE_SIZE, 0,
                                        TIMEOUT_NS, 0, rt, rt, &q);
    if (rc) { fprintf(stderr, "%s: queue create rc=%d\n", who, rc); return 2; }
    MmapqIndexedAppender *a = NULL;
    if (want_id >= 0) {
        /* Fixed id assigned by the supervisor. On a restart this lane was
         * reclaimed after the previous incarnation was reaped, so creation runs
         * the lane's crash recovery (the killed writer may have left an
         * uncommitted tail). A conflict means two live processes were given one
         * id — a supervisor bug, so fail rather than fall back. */
        rc = mmapq_indexed_queue_create_appender_with_id(q, (uint8_t)want_id, &a);
        if (rc == MMAPQ_EIDCONFLICT) printf("IDCONFLICT %d %u %d\n", trader, inc, want_id);
    } else {
        rc = mmapq_indexed_queue_create_appender(q, &a);
    }
    if (rc) { fprintf(stderr, "%s: appender create rc=%d (id pool exhausted?)\n", who, rc); return 2; }
    int appender_id = mmapq_indexed_appender_id(a);
    printf("ACQUIRED %d %u %d\n", trader, inc, appender_id);
    fflush(stdout);
    MmapqIndexedPoller *p = NULL;
    if (mmapq_indexed_queue_create_poller(q, &p)) { fprintf(stderr, "%s: poller\n", who); return 2; }

    /* F4: the batch lane. Same fixed-id scheme as the single-message lane
     * (the supervisor unsets the id on BOTH queues after reaping). */
    MmapqIndexedBatchQueue *bq = NULL;
    MmapqIndexedBatchAppender *bapp = NULL;
    MmapqIndexedBatchPoller *bp = NULL;
    Engine be; BReplay br; int baid = -1;
    if (batch_on) {
        rc = mmapq_indexed_batch_queue_create(dir, BATCH_QNAME, REGION_SIZE, RING_SIZE, 1,
                                              MAX_FILE_SIZE, 0, TIMEOUT_NS, 0, rt, rt, &bq);
        if (rc) { fprintf(stderr, "%s: batch queue create rc=%d\n", who, rc); return 2; }
        if (want_id >= 0) {
            rc = mmapq_indexed_batch_queue_create_appender_with_id(bq, (uint8_t)want_id, &bapp);
            if (rc == MMAPQ_EIDCONFLICT) printf("IDCONFLICT %d %u %d\n", trader, inc, want_id);
        } else {
            rc = mmapq_indexed_batch_queue_create_appender(bq, &bapp);
        }
        if (rc) { fprintf(stderr, "%s: batch appender create rc=%d\n", who, rc); return 2; }
        baid = mmapq_indexed_batch_appender_id(bapp);
        if (mmapq_indexed_batch_queue_create_poller(bq, &bp)) { fprintf(stderr, "%s: batch poller\n", who); return 2; }
        engine_init(&be);
        br = (BReplay){ .e = &be, .who = who };
        const char *bug = getenv("MKTSIM_BATCH_REORDER");
        br.reverse = bug && atoi(bug) != 0;
        const char *bug2 = getenv("MKTSIM_BATCH_PARTIAL_READ");
        br.partial = bug2 && atoi(bug2) != 0;
    }

    Engine e; engine_init(&e);
    Replay r = { .e = &e, .who = who, .self = (uint16_t)trader };

    /* JOIN: learn the last committed index, rebuild exactly up to it. */
    MmapqIndexedReader *rd = NULL;
    if (mmapq_indexed_queue_create_reader(q, &rd)) { fprintf(stderr, "%s: reader\n", who); return 2; }
    int64_t last = mmapq_indexed_reader_last_index(rd);
    mmapq_indexed_reader_close(rd);
    while (e.applied <= last) {
        MmapqPollResult pr = mmapq_indexed_poller_poll(p, on_msg, &r);
        if (r.fatal) return 3;
        if (pr != MMAPQ_POLL_ADVANCED) {
            fprintf(stderr, "%s: rebuild poll %d at %lld (target last=%lld)\n", who, (int)pr,
                    (long long)e.applied, (long long)last);
            return 4;
        }
    }
    uint64_t in, dg; hist_at(&e, e.applied, &in, &dg);
    printf("JOIN %d %u %d %lld %016llx %016llx\n", trader, inc, appender_id,
           (long long)e.applied, (unsigned long long)in, (unsigned long long)dg);
    fflush(stdout);

    /* Batch lane JOIN: rebuild the batch engine to the last committed batch. */
    uint64_t bseq = 0;
    if (batch_on) {
        MmapqIndexedBatchReader *brd = NULL;
        if (mmapq_indexed_batch_queue_create_reader(bq, &brd)) { fprintf(stderr, "%s: batch reader\n", who); return 2; }
        int64_t blast = mmapq_indexed_batch_reader_last_index(brd);
        mmapq_indexed_batch_reader_close(brd);
        while (br.batches <= blast) {
            BatchPollResult pr = mmapq_indexed_batch_poller_poll(bp, on_batch, &br);
            if (br.fatal) return 3;
            if (pr != MMAPQ_POLL_ADVANCED) {
                fprintf(stderr, "%s: batch rebuild poll %d at %lld (target last=%lld)\n", who, (int)pr,
                        (long long)br.batches, (long long)blast);
                return 4;
            }
        }
        hist_at(&be, be.applied, &in, &dg);
        printf("BATCHJOIN %d %u %d %lld %016llx %016llx\n", trader, inc, baid,
               (long long)be.applied, (unsigned long long)in, (unsigned long long)dg);
        fflush(stdout);
        bseq = (uint64_t)be.next_seq[trader];
    }

    /* Resume this trader's sequence from the log, not from local memory. */
    uint64_t seq = (uint64_t)e.next_seq[trader];
    int taker = trader % 4 == 0;
    int rcode = 0;

    while (access(stopfile, F_OK) != 0) {
        if ((rcode = drain(p, &r, NULL))) break;
        if (batch_on && (rcode = drain_batch(bp, &br, NULL))) break;

        /* decide from the derived view: own live orders, top of book */
        int own = 0, oldest = -1;
        for (int i = 0; i < e.n; i++) if (e.orders[i].owner == trader) { if (oldest < 0) oldest = i; own++; }
        int bb = best_index(&e, 1), ba = best_index(&e, -1);
        int32_t bid = bb >= 0 ? e.orders[bb].price : e.last_trade - 2;
        int32_t ask = ba >= 0 ? e.orders[ba].price : e.last_trade + 2;
        int32_t mid = (bid + ask) / 2; if (mid < 50) mid = 50;

        if (own >= MAX_LIVE && oldest >= 0) {
            int64_t cid = e.orders[oldest].id;
            if (batch_on) {
                /* F4: cancel the oldest to make room for a NEW — on the batch
                 * lane the pair [NEW, CANCEL] commits as one atomic batch. */
                int32_t side = (rnd() & 1) ? 1 : -1;
                int32_t px = side > 0 ? mid - rnd_range(1, 4) : mid + rnd_range(1, 4);
                if (px < 1) px = 1;
                int32_t qty = rnd_range(1, 10);
                rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_NEW, side, px, qty, 0);
                if (!rcode) rcode = publish(a, &e, (uint16_t)trader, inc, seq + 1, T_CANCEL, 0, 0, 0, cid);
                if (!rcode) rcode = publish_batch(bapp, &be, (uint16_t)trader, inc, bseq, 2,
                                                  T_NEW, side, px, qty, 0, T_CANCEL, 0, 0, 0, cid);
                seq += 2; bseq += 2;
            } else {
                rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_CANCEL, 0, 0, 0, cid);
                seq++;
            }
        } else if (taker) {
            int32_t side = (rnd() & 1) ? 1 : -1;
            int32_t px = side > 0 ? ask + rnd_range(0, 2) : bid - rnd_range(0, 2);
            if (px < 1) px = 1;
            int32_t qty = rnd_range(1, 5);
            rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_NEW, side, px, qty, 0);
            if (!rcode && batch_on) rcode = publish_batch(bapp, &be, (uint16_t)trader, inc, bseq, 1,
                                                          T_NEW, side, px, qty, 0, 0, 0, 0, 0, 0);
            seq++; bseq++;
        } else if (rnd() % 16 == 0 && own > 0) {
            /* occasionally cancel a random one of our own (exercises owner check) */
            int k = (int)(rnd() % (uint64_t)own), seen = 0;
            for (int i = 0; i < e.n; i++) if (e.orders[i].owner == trader && seen++ == k) {
                rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_CANCEL, 0, 0, 0, e.orders[i].id);
                if (!rcode && batch_on) rcode = publish_batch(bapp, &be, (uint16_t)trader, inc, bseq, 1,
                                                              T_CANCEL, 0, 0, 0, e.orders[i].id,
                                                              0, 0, 0, 0, 0);
                seq++; bseq++;
                break; }
        } else {
            int32_t side = (rnd() & 1) ? 1 : -1;
            int32_t px = side > 0 ? mid - rnd_range(1, 4) : mid + rnd_range(1, 4);
            if (px < 1) px = 1;
            int32_t qty = rnd_range(1, 10);
            rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_NEW, side, px, qty, 0);
            if (!rcode && batch_on) rcode = publish_batch(bapp, &be, (uint16_t)trader, inc, bseq, 1,
                                                          T_NEW, side, px, qty, 0, 0, 0, 0, 0, 0);
            seq++; bseq++;
        }
        if (rcode) break;
        /* jitter: interleave with other processes */
        if (rnd() % 4 == 0) { struct timespec ts = {0, (long)(rnd() % 200000)}; nanosleep(&ts, NULL); }
        else sched_yield();
    }

    /* Two-phase stop: announce we have stopped publishing, wait until the
     * driver confirms every survivor has, then read to the end. All survivors
     * then report the same final count — the whole log. */
    if (rcode == 0) {
        printf("QUIESCED %d %u\n", trader, inc);
        fflush(stdout);
        char final_flag[1024];
        snprintf(final_flag, sizeof final_flag, "%s.final", stopfile);
        while (access(final_flag, F_OK) != 0) {
            struct timespec ts = {0, 5000000}; nanosleep(&ts, NULL);
        }
        rcode = drain(p, &r, NULL);
        if (!rcode && batch_on) rcode = drain_batch(bp, &br, NULL);
    }
    hist_at(&e, e.applied, &in, &dg);
    printf("FINAL %d %u %d %lld %016llx %016llx claims=%lld rc=%d\n", trader, inc, appender_id,
           (long long)e.applied, (unsigned long long)in, (unsigned long long)dg, (long long)r.claims_checked, rcode);
    if (batch_on) {
        hist_at(&be, be.applied, &in, &dg);
        printf("BATCHFINAL %d %u %d %lld %016llx %016llx claims=%lld rc=%d\n", trader, inc, baid,
               (long long)be.applied, (unsigned long long)in, (unsigned long long)dg,
               (long long)br.claims_checked, rcode);
    }
    fflush(stdout);
    if (batch_on) {
        mmapq_indexed_batch_poller_close(bp);
        mmapq_indexed_batch_appender_close(bapp);
        mmapq_indexed_batch_queue_close(bq);
        engine_free(&be);
    }
    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    engine_free(&e);
    return rcode;
}

/* ── gateway (F5): arrival-order append, sequence-order commit ──────────── */

#define GW_DELAY_PERIOD 7    /* every 7th packet of the stream ... */
#define GW_DELAY        2    /* ... arrives 2 positions late (fixed permutation) */
#define GW_MAX_PENDING  64

typedef struct { uint64_t seq; int64_t off; int32_t len; int32_t aid; } GwRec;

typedef struct {
    GwRec    pend[GW_MAX_PENDING]; int npend;   /* appended but not committed */
    uint64_t next_emit;            /* next seq the arrival generator considers */
    int64_t  delayed;              /* seq held by the permutation (-1: none) */
    int      delay_left;
    int64_t  next_off;             /* predicted lane write position */
    uint64_t emitted, committed;
    int64_t  walked;               /* rejoin stats */
    int      reappended;
} Gw;

/* Predicted payload offset of the next record on this lane: records are
 * contiguous, skipping to the next region boundary when the remaining region
 * capacity cannot hold [length][payload]. This mirrors the single-message
 * payload framing; every append self-checks the prediction against the
 * returned offset and fails loudly on drift. */
static int64_t gw_predict(int64_t end, int32_t len)
{
    int64_t rem = REGION_SIZE - end % REGION_SIZE;
    if (rem < (int64_t)len + 4) end += rem;
    return end;
}

static int gw_pend_find(const Gw *g, uint64_t seq)
{
    for (int i = 0; i < g->npend; i++) if (g->pend[i].seq == seq) return i;
    return -1;
}

static void gw_pend_del(Gw *g, int i)
{
    g->pend[i] = g->pend[--g->npend];
}

/* The gateway's own log (survives SIGKILL, read by the next incarnation):
 * GAPAPPEND/GAPCOMMIT events, and after every append/commit the current
 * uncommitted set as GAPUNCOMMITTED lines, flushed. */
static void gw_snap(FILE *f, const Gw *g)
{
    for (int i = 0; i < g->npend; i++)
        fprintf(f, "GAPUNCOMMITTED %llu %lld\n",
                (unsigned long long)g->pend[i].seq, (long long)g->pend[i].off);
    fprintf(f, "GAPEND\n");
    fflush(f);
}

static void gw_log_append(FILE *f, Gw *g, uint64_t seq, int64_t off, int32_t len)
{
    fprintf(f, "GAPAPPEND %llu %lld %d\n", (unsigned long long)seq, (long long)off, len);
    gw_snap(f, g);
}

static void gw_log_commit(FILE *f, Gw *g, uint64_t seq)
{
    fprintf(f, "GAPCOMMIT %llu\n", (unsigned long long)seq);
    gw_snap(f, g);
}

/* The arrival permutation: every 7th packet is held for 2 arrival positions. */
static uint64_t gw_next_arrival(Gw *g)
{
    for (;;) {
        if (g->delayed >= 0 && g->delay_left == 0) {
            uint64_t s = (uint64_t)g->delayed; g->delayed = -1; return s;
        }
        uint64_t s = g->next_emit++;
        if (s % GW_DELAY_PERIOD == GW_DELAY_PERIOD - 1) { g->delayed = (int64_t)s; g->delay_left = GW_DELAY; continue; }
        if (g->delayed >= 0) g->delay_left--;
        return s;
    }
}

static int32_t gw_build(uint8_t *buf, uint16_t gw, uint32_t inc, uint64_t seq, int64_t off, int32_t pad)
{
    MsgHdr h = {0};
    h.magic = MAGIC; h.type = T_GATEWAY; h.trader = gw; h.incarnation = inc;
    h.pad_len = (uint32_t)pad;
    h.seq = seq; h.seen_count = -1;      /* gateway packets carry no claim */
    h.cancel_id = off;                   /* this packet's own payload offset */
    memcpy(buf, &h, sizeof h);
    for (int32_t i = 0; i < pad; i++) buf[sizeof h + i] = pad_byte(gw, seq, (uint32_t)i);
    size_t body = sizeof h + h.pad_len;
    uint64_t crc = fnv64(buf, body); memcpy(buf + body, &crc, 8);
    return (int32_t)(body + 8);
}

/* Scenario hooks (owner, stage-b rework): the gateway SIGKILLs itself
 * between the queue operation and its log line, once per run, on the fifth
 * append / fifth commit. These prove the two crash windows in which the gap
 * log is behind the queue; the restart must reconcile from the queue. */
static int gw_hook_after_append, gw_hook_after_commit;

/* Append packet `seq` uncommitted (arrival order), log it, remember it. */
static int gw_emit(MmapqIndexedAppender *a, FILE *lf, Gw *g, uint16_t gw, uint32_t inc, uint64_t seq)
{
    uint8_t buf[sizeof(MsgHdr) + MAX_PAD + 8];
    int32_t pad = rnd_range(0, 40);
    int32_t len = (int32_t)sizeof(MsgHdr) + pad + 8;
    int64_t want = gw_predict(g->next_off, len);
    int32_t len2 = gw_build(buf, gw, inc, seq, want, pad);
    int64_t off = mmapq_indexed_appender_append_uncommitted(a, buf, len2);
    if (off < 0) { fprintf(stderr, "gateway %u: append_uncommitted rc=%lld\n", gw, (long long)off); return 2; }
    if (off != want) {
        fprintf(stderr, "gateway %u: OFFSET predicted %lld got %lld (framing drift?)\n",
                gw, (long long)want, (long long)off);
        return 2;
    }
    if (g->npend == GW_MAX_PENDING) { fprintf(stderr, "gateway %u: pending table full\n", gw); return 2; }
    g->next_off = off + 4 + len2;
    g->pend[g->npend++] = (GwRec){ seq, off, len2, 0 };
    if (gw_hook_after_append && g->emitted == 4) {   /* appended, not yet logged */
        fflush(lf);
        raise(SIGKILL);
    }
    gw_log_append(lf, g, seq, off, len2);
    g->emitted++;
    return 0;
}

static int gw_commit_one(MmapqIndexedAppender *a, FILE *lf, Gw *g, int pi, uint16_t gw)
{
    uint64_t seq = g->pend[pi].seq; int64_t off = g->pend[pi].off;
    int64_t idx = mmapq_indexed_appender_commit(a, off);
    if (idx < 0) {
        fprintf(stderr, "gateway %u: commit seq %llu off %lld rc=%lld\n",
                gw, (unsigned long long)seq, (long long)off, (long long)idx);
        return 2;
    }
    gw_pend_del(g, pi);
    if (gw_hook_after_commit && g->committed == 4) {   /* committed, not yet logged */
        fflush(lf);
        raise(SIGKILL);
    }
    gw_log_commit(lf, g, seq);
    g->committed++;
    return 0;
}

/* A GAPDISCARD line records a torn-tail discard at a rejoin: the lane's
 * highest-offset uncommitted record was discarded (never committed) and its
 * seq re-appended. Discarded records stay uncommitted on the lane forever, so
 * later walks still yield them; the log is what exempts them. */
static void gw_log_discard(FILE *f, uint64_t seq, int64_t off)
{
    fprintf(f, "GAPDISCARD %llu %lld\n", (unsigned long long)seq, (long long)off);
    fflush(f);
}

/* The parsed gap log: every append carries the lane (appender id) it landed
 * on, so a rejoin can tell its own lane's records from a dead lane's. */
typedef struct {
    GwRec   *app; int napp, capa;  /* every GAPAPPEND, in file order */
    uint64_t *com; int ncom;       /* every GAPCOMMIT seq */
    int64_t *dis; int ndis;        /* every GAPDISCARD offset */
    int      last_aid;             /* lane of the previous incarnation (-1: none) */
} GwLog;

static void *gw_grow(void *p, int *cap, int n, size_t sz)
{
    if (n < *cap) return p;
    *cap = *cap ? *cap * 2 : 256;
    p = realloc(p, (size_t)*cap * sz);
    if (!p) { perror("realloc"); exit(5); }
    return p;
}

/* Replay the gap log. A final partial line (kill mid-write) is the file-level
 * torn tail: discarded. */
static int gw_load(const char *path, uint16_t gw, GwLog *L)
{
    memset(L, 0, sizeof *L);
    L->last_aid = -1;
    FILE *f = fopen(path, "r");
    if (!f) return 0;    /* no file: nothing was ever appended */
    int aid = -1, capc = 0, capd = 0;
    char line[256];
    while (fgets(line, sizeof line, f)) {
        size_t n = strlen(line);
        if (n == 0 || line[n - 1] != '\n') break;   /* torn tail line */
        unsigned long long s; long long o; int v;
        if (sscanf(line, "GAPLANE %d", &v) == 1) {
            aid = v; L->last_aid = v;
        } else if (sscanf(line, "GAPAPPEND %llu %lld %d", &s, &o, &v) == 3) {
            L->app = gw_grow(L->app, &L->capa, L->napp, sizeof *L->app);
            L->app[L->napp++] = (GwRec){ s, o, v, aid };
        } else if (sscanf(line, "GAPCOMMIT %llu", &s) == 1) {
            L->com = gw_grow(L->com, &capc, L->ncom, sizeof *L->com);
            L->com[L->ncom++] = s;
        } else if (sscanf(line, "GAPDISCARD %llu %lld", &s, &o) == 2) {
            L->dis = gw_grow(L->dis, &capd, L->ndis, sizeof *L->dis);
            L->dis[L->ndis++] = o;
        } else if (!strncmp(line, "GAPUNCOMMITTED", 14) || !strncmp(line, "GAPEND", 6)) {
            /* snapshot lines: the record; the event stream is authoritative */
        } else {
            fprintf(stderr, "gateway %u: gap log %s corrupt: %s", gw, path, line);
            fclose(f);
            return 2;
        }
    }
    fclose(f);
    return 0;
}

static int gw_in_dis(const GwLog *L, int64_t off)
{
    for (int i = 0; i < L->ndis; i++) if (L->dis[i] == off) return 1;
    return 0;
}

static int run_gateway(const char *dir, const char *qname, int gw, uint32_t inc,
                       const char *stopfile, int async, int want_id)
{
    char who[64]; snprintf(who, sizeof who, "gateway %d/%u", gw, inc);
    rng_state = mix64(((uint64_t)gw << 32) ^ inc ^ (uint64_t)getpid());   /* timing jitter only */

    MmapqRuntime *rt = NULL;
    if (async && mmapq_runtime_region_mapper(&rt) != 0) { fprintf(stderr, "%s: runtime\n", who); return 2; }
    MmapqIndexedQueue *q = NULL;
    int rc = mmapq_indexed_queue_create(dir, qname, REGION_SIZE, RING_SIZE, 1, MAX_FILE_SIZE, 0,
                                        TIMEOUT_NS, 0, rt, rt, &q);
    if (rc) { fprintf(stderr, "%s: queue create rc=%d\n", who, rc); return 2; }
    MmapqIndexedAppender *a = NULL;
    if (want_id >= 0) {
        /* fixed id from the supervisor; see run_trader for the contract */
        rc = mmapq_indexed_queue_create_appender_with_id(q, (uint8_t)want_id, &a);
        if (rc == MMAPQ_EIDCONFLICT) printf("IDCONFLICT %d %u %d\n", gw, inc, want_id);
    } else {
        rc = mmapq_indexed_queue_create_appender(q, &a);
    }
    if (rc) { fprintf(stderr, "%s: appender create rc=%d\n", who, rc); return 2; }
    int appender_id = mmapq_indexed_appender_id(a);

    const char *bug = getenv("MKTSIM_COMMIT_BEFORE_GAP");
    int commit_before_gap = bug && atoi(bug) != 0;
    const char *ha = getenv("MKTSIM_GW_KILL_AFTER_APPEND");
    gw_hook_after_append = ha && atoi(ha) != 0;
    const char *hc = getenv("MKTSIM_GW_KILL_AFTER_COMMIT");
    gw_hook_after_commit = hc && atoi(hc) != 0;

    char gapath[1100];
    snprintf(gapath, sizeof gapath, "%s/%s.gw%02d.gap", dir, qname, gw);
    GwLog L;
    if ((rc = gw_load(gapath, (uint16_t)gw, &L))) return rc;
    FILE *lf = fopen(gapath, "a");
    if (!lf) { fprintf(stderr, "%s: cannot open %s\n", who, gapath); return 2; }
    fprintf(lf, "GAPLANE %d\n", appender_id);
    fflush(lf);
    /* Without --reclaim a restart lands on a NEW lane (the dead lane's id is
     * never released): its stranded uncommitted seqs must be re-appended.
     * With --reclaim the rejoin is on the SAME lane and runs the restart
     * walk instead. */
    int same_lane = L.last_aid == appender_id;

    Gw g; memset(&g, 0, sizeof g);
    g.delayed = -1;
    g.next_off = mmapq_indexed_appender_start_offset(a);

    /* The queue is the source of truth (owner, stage-b rework): committed
     * seqs are derived by scanning this gateway's committed records in the
     * queue. The gap log's GAPCOMMIT lines are hints only — a kill between
     * a commit and its log line leaves the log short, and the queue is
     * right. */
    uint64_t *qcom = NULL; int nqcom = 0, capqc = 0;
    MmapqIndexedReader *qrd = NULL;
    if (mmapq_indexed_queue_create_reader(q, &qrd)) { fprintf(stderr, "%s: reader\n", who); return 2; }
    int64_t qfirst = mmapq_indexed_reader_start_index(qrd);
    int64_t qlast = mmapq_indexed_reader_last_index(qrd);
    for (int64_t i = qfirst; i <= qlast; i++) {
        MmapqReadingContext ctx = {0};
        mmapq_indexed_reader_read(qrd, i, &ctx);
        /* Status discipline (owner, stage b): AVAILABLE → use; ARCHIVED →
         * skip; anything else must never be skipped silently — a missed
         * committed seq is re-committed by the restart → a duplicate. */
        if (ctx.status == MMAPQ_ENTRY_AVAILABLE) {
            MsgHdr h;
            if (validate(ctx.data, ctx.length, &h)) continue;  /* verify catches committed corruption */
            if (h.type == T_GATEWAY && h.trader == (uint16_t)gw) {
                qcom = gw_grow(qcom, &capqc, nqcom, sizeof *qcom);
                qcom[nqcom++] = h.seq;
            }
            continue;
        }
        if (ctx.status == MMAPQ_ENTRY_ARCHIVED) continue;
        if (ctx.status == MMAPQ_ENTRY_ERROR
            && mmapq_indexed_reader_last_error(qrd) == MMAPQ_ETIMEDOUT) {
            int retries = 0;
            do {
                struct timespec ts = {0, 1000000}; nanosleep(&ts, NULL);
                memset(&ctx, 0, sizeof ctx);
                mmapq_indexed_reader_read(qrd, i, &ctx);
            } while (ctx.status == MMAPQ_ENTRY_ERROR
                     && mmapq_indexed_reader_last_error(qrd) == MMAPQ_ETIMEDOUT
                     && ++retries < 5);
            if (ctx.status == MMAPQ_ENTRY_AVAILABLE) { i--; continue; }  /* reprocess */
            if (ctx.status == MMAPQ_ENTRY_ARCHIVED) continue;
        }
        fprintf(stderr, "%s: committed scan: index %lld status %d (last_error %d) — refusing to restart\n",
                who, (long long)i, (int)ctx.status, mmapq_indexed_reader_last_error(qrd));
        return 2;
    }
    mmapq_indexed_reader_close(qrd);

    if (L.napp > 0 || nqcom > 0) {
        uint64_t maxseq = 0;
        for (int i = 0; i < L.napp; i++) if (L.app[i].seq > maxseq) maxseq = L.app[i].seq;
        for (int i = 0; i < nqcom; i++) if (qcom[i] > maxseq) maxseq = qcom[i];
        int64_t *latest_app = malloc((maxseq + 1) * sizeof *latest_app);
        uint8_t *is_committed = calloc(maxseq + 1, 1);
        int64_t *exp_off = malloc((size_t)(L.napp + 1) * sizeof *exp_off);
        uint64_t *exp_seq = malloc((size_t)(L.napp + 1) * sizeof *exp_seq);
        int *emark = calloc((size_t)(L.napp + 1), sizeof *emark);
        if (!latest_app || !is_committed || !exp_off || !exp_seq || !emark) { perror("alloc"); return 5; }
        for (uint64_t s = 0; s <= maxseq; s++) latest_app[s] = -1;
        for (int i = 0; i < L.napp; i++) latest_app[L.app[i].seq] = i;
        for (int i = 0; i < nqcom; i++) is_committed[qcom[i]] = 1;   /* queue truth */
        /* hint consistency: a logged commit the queue does not show means the
         * log is ahead of the queue — corrupt. */
        for (int i = 0; i < L.ncom; i++)
            if (L.com[i] <= maxseq && !is_committed[L.com[i]]) {
                fprintf(stderr, "%s: GWALK MISMATCH: gap log calls seq %llu committed but the queue does not have it\n",
                        who, (unsigned long long)L.com[i]);
                return 3;
            }
        /* this lane's write tail = end of its last logged append */
        int64_t lane_tail = -1;
        for (int i = L.napp - 1; i >= 0; i--)
            if (L.app[i].aid == appender_id) { lane_tail = L.app[i].off + 4 + L.app[i].len; g.next_off = lane_tail; break; }

        if (same_lane) {
            /* The restart walk against queue truth: the lane's uncommitted
             * iterator must yield exactly the lane's appended-but-uncommitted
             * set per the gap log (a seq the queue shows committed is
             * reconciled as committed: never re-appended, never
             * re-committed) — plus AT MOST ONE record the log does not know
             * about, at the lane's write tail (the kill landed between the
             * queue operation and its log line). Any other unlogged record,
             * or more than one, is a failure. */
            int nexp = 0;
            for (int i = 0; i < L.napp; i++) {
                if (L.app[i].aid != appender_id) continue;
                if (is_committed[L.app[i].seq] && latest_app[L.app[i].seq] == i) continue;
                exp_off[nexp] = L.app[i].off; exp_seq[nexp] = L.app[i].seq; nexp++;
            }
            MmapqUncommittedPayloadIterator *it = NULL;
            rc = mmapq_indexed_appender_uncommitted_iterator_checked(
                a, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, &it);
            if (rc) { fprintf(stderr, "%s: uncommitted iterator rc=%d\n", who, rc); return 2; }
            int64_t woff[GW_MAX_PENDING];
            uint64_t wseq[GW_MAX_PENDING];
            int32_t wlen[GW_MAX_PENDING];
            int wok[GW_MAX_PENDING], nw = 0;
            const void *data; int32_t len; int64_t off;
            while ((rc = mmapq_uncommitted_iterator_next(it, &data, &len, &off)) == 1) {
                if (nw == GW_MAX_PENDING) { fprintf(stderr, "%s: walk exceeds %d records\n", who, GW_MAX_PENDING); return 3; }
                MsgHdr h; const char *bad = validate(data, len, &h);
                woff[nw] = off; wseq[nw] = bad ? UINT64_MAX : h.seq;
                wlen[nw] = len; wok[nw] = bad == NULL;
                nw++;
            }
            if (rc < 0) { fprintf(stderr, "%s: uncommitted walk rc=%d\n", who, rc); return 2; }
            g.walked = nw;

            int64_t unlogged_off = -1; uint64_t unlogged_seq = 0;
            int32_t unlogged_len = 0; int unlogged_ok = 0, n_unlogged = 0;
            for (int j = 0; j < nw; j++) {
                int k = -1;
                for (int i = 0; i < nexp; i++) if (!emark[i] && exp_off[i] == woff[j]) { k = i; break; }
                if (k < 0) {
                    /* unlogged: only ever the write tail, only ever one */
                    n_unlogged++;
                    if (n_unlogged > 1) {
                        fprintf(stderr, "%s: GWALK MISMATCH: %d records the gap log doesn't know about (at most one, at the write tail, is possible)\n",
                                who, n_unlogged);
                        return 3;
                    }
                    if (lane_tail >= 0 && woff[j] < lane_tail) {
                        fprintf(stderr, "%s: GWALK MISMATCH: unlogged record at offset %lld is below the write tail %lld\n",
                                who, (long long)woff[j], (long long)lane_tail);
                        return 3;
                    }
                    unlogged_off = woff[j]; unlogged_seq = wseq[j];
                    unlogged_len = wlen[j]; unlogged_ok = wok[j];
                    continue;
                }
                emark[k] = 1;
                if (gw_in_dis(&L, woff[j])) continue;   /* stale discard: may be torn */
                if (!wok[j]) {
                    /* a logged append is intact by construction (the log line
                     * is written after append_uncommitted returns); a torn
                     * logged record means something else is wrong */
                    fprintf(stderr, "%s: GWALK INVALID logged record at offset %lld\n",
                            who, (long long)woff[j]);
                    return 3;
                }
                if (wseq[j] != exp_seq[k]) {
                    fprintf(stderr, "%s: GWALK MISMATCH: walked offset %lld has seq %llu, log says %llu\n",
                            who, (long long)woff[j], (unsigned long long)wseq[j],
                            (unsigned long long)exp_seq[k]);
                    return 3;
                }
            }
            /* every log-recorded uncommitted record must have been yielded */
            for (int i = 0; i < nexp; i++) {
                if (!emark[i]) {
                    fprintf(stderr, "%s: GWALK MISMATCH: log-recorded uncommitted offset %lld (seq %llu) not yielded by the walk\n",
                            who, (long long)exp_off[i], (unsigned long long)exp_seq[i]);
                    return 3;
                }
            }

            /* Torn-tail recipe (guide ch5), with the log written after each
             * queue operation: only a record whose append was in flight at
             * the kill can be torn, and it is never logged. An intact
             * unlogged tail record is kept — its seq comes from its payload
             * — and committed like any walked record; a torn one is
             * discarded and its seq re-derived as the never-logged hole in
             * the pending build. With no unlogged record, the highest-offset
             * logged record takes the conservative discard. */
            if (n_unlogged == 1) {
                if (unlogged_ok) {
                    if (unlogged_seq > maxseq) {
                        int64_t *na = realloc(latest_app, (unlogged_seq + 1) * sizeof *na);
                        uint8_t *nc = realloc(is_committed, unlogged_seq + 1);
                        if (!na || !nc) { perror("realloc"); return 5; }
                        for (uint64_t t = maxseq + 1; t <= unlogged_seq; t++) { na[t] = -1; nc[t] = 0; }
                        latest_app = na; is_committed = nc; maxseq = unlogged_seq;
                    }
                    L.app = gw_grow(L.app, &L.capa, L.napp, sizeof *L.app);
                    L.app[L.napp] = (GwRec){ unlogged_seq, unlogged_off, unlogged_len, appender_id };
                    latest_app[unlogged_seq] = L.napp;
                    L.napp++;
                    /* the write tail moves past the recovered record */
                    g.next_off = unlogged_off + 4 + unlogged_len;
                    lane_tail = g.next_off;
                    fprintf(lf, "GAPAPPEND %llu %lld %d\n",   /* recovered into the log */
                            (unsigned long long)unlogged_seq, (long long)unlogged_off, (int)unlogged_len);
                    fflush(lf);
                } else {
                    gw_log_discard(lf, 0, unlogged_off);   /* seq unknown — payload torn */
                    L.dis = realloc(L.dis, (size_t)(L.ndis + 1) * sizeof *L.dis);
                    if (!L.dis) { perror("realloc"); return 5; }
                    L.dis[L.ndis++] = unlogged_off;
                }
            } else {
                int64_t torn_off = -1;
                for (int i = 0; i < nexp; i++) if (exp_off[i] > torn_off) torn_off = exp_off[i];
                if (torn_off >= 0) {
                    uint64_t tseq = 0;
                    for (int i = 0; i < nexp; i++) if (exp_off[i] == torn_off) tseq = exp_seq[i];
                    gw_log_discard(lf, tseq, torn_off);
                    L.dis = realloc(L.dis, (size_t)(L.ndis + 1) * sizeof *L.dis);
                    if (!L.dis) { perror("realloc"); return 5; }
                    L.dis[L.ndis++] = torn_off;
                }
            }
        }

        /* Build the pending set in seq order: keep intact same-lane records
         * (the walk refilled them committable), re-append everything else
         * (the torn seq, and seqs stranded on a dead lane). */
        for (uint64_t s = 0; s <= maxseq; s++) {
            int64_t i = latest_app[s];
            if (i < 0 || is_committed[s]) continue;
            if (same_lane && L.app[i].aid == appender_id && !gw_in_dis(&L, L.app[i].off)) {
                if (g.npend == GW_MAX_PENDING) { fprintf(stderr, "%s: pending table full at load\n", who); return 2; }
                g.pend[g.npend++] = L.app[i];
            } else {
                if ((rc = gw_emit(a, lf, &g, (uint16_t)gw, inc, s))) return rc;
                g.reappended++;
            }
        }

        /* resume cursors from the log: emit after the highest appended seq,
         * re-derive a held (delayed, never appended) packet if the kill landed
         * inside its delay window, commit from the lowest uncommitted seq */
        g.next_emit = maxseq + 1;
        for (uint64_t s = 0; s <= maxseq; s++) {
            if (latest_app[s] >= 0) continue;
            if (g.delayed >= 0) { fprintf(stderr, "%s: gap log shows >1 held packet\n", who); return 2; }
            g.delayed = (int64_t)s; g.delay_left = 0;
        }
        free(latest_app); free(is_committed); free(exp_off); free(exp_seq); free(emark);
        free(qcom);
    }
    /* the commit cursor resumes at the lowest uncommitted seq — a pending
     * record or a held (never appended) packet alike */
    uint64_t next_commit = g.next_emit;
    for (int i = 0; i < g.npend; i++) if (g.pend[i].seq < next_commit) next_commit = g.pend[i].seq;
    if (g.delayed >= 0 && (uint64_t)g.delayed < next_commit) next_commit = (uint64_t)g.delayed;
    printf("GWJOIN %d %u %d lane=%s walked=%lld reappended=%d resume_emit=%llu resume_commit=%llu\n",
           gw, inc, appender_id, same_lane ? "same" : "new", (long long)g.walked, g.reappended,
           (unsigned long long)g.next_emit, (unsigned long long)next_commit);
    fflush(stdout);

    int rcode = 0;
    while (access(stopfile, F_OK) != 0) {
        uint64_t s = gw_next_arrival(&g);
        if ((rcode = gw_emit(a, lf, &g, (uint16_t)gw, inc, s))) break;
        /* planted defect MKTSIM_COMMIT_BEFORE_GAP: commit a later packet
         * before its gap fill — readers see a seq gap/inversion */
        if (commit_before_gap && gw_pend_find(&g, next_commit) < 0 && g.npend > 0) {
            int hi = 0;
            for (int i = 1; i < g.npend; i++) if (g.pend[i].seq > g.pend[hi].seq) hi = i;
            if ((rcode = gw_commit_one(a, lf, &g, hi, (uint16_t)gw))) break;
        }
        int pi;
        while ((pi = gw_pend_find(&g, next_commit)) >= 0) {
            if ((rcode = gw_commit_one(a, lf, &g, pi, (uint16_t)gw))) break;
            next_commit++;
        }
        if (rcode) break;
        /* jitter: interleave with other processes. While a gap is open (a
         * packet held by the permutation), linger: this is what makes a
         * random SIGKILL land inside an uncommitted window, so churn actually
         * exercises the restart walk. */
        if (g.delayed >= 0) { struct timespec ts = {0, 100000 + (long)(rnd() % 400000)}; nanosleep(&ts, NULL); }
        else if (rnd() % 4 == 0) { struct timespec ts = {0, (long)(rnd() % 200000)}; nanosleep(&ts, NULL); }
        else sched_yield();
    }

    /* clean stop: flush the held packet, commit everything in seq order */
    if (rcode == 0) {
        if (g.delayed >= 0) {
            uint64_t s = (uint64_t)g.delayed; g.delayed = -1;
            rcode = gw_emit(a, lf, &g, (uint16_t)gw, inc, s);
        }
        while (!rcode && g.npend > 0) {
            int lo = 0;
            for (int i = 1; i < g.npend; i++) if (g.pend[i].seq < g.pend[lo].seq) lo = i;
            rcode = gw_commit_one(a, lf, &g, lo, (uint16_t)gw);
        }
    }
    printf("GWFINAL %d %u %d emitted=%llu committed=%llu walked=%lld reappended=%d rc=%d\n",
           gw, inc, appender_id, (unsigned long long)g.emitted, (unsigned long long)g.committed,
           (long long)g.walked, g.reappended, rcode);
    fflush(stdout);
    fclose(lf);
    free(L.app); free(L.com); free(L.dis);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    return rcode;
}

/* ── verify ─────────────────────────────────────────────────────────────── */

static int check_log_lines(const Engine *e, const char *path, const char *kind1, const char *kind2)
{
    FILE *f = fopen(path, "r");
    if (!f) { fprintf(stderr, "verify: cannot open %s\n", path); return 1; }
    char kind[16]; int t, aid; unsigned inc; long long cnt; unsigned long long in, dg;
    char line[512]; int bad = 0, lines = 0;
    while (fgets(line, sizeof line, f)) {
        if (sscanf(line, "%15s %d %u %d %lld %llx %llx", kind, &t, &inc, &aid, &cnt, &in, &dg) != 7) continue;
        if (strcmp(kind, kind1) && strcmp(kind, kind2)) continue;
        lines++;
        uint64_t ein, edg;
        if (cnt < 0 || cnt > e->applied) { fprintf(stderr, "verify: %s %s count %lld beyond log\n", path, kind, cnt); bad = 1; continue; }
        hist_at(e, cnt, &ein, &edg);
        if (ein != in || edg != dg) {
            fprintf(stderr, "verify: %s %s trader %d inc %u state@%lld differs from replay\n", path, kind, t, inc, cnt);
            bad = 1;
        }
    }
    fclose(f);
    return bad;
}

static int run_verify(const char *dir, const char *qname, int nlogs, char **logs)
{
    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(dir, qname, REGION_SIZE, RING_SIZE, 1, MAX_FILE_SIZE, 0,
                                   TIMEOUT_NS, 0, NULL, NULL, &q)) { fprintf(stderr, "verify: queue\n"); return 1; }
    MmapqIndexedReader *rd = NULL;
    if (mmapq_indexed_queue_create_reader(q, &rd)) { fprintf(stderr, "verify: reader\n"); return 1; }
    int64_t last = mmapq_indexed_reader_last_index(rd);
    mmapq_indexed_reader_close(rd);
    MmapqIndexedPoller *p = NULL;
    if (mmapq_indexed_queue_create_poller(q, &p)) { fprintf(stderr, "verify: poller\n"); return 1; }

    Engine e; engine_init(&e);
    Replay r = { .e = &e, .who = "verify", .self = 0xFFFF, .strict_seq = 1 };
    int64_t n = 0;
    int bad = drain(p, &r, &n) != 0;
    if (!bad && e.applied != last + 1) {
        fprintf(stderr, "verify: replayed %lld messages but last_index is %lld\n", (long long)e.applied, (long long)last);
        bad = 1;
    }
    /* conservation */
    int64_t pos = 0, cash = 0; int traders = 0;
    for (int t = 0; t < MAX_TRADERS; t++) { pos += e.position[t]; cash += e.cash[t]; if (e.next_seq[t]) traders++; }
    if (pos != 0 || cash != 0) { fprintf(stderr, "verify: conservation broken pos=%lld cash=%lld\n", (long long)pos, (long long)cash); bad = 1; }
    int bb = best_index(&e, 1), ba = best_index(&e, -1);
    if (bb >= 0 && ba >= 0 && e.orders[bb].price >= e.orders[ba].price) { fprintf(stderr, "verify: book crossed\n"); bad = 1; }

    /* F5: gateway lanes — seqs strictly increasing and gap-free in index
     * order (checked inline above); offsets must be non-monotone in commit
     * order, which is the proof that resequencing actually happened. */
    if (r.gw_msgs > 0) {
        int gateways = 0;
        for (int t = 0; t < MAX_TRADERS; t++) gateways += r.gw_seen[t];
        int gw_bad = !r.gw_inv_set;
        if (gw_bad)
            fprintf(stderr, "verify: %lld gateway packets but payload offsets are monotone in commit "
                            "order — resequencing not exercised\n", (long long)r.gw_msgs);
        else
            printf("PROOF resequencing: gateway %d committed seq %llu (offset %lld) then seq %llu "
                   "(offset %lld) — offset decreased in commit order\n",
                   r.gw_inv_gw, (unsigned long long)r.gw_inv_prev_seq, (long long)r.gw_inv_prev_off,
                   (unsigned long long)r.gw_inv_seq, (long long)r.gw_inv_off);
        printf("GWVERIFY gateways=%d packets=%lld result=%s\n",
               gateways, (long long)r.gw_msgs, gw_bad ? "FAIL" : "PASS");
        bad |= gw_bad;
    }
    for (int i = 0; i < nlogs; i++) bad |= check_log_lines(&e, logs[i], "JOIN", "FINAL");

    /* F4: the batch lane — same checks over the batch queue's own index
     * space, evaluated by an independent engine; the batch digest must
     * reproduce exactly. Absent queue (batch disabled): skip. */
    char bdir[1100];
    snprintf(bdir, sizeof bdir, "%s/%s", dir, BATCH_QNAME);
    if (access(bdir, F_OK) == 0) {
        MmapqIndexedBatchQueue *bq = NULL;
        if (mmapq_indexed_batch_queue_create(dir, BATCH_QNAME, REGION_SIZE, RING_SIZE, 1,
                                             MAX_FILE_SIZE, 0, TIMEOUT_NS, 0, NULL, NULL, &bq)) {
            fprintf(stderr, "verify: batch queue\n"); return 1;
        }
        MmapqIndexedBatchReader *brd = NULL;
        if (mmapq_indexed_batch_queue_create_reader(bq, &brd)) { fprintf(stderr, "verify: batch reader\n"); return 1; }
        int64_t blast = mmapq_indexed_batch_reader_last_index(brd);
        mmapq_indexed_batch_reader_close(brd);
        MmapqIndexedBatchPoller *bp = NULL;
        if (mmapq_indexed_batch_queue_create_poller(bq, &bp)) { fprintf(stderr, "verify: batch poller\n"); return 1; }
        Engine be; engine_init(&be);
        BReplay br = { .e = &be, .who = "verify" };
        int64_t nb = 0;
        int bbad = drain_batch(bp, &br, &nb) != 0;
        if (!bbad && nb != blast + 1) {
            fprintf(stderr, "verify: replayed %lld batches but batch last_index is %lld\n",
                    (long long)nb, (long long)blast);
            bbad = 1;
        }
        for (int i = 0; i < nlogs; i++) bbad |= check_log_lines(&be, logs[i], "BATCHJOIN", "BATCHFINAL");
        printf("BATCHVERIFY batches=%lld msgs=%lld claims=%lld input=%016llx digest=%016llx result=%s\n",
               (long long)nb, (long long)be.applied, (long long)br.claims_checked,
               (unsigned long long)be.input_hash, (unsigned long long)be.digest, bbad ? "FAIL" : "PASS");
        bad |= bbad;
        mmapq_indexed_batch_poller_close(bp);
        mmapq_indexed_batch_queue_close(bq);
        engine_free(&be);
    }

    printf("VERIFY messages=%lld traders=%d claims=%lld trades=%lld volume=%lld self_trades=%lld "
           "rejects=%lld cancel_rejects=%lld resting=%d input=%016llx digest=%016llx result=%s\n",
           (long long)e.applied, traders, (long long)r.claims_checked, (long long)e.trades, (long long)e.volume,
           (long long)e.self_trades, (long long)e.rejects, (long long)e.cancel_rejects, e.n,
           (unsigned long long)e.input_hash, (unsigned long long)e.digest, bad ? "FAIL" : "PASS");
    mmapq_indexed_poller_close(p);
    mmapq_indexed_queue_close(q);
    engine_free(&e);
    return bad;
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc >= 7 && !strcmp(argv[1], "trader")) {
        int async = argc >= 8 ? !strcmp(argv[7], "async") : 0;
        int want_id = argc >= 9 ? atoi(argv[8]) : -1;
        int batch_on = argc >= 10 ? atoi(argv[9]) : 0;
        int t = atoi(argv[4]);
        if (t < 0 || t >= MAX_TRADERS) { fprintf(stderr, "trader id 0..%d\n", MAX_TRADERS - 1); return 2; }
        if (want_id > 255) { fprintf(stderr, "appender id 0..255 or -1\n"); return 2; }
        return run_trader(argv[2], argv[3], t, (uint32_t)strtoul(argv[5], NULL, 10), argv[6], async, want_id, batch_on);
    }
    if (argc >= 7 && !strcmp(argv[1], "gateway")) {
        int async = argc >= 8 ? !strcmp(argv[7], "async") : 0;
        int want_id = argc >= 9 ? atoi(argv[8]) : -1;
        int g = atoi(argv[4]);
        if (g < 0 || g >= MAX_TRADERS) { fprintf(stderr, "gateway id 0..%d\n", MAX_TRADERS - 1); return 2; }
        if (want_id > 255) { fprintf(stderr, "appender id 0..255 or -1\n"); return 2; }
        return run_gateway(argv[2], argv[3], g, (uint32_t)strtoul(argv[5], NULL, 10), argv[6], async, want_id);
    }
    if (argc >= 4 && !strcmp(argv[1], "verify"))
        return run_verify(argv[2], argv[3], argc - 4, argv + 4);
    fprintf(stderr, "usage:\n  %s trader  <dir> <queue> <trader_id> <incarnation> <stopfile> [sync|async] [appender_id|-1] [batch 0|1]\n"
                    "  %s gateway <dir> <queue> <gateway_id> <incarnation> <stopfile> [sync|async] [appender_id|-1]\n"
                    "  %s verify  <dir> <queue> [trader_log ...]\n", argv[0], argv[0], argv[0]);
    return 2;
}
