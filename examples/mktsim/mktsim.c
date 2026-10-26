/*
 * mktsim.c — agentic market simulation over one shared mmapq log.
 *
 * Every trader is an independent process. Traders publish INTENTS only
 * (NEW limit order, CANCEL). Every process embeds the same deterministic
 * matching engine and replays the log; fills are never published — each
 * process derives them. The log index is time priority; the order id of a
 * NEW is the log index it was committed at.
 *
 * Every intent carries a claim: "when I wrote this I had applied k messages,
 * my input hash was H and my derived-event digest was D". Every trader checks
 * every other trader's claims as it replays (online), and verify mode replays
 * the whole log at the end (offline). A claim mismatch means two processes
 * saw different histories or computed different markets.
 *
 * Modes:
 *   mktsim trader <dir> <queue> <trader_id> <incarnation> <stopfile> [sync|async] [appender_id|-1]
 *   mktsim verify <dir> <queue> [trader_log ...]
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
#include "runtime.h"
#include "mmap_platform.h"

#include <errno.h>
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
#define MAX_TRADERS  64
#define MAX_PAD      1500
#define MAX_LIVE     6             /* a trader cancels its oldest order beyond this */
#define ENGINE_SEED  0x6D6D617071ULL

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
    if (h->type != T_NEW && h->type != T_CANCEL) return "type";
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
    } else {
        int found = -1;
        for (int i = 0; i < e->n; i++) if (e->orders[i].id == h->cancel_id) { found = i; break; }
        if (found >= 0 && e->orders[found].owner == h->trader) {
            ev(e, 4, (uint64_t)index, (uint64_t)h->cancel_id, (uint64_t)(uint32_t)e->orders[found].qty);
            remove_at(e, found);
        } else {
            e->cancel_rejects++; ev(e, 5, (uint64_t)index, (uint64_t)h->cancel_id, 0);
        }
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
    int strict_seq;            /* verify mode: also check seq continuity */
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

/* ── trader ─────────────────────────────────────────────────────────────── */

static uint64_t rng_state;
static uint64_t rnd(void) { rng_state += 0x9E3779B97F4A7C15ULL; return mix64(rng_state); }
static int32_t rnd_range(int32_t lo, int32_t hi) { return lo + (int32_t)(rnd() % (uint64_t)(hi - lo + 1)); }

static int publish(MmapqIndexedAppender *a, Engine *e, uint16_t trader, uint32_t inc,
                   uint64_t seq, int type, int32_t side, int32_t price, int32_t qty, int64_t cancel_id)
{
    static uint8_t buf[sizeof(MsgHdr) + MAX_PAD + 8];
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
    int64_t idx = mmapq_indexed_appender_append(a, buf, (int32_t)(body + 8));
    if (idx < 0) { fprintf(stderr, "trader %u: append failed rc=%lld\n", trader, (long long)idx); return 2; }
    return 0;
}

static int run_trader(const char *dir, const char *qname, int trader, uint32_t inc,
                      const char *stopfile, int async, int want_id)
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

    /* Resume this trader's sequence from the log, not from local memory. */
    uint64_t seq = (uint64_t)e.next_seq[trader];
    int taker = trader % 4 == 0;
    int rcode = 0;

    while (access(stopfile, F_OK) != 0) {
        if ((rcode = drain(p, &r, NULL))) break;

        /* decide from the derived view: own live orders, top of book */
        int own = 0, oldest = -1;
        for (int i = 0; i < e.n; i++) if (e.orders[i].owner == trader) { if (oldest < 0) oldest = i; own++; }
        int bb = best_index(&e, 1), ba = best_index(&e, -1);
        int32_t bid = bb >= 0 ? e.orders[bb].price : e.last_trade - 2;
        int32_t ask = ba >= 0 ? e.orders[ba].price : e.last_trade + 2;
        int32_t mid = (bid + ask) / 2; if (mid < 50) mid = 50;

        if (own >= MAX_LIVE && oldest >= 0) {
            rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_CANCEL, 0, 0, 0, e.orders[oldest].id);
        } else if (taker) {
            int32_t side = (rnd() & 1) ? 1 : -1;
            int32_t px = side > 0 ? ask + rnd_range(0, 2) : bid - rnd_range(0, 2);
            if (px < 1) px = 1;
            rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_NEW, side, px, rnd_range(1, 5), 0);
        } else if (rnd() % 16 == 0 && own > 0) {
            /* occasionally cancel a random one of our own (exercises owner check) */
            int k = (int)(rnd() % (uint64_t)own), seen = 0;
            for (int i = 0; i < e.n; i++) if (e.orders[i].owner == trader && seen++ == k) {
                rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_CANCEL, 0, 0, 0, e.orders[i].id); break; }
        } else {
            int32_t side = (rnd() & 1) ? 1 : -1;
            int32_t px = side > 0 ? mid - rnd_range(1, 4) : mid + rnd_range(1, 4);
            if (px < 1) px = 1;
            rcode = publish(a, &e, (uint16_t)trader, inc, seq, T_NEW, side, px, rnd_range(1, 10), 0);
        }
        if (rcode) break;
        seq++;
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
    }
    hist_at(&e, e.applied, &in, &dg);
    printf("FINAL %d %u %d %lld %016llx %016llx claims=%lld rc=%d\n", trader, inc, appender_id,
           (long long)e.applied, (unsigned long long)in, (unsigned long long)dg, (long long)r.claims_checked, rcode);
    fflush(stdout);
    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    engine_free(&e);
    return rcode;
}

/* ── verify ─────────────────────────────────────────────────────────────── */

static int check_log_lines(const Engine *e, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) { fprintf(stderr, "verify: cannot open %s\n", path); return 1; }
    char kind[16]; int t, aid; unsigned inc; long long cnt; unsigned long long in, dg;
    char line[512]; int bad = 0, lines = 0;
    while (fgets(line, sizeof line, f)) {
        if (sscanf(line, "%15s %d %u %d %lld %llx %llx", kind, &t, &inc, &aid, &cnt, &in, &dg) != 7) continue;
        if (strcmp(kind, "JOIN") && strcmp(kind, "FINAL")) continue;
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
    Replay r = { .e = &e, .who = "verify", .self = 0xFFFF };
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
    for (int i = 0; i < nlogs; i++) bad |= check_log_lines(&e, logs[i]);

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
        int t = atoi(argv[4]);
        if (t < 0 || t >= MAX_TRADERS) { fprintf(stderr, "trader id 0..%d\n", MAX_TRADERS - 1); return 2; }
        if (want_id > 255) { fprintf(stderr, "appender id 0..255 or -1\n"); return 2; }
        return run_trader(argv[2], argv[3], t, (uint32_t)strtoul(argv[5], NULL, 10), argv[6], async, want_id);
    }
    if (argc >= 4 && !strcmp(argv[1], "verify"))
        return run_verify(argv[2], argv[3], argc - 4, argv + 4);
    fprintf(stderr, "usage:\n  %s trader <dir> <queue> <trader_id> <incarnation> <stopfile> [sync|async] [appender_id|-1]\n"
                    "  %s verify <dir> <queue> [trader_log ...]\n", argv[0], argv[0]);
    return 2;
}
