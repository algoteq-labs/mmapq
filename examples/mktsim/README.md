# mktsim — market simulation churn test for mmapq

Independent trader processes share one mmapq log and publish **intents only**
(NEW limit order, CANCEL). Every process runs the same deterministic matching
engine over the log, so each derives the same book, fills and positions.
Fills are never published.

Every intent carries a **claim**: "when I wrote this I had applied *k*
messages; my input hash was *H*; my derived-event digest was *D*". Every
trader checks every claim as it replays. The verifier replays the whole log
at the end and checks everything again.

## Build and run

From the repository root:

```bash
make mktsim            # builds examples/mktsim/mktsim
python3 examples/mktsim/churn.py --dir /tmp/mktsim_run --mode sync  --seed 1
python3 examples/mktsim/churn.py --dir /tmp/mktsim_run --mode async --seed 1
```

Options: `--traders 8 --kills 40 --restart-prob 0.7 --survivors 2
--min-gap 0.05 --max-gap 0.4 --gateways 1 --batch 1 --archive
--hand-delete`. Exit code 0 means PASS. `--seed` replays the
same kill schedule (when, who, restart or join). The interleaving inside the
queue is not seeded: it depends on the OS scheduler.

## What a run does

1. Starts N traders on a fresh queue. IDs divisible by 4 are takers (they
   cross the spread); the rest are market makers (they quote around mid,
   keep at most 6 live orders, and cancel their oldest).
2. **Churn:** SIGKILLs a random trader, then either
   - **restarts it:** same trader ID, next incarnation. It reads the last
     committed index, rebuilds book, positions and its own resting orders up
     to that index, resumes its sequence numbers from the log, and carries
     on managing its own orders; or
   - **adds a new trader** that joins mid-session.
3. **Attrition:** kills without replacement until `--survivors` remain.
4. **Two-phase stop:** survivors stop publishing and report QUIESCED. Once
   all have, they read to the end and report FINAL.
5. **Verify:** replays the log and checks every claim and every JOIN/FINAL
   line.

## Engine rules (the determinism contract)

- Integer ticks and lots. A non-positive price or quantity is REJECTED
  (an event with no book change).
- Price-time priority. Time = log index; ties go to the lowest order ID.
- Fills execute at the resting order's price.
- Self-trade is allowed (explicit rule, counted).
- Order ID = the log index of its NEW.
- CANCEL removes the order only if it exists and the sender owns it.
  Otherwise CANCEL_REJECT, with no book change.
- No wall clock, no floating point, no randomness inside the engine.
  Randomness only shapes what a trader decides to publish.

## Checks

| Check | Catches |
|---|---|
| Index contiguity; replay count = `last_index + 1` | lost or skipped entries |
| Framing: magic, length, CRC, deterministic padding bytes | torn or corrupt payloads |
| Claim `(k, H, D)` equals the replayer's state after k messages | divergent history (H) or divergent market (D) |
| `seen_count ≤ own index` | causality |
| Per-trader sequence contiguous across incarnations; incarnation never goes backwards | a dead incarnation's commit after its replacement started; duplicates |
| Every JOIN/FINAL line matches the replay | wrong rebuild on rejoin |
| Survivors' FINAL = whole log, all identical | stale or partial final views |
| Positions sum to 0, cash sums to 0, book never crossed | engine bugs |
| Process exit codes (killed = −9, survivors = 0, anything else fails) | crashes and hangs |

## Validation (Opus, 2026-10-10, against the A6 tree)

| Run | Result |
|---|---|
| Fixed tree, sync, seeds 11/12/13 | PASS: 540k–595k intents, 46 kills, 27–30 restarts, 10–13 joiners each |
| Fixed tree, async, seeds 11/12/13 | PASS: 450k–505k intents, same churn |
| Fixed tree under ASan, sync and async | PASS, no sanitizer reports |
| **C-08 reinstated** (fast-path invalidation removed), seeds 1/2/3/11/12/13 | **FAIL, 6/6**: traders SIGSEGV within about 1 s; the verifier itself crashed once |
| Planted engine divergence (one trader breaks ties the wrong way) | **FAIL**: DIVERGENCE at index 9–10, same input hash, different digest |
| One flipped payload byte after a run | **FAIL**: INVALID (crc) at the exact message |

Lessons from writing it:
- The first planted-divergence run passed, because no trades happened
  without a taker. A planted bug needs a scenario that reaches it.
- An off-by-one in the claim history was invisible to passing runs, because
  every process shared it. It showed up only as a zero state in a JOIN line.

## Appender ID reclamation (`--reclaim`, with tools/mmapq-ids)

Without reclamation, the library never releases the ID of a killed process.
Every incarnation takes a fresh ID: a 48-incarnation run created 48 lanes
(IDs 0–47), and a queue supports at most 256 participants over its lifetime.

With `--reclaim path/to/mmapq-ids`, the driver acts as a **supervisor with
fixed ID assignment**: trader N always writes on appender ID N
(`create_appender_with_id`). On a kill it:

1. SIGKILLs and reaps the process.
2. Runs `mmapq-ids <dir> <queue> --unset N`. Exit 1 ("already free") means
   the process died before acquiring; that's counted, and nothing leaked.
3. Restarts the trader on ID N, which runs the lane's crash recovery.

Reaping is what makes the release safe. It establishes the precondition
mmapq-ids documents but cannot check itself: nobody owns the ID.

Fixed assignment (rather than reading the ID back from the process) closes
two gaps:
- a process killed before reporting its ID can't leak it;
- nothing scans the pool, so a joiner can't take a freed ID between release
  and re-acquire.

An `EIDCONFLICT` is therefore a supervisor bug, and the trader fails (exit 2).

```bash
make mmapq-ids         # the operator tool, in tools/mmapq-ids/
python3 examples/mktsim/churn.py --dir /tmp/mktsim_run --mode sync --seed 1 \
    --reclaim tools/mmapq-ids/build/release/mmapq-ids
```

Extra checks in this mode:
- 0 IDs taken once all writers have exited;
- exactly one lane per trader;
- no `IDCONFLICT`.

| Run | Result |
|---|---|
| Fixed IDs, sync, seeds 41/42/43 | PASS: 544k–647k intents, 46 IDs reclaimed, 27–30 restarts on the trader's own lane, lanes = traders (18–21), 0 IDs taken at end, 0 conflicts |
| Fixed IDs, async, seeds 41/42/43 | PASS: 471k–526k intents, same churn and accounting |
| Earlier variant: pool-assigned IDs plus reclaim, sync and async, seeds 31/32/33 | PASS: joiners took over *other* traders' dead lanes (8 lanes for 48 incarnations). That exercises lane takeover by a different writer, which fixed assignment no longer does |
| Pool IDs, no reclaim (seed 44) | PASS, with 21 IDs still taken at the end: the leak, measured |
| **Misuse:** release a *live* writer's ID, start a second writer on it | Online, all 4 traders agree on the final state. **Offline replay FAILS:** `CAUSALITY at 72414: seen_count 77141` |

The misuse result matters. Two writers on one lane overwrote **committed**
payload after it had been read. Readers that polled early saw one message at
index 72414; a later replay sees different bytes there. The online checks
cannot see a retroactive rewrite, so the end-of-run replay is the check that
catches it.

Not measured: how many kills landed inside an append call (as opposed to
between appends). Kills are random during continuous publishing, so some do,
but the harness does not count them.

## The batch lane (F4)

Every trader also holds an appender on the batch queue `market_batch` (same
directory, same fixed-ID scheme under `--reclaim`; the driver releases both
lanes' IDs). The batch lane mirrors the trader's intent stream, committed
atomically: the MAX_LIVE eviction path publishes [NEW, CANCEL-oldest] as one
batch; other intents commit as singleton batches. Batch messages use the
same wire format and the same (k,H,D) claim scheme, evaluated over the
batch lane's own message count.

Checks (online in traders, offline in verify over both queues): batch
atomicity (framing/CRC/claims validate per message and per batch; a reader
only ever sees whole batches), per-trader batch sequence continuity across
incarnations, and a batch-lane digest the verifier reproduces exactly
(`BATCHVERIFY`).

Planted defects (both observed failing with a verify DIVERGENCE →
RESULT: FAIL): `MKTSIM_BATCH_REORDER=1` applies each batch's messages in
reverse order; `MKTSIM_BATCH_PARTIAL_READ=1` applies only the first message
of a multi-message batch.

## The gateway (F5): resequenced commits

A gateway participant (`--gateways N`, default 1) writes to the main queue
with `append_uncommitted` + `commit`: packets are **appended in arrival
order** (a deterministic permutation holds every 7th packet back by 2
positions) and **committed in sequence order** — a late packet is appended
on arrival and committed when its gap closes. There are no reserved
offsets. `T_GATEWAY` payloads carry (gateway id, incarnation, seq, payload
offset); they are book no-ops that enter the digest from payload bytes
only.

Checks: in the committed log the gateway lane's sequence numbers are
strictly increasing and gap-free in index order; verify prints a PROOF
line when the committed offsets are non-monotone in commit order (proof
that resequencing happened); under churn the gateway is killable like a
trader, and the rejoining incarnation walks its uncommitted iterator with
**the queue as the source of truth**: committed seqs are derived by
scanning the gateway's committed records in the queue (the gap log's
commit lines are hints only); the walk must yield exactly the lane's
appended-but-uncommitted set per the log, plus at most one unlogged record
at the lane's write tail — intact: its seq is taken from its payload and
it is committed normally; torn: discarded per the chapter-5 recipe (its
seq is re-derived as the never-logged hole). A seq the log calls
uncommitted but the queue shows committed is reconciled as committed:
never re-appended, never re-committed. `GWJOIN` reports the
walked/reappended counts.

Scenario hooks (deterministic crash windows, fired once per run on a
counter — the correct result after them is PASS):
`MKTSIM_GW_KILL_AFTER_APPEND=1` (SIGKILL right after `append_uncommitted`,
before the log line) and `MKTSIM_GW_KILL_AFTER_COMMIT=1` (right after
`commit`, before the log line). Fail-first evidence against the pre-rework
(log-truth) code: same lane — `GWALK MISMATCH: walk yields 1 records, gap
log recorded 0 uncommitted` / `walk yields 0 records, gap log recorded 1
uncommitted`; new lane under kill-after-commit — `verify: GATEWAY SEQ gw 0
… expected 5 got 4` (the duplicate-seq re-append). After the rework: all
four variants PASS.

Planted defect: `MKTSIM_COMMIT_BEFORE_GAP=1` — the gateway commits a later
packet before its gap fill. Observed failing: `verify: GATEWAY SEQ gw 0 …
expected 6 got 7 (gap or inversion)` → RESULT: FAIL.

## Archiving, reclamation and snapshots (F6/F7, `--archive`)

With `--archive` the queue is created `archivable=1` and a **snapshotter
process** (`mktsim snapshotter`, stage (e) E1) runs under the churn load.
Each round it reads the committed queue in order, holds one engine in
memory across rounds, picks `k`, writes `snap_<k>` atomically (tmp +
`rename`) after verifying `(k, H, D)` against a committed participant's
claim at `k`, calls `archive_up_to_index(k)`, and only then runs the
guide-chapter-6 tail: `archivable_files()` (the mark) → byte-copy the marked
files to `<dir>/archive/` (verified by size + CRC) → `reclaim_files()`.

`k` is the latest index at or below the committed end that has a **covering
participant claim** (a committed message whose `seen_count` equals its own
index — an independent statement of the state at `k`). E5 removed the
watermarks, so the snapshotter no longer needs to know who is alive: the
snapshotter is the **only** process that moves `start_index`, and it does so
only after `snap_<k>` exists. Removing the watermark also removed the only
throttle on archiving, so the cadence is explicit: **`--snap-every <N>`**
publishes a snapshot only once the latest covering claim is at least `N`
indices past the previous one. The cadence is in **indices**, not
milliseconds, so the throttle is deterministic and independent of the message
rate; **a reader keeps up as long as its rebuild from a snapshot takes less
than the snapshot interval** (the time between two `snap_<k>` writes at the
current rate).

**Two defaults on purpose.** The mktsim binary's own default is **65536**
(production-like: few snapshots, low churn). `churn.py` passes **4096** by
default so a short recipe run still writes several snapshots, fires GC guards
and produces undercut recoveries — at 65536 a recipe run wrote 1–2 snapshots,
0 GC guards and 0 undercuts, so it no longer exercised E1–E4. Pass
`--snap-every 1` for the recovery stress run ("as fast as claims allow") and
`--snap-every 65536` for the production-like cadence. With `--hand-delete`
the reclaim step is replaced: the marked files are unlinked by hand under the
X5 rule (no undo between the listing and the last unlink — this loop never
undoes).

**One snapshotter, and the dead-snapshotter safety property.** Exactly one
snapshotter (or archiver) runs at a time, enforced by an exclusive
`flock` on `<dir>/snapshotter.lock`; a second instance prints
`SNAPSHOTTER lock=busy result=FAIL` and exits non-zero. Because only the
snapshotter moves `start_index`, **a dead snapshotter pauses archiving —
`start_index` does not move — so no live reader is undercut.** A restart
recovers via `start_index → snap_<start_index> → replay from start_index`
(bounded no-progress retry); it never replays below `start_index`, and if
`start_index > 0` with `snap_<start_index>` missing after the bounded retry
it prints `SNAPRECOVER who=snapshotter … result=FAIL` and exits non-zero
rather than starting empty.

A snapshot's header `(k, H, D)` is verified when it is written (against a
committed participant's claim at `k`); its contents — the book, the
per-trader sequences and the per-gateway committed prefix — are verified on
first use by the recovering participant's own SEQ and claim checks. The
format is versioned: v2 (`MKSNAP20`) adds the per-gateway field, and
`snap_load` refuses a v1 (`MKSNAP10`) snapshot with a clear error rather than
misreading it.

The copy-aside **never replaces an existing archived copy** (CAND-59 spec
§3.5): the copy lands via `link(2)`, and an `EEXIST` is skipped and logged
(`ARCHSKIP <base>`). A file that reappears in a later listing (the
residual-window stray) is skipped loudly, and `reclaim_files()` then deletes
it from the live queue. `archskip.py` drives the real archiver against a
planted stray and asserts the archived copy's `fnv64` is unchanged, the skip
is logged, and the stray is reclaimed.

The **only** snapshot source is the snapshotter: `<dir>/snaps/snap_<k>`
(derived state + digest + log index, two kept: the previous and the current).
Every participant recovers the same way, at start and on `ARCHIVED`: read
`start_index`, load `snap_<start_index>` (bounded retry, `snap_recover`), then
read the queue from `start_index`. The snapshot joiner (`mktsim snapjoin`)
does exactly that at the end of the run, so its base is `start_index` and it
replays **no** archived copies (`archived=0` by design); its digest must equal
the verifier's. There are no per-trader snapshots and no per-reader side
files: recovery reads only the queue and the snapshotter's snapshot.
The offline verifier keeps replaying the archived copies plus the live queue
and asserts boundary contiguity exactly (`ARCHVERIFY archived=N boundary=N`) —
archived-history coverage belongs to the verifier, not to any participant's
recovery. `archive/` stays for durability and export.

The **gateway restart under archiving** (stage (e) E4): its committed seqs
are the prefix `0..C`. C comes from two places only — the snapshot at
`start_index`, whose per-gateway field records the highest committed gateway
seq below `start_index` (entries `[0, start_index)`), and the live queue scan
from `start_index`. The recovery reads `start_index`, loads
`snap_<start_index>` (bounded no-progress retry), scans the queue from
`start_index` for this gateway's committed seqs, and completes the
uncommitted tail with the library's uncommitted iterator; if archiving moves
`start_index` under the scan it sees `ARCHIVED` and runs the same recovery
again. No pin, no `archive/` reads, no log-trust boundary: a kill between a
commit and its log line leaves the log short and the queue (and hence the
snapshot) is right, so the restart is exactly-once with no duplicate. The
log-ahead check compares every logged commit against C. The verdict line is
`GWRECOVER gw=<id> prefix=<C> snap=<k> result=PASS|FAIL`.

Checks: `ARCHVERIFY` boundary contiguity and full replay over archived
copies + live queue; `SNAPJOIN` digest equals the verifier's, with
`base == start_index` and `archived=0` (the joiner starts at the snapshot
boundary); `GWJOIN prefix=C live_committed=L` lines show restarts with
committed records already archived (L < C + 1).

Planted defects:
- `MKTSIM_RECLAIM_AHEAD_OF_MARK=1` — the snapshotter unlinks one payload file
  beyond the mark. Observed failing: `verify: MISSING HISTORY at index
  41562: payload_64_1 absent from archive/ and the live queue` →
  RESULT: FAIL (the missing range is identified; never a silent pass).
- `MKTSIM_SNAPSHOTTER_OFF_BY_ONE=1` — the **snapshotter** writes `snap_<k>`
  recording index k but carrying the state at k−1. Observed failing: the
  write-time claim check catches it at publication —
  `SNAPWRITE k=… si=… claim=FAIL result=FAIL` → RESULT: FAIL. (Fail-first:
  with the check removed in a scratch copy the same run reports `claim=ok`
  and PASSes, i.e. the plant is real and the check is what catches it.)
- `MKTSIM_GW_NO_SNAPSEQ=1` — the snapshotter writes the per-gateway field
  zeroed, so a gateway restart whose committed records are archived cannot
  derive its prefix. Observed failing: `GWRECOVER gw=0 prefix=0
  snap=<k> result=FAIL` (the log is ahead of the recovered prefix); without
  the plant the same scenario prints `GWRECOVER gw=0 prefix=<C> snap=<k>
  result=PASS` and `GWJOIN … live_committed=0`, GWVERIFY PASS, no duplicate.
- `MKTSIM_SNAP_OLD_MAGIC=1` — the snapshotter stamps `snap_<k>` with the
  previous (v1, `MKSNAP10`) magic (CRC refreshed). Observed failing: a
  recovering participant's `snap_load` refuses it with
  `old snapshot format MKSNAP10 refused` → `SNAPRECOVER … result=FAIL
  (corrupt)`; never misread.
- `MKTSIM_SNAP_BODY_CORRUPT=1` (E6) — the snapshotter corrupts one snapshot
  **body** field (trader 0's `next_seq`, +1) after the write-time claim check
  and refreshes the CRC, leaving the header `(k, H, D)` and the magic intact.
  Observed failing: the write-time check prints `SNAPWRITE … claim=ok` (it
  compares only the header claim and cannot see a body field), then the first
  participant to recover from that snapshot fails its own per-trader check:
  `SEQ trader 0 … expected <s+1> got <s>` → `SNAPRECOVER`/`SNAPJOIN` FAIL. This
  is the second guard on snapshot contents; it is the counterpart of the
  E3 wrong-snapshot-content check.

Fail-first for the gateway snapshot prefix (stage (e) E4): the committed
prefix is recovered from `snap_<start_index>` plus the queue scan, never from
`archive/`. `MKTSIM_GW_NO_SNAPSEQ=1` red, the real tree green (see the plant
above). (Before E4 a joiner from a per-trader snapshot failed with
`GATEWAY SEQ gw 0 … expected 0 got <N>`; E5 removed the per-trader snapshots
entirely — the snapshotter is the only snapshot source.)

## Validation (stage c, 2026-10-27, house machine)

All runs `--traders 4 --kills 6 --survivors 2 --reclaim` (fixed ids), 64 KiB
regions / 256 KiB files:

| Run | Result |
|---|---|
| sync `--archive` seed 1 | PASS: 118,621 messages, 101,154 archived (ARCHVERIFY contiguity OK), SNAPJOIN digest == VERIFY (97,056 archived replayed), gateway restart with `prefix=7380 live_committed=2017` |
| async `--archive` seed 3 | PASS: 110,834 messages, 90,254 archived, SNAPJOIN digest == VERIFY |
| sync `--archive --hand-delete` seed 2 | PASS: 154,524 messages, 117 files hand-unlinked (X5), contiguity OK |
| `MKTSIM_GW_KILL_AFTER_APPEND=1` under `--archive` | PASS (hook fired once, restart reconciled) |
| `MKTSIM_GW_KILL_AFTER_COMMIT=1` under `--archive` | PASS (`prefix=4 live_committed=0` — the unlogged committed seq recovered from the snapshot field) |
| `MKTSIM_RECLAIM_AHEAD_OF_MARK=1` | FAIL: verifier + joiner report the missing range (`payload_64_1`, first missing index 41562) |
| `MKTSIM_SNAPSHOT_OFF_BY_ONE=1` | FAIL: snapshot joiner DIVERGENCE at the corrupt boundary; a restart consuming the planted snapshot diverges online |
| `MKTSIM_GW_NO_SNAPSEQ=1` (E4) | FAIL: `GWRECOVER … result=FAIL` (the snapshot's gateway field is zeroed) |
| `MKTSIM_SNAP_OLD_MAGIC=1` (E4) | FAIL: `snap_load: … old snapshot format MKSNAP10 refused` (never misread) |

## Gating scripts (stage (e) E6)

Two scripts consolidate the scenarios and plants into runnable gates. Both
invoke the real binary; neither runs in `test.sh`.

```bash
make mktsim
bash examples/mktsim/run_short.sh              # <= 5 min, run by hand until CI exists
bash examples/mktsim/run_soak.sh               # 30-60 min, the release gate
```

`run_short.sh` runs, in order: every plant (current and inherited) once and
expects it **red**; every scenario harness and checks it against its declared
outcome; and one clean churn per mode (sync, async, mixed) at the churn
default cadence plus one at `--snap-every 1`, all expected **green**. It prints
one summary table and exits non-zero if any row is `MISMATCH`. A plant that
comes out green is a suite failure. Its plant list is checked against
`mktsim.c` with `grep`; a plant named in the list but absent from the code is
reported as a `plant-list mismatch` (see below).

`run_soak.sh` is the release gate: many seeds, sync/async/mixed, both
cadences (`--snap-every 4096` and `1`), the hand-deletion run, and the two
gateway kill hooks. No plants. Per run it reports messages, kills, undercuts,
snapshot cycles, peak live-queue disk, and that every survivor reached the
live digest; it prints the totals and exits non-zero if any run failed. Knobs:
`SOAK_SEEDS` (default 12), `SOAK_KILLS` (60), `SOAK_TRADERS` (8), `SOAK_ONLY`.

**Scenario harnesses** (`examples/mktsim/scenarios/`, each prints one
`SCENARIO <name> expect=<PASS|FAIL> got=<PASS|FAIL|VACUOUS> result=<OK|MISMATCH>
[reason=<token>]` line and exits non-zero on `MISMATCH`; the scenario directory
is `$MKTSIM_SCENARIO_DIR` when set, else a per-name `/tmp` path, and a
harness that never reached its point reports `got=VACUOUS` with a `reason`
rather than a silent green):

| harness | topology | expect |
|---|---|---|
| `undercut.py` | SIGSTOP a trader for several cadence-1 rounds, SIGCONT; it recovers from `snap_<start_index>` | PASS |
| `rejoin_delay.py` | restart a trader with `MKTSIM_REJOIN_DELAY_MS`; the ARCHIVED-move window forces a re-recovery | PASS |
| `gwsnap.py` | kill/restart the gateway after `start_index` passed its committed prefix C; `GWRECOVER prefix>=C snap>0` | PASS |
| `die_after_write.py` | `MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE=2` red, then a restart GCs the orphan | PASS |
| `lock.py` | a second snapshotter gets `lock=busy` and exits non-zero | PASS |
| `removed_snapshot.py` | delete `snap_<start_index>`; a restart fails bounded (`tries=2`) | FAIL |
| `corrupt_snapshot.py` | `MKTSIM_SNAP_BODY_CORRUPT`; the recovering trader fails its SEQ check | FAIL |
| `arch_before_snap.py` | `MKTSIM_ARCH_BEFORE_SNAP`; a restart fails bounded (`tries=2`) | FAIL |
| `tiebreak.py` | `MKTSIM_TIEBREAK_BUG` armed on one trader; the others catch DIVERGENCE | FAIL |
| `rejoin_stale_seq.py` | trader 0 commits past the latest `snap_<k>`, is SIGKILLed and restarted with `MKTSIM_REJOIN_STALE_SEQ`; trader 1 catches the duplicate | FAIL |
| `runtime_misclassify.py` | an async trader with `MKTSIM_RUNTIME_MISCLASSIFY` (never killed) aborts on the first mapping timeout | FAIL |
| `gw_no_snapseq.py` | gateway killed, its records archived (`live_committed=0`), restarted with `MKTSIM_GW_NO_SNAPSEQ`; `GWRECOVER … result=FAIL` | FAIL |

**Every plant row must prove its plant fired (v1.18).** Each plant prints a
`PLANT <name> …` line at the moment it changes behaviour: the snapshotter
plants in `snapshotter.log` (`PLANT snapshotter-off-by-one`, `gc-keep-one`,
`snap-body-corrupt`, `gw-no-snapseq`, `snap-old-magic`, `die-after-write`,
`arch-before-snap`, `reclaim-ahead-of-mark`), the trader plants in the trader
log (`PLANT batch-reorder`, `batch-partial-read`, `runtime-misclassify`,
`rejoin-stale-seq` — the last only when the snapshot's `next_seq` differs from
the rebuilt one), the gateway plant (`PLANT commit-before-gap`), the export
plant (`PLANT export-truncate`), and the engine plant (`PLANT tiebreak-bug`).
A plant row whose run has no `PLANT` line is `got=VACUOUS
reason=plant_not_fired`, never `got=PASS`. Three plants that are inherently
racy under churn (`MKTSIM_REJOIN_STALE_SEQ`, `MKTSIM_RUNTIME_MISCLASSIFY`,
`MKTSIM_GW_NO_SNAPSEQ`) run as dedicated scenario harnesses instead of churn
rows, so the plant fires deterministically.

**Measured runtimes** (house machine, 2026-11-01, `-O3`): `run_short.sh`
**~56 s** for 27 rows; determinism check **10× sequential and 5× two-at-once,
0 mismatches**; one `run_soak.sh` at the defaults (12 seeds, 60 kills, 8
traders; 76 runs, 29,746,451 messages, 4,401 kills, 6,630 undercuts, 8,863
snapshot cycles, peak live disk 102,088 KiB, all 152 survivor FINALs at the
live digest) **33 m 14 s** — see `audit/DEMO-01-stage-e/E6/`.

**Scratch mutations (recorded evidence, not scripts).** These four edit the
source in a throwaway copy (never the tree) to show the guard each plant is
meant to exercise is what catches it; transcripts are in
`audit/DEMO-01-stage-e/E6/scratch/`:

- **no-delete GC** — comment out the `unlink` in `snap_gc`: snapshots
  accumulate and the third write gives `SNAPGUARD kept=3 … result=FAIL`
  (`no_delete_gc.txt`).
- **spin** — make `drain()` always return `DRAIN_ARCHIVED`: without the bound
  the trader spins; with it the same `start_index` twice gives the bounded
  `SNAPRECOVER … result=FAIL (no progress)` (`spin.txt`).
- **guard removed** — delete the `SNAPGUARD` exactness abort: the
  `MKTSIM_GC_KEEP_ONE` plant still makes the line say
  `SNAPGUARD kept=1 … result=FAIL`, but the snapshotter no longer aborts and
  the run passes green — the guard is what catches it
  (`guard_removed.txt`).
- **claim check removed** — skip the write-time `snap_claim_ok` call: the
  `MKTSIM_SNAPSHOTTER_OFF_BY_ONE` plant then publishes its wrong snapshot and
  the snapshotter prints `SNAPWRITE … claim=ok result=PASS` (the guard is what
  would have produced `claim=FAIL`); the defect then propagates and a
  recovering reader diverges (`claim_removed.txt`).

**Soak-evidence wording (§4, binding).** Churn runs are presented as **soak
evidence** — "ran N messages under M kills without a check failure" — never as
proof of race-freedom; the race-freedom claim belongs to the TLA+ model
(`doc/model/`).

**Dead-snapshotter safety property.** Because only the snapshotter moves
`start_index`, a dead snapshotter pauses archiving — `start_index` does not
move — so no live reader is undercut; recovery from a later snapshot plus the
live queue is always possible.

**`MKTSIM_REJOIN_STALE_SEQ` (E6 follow-up, v1.17).** On rejoin, the trader
resumes its own sequence from the snapshot's `next_seq` — the value right
after recovery — **without** counting its own messages drained in the rebuild.
The resumed sequence is therefore stale (it repeats seqs the snapshot already
counted), so another participant's per-trader `SEQ` check and the offline
`VERIFY` catch the duplicate:
`SEQ trader <t> … expected <s> got <s-1>`. Without the plant the value is read
after the rebuild and accounts for every drained message. This hook did not
exist before v1.17; the 2026-10-30 reconciliation was wrong to list it as
present. `run_short.sh` now greps the code for every named plant and all are
present (no plant-list mismatch).

**No silent green in the harnesses (v1.17).** Each scenario reports one of
three outcomes: the expected red, an unexpected green, or
`got=VACUOUS result=MISMATCH reason=<token>` when it never reached its point
(a plant did not fire, an expected line was never seen, or a process did not
exit in time). A VACUOUS outcome is never a pass and its `reason` appears in
the `run_short.sh` summary row. `run_short.sh` and `run_soak.sh` put every run
and every scenario directory under their work directory and, on any
`MISMATCH`, leave it in place and print its path (on success it is removed).

**The snapshotter startup race (CAND-60).** A snapshotter started alongside
its traders can read the queue's archivable flag while the meta file is still
being created. `mmapq_indexed_archiver_archivable_checked()`
(`src/indexed_archiver.c`) distinguishes the cases: while the meta is not
determinable it returns the meta-open code unchanged — `MMAPQ_ENOENT` (no meta
file), `MMAPQ_ENOTREADY` (meta exists but is not fully written) or
`MMAPQ_ETIMEDOUT` (mapping timed out) — and the call is retryable on the same
archiver; a zero return with `out == 0` is a genuine non-archivable answer;
any other code is a terminal (sticky) failure. `run_snapshotter` retries only
the not-ready trio for a bounded 5 s and prints a distinct line for each
outcome:

```
SNAPSHOTTER not_archivable queue=<q> result=FAIL                    (genuine 0)
SNAPSHOTTER not_ready queue=<q> retries=<n> rc=<rc> result=FAIL     (transient)
SNAPSHOTTER archivable_error queue=<q> rc=<rc> result=FAIL          (sticky)
```

The harnesses additionally start the snapshotter only after the traders'
`JOIN` lines (`_harness.Topo.wait_traders_joined`). The old behaviour — a
boolean `archivable()` that said 0 for a queue that becomes archivable — is
gone (CAND-60; the C symbol is removed).
