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
--min-gap 0.05 --max-gap 0.4`. Exit code 0 means PASS. `--seed` replays the
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
