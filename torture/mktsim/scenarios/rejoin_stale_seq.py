#!/usr/bin/env python3
"""rejoin_stale_seq.py — the F2 rejoin-stale-sequence plant (MKTSIM_REJOIN_STALE_SEQ).

Two traders and a cadence-1 snapshotter run on an archivable queue. The
snapshotter is stopped once start_index has advanced, trader 0 keeps
publishing (so it has its own messages past the latest snap_<k>), then trader
0 is SIGKILLed and restarted with the plant armed. The restart recovers from
snap_<start_index>, rebuilds the tail, but resumes its own sequence from the
snapshot's next_seq instead of the rebuilt one — so it re-publishes sequence
numbers it already used. Trader 1's per-trader SEQ check catches the duplicate.
Expected FAIL (the defect the SEQ check catches).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, snapshotter_si, vacuous, verdict, wait_prefix,
                      wait_substr)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rejoin_stale_seq")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rejoin-stale-seq", "FAIL", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        # Freeze start_index, then let trader 0 publish past the snapshot so its
        # rebuild has its own messages to skip.
        t.kill(t.snapshotter[0])
        time.sleep(0.6)
        # SIGKILL and reap trader 0, let trader 1 catch up to the tail, then
        # restart trader 0 with the plant armed.
        t.kill(t.traders[0][2])
        time.sleep(0.6)
        env = dict(os.environ)
        env["MKTSIM_REJOIN_STALE_SEQ"] = "1"
        _, log0 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(log0, "PLANT rejoin-stale-seq", timeout=25.0):
            return vacuous("rejoin-stale-seq", "FAIL", "plant_not_fired")
        # Trader 1 must catch the duplicate sequence.
        caught = wait_substr(t.traders[1][3], "SEQ trader 0", timeout=25.0)
        sys.stderr.write("rejoin-stale-seq plant=%s t1_caught=%s\n" % (True, caught))
        return verdict("rejoin-stale-seq", "FAIL", "FAIL" if caught else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
