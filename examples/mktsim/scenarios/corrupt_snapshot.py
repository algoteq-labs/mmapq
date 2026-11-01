#!/usr/bin/env python3
"""corrupt_snapshot.py — the second guard on snapshot contents (E6 plant).

MKTSIM_SNAP_BODY_CORRUPT corrupts one snapshot body field (trader 0's
next_seq) and refreshes the CRC, leaving the header claim (k, H, D) intact.
The snapshotter's write-time claim check cannot see it (it prints claim=ok),
so a recovering participant is the guard: a restarted trader loads the
snapshot, passes the claim check, and fails its own per-trader SEQ check on
the first trader-0 message. Expected FAIL (the defect the SEQ check catches).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, snapshotter_si, vacuous, verdict, wait_prefix,
                      wait_proc, wait_substr)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_corrupt_snapshot")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        env = dict(os.environ)
        env["MKTSIM_SNAP_BODY_CORRUPT"] = "1"
        t.snapshotter_start(snap_every=1, env=env)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("corrupt-snapshot", "FAIL", "no_snapwrite")
        # Let start_index move well past 0 so the tail below the recovered
        # snapshot certainly carries a trader-0 message: the corrupted
        # next_seq[0] is only observed on the first trader-0 message after it.
        end = time.time() + 4.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        si = snapshotter_si(snap_log)
        # The snapshotter's own write-time check passed (claim=ok) — that is
        # exactly the point: the header check cannot see a body corruption.
        claim_ok = any(l.startswith("SNAPWRITE") and "claim=ok" in l
                       for l in open(snap_log).read().splitlines())
        if not claim_ok:
            return vacuous("corrupt-snapshot", "FAIL", "no_claim_ok")
        t.kill(t.snapshotter[0])
        # Stop all publishers first: the tail [si, last] is then fixed and
        # certainly carries a trader-0 message (start_index was well behind the
        # end), so the restart's rebuild must hit the corrupted next_seq[0].
        for (_, _, p, _) in list(t.traders):
            t.kill(p)
        proc, log1 = t.restart_trader(1, inc=1)
        if not wait_substr(log1, "SEQ trader 0", timeout=25.0):
            rc = proc.poll()
            if rc is None:
                return vacuous("corrupt-snapshot", "FAIL", "no_seq_line")
            return verdict("corrupt-snapshot", "FAIL", "FAIL" if rc != 0 else "PASS")
        rc = wait_proc(proc, timeout=15.0)
        if rc is None:
            return vacuous("corrupt-snapshot", "FAIL", "proc_no_exit")
        sys.stderr.write("corrupt-snapshot si=%d rc=%s\n" % (si, rc))
        return verdict("corrupt-snapshot", "FAIL", "FAIL" if rc != 0 else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
