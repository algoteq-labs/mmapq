#!/usr/bin/env python3
"""rebuild_stuck.py — the rebuild stuck poll fails cleanly, not a hang.

Trader 0 is restarted with MKTSIM_REBUILD_STUCK armed, which forces
every rebuild poll result to ERROR+ETIMEDOUT with start_index unchanged. The
retry bound (the main loop's, five) is exhausted, so the fixed rule fails with
ONE diagnostic line — poll result, last_error, entry_status(index), index,
target last, current last_index, start_index — and the trader exits rc=4
rather than spinning. Expected FAIL (a clean, bounded failure).
"""
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, field, last_prefix, snapshotter_si, vacuous,
                      verdict, wait_prefix, wait_proc)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rebuild_stuck")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rebuild-stuck", "FAIL", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        env = dict(os.environ)
        env["MKTSIM_REBUILD_STUCK"] = "1"
        proc, t0log1 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(t0log1, "PLANT rebuild-stuck", timeout=25.0):
            return vacuous("rebuild-stuck", "FAIL", "plant_not_fired")
        rc = wait_proc(proc, timeout=25.0)
        if rc is None:
            return vacuous("rebuild-stuck", "FAIL", "trader_hung")
        diag = [l for l in open(t0log1).read().splitlines()
                if "rebuild poll 5" in l and "entry_status=" in l
                and "target_last=" in l and "last_index=" in l and "start_index=" in l]
        if not diag:
            return vacuous("rebuild-stuck", "FAIL", "no_diagnostic")
        pl = last_prefix(t0log1, "PLANT rebuild-stuck")
        pidx, didx = field(pl, "index"), field(diag[-1], "index")
        if pidx is None or didx is None or pidx != didx:
            return vacuous("rebuild-stuck", "FAIL", "index_mismatch")
        sys.stderr.write("rebuild-stuck rc=%s diag=%s\n" % (rc, diag[-1]))
        got = "FAIL" if rc not in (0, None, -signal.SIGKILL) else "PASS"
        return verdict("rebuild-stuck", "FAIL", got)
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
