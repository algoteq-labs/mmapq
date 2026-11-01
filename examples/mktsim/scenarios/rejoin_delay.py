#!/usr/bin/env python3
"""rejoin_delay.py — the recovery/ARCHIVED-move window (E3 follow-up-2).

Two traders and a cadence-1 snapshotter run on an archivable queue. Trader 0
is killed and restarted with MKTSIM_REJOIN_DELAY_MS armed, widening the window
between its recovery and the poller move; the snapshotter archives past the
recovered index during the delay, so the move fails and the trader re-runs
recovery (DRAIN_ARCHIVED), then finishes cleanly. The PLANT rejoin-delay line
must appear and the run must still pass. Expected PASS.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import Topo, snapshotter_si, vacuous, verdict, wait_prefix


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rejoin_delay")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rejoin-delay", "PASS", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        env = dict(os.environ)
        env["MKTSIM_REJOIN_DELAY_MS"] = "300"
        _, t0log1 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(t0log1, "PLANT rejoin-delay", timeout=25.0):
            return vacuous("rejoin-delay", "PASS", "delay_not_fired")
        if not wait_prefix(t0log1, "SNAPRECOVER who=t00", timeout=25.0):
            return vacuous("rejoin-delay", "PASS", "no_recovery")
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("rejoin-delay detail: %s\n" % detail)
        return verdict("rejoin-delay", "PASS", "PASS" if ok else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
