#!/usr/bin/env python3
"""undercut.py — a frozen reader is undercut and recovers (E3/E5).

Two traders and a cadence-1 snapshotter run on an archivable queue. Trader 0
is SIGSTOPped for a second and a half while the snapshotter archives past its
cursor (E5 removed the watermark that used to pin archiving to a lagging
reader). On SIGCONT the trader's next poll hits MMAPQ_ENTRY_ARCHIVED and it
recovers from snap_<start_index>, printing SNAPRECOVER ... result=PASS, then
finishes cleanly. Expected PASS.
"""
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, count_prefix, lines, snapshotter_si, vacuous,
                      verdict, wait_prefix)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_undercut")
    t = Topo(d)
    t.fresh()
    try:
        t0p, t0log = t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("undercut", "PASS", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        before = count_prefix(t0log, "SNAPRECOVER")
        t0p.send_signal(signal.SIGSTOP)
        time.sleep(1.5)
        t0p.send_signal(signal.SIGCONT)
        recovered = wait_prefix(t0log, "SNAPRECOVER", timeout=25.0, count_gt=before)
        if not recovered:
            return vacuous("undercut", "PASS", "no_recovery")
        new_ok = any(l.startswith("SNAPRECOVER who=t00") and "result=PASS" in l
                     for l in lines(t0log)[before:])
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("undercut detail: %s\n" % detail)
        return verdict("undercut", "PASS", "PASS" if (new_ok and ok) else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
