#!/usr/bin/env python3
"""removed_snapshot.py — start_index names a snapshot that is not there.

A queue with a moved start_index has its snap_<start_index> deleted by hand
while the snapshotter is dead (so start_index cannot advance). A restarting
trader's bounded recovery hits the same start_index twice with the snapshot
still missing, prints SNAPRECOVER ... result=FAIL and exits non-zero — a
bounded failure, never a hang and never an empty engine at start_index > 0.
Expected FAIL (the defect the guard catches).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, last_prefix, snapshotter_si, vacuous, verdict,
                      wait_prefix, wait_proc)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_removed_snapshot")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("removed-snapshot", "FAIL", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 100:
            time.sleep(0.05)
        si = snapshotter_si(snap_log)
        t.kill(t.snapshotter[0])          # freeze start_index
        path = os.path.join(d, "snaps", "snap_%d" % si)
        if os.path.exists(path):
            os.unlink(path)
        # Stop every publisher before the restart (a realistic restart: no
        # other incarnation of trader 1 is left running).
        for (_, _, p, _) in list(t.traders):
            t.kill(p)
        proc, log1 = t.restart_trader(1, inc=1)
        if not wait_prefix(log1, "SNAPRECOVER who=t01", timeout=25.0):
            rc = wait_proc(proc, timeout=5.0)
            if rc is None:
                return vacuous("removed-snapshot", "FAIL", "no_recover_line")
            return verdict("removed-snapshot", "FAIL", "FAIL" if rc != 0 else "PASS")
        line = last_prefix(log1, "SNAPRECOVER who=t01")
        if "result=FAIL" not in line:
            sys.stderr.write("removed-snapshot si=%d recovered: %s\n" % (si, line))
            return verdict("removed-snapshot", "FAIL", "PASS")
        rc = wait_proc(proc, timeout=15.0)
        if rc is None:
            return vacuous("removed-snapshot", "FAIL", "proc_no_exit")
        sys.stderr.write("removed-snapshot si=%d rc=%s line=%s\n" % (si, rc, line))
        return verdict("removed-snapshot", "FAIL", "FAIL" if rc != 0 else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
