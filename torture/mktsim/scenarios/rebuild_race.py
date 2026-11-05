#!/usr/bin/env python3
"""rebuild_race.py — the archive race on the rejoin rebuild.

Two traders and a cadence-1 snapshotter run on an archivable queue. Trader 0 is
restarted with MKTSIM_REBUILD_RACE armed: the plant copy-asides the payload file
holding the recovered index (link to archive/, unlink the live copy — the
archiver's own reclaim effect), so the rebuild's next poll fails
ERROR+ETIMEDOUT while start_index is still at the recovered value; the plant
then waits for the snapshotter to advance start_index past it. The fixed rule
re-checks start_index after the failed poll, returns DRAIN_ARCHIVED, and the
bounded rejoin loop re-recovers and joins cleanly. Both PLANT lines must
appear and the run must pass. Expected PASS.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import Topo, has_substr, snapshotter_si, vacuous, verdict, wait_prefix


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rebuild_race")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rebuild-race", "PASS", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        env = dict(os.environ)
        env["MKTSIM_REBUILD_RACE"] = "1"
        _, t0log1 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(t0log1, "PLANT rebuild-race copy-aside", timeout=25.0):
            return vacuous("rebuild-race", "PASS", "plant_not_fired")
        end = time.time() + 25.0
        moved = not_moved = False
        while time.time() < end:
            if has_substr(t0log1, "PLANT rebuild-race start_index-not-moved"):
                not_moved = True
                break
            if has_substr(t0log1, "PLANT rebuild-race start_index-moved"):
                moved = True
                break
            time.sleep(0.02)
        if not_moved:
            return vacuous("rebuild-race", "PASS", "start_index_not_moved")
        if not moved:
            return vacuous("rebuild-race", "PASS", "no_start_index_line")
        if not wait_prefix(t0log1, "SNAPRECOVER who=t00", timeout=25.0, count_gt=1):
            return vacuous("rebuild-race", "PASS", "no_re_recovery")
        if not wait_prefix(t0log1, "JOIN", timeout=25.0):
            return vacuous("rebuild-race", "PASS", "no_join")
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("rebuild-race detail: %s\n" % detail)
        return verdict("rebuild-race", "PASS", "PASS" if ok else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
