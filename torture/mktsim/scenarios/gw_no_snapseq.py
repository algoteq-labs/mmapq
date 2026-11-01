#!/usr/bin/env python3
"""gw_no_snapseq.py — the E4 gateway-field plant (MKTSIM_GW_NO_SNAPSEQ).

Two traders, one gateway and a cadence-1 snapshotter run on an archivable
queue. The gateway is SIGKILLed once it has committed a prefix C, and the
snapshotter is then allowed to archive past C — so every committed record is
archived and a restart sees live_committed=0. The snapshotter is armed with
MKTSIM_GW_NO_SNAPSEQ (its snapshot's per-gateway field is zeroed), so the
restart cannot derive C from the snapshot and the gap log is ahead of the
recovered prefix → GWRECOVER … result=FAIL. Expected FAIL (the defect the
snapshot field fixes).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, last_prefix, read, snapshotter_si, vacuous,
                      verdict, wait_prefix, wait_proc)


def max_gapcommit(path):
    best = -1
    for l in read(path).splitlines():
        if l.startswith("GAPCOMMIT"):
            try:
                s = int(l.split()[1])
            except (IndexError, ValueError):
                continue
            if s > best:
                best = s
    return best


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_gw_no_snapseq")
    t = Topo(d)
    t.fresh()
    gap = os.path.join(d, "market.gw00.gap")
    try:
        t.trader(0, batch=0)
        t.trader(1, batch=0)
        g0p, g0log = t.gateway(0)
        env = dict(os.environ)
        env["MKTSIM_GW_NO_SNAPSEQ"] = "1"
        t.snapshotter_start(snap_every=1, env=env)
        snap_log = t.snapshotter[1]
        if not wait_prefix(g0log, "GWJOIN", timeout=25.0):
            return vacuous("gw-no-snapseq", "FAIL", "no_gwjoin")
        # Let the gateway commit a useful prefix, then kill it so C freezes.
        deadline = time.time() + 15.0
        C = -1
        while time.time() < deadline and max_gapcommit(gap) < 50:
            time.sleep(0.05)
        C = max_gapcommit(gap)
        if C < 50:
            return vacuous("gw-no-snapseq", "FAIL", "no_committed_prefix")
        t.kill(g0p)
        # Archive well past the gateway's records. C is a sequence number, not
        # an index, so start_index > C does not prove the records are archived;
        # the traders keep publishing and the snapshotter advances far beyond
        # the (now frozen) gateway records, so the restart sees live_committed=0.
        deadline = time.time() + 15.0
        while time.time() < deadline and snapshotter_si(snap_log) <= 50000:
            time.sleep(0.05)
        if snapshotter_si(snap_log) <= 50000:
            return vacuous("gw-no-snapseq", "FAIL", "si_never_passed_records")
        if not wait_prefix(snap_log, "PLANT gw-no-snapseq", timeout=10.0):
            return vacuous("gw-no-snapseq", "FAIL", "plant_not_fired")
        proc, g1log = t.restart_gateway(0, inc=1)
        if not wait_prefix(g1log, "GWRECOVER", timeout=25.0):
            return vacuous("gw-no-snapseq", "FAIL", "no_gwrecover")
        rec = last_prefix(g1log, "GWRECOVER")
        failed = "result=FAIL" in rec
        rc = wait_proc(proc, timeout=15.0)
        sys.stderr.write("gw-no-snapseq C=%d recover=%s rc=%s\n" % (C, rec, rc))
        return verdict("gw-no-snapseq", "FAIL",
                       "FAIL" if (failed and rc not in (0, None)) else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
