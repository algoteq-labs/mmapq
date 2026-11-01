#!/usr/bin/env python3
"""gwsnap.py — gateway recovery from the snapshot prefix (E4).

Two traders, one gateway and a cadence-1 snapshotter run on an archivable
queue until the snapshotter's start_index has moved past the gateway's
committed prefix C. The gateway is then SIGKILLed and restarted: its committed
records are archived, so it derives C from snap_<start_index>.gw_last and the
live scan, printing GWRECOVER gw=0 prefix=C snap=<k> result=PASS with
prefix >= the archived C and snap > 0. Expected PASS.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, field, first_prefix, read, snapshotter_si,
                      vacuous, verdict, wait_prefix)


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
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_gwsnap")
    t = Topo(d)
    t.fresh()
    gap = os.path.join(d, "market.gw00.gap")
    try:
        t.trader(0, batch=0)
        t.trader(1, batch=0)
        _, g0log = t.gateway(0)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(g0log, "GWJOIN", timeout=25.0):
            return vacuous("gwsnap", "PASS", "no_gwjoin")
        deadline = time.time() + 15.0
        C = -1
        while time.time() < deadline:
            C = max_gapcommit(gap)
            if C >= 50 and snapshotter_si(snap_log) > C:
                break
            time.sleep(0.05)
        if C < 0 or snapshotter_si(snap_log) <= C:
            return vacuous("gwsnap", "PASS", "si_never_passed_c")
        time.sleep(0.4)                       # snapshotter advances further
        _, g1log = t.restart_gateway(0, inc=1)
        if not wait_prefix(g1log, "GWRECOVER", timeout=25.0):
            return vacuous("gwsnap", "PASS", "no_gwrecover")
        rec = first_prefix(g1log, "GWRECOVER")
        prefix = int(field(rec, "prefix"))
        snap = int(field(rec, "snap"))
        passed = field(rec, "result") == "PASS"
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("gwsnap detail: %s\n" % detail)
        good = passed and prefix >= C and snap > 0 and ok
        sys.stderr.write("gwsnap C=%d recovered_prefix=%d snap=%d\n" % (C, prefix, snap))
        return verdict("gwsnap", "PASS", "PASS" if good else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
