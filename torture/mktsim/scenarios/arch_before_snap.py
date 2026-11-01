#!/usr/bin/env python3
"""arch_before_snap.py — archive without the snapshot (E3 plant).

MKTSIM_ARCH_BEFORE_SNAP makes the snapshotter archive to k without writing
snap_<k>, print PLANT arch-before-snap and exit rc=9, leaving start_index == k
with snap_<k> absent. A restarting trader's bounded recovery must fail (the
same start_index returns twice with the snapshot still missing), not hang and
not start empty. Expected FAIL (the defect the no-progress guard catches).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, last_prefix, vacuous, verdict, wait_prefix,
                      wait_proc)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_arch_before_snap")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        env = dict(os.environ)
        env["MKTSIM_ARCH_BEFORE_SNAP"] = "1"
        p, snap_log = t.snapshotter_start(snap_every=1, env=env)
        if not wait_prefix(snap_log, "PLANT arch-before-snap", timeout=25.0):
            return vacuous("arch-before-snap", "FAIL", "plant_not_fired")
        plant_rc = wait_proc(p, timeout=15.0)
        if plant_rc != 9:
            return vacuous("arch-before-snap", "FAIL", "plant_rc_%s" % plant_rc)
        # Stop every publisher before the restart (a realistic restart: no
        # other incarnation of trader 1 is left running).
        for (_, _, tp, _) in list(t.traders):
            t.kill(tp)
        proc, log1 = t.restart_trader(1, inc=1)
        if not wait_prefix(log1, "SNAPRECOVER who=t01", timeout=25.0):
            rc = wait_proc(proc, timeout=5.0)
            if rc is None:
                return vacuous("arch-before-snap", "FAIL", "no_fail_line")
            return verdict("arch-before-snap", "FAIL", "FAIL" if rc != 0 else "PASS")
        line = last_prefix(log1, "SNAPRECOVER who=t01")
        if "result=FAIL" not in line:
            sys.stderr.write("arch-before-snap recovered: %s\n" % line)
            return verdict("arch-before-snap", "FAIL", "PASS")
        rc = wait_proc(proc, timeout=15.0)
        if rc is None:
            return vacuous("arch-before-snap", "FAIL", "proc_no_exit")
        sys.stderr.write("arch-before-snap plant_rc=%s rc=%s line=%s\n" % (plant_rc, rc, line))
        return verdict("arch-before-snap", "FAIL", "FAIL" if rc != 0 else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
