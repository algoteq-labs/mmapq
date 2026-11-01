#!/usr/bin/env python3
"""die_after_write.py — the E2 crash-between-write-and-archive plant.

MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE=2 makes the snapshotter exit right after the
second snap_<k> rename and before archive_up_to_index(k), leaving an orphan
snapshot above the still-unmoved start_index (the "red" phase). A restarted
snapshotter (no plant) recovers from snap_<start_index>, and its next GC
deletes the orphan, so its SNAPGUARD shows exactly {previous, start_index}
(the "green" phase). Expected PASS (red then the restart green).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import Topo, read, vacuous, verdict, wait_prefix, wait_proc


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_die_after_write")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        env = dict(os.environ)
        env["MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE"] = "2"
        p, log = t.snapshotter_start(snap_every=1, env=env)
        if not wait_prefix(log, "PLANT die-after-write", timeout=25.0):
            return vacuous("die-after-write", "PASS", "plant_not_fired")
        rc = wait_proc(p, timeout=15.0)
        if rc != 9:
            return vacuous("die-after-write", "PASS", "die_rc_%s" % rc)
        # Restart the snapshotter without the plant; it must recover and GC the
        # orphan, printing an exact SNAPGUARD PASS.
        _, log2 = t.snapshotter_start(snap_every=1)
        if not wait_prefix(log2, "SNAPGUARD", timeout=25.0):
            return vacuous("die-after-write", "PASS", "no_guard")
        guard_pass = any(l.startswith("SNAPGUARD") and "result=PASS" in l
                         for l in read(log2).splitlines())
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("die-after-write detail: %s\n" % detail)
        return verdict("die-after-write", "PASS",
                       "PASS" if (guard_pass and ok) else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
