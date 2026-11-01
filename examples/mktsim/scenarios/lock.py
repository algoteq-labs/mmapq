#!/usr/bin/env python3
"""lock.py — the one-snapshotter exclusive guard (amendment 5, E1).

A first snapshotter acquires <dir>/snapshotter.lock. A second instance started
against the same directory must fail immediately: it prints
SNAPSHOTTER lock=busy result=FAIL and exits non-zero, so it can never move
start_index concurrently. Expected PASS.
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import BIN, QNAME, Topo, vacuous, verdict, wait_prefix


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_lock")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        qdir = os.path.join(d, QNAME)
        end = time.time() + 15.0
        while time.time() < end and not os.path.isdir(qdir):
            time.sleep(0.02)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPSHOTTER lock=acquired", timeout=25.0):
            return vacuous("lock", "PASS", "no_lock_acquired")
        r = subprocess.run([BIN, "snapshotter", d, QNAME, t.stop, "0",
                            "--snap-every", "1"], capture_output=True, text=True)
        busy = "SNAPSHOTTER lock=busy result=FAIL" in r.stdout
        second_rc_nonzero = r.returncode != 0
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("lock detail: %s\n" % detail)
        good = busy and second_rc_nonzero and ok
        sys.stderr.write("lock second_rc=%d busy=%s\n" % (r.returncode, busy))
        return verdict("lock", "PASS", "PASS" if good else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
