#!/usr/bin/env python3
"""rebuild_timeout.py — the rebuild retryable-timeout path.

Trader 0 is restarted with MKTSIM_REBUILD_TIMEOUT armed, which forces
the rebuild's first poll result to ERROR+ETIMEDOUT (start_index unchanged, so
the ARCHIVED re-check cannot fire). The fixed rule retries the mapping timeout
with the main loop's bound and counter, then the rebuild completes and the
trader joins. The PLANT line must appear and the run must pass. Expected PASS.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, field, last_prefix, snapshotter_si, vacuous,
                      verdict, wait_prefix)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rebuild_timeout")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rebuild-timeout", "PASS", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        env = dict(os.environ)
        env["MKTSIM_REBUILD_TIMEOUT"] = "1"
        _, t0log1 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(t0log1, "PLANT rebuild-timeout", timeout=25.0):
            return vacuous("rebuild-timeout", "PASS", "plant_not_fired")
        if not wait_prefix(t0log1, "PLANT rebuild-timeout retried", timeout=25.0):
            return vacuous("rebuild-timeout", "PASS", "no_retry")
        pl = last_prefix(t0log1, "PLANT rebuild-timeout who=")
        rt = last_prefix(t0log1, "PLANT rebuild-timeout retried")
        pidx, ridx = field(pl, "index"), field(rt, "index")
        if pidx is None or ridx is None or pidx != ridx:
            return vacuous("rebuild-timeout", "PASS", "retry_index_mismatch")
        if not wait_prefix(t0log1, "JOIN", timeout=25.0):
            return vacuous("rebuild-timeout", "PASS", "no_join")
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("rebuild-timeout detail: %s\n" % detail)
        return verdict("rebuild-timeout", "PASS", "PASS" if ok else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
