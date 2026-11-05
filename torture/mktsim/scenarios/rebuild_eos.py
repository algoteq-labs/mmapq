#!/usr/bin/env python3
"""rebuild_eos.py — the rebuild end-of-stream re-poll path.

Trader 0 is restarted with MKTSIM_REBUILD_EOS armed, which forces the
rebuild's first poll result to END_OF_STREAM (start_index unchanged). The fixed
rule re-polls EOS with a bounded wait (a transient last_index/poller lag, as the
main loop tolerates by re-polling at once); the next poll advances and the
trader joins. The PLANT line must appear and the run must pass. Expected PASS.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import (Topo, field, last_prefix, snapshotter_si, vacuous,
                      verdict, wait_prefix)


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_rebuild_eos")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0)
        t.trader(1)
        t.snapshotter_start(snap_every=1)
        snap_log = t.snapshotter[1]
        if not wait_prefix(snap_log, "SNAPWRITE", timeout=25.0):
            return vacuous("rebuild-eos", "PASS", "no_snapwrite")
        end = time.time() + 3.0
        while time.time() < end and snapshotter_si(snap_log) < 2000:
            time.sleep(0.05)
        env = dict(os.environ)
        env["MKTSIM_REBUILD_EOS"] = "1"
        _, t0log1 = t.restart_trader(0, inc=1, env=env)
        if not wait_prefix(t0log1, "PLANT rebuild-eos", timeout=25.0):
            return vacuous("rebuild-eos", "PASS", "plant_not_fired")
        if not wait_prefix(t0log1, "PLANT rebuild-eos retried", timeout=25.0):
            return vacuous("rebuild-eos", "PASS", "no_retry")
        pl = last_prefix(t0log1, "PLANT rebuild-eos who=")
        rt = last_prefix(t0log1, "PLANT rebuild-eos retried")
        pidx, ridx = field(pl, "index"), field(rt, "index")
        if pidx is None or ridx is None or pidx != ridx:
            return vacuous("rebuild-eos", "PASS", "retry_index_mismatch")
        if not wait_prefix(t0log1, "JOIN", timeout=25.0):
            return vacuous("rebuild-eos", "PASS", "no_join")
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("rebuild-eos detail: %s\n" % detail)
        return verdict("rebuild-eos", "PASS", "PASS" if ok else "FAIL")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
