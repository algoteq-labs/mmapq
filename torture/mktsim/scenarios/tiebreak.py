#!/usr/bin/env python3
"""tiebreak.py — the F1 engine-divergence plant (MKTSIM_TIEBREAK_BUG).

Two traders run on a fresh non-archivable queue; trader 0's engine is armed
with the wrong price-time tie-break, trader 1 (and verify) are not. Trader 1
and the verifier must catch the divergent claim at a low index
(DIVERGENCE ...). This is the F1 planted defect, kept as the demo's proof that
the claim (H, D) check catches a divergent read. The hook must be armed on one
process only — armed globally, every engine (verify included) diverges
identically and the check cannot fire. Expected FAIL (the defect is caught).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import Topo, field, read, vacuous, verdict


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_tiebreak")
    t = Topo(d)
    t.fresh()
    try:
        env = dict(os.environ)
        env["MKTSIM_TIEBREAK_BUG"] = "1"
        t.trader(0, arch=0, env=env)
        t.trader(1, arch=0)
        time.sleep(1.5)
        if not any("PLANT tiebreak-bug" in read(log) for (_, _, _, log) in t.traders):
            return vacuous("tiebreak", "FAIL", "plant_not_fired")
        ok, detail = t.clean_stop()
        if detail:
            sys.stderr.write("tiebreak detail: %s\n" % detail)
        vline = next((l for l in t.last_verify.splitlines()
                      if l.startswith("VERIFY ")), "")
        trades = int(field(vline, "trades") or "0")
        diverged = any("DIVERGENCE" in read(log) for (_, _, _, log) in t.traders)
        if trades == 0 and not diverged:
            return vacuous("tiebreak", "FAIL", "no_trades")
        return verdict("tiebreak", "FAIL", "FAIL" if (diverged or not ok) else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
