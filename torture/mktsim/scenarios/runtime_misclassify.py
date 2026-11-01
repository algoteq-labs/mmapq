#!/usr/bin/env python3
"""runtime_misclassify.py — the F8 error-classification plant.

MKTSIM_RUNTIME_MISCLASSIFY lowers a trader's mapping timeout and makes it
treat a retryable mapping timeout (MMAPQ_ETIMEDOUT) as fatal corruption. A
dedicated async trader runs with the plant while a normal trader generates
traffic; when the timeout lands, the armed trader prints
`PLANT runtime-misclassify …` and aborts (non-zero). Expected FAIL (the defect
the classification check catches).

This is a scenario rather than a churn row because churn.py arms the plant on
trader 1's *first incarnation only*: if churn kills that incarnation before a
mapping timeout lands, the plant never fires. Here the armed trader is never
killed, so the timeout lands deterministically enough.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness import Topo, vacuous, verdict, wait_prefix, wait_proc


def main():
    d = os.environ.get("MKTSIM_SCENARIO_DIR", "/tmp/mktsim_scenario_runtime_misclassify")
    t = Topo(d)
    t.fresh()
    try:
        t.trader(0, arch=0)
        env = dict(os.environ)
        env["MKTSIM_RUNTIME_MISCLASSIFY"] = "1"
        proc, log = t.trader(1, mode="async", arch=0, env=env)
        if not wait_prefix(log, "PLANT runtime-misclassify", timeout=30.0):
            return vacuous("runtime-misclassify", "FAIL", "plant_not_fired")
        rc = wait_proc(proc, timeout=15.0)
        sys.stderr.write("runtime-misclassify rc=%s\n" % rc)
        return verdict("runtime-misclassify", "FAIL", "FAIL" if rc not in (0, None) else "PASS")
    finally:
        t.kill_all()


if __name__ == "__main__":
    sys.exit(main())
