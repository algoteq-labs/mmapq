#!/usr/bin/env python3
"""_soak.py — one soak churn run with live disk sampling.

Runs churn.py once (no plants) and prints a single SOAKRUN metrics line:

    SOAKRUN <name> mode=<m> snap_every=<n> seed=<s> messages=<n> kills=<n>
            undercuts=<n> snaps=<n> peak_kb=<n> finals=<n> at_digest=<n>
            gw_hook=<h> hand_delete=<0|1> result=<PASS|FAIL>

`undercuts` is the number of SNAPRECOVER lines across the trader logs (the
initial join at start_index 0 prints none, so each is a real recovery);
`snaps` is the SNAPWRITE count; `peak_kb` is the peak of the live queues'
on-disk size sampled while the run is in flight; `at_digest` is the number of
survivor FINAL lines whose digest equals the verifier's. run_soak.sh owns the
matrix and the totals.
"""
import argparse
import glob
import os
import re
import subprocess
import threading
import time


def du_kb(paths):
    total = 0
    for p in paths:
        for root, _dirs, files in os.walk(p):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    return total // 1024


def field(line, key):
    m = re.search(r"(?:^|\s)" + re.escape(key) + r"=(\S+)", line)
    return m.group(1) if m else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--churn", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--mode", required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--snap-every", type=int, default=4096)
    ap.add_argument("--traders", type=int, default=8)
    ap.add_argument("--kills", type=int, default=60)
    ap.add_argument("--survivors", type=int, default=2)
    ap.add_argument("--hand-delete", action="store_true")
    ap.add_argument("--gw-hook", default="")
    a = ap.parse_args()

    cmd = ["python3", a.churn, "--bin", a.bin, "--dir", a.dir, "--mode", a.mode,
           "--seed", str(a.seed), "--traders", str(a.traders), "--kills", str(a.kills),
           "--survivors", str(a.survivors), "--archive", "--snap-every", str(a.snap_every)]
    if a.hand_delete:
        cmd.append("--hand-delete")
    env = dict(os.environ)
    if a.gw_hook:
        env[a.gw_hook] = "1"

    peak = {"kb": 0}
    done = {"v": False}
    qpaths = [os.path.join(a.dir, "market"), os.path.join(a.dir, "market_batch")]

    def sampler():
        while not done["v"]:
            kb = du_kb(qpaths)
            if kb > peak["kb"]:
                peak["kb"] = kb
            time.sleep(0.2)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    done["v"] = True
    th.join(timeout=2.0)
    out = r.stdout
    lines = out.splitlines()
    # Keep churn's stderr: capture_output separates it, and reading only stdout
    # would silently drop a reason printed there. Full text on disk; the tail
    # joins the SOAKTAIL block below.
    err = r.stderr or ""
    try:
        with open(os.path.join(a.dir, "churn.stderr"), "w") as f:
            f.write(err)
    except OSError:
        pass

    verify = [l for l in lines if l.startswith("VERIFY ")]
    vdig = field(verify[-1], "digest") if verify else None
    msgs = field(verify[-1], "messages") if verify else "-"
    result = "PASS" if any(l.startswith("RESULT: PASS") for l in lines) else "FAIL"
    kills = field([l for l in lines if l.startswith("kills=")][-1], "kills") if any(l.startswith("kills=") for l in lines) else "-"
    # Survivor FINAL lines live in the trader logs, not in churn's stdout.
    finals = []
    for lg in glob.glob(os.path.join(a.dir, "logs", "t*.log")):
        try:
            with open(lg) as f:
                finals += [l for l in f if l.startswith("FINAL ")]
        except OSError:
            pass
    at_digest = sum(1 for l in finals if len(l.split()) > 6 and l.split()[6] == vdig)

    under = 0
    for lg in glob.glob(os.path.join(a.dir, "logs", "t*.log")):
        try:
            with open(lg) as f:
                under += sum(1 for l in f if l.startswith("SNAPRECOVER who=t"))
        except OSError:
            pass
    snaps = 0
    sl = os.path.join(a.dir, "logs", "snapshotter.log")
    if os.path.exists(sl):
        with open(sl) as f:
            snaps = sum(1 for l in f if l.startswith("SNAPWRITE"))

    print("SOAKRUN %s mode=%s snap_every=%d seed=%d messages=%s kills=%s "
          "undercuts=%d snaps=%d peak_kb=%d finals=%d at_digest=%d gw_hook=%s "
          "hand_delete=%d result=%s"
          % (a.name, a.mode, a.snap_every, a.seed, msgs, kills, under, snaps,
             peak["kb"], len(finals), at_digest, a.gw_hook or "-",
             1 if a.hand_delete else 0, result))
    if result != "PASS":
        tail = "\n".join(lines[-15:])
        if err.strip():
            tail += "\n--- churn stderr (tail; full text in %s) ---\n%s" % (
                os.path.join(a.dir, "churn.stderr"), "\n".join(err.splitlines()[-15:]))
        print("SOAKTAIL %s\n%s" % (a.name, tail))
    return 0 if result == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
