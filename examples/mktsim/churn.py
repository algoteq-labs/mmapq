#!/usr/bin/env python3
"""churn.py — kill/restart/join churn over the mktsim market simulation.

Phases:
  1. start N traders on a fresh queue
  2. churn: repeatedly SIGKILL a random live trader; then either restart it
     (same trader id, next incarnation: rebuilds book + own orders, resumes
     its sequence) or start a brand-new trader that joins mid-session
  3. attrition: SIGKILL without replacement until --survivors remain
  4. stop: survivors catch up and report their final state; verifier replays
     the whole log and checks every claim, every JOIN/FINAL line,
     per-trader sequence continuity, contiguity, and conservation.

A killed process is always reaped before its replacement starts, so two
incarnations of one trader never run at the same time.

Without --reclaim each incarnation takes a fresh appender id: ids held by
killed processes are never released by the library, so a queue supports at
most 256 participants over its lifetime; the driver refuses a plan that
exceeds it. With --reclaim (tools/mmapq-ids), the driver acts as a
supervisor: it reaps the killed process — which is what establishes that the
id is unowned — releases the id, and restarts the trader on the same lane.

Exit 0 only if no process failed and the verifier passed.
"""
import argparse, os, random, shutil, signal, subprocess, sys, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "mktsim"))
    ap.add_argument("--dir", required=True, help="parent directory (wiped)")
    ap.add_argument("--traders", type=int, default=8)
    ap.add_argument("--kills", type=int, default=40, help="kills during churn")
    ap.add_argument("--restart-prob", type=float, default=0.7, help="else a new trader joins")
    ap.add_argument("--survivors", type=int, default=2)
    ap.add_argument("--min-gap", type=float, default=0.05)
    ap.add_argument("--max-gap", type=float, default=0.4)
    ap.add_argument("--mode", choices=["sync", "async"], default="sync")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--reclaim", metavar="MMAPQ_IDS",
                    help="path to mmapq-ids: after reaping a killed trader, release its appender id "
                         "and restart it on the same lane")
    a = ap.parse_args()

    seed = a.seed if a.seed is not None else random.randrange(1 << 30)
    rnd = random.Random(seed)
    if not a.reclaim and a.traders + a.kills > 250:
        sys.exit("plan needs more than 250 appender ids; without --reclaim the pool never frees them")

    shutil.rmtree(a.dir, ignore_errors=True)
    os.makedirs(os.path.join(a.dir, "logs"))
    stop = os.path.join(a.dir, "STOP")
    qname = "market"

    live = {}            # trader id -> (Popen, incarnation, logpath)
    logs, failures = [], []
    next_inc = {}
    next_new_id = a.traders
    stats = dict(kills=0, restarts=0, joiners=0, incarnations=0, reclaimed=0,
                 killed_before_acquire=0)

    def start(tid):
        # With --reclaim, ids are assigned up front: trader N writes on appender
        # id N. No pool scan, so a freed id cannot be taken by someone else
        # between release and re-acquire, and the supervisor never has to learn
        # an id from the process. Without --reclaim, ids come from the pool.
        want_id = tid if a.reclaim else -1
        inc = next_inc.get(tid, 0)
        next_inc[tid] = inc + 1
        log = os.path.join(a.dir, "logs", f"t{tid:02d}_i{inc:03d}.log")
        f = open(log, "w")
        p = subprocess.Popen([a.bin, "trader", a.dir, qname, str(tid), str(inc), stop, a.mode, str(want_id)],
                             stdout=f, stderr=subprocess.STDOUT)
        f.close()
        live[tid] = (p, inc, log)
        logs.append(log)
        stats["incarnations"] += 1

    def reap_unexpected():
        for tid, (p, inc, log) in list(live.items()):
            rc = p.poll()
            if rc is not None:
                failures.append(f"trader {tid}/{inc} exited on its own rc={rc} (see {log})")
                del live[tid]

    def kill(tid):
        """SIGKILL and reap. With --reclaim, release the dead process's appender id:
        the supervisor has reaped it, so it knows the id is unowned — the
        precondition mmapq-ids cannot check for itself. Returns the freed id or -1."""
        p, inc, log = live.pop(tid)
        p.send_signal(signal.SIGKILL)
        rc = p.wait()
        if rc != -signal.SIGKILL:
            failures.append(f"trader {tid}/{inc} had already exited rc={rc} before kill (see {log})")
        stats["kills"] += 1
        if not a.reclaim:
            return
        aid = tid    # fixed assignment
        r = subprocess.run([a.reclaim, a.dir, qname, "--unset", str(aid)], capture_output=True, text=True)
        if r.returncode == 1:
            # already free: the process died before acquiring — nothing leaked
            stats["killed_before_acquire"] += 1
        elif r.returncode != 0:
            failures.append(f"mmapq-ids --unset {aid} for dead trader {tid}/{inc} rc={r.returncode}: "
                            f"{r.stdout.strip()} {r.stderr.strip()[-300:]}")
        else:
            stats["reclaimed"] += 1

    print(f"seed={seed} traders={a.traders} kills={a.kills} survivors={a.survivors} mode={a.mode}")
    for t in range(a.traders):
        start(t)
        time.sleep(rnd.uniform(0, 0.05))
    time.sleep(0.5)

    # churn
    for _ in range(a.kills):
        time.sleep(rnd.uniform(a.min_gap, a.max_gap))
        reap_unexpected()
        if failures or not live:
            break
        tid = rnd.choice(sorted(live))
        kill(tid)
        if rnd.random() < a.restart_prob or next_new_id >= 64:
            start(tid); stats["restarts"] += 1
        else:
            start(next_new_id); next_new_id += 1; stats["joiners"] += 1

    # attrition
    while not failures and len(live) > a.survivors:
        time.sleep(rnd.uniform(a.min_gap, a.max_gap))
        reap_unexpected()
        if live:
            kill(rnd.choice(sorted(live)))
    time.sleep(0.3)
    reap_unexpected()

    # stop, phase 1: stop publishing; wait until every survivor says so
    open(stop, "w").close()
    deadline = time.time() + 60
    while time.time() < deadline:
        reap_unexpected()
        if all(any(l.startswith("QUIESCED") for l in open(log)) for (_, _, log) in live.values()):
            break
        time.sleep(0.02)
    else:
        failures.append("survivors did not quiesce within 60 s (hang?)")
    # phase 2: nobody publishes any more — read to the end and report
    open(stop + ".final", "w").close()
    for tid, (p, inc, log) in live.items():
        try:
            rc = p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill(); rc = "TIMEOUT(hang)"
        if rc != 0:
            failures.append(f"survivor {tid}/{inc} rc={rc} (see {log})")

    v = subprocess.run([a.bin, "verify", a.dir, qname] + logs, capture_output=True, text=True)
    print(v.stdout.strip())
    if v.returncode != 0:
        failures.append(f"verifier failed rc={v.returncode}:\n" + v.stderr.strip())

    finals = []
    for log in logs:
        for line in open(log):
            if line.startswith("FINAL"):
                finals.append(line.split()[4:7])
    agree = len({tuple(f) for f in finals}) <= 1
    if not agree:
        failures.append(f"survivors disagree on final state: {finals}")
    total = next((tok.split("=")[1] for tok in v.stdout.split() if tok.startswith("messages=")), None)
    if finals and total is not None and any(f[0] != total for f in finals):
        failures.append(f"a survivor's final count differs from the log length {total}: {finals}")

    # pool and lane accounting (all survivors have closed cleanly by now)
    qdir = os.path.join(a.dir, qname)
    pool = open(os.path.join(qdir, "appender_id_pool"), "rb").read()[:32]
    taken = sum(bin(b).count("1") for b in pool)
    lanes = sum(1 for n in os.listdir(qdir) if n.startswith("appender_meta_"))
    conflicts = sum(1 for log in logs for l in open(log) if l.startswith("IDCONFLICT"))
    if a.reclaim and taken != 0:
        failures.append(f"{taken} ids still taken after all writers exited (expected 0)")
    if a.reclaim and lanes != len(next_inc):
        failures.append(f"{lanes} lanes for {len(next_inc)} traders (expected one lane per trader)")

    print(f"kills={stats['kills']} restarts={stats['restarts']} joiners={stats['joiners']} "
          f"incarnations={stats['incarnations']} survivors={len(live)} "
          f"survivor_final_states_agree={agree}")
    print(f"reclaim={'on (fixed ids)' if a.reclaim else 'off (pool ids)'} ids_reclaimed={stats['reclaimed']} "
          f"killed_before_acquire={stats['killed_before_acquire']} id_conflicts={conflicts} "
          f"traders={len(next_inc)} lanes_created={lanes} ids_taken_at_end={taken}")
    for f in failures:
        print("FAILURE:", f)
        # surface the failing process's own diagnosis
        if "see " in f:
            path = f.split("see ")[1].rstrip(")")
            try:
                print("   ", "".join(l for l in open(path) if not l.startswith(("JOIN", "FINAL")))[-2000:].strip())
            except OSError:
                pass
    print("RESULT:", "PASS" if not failures else "FAIL")
    return 0 if not failures else 1

if __name__ == "__main__":
    sys.exit(main())
