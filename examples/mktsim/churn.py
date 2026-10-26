#!/usr/bin/env python3
"""churn.py — kill/restart/join churn over the mktsim market simulation.

Phases:
  1. start N traders on a fresh queue (plus --gateways gateways)
  2. churn: repeatedly SIGKILL a random live participant; a killed trader is
     either restarted (same trader id, next incarnation: rebuilds book + own
     orders, resumes its sequence) or replaced by a brand-new trader that
     joins mid-session; a killed gateway is always restarted (same gateway id,
     next incarnation: the restart walk completes or re-appends its
     uncommitted tail)
  3. attrition: SIGKILL traders without replacement until --survivors remain
     (gateways are never attrition victims: their uncommitted tail must be
     committed by a live incarnation before the run ends)
  4. stop: survivors catch up and report their final state; gateways flush
     their pending packets and exit; verifier replays both queues and checks
     every claim, every JOIN/FINAL line, per-trader sequence continuity,
     contiguity, conservation, and the gateway lane's committed order.

A killed process is always reaped before its replacement starts, so two
incarnations of one participant never run at the same time.

Without --reclaim each incarnation takes a fresh appender id: ids held by
killed processes are never released by the library, so a queue supports at
most 256 participants over its lifetime; the driver refuses a plan that
exceeds it. With --reclaim (tools/mmapq-ids), the driver acts as a
supervisor: it reaps the killed process — which is what establishes that the
id is unowned — releases the id, and restarts the participant on the same
lane. Fixed assignment: trader N = appender id N on BOTH queues (the batch
lane mirrors the single-message lane), gateway G = appender id 64+G on the
single-message queue.

Exit 0 only if no process failed and the verifier passed.
"""
import argparse, os, random, shutil, signal, subprocess, sys, time

GW_ID_BASE = 64        # gateway G uses appender id 64+G; trader ids stay below 64

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
    ap.add_argument("--gateways", type=int, default=1, help="gateway participants (0 disables F5)")
    ap.add_argument("--batch", type=int, choices=[0, 1], default=1,
                    help="1: traders also drive the market_batch queue (F4); 0: exactly the pre-F4 run")
    ap.add_argument("--reclaim", metavar="MMAPQ_IDS",
                    help="path to mmapq-ids: after reaping a killed participant, release its appender id "
                         "and restart it on the same lane")
    # Scenario hooks (stage b rework): the driver arms the FIRST incarnation of
    # gateway 0 with the hook and disarms its own environment, so no other
    # process inherits it. The armed gateway SIGKILLs itself between the queue
    # operation and its log line; the driver treats that death as the hook
    # firing (not a failure) and restarts the gateway disarmed.
    gw_hook = None
    if os.environ.pop("MKTSIM_GW_KILL_AFTER_APPEND", None):
        gw_hook = "MKTSIM_GW_KILL_AFTER_APPEND"
    if os.environ.pop("MKTSIM_GW_KILL_AFTER_COMMIT", None):
        gw_hook = "MKTSIM_GW_KILL_AFTER_COMMIT"

    a = ap.parse_args()

    seed = a.seed if a.seed is not None else random.randrange(1 << 30)
    rnd = random.Random(seed)
    if not 0 <= a.gateways <= 64:
        sys.exit("--gateways must be in 0..64")
    if not a.reclaim and a.traders + a.gateways + 2 * a.kills > 250:
        sys.exit("plan needs more than 250 appender ids; without --reclaim the pool never frees them")

    shutil.rmtree(a.dir, ignore_errors=True)
    os.makedirs(os.path.join(a.dir, "logs"))
    stop = os.path.join(a.dir, "STOP")
    qname = "market"
    bname = "market_batch"

    live = {}            # trader id -> (Popen, incarnation, logpath)
    live_g = {}          # gateway id -> (Popen, incarnation, logpath)
    logs, gw_logs, failures = [], [], []
    next_inc = {}
    next_ginc = {}
    next_new_id = a.traders
    stats = dict(kills=0, restarts=0, joiners=0, incarnations=0, reclaimed=0,
                 killed_before_acquire=0, batch_reclaimed=0,
                 gateway_kills=0, gateway_restarts=0)

    def start(tid):
        # With --reclaim, ids are assigned up front: trader N writes on appender
        # id N (on both queues). No pool scan, so a freed id cannot be taken by
        # someone else between release and re-acquire, and the supervisor never
        # has to learn an id from the process. Without --reclaim, ids come from
        # the pool.
        want_id = tid if a.reclaim else -1
        inc = next_inc.get(tid, 0)
        next_inc[tid] = inc + 1
        log = os.path.join(a.dir, "logs", f"t{tid:02d}_i{inc:03d}.log")
        f = open(log, "w")
        p = subprocess.Popen([a.bin, "trader", a.dir, qname, str(tid), str(inc), stop,
                              a.mode, str(want_id), str(a.batch)],
                             stdout=f, stderr=subprocess.STDOUT)
        f.close()
        live[tid] = (p, inc, log)
        logs.append(log)
        stats["incarnations"] += 1

    gw_hook_armed = None       # gid of the armed incarnation, until it fires

    def start_gateway(gid):
        nonlocal gw_hook_armed
        want_id = GW_ID_BASE + gid if a.reclaim else -1
        inc = next_ginc.get(gid, 0)
        next_ginc[gid] = inc + 1
        log = os.path.join(a.dir, "logs", f"g{gid:02d}_i{inc:03d}.log")
        env = None
        if gw_hook and gid == 0 and inc == 0:
            env = dict(os.environ); env[gw_hook] = "1"
            gw_hook_armed = gid
        f = open(log, "w")
        p = subprocess.Popen([a.bin, "gateway", a.dir, qname, str(gid), str(inc), stop,
                              a.mode, str(want_id)],
                             stdout=f, stderr=subprocess.STDOUT, env=env)
        f.close()
        live_g[gid] = (p, inc, log)
        gw_logs.append(log)

    def reap_unexpected(gw_ok=False):
        """Flag participants that exited on their own. gw_ok: at stop phase a
        gateway exiting rc=0 is the expected flush-and-quit, not a failure."""
        nonlocal gw_hook_armed
        for tid, (p, inc, log) in list(live.items()):
            rc = p.poll()
            if rc is not None:
                failures.append(f"trader {tid}/{inc} exited on its own rc={rc} (see {log})")
                del live[tid]
        for gid, (p, inc, log) in list(live_g.items()):
            rc = p.poll()
            if rc is not None:
                if gw_hook_armed == gid and rc == -signal.SIGKILL:
                    # the armed scenario hook fired: release the lane (the
                    # process is reaped, so the id is unowned), restart
                    # disarmed — not a failure
                    gw_hook_armed = None
                    stats["gw_hook_kills"] = stats.get("gw_hook_kills", 0) + 1
                    del live_g[gid]
                    if a.reclaim:
                        unset(qname, GW_ID_BASE + gid, f"gateway {gid}/{inc} (hook)")
                    start_gateway(gid)
                    continue
                if not (gw_ok and rc == 0):
                    failures.append(f"gateway {gid}/{inc} exited on its own rc={rc} (see {log})")
                del live_g[gid]

    def unset(queue, aid, who):
        """Release a dead process's appender id: the supervisor has reaped it, so
        it knows the id is unowned — the precondition mmapq-ids cannot check for
        itself."""
        r = subprocess.run([a.reclaim, a.dir, queue, "--unset", str(aid)], capture_output=True, text=True)
        if r.returncode == 1:
            # already free: the process died before acquiring — nothing leaked
            stats["killed_before_acquire"] += 1
        elif r.returncode != 0:
            failures.append(f"mmapq-ids --unset {aid} on {queue} for dead {who} rc={r.returncode}: "
                            f"{r.stdout.strip()} {r.stderr.strip()[-300:]}")
        else:
            stats["reclaimed" if queue == qname else "batch_reclaimed"] += 1

    def kill(tid):
        """SIGKILL and reap a trader; with --reclaim, unset its id on BOTH queues."""
        p, inc, log = live.pop(tid)
        p.send_signal(signal.SIGKILL)
        rc = p.wait()
        if rc != -signal.SIGKILL:
            failures.append(f"trader {tid}/{inc} had already exited rc={rc} before kill (see {log})")
        stats["kills"] += 1
        if not a.reclaim:
            return
        unset(qname, tid, f"trader {tid}/{inc}")
        if a.batch:
            unset(bname, tid, f"trader {tid}/{inc}")

    def kill_gateway(gid):
        p, inc, log = live_g.pop(gid)
        p.send_signal(signal.SIGKILL)
        rc = p.wait()
        if rc != -signal.SIGKILL:
            failures.append(f"gateway {gid}/{inc} had already exited rc={rc} before kill (see {log})")
        stats["gateway_kills"] += 1
        if a.reclaim:
            unset(qname, GW_ID_BASE + gid, f"gateway {gid}/{inc}")

    print(f"seed={seed} traders={a.traders} kills={a.kills} survivors={a.survivors} mode={a.mode} "
          f"gateways={a.gateways} batch={a.batch}")
    for t in range(a.traders):
        start(t)
        time.sleep(rnd.uniform(0, 0.05))
    for g in range(a.gateways):
        start_gateway(g)
        time.sleep(rnd.uniform(0, 0.05))
    time.sleep(0.5)

    # churn (gateways are in the kill pool; they are always restarted)
    for _ in range(a.kills):
        time.sleep(rnd.uniform(a.min_gap, a.max_gap))
        reap_unexpected()
        if failures or (not live and not live_g):
            break
        pool = [("t", t) for t in sorted(live)] + \
               [("g", g) for g in sorted(live_g) if g != gw_hook_armed]
        kind, vid = rnd.choice(pool)
        if kind == "t":
            kill(vid)
            if rnd.random() < a.restart_prob or next_new_id >= 64:
                start(vid); stats["restarts"] += 1
            else:
                start(next_new_id); next_new_id += 1; stats["joiners"] += 1
        else:
            kill_gateway(vid)
            start_gateway(vid); stats["gateway_restarts"] += 1

    # attrition (traders only: a dead gateway's uncommitted tail would stay
    # uncommitted, so gateways always live to the stop phase)
    while not failures and len(live) > a.survivors:
        time.sleep(rnd.uniform(a.min_gap, a.max_gap))
        reap_unexpected()
        if live:
            kill(rnd.choice(sorted(live)))
    time.sleep(0.3)
    reap_unexpected()

    # stop, phase 1: stop publishing; wait until every survivor says so and
    # every gateway has flushed its pending packets and exited
    open(stop, "w").close()
    deadline = time.time() + 60
    while time.time() < deadline:
        reap_unexpected(gw_ok=True)
        if not live_g and all(any(l.startswith("QUIESCED") for l in open(log)) for (_, _, log) in live.values()):
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
    btaken = blanes = 0
    bqdir = os.path.join(a.dir, bname)
    if a.batch and os.path.isdir(bqdir):
        bpool = open(os.path.join(bqdir, "appender_id_pool"), "rb").read()[:32]
        btaken = sum(bin(b).count("1") for b in bpool)
        blanes = sum(1 for n in os.listdir(bqdir) if n.startswith("appender_meta_"))
    conflicts = sum(1 for log in logs + gw_logs for l in open(log) if l.startswith("IDCONFLICT"))
    if a.reclaim and taken != 0:
        failures.append(f"{taken} ids still taken after all writers exited (expected 0)")
    if a.reclaim and lanes != len(next_inc) + a.gateways:
        failures.append(f"{lanes} lanes for {len(next_inc)} traders + {a.gateways} gateways "
                        "(expected one lane per participant)")
    if a.reclaim and a.batch and btaken != 0:
        failures.append(f"{btaken} batch-lane ids still taken after all writers exited (expected 0)")
    if a.reclaim and a.batch and blanes != len(next_inc):
        failures.append(f"{blanes} batch lanes for {len(next_inc)} traders (expected one lane per trader)")

    print(f"kills={stats['kills']} restarts={stats['restarts']} joiners={stats['joiners']} "
          f"incarnations={stats['incarnations']} survivors={len(live)} "
          f"survivor_final_states_agree={agree}")
    print(f"gateways={a.gateways} gateway_kills={stats['gateway_kills']} "
          f"gateway_restarts={stats['gateway_restarts']}")
    if gw_hook:
        print(f"gw_hook={gw_hook} fired={stats.get('gw_hook_kills', 0)}")
    print(f"reclaim={'on (fixed ids)' if a.reclaim else 'off (pool ids)'} ids_reclaimed={stats['reclaimed']} "
          f"killed_before_acquire={stats['killed_before_acquire']} id_conflicts={conflicts} "
          f"traders={len(next_inc)} lanes_created={lanes} ids_taken_at_end={taken} "
          f"batch_lanes={blanes} batch_ids_reclaimed={stats['batch_reclaimed']} "
          f"batch_ids_taken_at_end={btaken}")
    for f in failures:
        print("FAILURE:", f)
        # surface the failing process's own diagnosis
        if "see " in f:
            path = f.split("see ")[1].rstrip(")")
            try:
                print("   ", "".join(l for l in open(path) if not l.startswith(("JOIN", "FINAL")))[-2000:].strip())
            except OSError:
                pass
    if gw_hook and stats.get("gw_hook_kills", 0) != 1:
        failures.append(f"gateway hook {gw_hook} fired {stats.get('gw_hook_kills', 0)} times (expected exactly 1)")
    print("RESULT:", "PASS" if not failures else "FAIL")
    return 0 if not failures else 1

if __name__ == "__main__":
    sys.exit(main())
