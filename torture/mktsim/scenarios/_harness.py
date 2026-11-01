#!/usr/bin/env python3
"""Shared helpers for the mktsim scenario harnesses (DEMO-01 stage (e) E6).

Each scenario script drives the real mktsim binary through a focused,
deterministic topology (not the random churn), asserts one outcome, and
prints exactly one verdict line:

    SCENARIO <name> expect=<PASS|FAIL> got=<PASS|FAIL|VACUOUS> result=<OK|MISMATCH> [reason=<token>]

`got` is one of:
  * PASS     — the run finished cleanly and verify passed;
  * FAIL     — a participant or the verifier failed;
  * VACUOUS  — the scenario never reached its point (a plant did not fire, an
               expected verdict line was never seen, or a process did not exit
               in time). A VACUOUS outcome is never a pass: it is a MISMATCH and
               `reason=<token>` says which precondition was missed.

`expect` is what the scenario exists to demonstrate. result is OK when
expect == got, else MISMATCH; a MISMATCH makes the script exit non-zero.

The scenario directory comes from MKTSIM_SCENARIO_DIR (run_short.sh sets it to
a path under its work directory so failure evidence is kept). The STOP protocol
matches churn.py: write <dir>/STOP, wait for every trader's QUIESCED line, let
the snapshotter finish its final round, write <dir>/STOP.final, then wait.
"""
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MKTSIM_DIR = os.path.dirname(HERE)
BIN = os.environ.get("MKTSIM_BIN", os.path.join(MKTSIM_DIR, "mktsim"))
QNAME = "market"


def verdict(name, expect, got, reason=""):
    ok = expect == got
    line = "SCENARIO %s expect=%s got=%s result=%s" % (
        name, expect, got, "OK" if ok else "MISMATCH")
    if reason:
        line += " reason=%s" % reason
    print(line)
    sys.stdout.flush()
    return 0 if ok else 1


def vacuous(name, expect, reason):
    return verdict(name, expect, "VACUOUS", reason)


def read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def lines(path):
    return read(path).splitlines()


def count_prefix(path, prefix):
    return sum(1 for l in lines(path) if l.startswith(prefix))


def has_prefix(path, prefix):
    return count_prefix(path, prefix) > 0


def first_prefix(path, prefix):
    for l in lines(path):
        if l.startswith(prefix):
            return l
    return None


def last_prefix(path, prefix):
    found = None
    for l in lines(path):
        if l.startswith(prefix):
            found = l
    return found


def has_substr(path, needle):
    return needle in read(path)


def wait_substr(path, needle, timeout=25.0):
    end = time.time() + timeout
    while time.time() < end:
        if needle in read(path):
            return True
        time.sleep(0.02)
    return False


def wait_prefix(path, prefix, timeout=25.0, count_gt=0):
    end = time.time() + timeout
    while time.time() < end:
        if count_prefix(path, prefix) > count_gt:
            return True
        time.sleep(0.02)
    return False


def wait_proc(proc, timeout=25.0):
    """Return the exit code, or None on timeout."""
    end = time.time() + timeout
    while time.time() < end:
        rc = proc.poll()
        if rc is not None:
            return rc
        time.sleep(0.02)
    return None


def field(line, key):
    for tok in line.split():
        if tok.startswith(key + "="):
            return tok.split("=", 1)[1]
    return None


def snapshotter_si(path):
    """Highest start_index reported by SNAPWRITE lines (or -1)."""
    best = -1
    for l in lines(path):
        if l.startswith("SNAPWRITE"):
            v = field(l, "si")
            if v is not None and int(v) > best:
                best = int(v)
    return best


class Topo:
    def __init__(self, d):
        self.d = d
        self.stop = os.path.join(d, "STOP")
        self.traders = []       # (tid, inc, proc, log)
        self.gateways = []      # (gid, inc, proc, log)
        self.snapshotter = None  # (proc, log)
        self.last_verify = ""    # stdout of the most recent verify run

    def fresh(self):
        shutil.rmtree(self.d, ignore_errors=True)
        for sub in ("logs", "snaps", "archive"):
            os.makedirs(os.path.join(self.d, sub))

    def _spawn(self, args, log, env):
        f = open(log, "w")
        p = subprocess.Popen([BIN] + args, stdout=f, stderr=subprocess.STDOUT, env=env)
        f.close()
        return p

    def trader(self, tid, inc=0, mode="sync", batch=0, arch=1, env=None):
        log = os.path.join(self.d, "logs", "t%02d_i%03d.log" % (tid, inc))
        p = self._spawn(["trader", self.d, QNAME, str(tid), str(inc), self.stop,
                         mode, "-1", str(batch), str(arch)], log, env)
        self.traders.append((tid, inc, p, log))
        return p, log

    def restart_trader(self, tid, inc, mode="sync", batch=0, arch=1, env=None):
        """SIGKILL and reap every live incarnation of `tid`, then start `inc`."""
        for (t, i, p, _) in list(self.traders):
            if t == tid:
                self.kill(p)
        return self.trader(tid, inc, mode, batch, arch, env)

    def gateway(self, gid, inc=0, mode="sync", arch=1, env=None):
        log = os.path.join(self.d, "logs", "g%02d_i%03d.log" % (gid, inc))
        p = self._spawn(["gateway", self.d, QNAME, str(gid), str(inc), self.stop,
                         mode, "-1", str(arch)], log, env)
        self.gateways.append((gid, inc, p, log))
        return p, log

    def restart_gateway(self, gid, inc, mode="sync", arch=1, env=None):
        for (g, i, p, _) in list(self.gateways):
            if g == gid:
                self.kill(p)
        return self.gateway(gid, inc, mode, arch, env)

    def wait_traders_joined(self, timeout=25.0):
        """Wait until every currently-tracked trader has printed JOIN, i.e.
        has fully created the queue. The snapshotter's archivable check reads
        the meta flag, which can be mid-creation if the snapshotter starts too
        early; joining first removes that startup race."""
        for (tid, inc, p, log) in self.traders:
            if p.poll() is None and not has_prefix(log, "JOIN"):
                wait_prefix(log, "JOIN", timeout)

    def snapshotter_start(self, snap_every=1, env=None, hand=0):
        self.wait_traders_joined()
        log = os.path.join(self.d, "logs", "snapshotter.log")
        p = self._spawn(["snapshotter", self.d, QNAME, self.stop, str(hand),
                         "--snap-every", str(snap_every)], log, env)
        self.snapshotter = (p, log)
        return p, log

    def clean_stop(self, verify=True, timeout=60.0):
        """STOP -> QUIESCED -> STOP.final -> verify. Returns (ok, detail)."""
        open(self.stop, "w").close()
        for (tid, inc, p, log) in self.traders:
            if p.poll() is None:
                wait_prefix(log, "QUIESCED", timeout)
        if self.snapshotter is not None:
            wait_proc(self.snapshotter[0], timeout)
        open(self.stop + ".final", "w").close()
        bad = []
        # A process the scenario intentionally SIGKILLed (rc == -SIGKILL) is
        # not a failure; every other non-zero exit is.
        for (tid, inc, p, log) in self.traders:
            rc = wait_proc(p, timeout)
            if rc not in (0, -signal.SIGKILL):
                bad.append("trader %d/%d rc=%s" % (tid, inc, rc))
        for (gid, inc, p, log) in self.gateways:
            rc = wait_proc(p, timeout)
            if rc not in (0, -signal.SIGKILL):
                bad.append("gateway %d/%d rc=%s" % (gid, inc, rc))
        if self.snapshotter is not None:
            rc = wait_proc(self.snapshotter[0], timeout)
            if rc not in (0, -signal.SIGKILL):
                bad.append("snapshotter rc=%s" % rc)
        if verify:
            logs = [log for (_, _, _, log) in self.traders]
            v = subprocess.run([BIN, "verify", self.d, QNAME] + logs,
                               capture_output=True, text=True)
            self.last_verify = v.stdout
            sys.stdout.write(v.stdout)
            if v.returncode != 0:
                bad.append("verify rc=%d" % v.returncode)
                sys.stdout.write(v.stderr)
        return (not bad), "; ".join(bad)

    def kill(self, proc):
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()

    def kill_all(self):
        for (_, _, p, _) in self.traders:
            self.kill(p)
        for (_, _, p, _) in self.gateways:
            self.kill(p)
        if self.snapshotter is not None:
            self.kill(self.snapshotter[0])
