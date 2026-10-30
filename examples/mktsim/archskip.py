#!/usr/bin/env python3
"""archskip.py — the ARCHSKIP scenario (DEMO-01 stage-(d) item 5).

Drives the real mktsim archiver against a planted stray: a file that was
archived and reclaimed reappears in the live queue under the same base name
(the residual-window stray). The archiver's copy-aside must never overwrite
the existing archived copy (CAND-59 spec §3.5 / §8.10).

Asserted:
  - fnv64(archive/payload_0_0) is unchanged — the stray did not overwrite it;
  - the archiver logged `ARCHSKIP payload_0_0`;
  - reclaim deleted the stray from the live queue.

Prints `ARCHSKIPRESULT result=PASS|FAIL`; exits non-zero on FAIL.
"""
import argparse, os, shutil, subprocess, sys, time

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK = (1 << 64) - 1
PAYLOAD_FILE = 262144          # demo MAX_FILE_SIZE (REGION_SIZE*4)


def fnv64(path):
    h = FNV_OFFSET
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            for c in chunk:
                h = ((h ^ c) * FNV_PRIME) & MASK
    return h


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "mktsim"))
    ap.add_argument("--dir", required=True)
    ap.add_argument("--timeout", type=float, default=30.0)
    a = ap.parse_args()

    d = a.dir
    shutil.rmtree(d, ignore_errors=True)
    for sub in ("logs", "progress", "snaps", "archive"):
        os.makedirs(os.path.join(d, sub), exist_ok=True)
    stop = os.path.join(d, "STOP")
    stop2 = os.path.join(d, "STOP2")
    qname = "market"

    # 1. Seed one lane with committed data until a second payload file
    #    exists, so payload_0_0 is fully covered by the lane's archive line
    #    (and therefore listed/reclaimed/archived).
    f = open(os.path.join(d, "logs", "seed.log"), "w")
    p = subprocess.Popen([a.bin, "trader", d, qname, "0", "0", stop,
                          "sync", "0", "0", "1"],
                         stdout=f, stderr=subprocess.STDOUT)
    f.close()
    second = os.path.join(d, qname, "payload_0_1")
    deadline = time.time() + a.timeout
    while time.time() < deadline and not os.path.exists(second) and p.poll() is None:
        time.sleep(0.02)
    open(stop, "w").close()
    try:
        p.wait(timeout=a.timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    if not os.path.exists(second):
        print("ARCHSKIPRESULT result=FAIL seed: payload_0_1 never appeared")
        print(open(os.path.join(d, "logs", "seed.log")).read()[-1500:])
        return 1

    # 2. Archiver round 1: archive -> mark -> copy-aside -> reclaim.
    open(stop2, "w").close()
    r1 = subprocess.run([a.bin, "archiver", d, qname, stop2, "0"],
                        capture_output=True, text=True)
    src = os.path.join(d, "archive", "payload_0_0")
    if not os.path.exists(src):
        print("ARCHSKIPRESULT result=FAIL setup: archive/payload_0_0 not created")
        print("round1 stdout tail:", r1.stdout[-800:])
        print("round1 stderr tail:", r1.stderr[-800:])
        return 1
    before = fnv64(src)

    # 3. Plant the stray: a same-named live file (zeros).
    stray = os.path.join(d, qname, "payload_0_0")
    with open(stray, "wb") as f:
        f.write(b"\0" * PAYLOAD_FILE)

    # 4. Archiver round 2: must skip, not overwrite.
    r2 = subprocess.run([a.bin, "archiver", d, qname, stop2, "0"],
                        capture_output=True, text=True)

    # 5. Checks.
    after = fnv64(src)
    skipped = "ARCHSKIP payload_0_0" in r2.stdout
    stray_gone = not os.path.exists(stray)
    ok = (before == after) and skipped and stray_gone
    print(f"ARCHSKIPRESULT fnv64_before={before:016x} fnv64_after={after:016x} "
          f"archskip_logged={int(skipped)} stray_reclaimed={int(stray_gone)} "
          f"result={'PASS' if ok else 'FAIL'}")
    if not ok:
        print("round2 stdout tail:", r2.stdout[-800:])
        print("round2 stderr tail:", r2.stderr[-800:])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
