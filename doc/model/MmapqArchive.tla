--------------------------- MODULE MmapqArchive ---------------------------
(***************************************************************************)
(* Model of mmapq's archive / commit / join / delete protocol with         *)
(* resequenced (out-of-order) commits.  Source of truth for the protocol:  *)
(* doc/archiving-design.md (CAND-30) and the code paths named in the       *)
(* comments below (mmapq 20a37d1).                                         *)
(*                                                                         *)
(* Abstractions                                                            *)
(*  - Each appender owns a payload lane.  A message occupies one position  *)
(*    slot; its end is p+1.  Payload file f of a lane holds positions      *)
(*    [f*F, (f+1)*F).  Header file h holds indices [h*HF, (h+1)*HF).       *)
(*  - Headers are claimed lowest-NULL-first by an atomic CAS, so indices   *)
(*    stay contiguous (move_to_last_header + CAS loop).                    *)
(*  - Memory is sequentially consistent EXCEPT where a fence flag is       *)
(*    FALSE: then the archiver's start_offset stores sit in a store        *)
(*    buffer (ArchFence=FALSE) or the committer's re-check load may be     *)
(*    performed before its header CAS is visible (CommitFence=FALSE).      *)
(*    This is the store-buffering litmus the two seq_cst fences close.     *)
(*  - Ghost state (written, lost, overwrite) records what happened to      *)
(*    bytes so safety can be stated directly.                              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    NA,              \* number of appenders (lanes)
    NIdx,            \* header slots 0..NIdx-1
    NPos,            \* positions per lane 0..NPos-1
    F,               \* positions per payload file
    HF,              \* indices per header file
    MaxCrash,        \* bound on appender crashes
    \* ---- variant switches ----
    ArchFence,       \* archiver: seq_cst fence between publish and live pass
    CommitFence,     \* committer: seq_cst fence between CAS and re-check
    WaitAssumption,  \* deleter's 2nd listing happens only when no commit is
                     \* inside its pre-check..re-check window
    NaiveDelete,     \* delete straight after one listing (no mark/wait)
    BoundedPass,     \* CAND-31 proposal: live pass stops at the tail seen
                     \* right after the fence
    AllowCrash,      \* appenders may crash at any step
    AllowUndo,       \* archiver may undo (archive to a lower index)
    AllowTimeout,    \* live-pass header read may fail -> rollback
    ArbitraryCommit, \* commit() accepts offsets that are not pending
    ListingLivePass, \* proposal: listing caps each lane's boundary at the
                     \* lowest live payload it can see in the headers
    AtomicDelete,    \* proposal: re-list + unlink in one step under
                     \* archive.lock SH (a library-side reclaim call)
    PublishCAS,      \* proposal: publish raises start_offset only by a CAS
                     \* from the snapshot value (never clobbers a lowering)
    UndoRule,        \* documented hand-deleter rule (CAND-36): undo never
                     \* starts between the re-list and the unlink
    WaitPreCasOnly,  \* historical record (CAND-35 review): the wait
                     \* covering only the pre-CAS window — UNSAFE even with
                     \* the listing cap (cand35_narrow_wait FAILS)
    IterateRefill    \* CAND-49: IterateUncommitted(a) refills pend[a] from
                     \* the lane's written-but-uncommitted positions
                     \* surviving on disk (default FALSE; inert by
                     \* construction when FALSE)

Appenders == 1..NA
Pos       == 0..(NPos-1)
Idx       == 0..(NIdx-1)
Files     == 0..((NPos-1) \div F)
HFiles    == 0..((NIdx-1) \div HF)
Null      == <<0, -1>>
FileOf(p) == p \div F
HFileOf(i)== i \div HF

Max(S)    == CHOOSE x \in S : \A y \in S : y <= x
MinOf(S)  == CHOOSE x \in S : \A y \in S : x <= y
Min2(x,y) == IF x < y THEN x ELSE y
Max2(x,y) == IF x > y THEN x ELSE y

\* appender program counters
\* down   : not joined (initial state, or crashed)
\* idle   : between operations
\* fc     : fast-path commit pending (payload written, header CAS next)
\* c1..c4 : slow-path commit: c1 pre-check load, c2 CAS, c3 re-check load,
\*          c4 atomic-min.  c2e/c2l: re-check load hoisted above the CAS
\*          (CommitFence = FALSE).
PreCasWindow  == {"c2", "c2e", "c2l"}          \* pre-check passed, no header yet
PostCasWindow == {"c3", "c4"}                  \* header visible, re-check pending
WindowStates  == PreCasWindow \cup PostCasWindow

VARIABLES
    \* ---- shared / on-disk state ----
    hdr,        \* [Idx -> Null \cup Appenders \X Pos]
    si,         \* meta.start_index
    so,         \* [Appenders -> 0..NPos]  appender_meta.start_offset
                \*   (readability floor: may be lowered — commit re-check
                \*   min, live pass, rollback, undo)
    aoff,       \* [Appenders -> 0..NPos]  appender_meta.archive_offset
                \*   (D-G: the archive line — only a forward archive raises
                \*   it; NOTHING lowers it.  Do not merge with so;
                \*   see doc/design/archive-boundaries.md)
    aoffPrev,   \* ghost: pre-step aoff, for ArchiveLineMonotonic
    \* ---- ghost: bytes ----
    written,    \* set of <<a,p>> message starts ever written
    lost,       \* set of <<a,p>> whose bytes were destroyed by a delete
    delP,       \* set of <<a,f>> payload files currently deleted
    delH,       \* set of header files currently deleted
    overwrite,  \* TRUE once a write lands on existing bytes
    \* ---- appenders ----
    apc, cur, pos, lastApp, pend, flag, loaded, crashes,
    \* ---- archiver ----
    xpc, xN, cand, old, xi, xlane, snap, sbSo, sbAoff,
    \* ---- deleter ----
    dpc, markedH, markedP, todoH, todoP

dataVars == <<hdr, si, so, aoff, aoffPrev, written, lost, delP, delH, overwrite>>
appVars  == <<apc, cur, pos, lastApp, pend, flag, loaded, crashes>>
arcVars  == <<xpc, xN, cand, old, xi, xlane, snap, sbSo, sbAoff>>
delVars  == <<dpc, markedH, markedP, todoH, todoP>>
vars     == <<dataVars, appVars, arcVars, delVars>>

FreeSlots == {i \in Idx : hdr[i] = Null}
NextSlot  == MinOf(FreeSlots)
Tail      == IF FreeSlots = {} THEN NIdx ELSE NextSlot
LiveIdx   == {i \in Idx : i >= si /\ hdr[i] # Null}

-----------------------------------------------------------------------------
(* Initial state: queue created, nobody joined yet.  start_offset starts at*)
(* the lane origin (INITIAL_PAYLOAD_POSITION in the code).                 *)
Init ==
    /\ hdr = [i \in Idx |-> Null]
    /\ si = 0
    /\ so = [a \in Appenders |-> 0]
    /\ aoff = [a \in Appenders |-> 0]
    /\ aoffPrev = [a \in Appenders |-> 0]
    /\ written = {} /\ lost = {} /\ delP = {} /\ delH = {}
    /\ overwrite = FALSE
    /\ apc = [a \in Appenders |-> "down"]
    /\ cur = [a \in Appenders |-> 0]
    /\ pos = [a \in Appenders |-> 0]
    /\ lastApp = [a \in Appenders |-> -1]
    /\ pend = [a \in Appenders |-> {}]
    /\ flag = [a \in Appenders |-> -1]
    /\ loaded = [a \in Appenders |-> 0]
    /\ crashes = 0
    /\ xpc = "idle" /\ xN = 0
    /\ cand = [a \in Appenders |-> -1]
    /\ old  = [a \in Appenders |-> -1]
    /\ xi = 0 /\ xlane = 1 /\ snap = 0
    /\ sbSo = [a \in Appenders |-> -1]
    /\ sbAoff = [a \in Appenders |-> -1]
    /\ dpc = "idle"
    /\ markedH = {} /\ markedP = {} /\ todoH = {} /\ todoP = {}

-----------------------------------------------------------------------------
(* ============================ APPENDER ==================================*)

(* Join (indexed_appender.c join path; design "Appender join"):            *)
(* search own latest entry in the LIVE range only; walk forward from it,   *)
(* else from start_offset.  The walk stops at the first position that is   *)
(* unwritten or whose file is missing ("no file = end of stream").         *)
JoinBase(a) ==
    LET own == {p \in Pos : \E i \in Idx : i >= si /\ hdr[i] = <<a, p>>}
    IN  IF own # {} THEN Max(own) ELSE so[a]

Stops(a, q) == q = NPos \/ <<a, q>> \notin written \/ <<a, FileOf(q)>> \in delP

Walk(a, base) == MinOf({q \in base..NPos : Stops(a, q)})

Join(a) ==
    /\ apc[a] = "down"
    /\ pos' = [pos EXCEPT ![a] = Walk(a, JoinBase(a))]
    /\ apc' = [apc EXCEPT ![a] = "idle"]
    /\ lastApp' = [lastApp EXCEPT ![a] = -1]
    /\ pend' = [pend EXCEPT ![a] = {}]
    /\ flag' = [flag EXCEPT ![a] = -1]
    /\ UNCHANGED <<dataVars, cur, loaded, crashes, arcVars, delVars>>

(* write_payload: bytes at pos; recreates the file if it was deleted.      *)
WriteAt(a) ==
    /\ overwrite' = (overwrite \/ <<a, pos[a]>> \in written)
    /\ written' = written \cup {<<a, pos[a]>>}
    /\ delP' = delP \ {<<a, FileOf(pos[a])>>}
    /\ pos' = [pos EXCEPT ![a] = @ + 1]
    /\ lastApp' = [lastApp EXCEPT ![a] = pos[a]]

(* append(): write, then the header CAS (commit_header, no shared loads).  *)
(* Atomic append touches neither the flag nor the set (CAND-49 §3.3).      *)
Append(a) ==
    /\ apc[a] = "idle" /\ pos[a] < NPos /\ FreeSlots # {}
    /\ WriteAt(a)
    /\ cur' = [cur EXCEPT ![a] = pos[a]]
    /\ apc' = [apc EXCEPT ![a] = "fc"]
    /\ UNCHANGED <<hdr, si, so, aoff, aoffPrev, lost, delH, pend, flag, loaded, crashes,
                   arcVars, delVars>>

(* append_uncommitted(): write only; the offset is handed to the caller.   *)
(* CAND-49 §3.3: the LATEST uncommitted offset is tracked by the flag; the *)
(* previous flag-tracked offset (if any) moves into the set.               *)
AppendUncommitted(a) ==
    /\ apc[a] = "idle" /\ pos[a] < NPos
    /\ WriteAt(a)
    /\ pend' = [pend EXCEPT ![a] = @ \cup (IF flag[a] = -1 THEN {} ELSE {flag[a]})]
    /\ flag' = [flag EXCEPT ![a] = pos[a]]
    /\ UNCHANGED <<hdr, si, so, aoff, aoffPrev, lost, delH, apc, cur, loaded, crashes,
                   arcVars, delVars>>

(* The offsets commit() will accept: the set plus the flag-tracked tail.   *)
Track(a) == pend[a] \cup (IF flag[a] = -1 THEN {} ELSE {flag[a]})

(* commit(o): fast path iff o is the last written offset AND flag-tracked  *)
(* (CAND-49 §3.4: offset == last_appended && offset == last_pending_      *)
(* offset); everything else takes the slow path with the boundary checks.  *)
CommitStart(a, o) ==
    /\ apc[a] = "idle" /\ FreeSlots # {}
    /\ cur' = [cur EXCEPT ![a] = o]
    /\ apc' = [apc EXCEPT ![a] =
                 IF o = lastApp[a] /\ o = flag[a] THEN "fc" ELSE "c1"]

Commit(a) ==
    \/ \E o \in Track(a) :
          /\ CommitStart(a, o)
          /\ pend' = [pend EXCEPT ![a] = @ \ {o}]
          \* The model has no commit_header failure, so dropping the
          \* tracking here equals §3.4's clear-on-success/overtaken-
          \* rejection exactly (every in-flight commit terminates in one
          \* of the two).
          /\ flag' = [flag EXCEPT ![a] = IF o = flag[a] THEN -1 ELSE @]
          /\ UNCHANGED <<dataVars, pos, lastApp, loaded, crashes, arcVars, delVars>>
    \/ /\ ArbitraryCommit           \* caller passes an offset that is not pending
       /\ \E o \in Pos \ pend[a] :
             /\ o # lastApp[a]
             /\ CommitStart(a, o)
             /\ UNCHANGED <<dataVars, pend, flag, pos, lastApp, loaded, crashes, arcVars, delVars>>

CAS(a) == hdr' = [hdr EXCEPT ![NextSlot] = <<a, cur[a]>>]

FastCommit(a) ==
    /\ apc[a] = "fc" /\ FreeSlots # {}
    /\ CAS(a)
    /\ apc' = [apc EXCEPT ![a] = "idle"]
    /\ UNCHANGED <<si, so, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

(* c1: pre-check.  offset < start_offset -> MMAPQ_EINVAL, payload dropped  *)
(* (the caller must re-append it; I2).                                     *)
SlowPreCheck(a) ==
    /\ apc[a] = "c1"
    /\ apc' = [apc EXCEPT ![a] =
                 IF cur[a] < so[a] THEN "idle"
                 ELSE IF CommitFence THEN "c2" ELSE "c2e"]
    /\ UNCHANGED <<dataVars, cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

SlowCAS(a) ==
    /\ apc[a] = "c2" /\ FreeSlots # {}
    /\ CAS(a)
    /\ apc' = [apc EXCEPT ![a] = "c3"]
    /\ UNCHANGED <<si, so, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

(* c3: after the seq_cst fence, re-load start_offset.                      *)
SlowRecheck(a) ==
    /\ apc[a] = "c3"
    /\ apc' = [apc EXCEPT ![a] = IF cur[a] < so[a] THEN "c4" ELSE "idle"]
    /\ UNCHANGED <<dataVars, cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

(* c4: appender_meta_start_offset_min (atomic-min CAS loop).               *)
SlowMin(a) ==
    /\ apc[a] = "c4"
    /\ so' = [so EXCEPT ![a] = Min2(@, cur[a])]
    /\ apc' = [apc EXCEPT ![a] = "idle"]
    /\ UNCHANGED <<hdr, si, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

(* CommitFence = FALSE: the re-check load is satisfied before the CAS is   *)
(* visible (acq_rel CAS does not order a later load of another location).  *)
SlowEarlyLoad(a) ==
    /\ apc[a] = "c2e"
    /\ loaded' = [loaded EXCEPT ![a] = so[a]]
    /\ apc' = [apc EXCEPT ![a] = "c2l"]
    /\ UNCHANGED <<dataVars, cur, pos, lastApp, pend, flag, crashes, arcVars, delVars>>

SlowLateCAS(a) ==
    /\ apc[a] = "c2l" /\ FreeSlots # {}
    /\ CAS(a)
    /\ apc' = [apc EXCEPT ![a] = IF cur[a] < loaded[a] THEN "c4" ELSE "idle"]
    /\ UNCHANGED <<si, so, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   cur, pos, lastApp, pend, flag, loaded, crashes, arcVars, delVars>>

(* Crash at any step: pending payloads become dead; a CAS already done     *)
(* stays done, a re-check not yet done never happens.                      *)
Crash(a) ==
    /\ AllowCrash /\ crashes < MaxCrash /\ apc[a] # "down"
    /\ apc' = [apc EXCEPT ![a] = "down"]
    /\ pend' = [pend EXCEPT ![a] = {}]
    /\ flag' = [flag EXCEPT ![a] = -1]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<dataVars, cur, pos, lastApp, loaded, arcVars, delVars>>

(* IterateUncommitted(a): the uncommitted-payload iterator (CAND-49).     *)
(* Crash/Join clear pend[a] and flag[a] — the appender's *memory* — but   *)
(* the lane keeps the written-but-uncommitted positions on disk; the      *)
(* iterator refills pend[a] from them: positions at or above the archive  *)
(* line whose payload file still exists and that no LIVE header commits   *)
(* (the code's header scan has the D8/S3 start-index floor, so only live  *)
(* entries are visible).  Snapshot semantics: the iterator run is one     *)
(* atomic step here.  The set is uniform; the flag is modelled (owner     *)
(* ruling 2026-10-18): IterateUncommitted never sets the flag, and the    *)
(* refill skips the flag-tracked tail (§3.3 skip rule — the M7            *)
(* interaction).  Commit-after-overtake of a refilled offset is the       *)
(* existing SlowPreCheck drop (cur[a] < so[a]).                           *)
(*                                                                        *)
(* D-G (owner ruling 2026-10-18): init acquire-loads si FIRST, then       *)
(* reads archive_offset — the archive line, never the start_offset        *)
(* readability floor; the gap walk starts at the archive line.  The       *)
(* archiver stores archive_offset before the release si store             *)
(* (indexed_archiver.c:264-271 at 544df3a), so a buffered archive-line    *)
(* store that precedes the visible si is visible to this reader: when si  *)
(* already shows the in-flight archive's value while lane a's store is    *)
(* still buffered (sbAoff), the iterator observes the BUFFERED value.     *)
(* The harmful pair (new si, old archive line) is unreachable.  When si   *)
(* does NOT yet show the archive, the new line may already be visible     *)
(* in C (the line store can land before si): both reads are harmless      *)
(* (a consistent older snapshot, or the chopped region skipped), and both *)
(* are modelled — a nondeterministic choice (owner amendment 2026-10-19:  *)
(* the harmless direction must be checked, not just argued).  The two     *)
(* roles of the old start_offset are split: do not merge them back        *)
(* (doc/design/archive-boundaries.md).                                    *)
IterEffAoffChoices(a) ==
    IF sbAoff[a] = -1 THEN {aoff[a]}
    ELSE IF si = xN THEN {Max2(aoff[a], sbAoff[a])}
    ELSE {aoff[a], Max2(aoff[a], sbAoff[a])}

IterateUncommitted(a) ==
    /\ IterateRefill
    /\ apc[a] = "idle"
    /\ \E eff \in IterEffAoffChoices(a) :
         LET liveCommitted == {hdr[i][2] : i \in {j \in LiveIdx : hdr[j][1] = a}}
             refill == {p \in Pos : /\ <<a, p>> \in written
                                    /\ p >= eff
                                    /\ <<a, FileOf(p)>> \notin delP
                                    /\ p \notin liveCommitted
                                    /\ p # flag[a]}
         IN  /\ refill \ pend[a] # {}
             /\ pend' = [pend EXCEPT ![a] = @ \cup refill]
    /\ UNCHANGED <<dataVars, apc, cur, pos, lastApp, flag, loaded, crashes,
                   arcVars, delVars>>

AppenderNext(a) ==
    \/ Join(a) \/ Append(a) \/ AppendUncommitted(a) \/ Commit(a)
    \/ FastCommit(a) \/ SlowPreCheck(a) \/ SlowCAS(a) \/ SlowRecheck(a)
    \/ SlowMin(a) \/ SlowEarlyLoad(a) \/ SlowLateCAS(a) \/ Crash(a)
    \/ IterateUncommitted(a)

-----------------------------------------------------------------------------
(* ============================ ARCHIVER ==================================*)
(* Holds archive.lock EX for the whole operation (xpc # "idle").           *)

(* archive_forward_variable step 1-2: validate [si, N), candidate per lane *)
(* = end of the archived message with the HIGHEST POSITION; snapshot the   *)
(* pre-call boundary (old) for rollback.                                   *)
ForwardStart ==
    /\ xpc = "idle"
    /\ \E N \in (si+1)..NIdx :
         /\ \A i \in si..(N-1) : hdr[i] # Null
         /\ LET ps(a) == {hdr[i][2] : i \in {j \in si..(N-1) : hdr[j][1] = a}}
                c == [a \in Appenders |-> IF ps(a) = {} THEN -1 ELSE Max(ps(a)) + 1]
            IN /\ cand' = c
               /\ old' = [a \in Appenders |-> IF c[a] = -1 THEN -1 ELSE so[a]]
         /\ xN' = N
    /\ xlane' = 1
    /\ xpc' = "pub"
    /\ UNCHANGED <<dataVars, appVars, xi, snap, sbSo, sbAoff, delVars>>

(* step 3a: publish — plain store of the candidate if above the snapshot   *)
(* (appender_meta_set_start_offset).  Goes through the store buffer.       *)
(* D-G: the same store raises the archive line — archive_offset is stored  *)
(* for every affected lane (never decreasing; the max is applied when the  *)
(* buffer drains), THEN the release si store (PublishSi).                  *)
Publish ==
    /\ xpc = "pub"
    /\ LET a == xlane IN
         /\ sbSo' = IF cand[a] # -1 /\ cand[a] > old[a]
                      THEN [sbSo EXCEPT ![a] = cand[a]] ELSE sbSo
         /\ sbAoff' = IF cand[a] # -1
                        THEN [sbAoff EXCEPT ![a] = cand[a]] ELSE sbAoff
    /\ xlane' = IF xlane = NA THEN xlane ELSE xlane + 1
    /\ xpc' = IF xlane = NA THEN "si" ELSE "pub"
    /\ UNCHANGED <<dataVars, appVars, xN, cand, old, xi, snap, delVars>>

(* A buffered start_offset / archive_offset store becomes globally         *)
(* visible.                                                                *)
Flush(a) ==
    /\ sbSo[a] # -1 \/ sbAoff[a] # -1
    /\ so' = IF sbSo[a] # -1
               THEN [so EXCEPT ![a] =
                        IF PublishCAS THEN (IF @ = old[a] THEN sbSo[a] ELSE @)
                        ELSE sbSo[a]]
               ELSE so
    /\ aoffPrev' = [aoffPrev EXCEPT ![a] = aoff[a]]
    /\ aoff' = IF sbAoff[a] # -1
                 THEN [aoff EXCEPT ![a] = Max2(@, sbAoff[a])]
                 ELSE aoff
    /\ sbSo' = [sbSo EXCEPT ![a] = -1]
    /\ sbAoff' = [sbAoff EXCEPT ![a] = -1]
    /\ UNCHANGED <<hdr, si, written, lost, delP, delH, overwrite,
                   appVars, xpc, xN, cand, old, xi, xlane, snap, delVars>>

(* step 3b: start_index = N.                                               *)
PublishSi ==
    /\ xpc = "si"
    /\ si' = xN
    /\ xpc' = "fence"
    /\ UNCHANGED <<hdr, so, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   appVars, xN, cand, old, xi, xlane, snap, sbSo, sbAoff, delVars>>

(* seq_cst fence: with ArchFence the buffered stores must drain first.     *)
Fence ==
    /\ xpc = "fence"
    /\ ArchFence => \A a \in Appenders : sbSo[a] = -1 /\ sbAoff[a] = -1
    /\ snap' = Tail
    /\ xi' = xN
    /\ xpc' = "pass"
    /\ UNCHANGED <<dataVars, appVars, xN, cand, old, xlane, sbSo, sbAoff, delVars>>

(* step 4: one pass over live headers from N with the CHECKED read,        *)
(* lowering each lane's boundary below any live payload under it.          *)
(* An atomic-min on a lane needs that lane's own buffered store applied    *)
(* first (same-location coherence).                                        *)
PassEnd ==
    \/ xi >= NIdx
    \/ hdr[xi] = Null
    \/ (BoundedPass /\ xi >= snap)

PassStep ==
    /\ xpc = "pass"
    /\ IF PassEnd
         THEN /\ xpc' = "fin"
              /\ UNCHANGED <<dataVars, xi>>
         ELSE LET a == hdr[xi][1]
                  p == hdr[xi][2]
              IN /\ sbSo[a] = -1
                 /\ so' = [so EXCEPT ![a] = Min2(@, p)]
                 /\ xi' = xi + 1
                 /\ UNCHANGED <<hdr, si, aoff, aoffPrev, written, lost, delP, delH, overwrite, xpc>>
    /\ UNCHANGED <<appVars, xN, cand, old, xlane, snap, sbSo, sbAoff, delVars>>

(* step 5: a header read fails (timeout/corruption): lower every boundary  *)
(* back to its pre-call value; start_index = N stands.                     *)
PassFail ==
    /\ AllowTimeout /\ xpc = "pass" /\ ~PassEnd
    /\ \A a \in Appenders : sbSo[a] = -1
    /\ so' = [a \in Appenders |-> IF old[a] = -1 THEN so[a] ELSE Min2(so[a], old[a])]
    /\ xpc' = "fin"
    /\ UNCHANGED <<hdr, si, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   appVars, xN, cand, old, xi, xlane, snap, sbSo, sbAoff, delVars>>

(* lock release: buffered stores drain before the unlock (release).        *)
Finish ==
    /\ xpc = "fin"
    /\ \A a \in Appenders : sbSo[a] = -1 /\ sbAoff[a] = -1
    /\ xpc' = "idle"
    /\ UNCHANGED <<dataVars, appVars, xN, cand, old, xi, xlane, snap, sbSo, sbAoff, delVars>>

(* Undo: archive_up_to_index(N), N < si.  Checks the restored entries'     *)
(* header and payload files still exist, lowers each lane to its lowest    *)
(* restored payload (never raises), then stores start_index.               *)
UndoStart ==
    /\ AllowUndo /\ xpc = "idle"
    /\ UndoRule => dpc # "del"
    /\ \E N \in 0..(si-1) :
         LET R == N..(si-1) IN
         /\ \A i \in R : /\ hdr[i] # Null
                         /\ HFileOf(i) \notin delH
                         /\ <<hdr[i][1], FileOf(hdr[i][2])>> \notin delP
         /\ so' = [a \in Appenders |->
                     LET ps == {hdr[i][2] : i \in {j \in R : hdr[j][1] = a}}
                     IN IF ps = {} THEN so[a] ELSE Min2(so[a], MinOf(ps))]
         /\ xN' = N
    /\ xpc' = "undo2"
    /\ UNCHANGED <<hdr, si, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   appVars, cand, old, xi, xlane, snap, sbSo, sbAoff, delVars>>

UndoSi ==
    /\ xpc = "undo2"
    /\ si' = xN
    /\ xpc' = "idle"
    /\ UNCHANGED <<hdr, so, aoff, aoffPrev, written, lost, delP, delH, overwrite,
                   appVars, xN, cand, old, xi, xlane, snap, sbSo, sbAoff, delVars>>

ArchiverNext ==
    \/ ForwardStart \/ Publish \/ PublishSi \/ Fence \/ PassStep
    \/ PassFail \/ Finish \/ UndoStart \/ UndoSi
    \/ \E a \in Appenders : Flush(a)

-----------------------------------------------------------------------------
(* ============================ DELETER ===================================*)
(* archivable_files() under archive.lock SH (only while the archiver is    *)
(* idle); the caller marks, waits, re-lists, deletes the intersection.     *)

LowestLive(a) ==
    LET ps == {hdr[i][2] : i \in {j \in LiveIdx : hdr[j][1] = a}}
    IN IF ps = {} THEN NPos ELSE MinOf(ps)

Bound(a) == IF ListingLivePass THEN Min2(so[a], LowestLive(a)) ELSE so[a]

ListedH == {h \in HFiles : (h + 1) * HF <= si}
ListedP == {<<a, f>> \in Appenders \X Files : (f + 1) * F <= Bound(a)}

List1 ==
    /\ dpc = "idle" /\ xpc = "idle"
    /\ IF NaiveDelete
         THEN /\ todoH' = ListedH /\ todoP' = ListedP
              /\ dpc' = "del"
              /\ UNCHANGED <<markedH, markedP>>
         ELSE /\ markedH' = ListedH /\ markedP' = ListedP
              /\ dpc' = "wait"
              /\ UNCHANGED <<todoH, todoP>>
    /\ UNCHANGED <<dataVars, appVars, arcVars>>

List2 ==
    /\ dpc = "wait" /\ xpc = "idle"
    /\ WaitAssumption => \A a \in Appenders :
         apc[a] \notin (IF WaitPreCasOnly THEN PreCasWindow ELSE WindowStates)
    /\ IF AtomicDelete
         THEN LET tP == markedP \cap ListedP
                  tH == markedH \cap ListedH
              IN /\ lost' = lost \cup {w \in written : <<w[1], FileOf(w[2])>> \in tP}
                 /\ delP' = delP \cup tP
                 /\ delH' = delH \cup tH
                 /\ dpc' = "idle"
                 /\ UNCHANGED <<todoH, todoP, hdr, si, so, aoff, aoffPrev, written, overwrite>>
         ELSE /\ todoH' = markedH \cap ListedH
              /\ todoP' = markedP \cap ListedP
              /\ dpc' = "del"
              /\ UNCHANGED dataVars
    /\ UNCHANGED <<markedH, markedP, appVars, arcVars>>

(* unlink() happens outside the lock.                                      *)
Delete ==
    /\ dpc = "del"
    /\ lost' = lost \cup {w \in written : <<w[1], FileOf(w[2])>> \in todoP}
    /\ delP' = delP \cup todoP
    /\ delH' = delH \cup todoH
    /\ dpc' = "idle"
    /\ UNCHANGED <<hdr, si, so, aoff, aoffPrev, written, overwrite, appVars, arcVars,
                   markedH, markedP, todoH, todoP>>

DeleterNext == List1 \/ List2 \/ Delete

-----------------------------------------------------------------------------
Next ==
    \/ \E a \in Appenders : AppenderNext(a)
    \/ ArchiverNext
    \/ DeleterNext

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* ============================ PROPERTIES ================================*)

TypeOK ==
    /\ si \in 0..NIdx
    /\ \A a \in Appenders : so[a] \in 0..NPos /\ pos[a] \in 0..NPos
    /\ \A a \in Appenders : aoff[a] \in 0..NPos

(* THE safety property: every live entry is readable — its header file     *)
(* exists and its payload bytes were never destroyed.                      *)
NoLiveLoss ==
    \A i \in LiveIdx :
        /\ HFileOf(i) \notin delH
        /\ hdr[i] \notin lost

(* I5: a joining appender never writes over existing bytes.                *)
NoOverwrite == ~overwrite

(* Every header references a message that was actually written.            *)
HeadersReferenceMessages ==
    \A i \in Idx : hdr[i] # Null => hdr[i] \in written

(* I1/I3: outside an archive operation, start_offset <= every live payload *)
(* of its lane, except a commit between its CAS and its re-check.          *)
InPostCasWindow(a, p) == apc[a] \in PostCasWindow /\ cur[a] = p

I1 ==
    xpc = "idle" =>
        \A i \in LiveIdx :
            LET a == hdr[i][1] p == hdr[i][2]
            IN p >= so[a] \/ InPostCasWindow(a, p)

(* CAND-31: the live pass never reads past the tail it saw at the fence.   *)
PassWithinSnapshot == xpc = "pass" => xi <= snap

(* CAND-49: no two header slots ever reference the same message — a        *)
(* duplicate is a double delivery to pollers.  hdr entries persist in the  *)
(* model (deletion removes files, not entries), so archived entries are   *)
(* covered without a history variable.  Model-only: the C counterpart is  *)
(* the test-only lane validator's header-uniqueness check, never a        *)
(* commit-path runtime check (the hot path stays clean).                  *)
NoDoubleCommit ==
    \A i, j \in Idx :
        (i # j /\ hdr[i] # Null /\ hdr[j] # Null) => hdr[i] # hdr[j]

(* D-G: the archive line never decreases — only a forward archive raises  *)
(* it; the commit re-check min, the live pass, rollback and undo never    *)
(* touch it.  Action property, encoded with the aoffPrev ghost (it holds  *)
(* the pre-step value whenever a step may change aoff).                   *)
ArchiveLineMonotonic ==
    \A a \in Appenders : aoff[a] >= aoffPrev[a]

(* The TOTAL form (owner amendment 2026-10-19): every step — not just the *)
(* Flush the ghost watches — keeps the line.  A lowering added to any     *)
(* other action violates this on the spot; the ghost misses it unless it  *)
(* drops below the pre-Flush value.  Mutation-checked (2026-10-19):       *)
(* PassFail with aoff' := 0 fails THIS property on the spot (0 s), while  *)
(* the ghost invariant fails only later (2 s — it must wait for a Flush   *)
(* to push aoffPrev up first), and NoDoubleCommit dies at 1 s to the      *)
(* reopened D-G.                                                        *)
ArchiveLineNeverDecreases ==
    [][\A a \in Appenders : aoff'[a] >= aoff[a]]_vars

=============================================================================
