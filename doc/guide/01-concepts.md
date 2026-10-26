# 1. Concepts

Terms below are defined once in the [glossary](../reference/glossary.md);
this chapter shows how they fit together. A visual walk-through:
[Core Concepts](https://claude.ai/artifact/WdWRfGy7Q8VkRsLYcoKDSD).

## Lanes and the header lane

```
  ─────────────────────────────────────────────► Header

       poll header (1)   ▲          ▲  commit header (2)
                         │          │
               POLLER    │          │    APPENDER
                         │          │
    resolve payload (2)  ▼          ▼  write payload (1)

  ─────────────────────────────────────────────► Payload
```

Every entry in a queue has an 8-byte **header** in the shared header lane
and a **payload** in one per-appender payload lane. The header packs the
8-bit appender id and a 56-bit payload position into one 64-bit word.

## Commit by CAS

The appender writes the payload to its private lane first (step 1). Only
after the payload is fully written does it **commit** — publishing the
header with one atomic CAS (step 2). A reader that sees a committed header
is guaranteed to see the complete payload (the evidence for that ordering is
a row in [guarantees](../reference/guarantees.md)).

The **poller** (sequential consumer) scans the header lane in index order,
decodes `(appenderId, payloadPosition)`, follows it into the payload lane,
and delivers the message to a callback. The **reader** (random-access
consumer) follows the same two-step protocol for any index without
advancing a cursor.

## Ordering

Index order is commit order: the header lane is the single ordered history,
no matter how many writers race to append. Up to 256 appenders coexist with
minimal contention because each writes its own lane, and the vertical-walk
layout of the header lane (consecutive indices 512 bytes apart across 64
sectors) keeps concurrent CAS operations off each other's cache lines.
Resequenced commits — payloads written in one order and committed in
another — are a first-class pattern: see
[chapter 4](04-resequenced-commits.md).

## What's on disk

```
<directory>/<queue_name>/
  meta                  queue config (region_size, max_file_size, schema version,
                        start_index, archivable flag, queue_type)
  header_N              shared header file (N = file index 0, 1, 2...; the
                        highest-indexed file is the current active one)
  payload_A_N           per-appender payload file (A = appender ID 0–255,
                        N = file index 0, 1, 2...; same pattern)
  appender_meta_A       per-appender state (start_offset, archive line, intent)
  appender_id_pool      CAS bitset — allocates appender IDs
```

All files are plain OS files. The normative byte-level specification is
[wire-format.md](../reference/wire-format.md).
