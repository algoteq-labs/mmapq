#ifndef MMAPQ_H
#define MMAPQ_H

/* Umbrella header — includes the full public API.
   The header set is defined by include/PUBLIC_HEADERS — that file is
   authoritative. Adding a public header must add it here too, or the
   manifest-completeness gate (check_sync.sh) fails.
   For finer control, include individual headers directly. */

#include "mmap_platform.h"
#include "runtime.h"
#include "poller.h"
#include "queue_transfer.h"
#include "batch_queue_transfer.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "indexed_archiver.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_archiver.h"
#include "batch_payload_iterator.h"

#endif /* MMAPQ_H */
