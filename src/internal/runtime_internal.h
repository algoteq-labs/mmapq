#ifndef RUNTIME_INTERNAL_H
#define RUNTIME_INTERNAL_H

#include "runtime.h"

/**
 * mmapqi_runtime_thread_detached — 1 if close() detached the runtime
 * thread after a join timeout (C-02), 0 otherwise.
 *
 * Precondition: same as mmapq_runtime_register — rt must still be alive.
 * Participants hold a raw runtime pointer and never clone it, so this is
 * only callable while the embedder keeps the runtime alive past its
 * participants (joined case), or after a detach, where
 * mmapq_runtime_destroy() deliberately leaks the runtime intact.
 */
int mmapqi_runtime_thread_detached(const MmapqRuntime *rt);

#endif
