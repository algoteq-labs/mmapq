#include "mmap_platform.h"

const char *mmapq_strerror(int rc)
{
    switch (rc) {
    case MMAPQ_ETIMEDOUT:  return "mapping timeout or I/O — transient, retry";
    case MMAPQ_EINVAL:     return "invalid argument (NULL, zero length, etc.)";
    case MMAPQ_ECLOSED:    return "operation on closed handle";
    case MMAPQ_ECORRUPT:   return "on-disk corruption detected";
    case MMAPQ_ESCHEMA:    return "schema version or queue type mismatch";
    case MMAPQ_EEXIST:     return "destination queue already exists";
    case MMAPQ_ENOENT:     return "file or queue does not exist";
    case MMAPQ_EIDCONFLICT: return "requested appender ID already in use";
    case MMAPQ_ENOMEM:     return "internal allocation failure — back off, do not retry hot-loop";
    case MMAPQ_EIO:        return "I/O error — metadata file write failure, retry";
    case MMAPQ_ENOTREADY:  return "meta file exists but not yet fully initialised — transient, retry";
    default:               return "unknown error";
    }
}
