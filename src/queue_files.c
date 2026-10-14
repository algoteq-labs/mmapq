#include "internal/queue_files.h"
#include "mmap_platform.h"
#include <sys/stat.h>
#include <sys/file.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

/* ── File existence ───────────────────────────────────────────────────────── */

int mmapqi_queue_file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

/* ── Appender ID discovery ───────────────────────────────────────────────── */

int mmapqi_queue_files_get_appender_ids(
    const char *directory,
    const char *queue_name,
    uint8_t    *ids,
    int         max_ids)
{
    char dir_path[4096];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", directory, queue_name);

    DIR *d = opendir(dir_path);
    if (!d) return 0;

    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL && count < max_ids) {
        unsigned int id;
        if (sscanf(entry->d_name, "appender_meta_%u", &id) == 1 && id < 256)
            ids[count++] = (uint8_t)id;
    }
    closedir(d);
    return count;
}

/* ── Archive lock ─────────────────────────────────────────────────────────── */

int mmapqi_archive_lock_acquire(
    const char *directory,
    const char *queue_name,
    int         exclusive,
    int        *out_fd)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s/archive.lock", directory, queue_name);

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
        return (errno == ENOENT) ? MMAPQ_EINVAL : MMAPQ_EIO;

    int op = exclusive ? LOCK_EX : LOCK_SH;
    while (flock(fd, op) != 0) {
        if (errno == EINTR) continue;
        close(fd);
        return MMAPQ_EIO;
    }
    *out_fd = fd;
    return 0;
}

/* ── FileIndexSet iteration ───────────────────────────────────────────────── */

int64_t mmapqi_file_index_set_first(const FileIndexSet *s)
{
    if (!s->has_base) return -1;
    for (int w = 0; w < FILE_INDEX_SET_WORDS; w++) {
        if (s->bits[w]) {
            int bit = __builtin_ctz(s->bits[w]);
            return s->base + w * 32 + bit;
        }
    }
    return -1;
}

int64_t mmapqi_file_index_set_next(const FileIndexSet *s, int64_t after)
{
    if (!s->has_base) return -1;
    int64_t start = after - s->base + 1;
    if (start < 0) start = 0;
    if (start >= FILE_INDEX_SET_MAX) return -1;
    int w = (int)(start >> 5);
    int b = (int)(start & 31);

    /* Check remaining bits in the current word */
    uint32_t mask = s->bits[w] >> b;
    if (mask) return s->base + w * 32 + b + __builtin_ctz(mask);

    /* Check subsequent words */
    for (w++; w < FILE_INDEX_SET_WORDS; w++) {
        if (s->bits[w])
            return s->base + w * 32 + __builtin_ctz(s->bits[w]);
    }
    return -1;
}
