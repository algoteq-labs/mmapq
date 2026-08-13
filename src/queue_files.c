#include "internal/queue_files.h"
#include <sys/stat.h>
#include <dirent.h>
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

/* ── FileIndexSet iteration ───────────────────────────────────────────────── */

int mmapqi_file_index_set_first(const FileIndexSet *s)
{
    for (int w = 0; w < FILE_INDEX_SET_WORDS; w++) {
        if (s->bits[w]) {
            int bit = __builtin_ctz(s->bits[w]);
            return w * 32 + bit;
        }
    }
    return -1;
}

int mmapqi_file_index_set_next(const FileIndexSet *s, int after)
{
    if (after < 0 || after >= FILE_INDEX_SET_MAX - 1) return -1;
    int start = after + 1;
    int w = start >> 5;
    int b = start & 31;

    /* Check remaining bits in the current word */
    uint32_t mask = s->bits[w] >> b;
    if (mask) return w * 32 + b + __builtin_ctz(mask);

    /* Check subsequent words */
    for (w++; w < FILE_INDEX_SET_WORDS; w++) {
        if (s->bits[w])
            return w * 32 + __builtin_ctz(s->bits[w]);
    }
    return -1;
}
