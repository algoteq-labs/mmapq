#include "mmapq_appender_id_admin.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char responsibility[] =
    "This tool does not verify ownership or process liveness. The operator must\n"
    "ensure the ID is unused and prevent acquisition/restart during release.\n"
    "Prefer stopped producers with automatic restarts disabled. Clearing an\n"
    "active ID can allow two writers into one lane and corrupt the queue.\n";

static void usage(const char *program)
{
    fprintf(stderr,
        "usage: %s <directory> <queue-name>\n"
        "       %s <directory> <queue-name> --unset <id>\n"
        "\n%s\n"
        "exit codes: 0 = inspected/released, 1 = already free, 2 = error\n",
        program, program, responsibility);
}

static int list(const char *directory, const char *queue)
{
    MmapqAppenderIdFacts facts[MMAPQ_APPENDER_ID_ADMIN_COUNT];
    int error = mmapq_appender_id_admin_inspect(directory, queue, facts);
    if (error) {
        fprintf(stderr, "inspection failed: %s (errno=%d)\n", strerror(error), error);
        return 2;
    }
    printf("pool: %s/%s/appender_id_pool\n\n", directory, queue);
    printf("%-4s %-6s %-6s %-9s %-14s %s\n",
           "ID", "taken", "meta", "payloads", "bytes", "latest-mtime (UTC)");
    int taken = 0, shown = 0;
    for (unsigned id = 0; id < MMAPQ_APPENDER_ID_ADMIN_COUNT; id++) {
        const MmapqAppenderIdFacts *fact = &facts[id];
        taken += fact->taken;
        if (!fact->taken && !fact->has_meta && !fact->payload_files) continue;
        shown++;
        char when[32] = "-";
        struct tm tm;
        if (fact->has_mtime &&
            (!gmtime_r(&fact->latest_mtime, &tm) ||
             !strftime(when, sizeof(when), "%Y-%m-%dT%H:%M:%SZ", &tm)))
            strcpy(when, "unrepresentable");
        printf("%-4u %-6s %-6s %-9" PRIu64 " %-14" PRIu64 " %s\n",
               id, fact->taken ? "yes" : "no", fact->has_meta ? "yes" : "no",
               fact->payload_files, fact->payload_bytes, when);
    }
    if (!shown) puts("(no IDs taken, no appender files on disk)");
    printf("\n%d/%d IDs taken\n", taken, MMAPQ_APPENDER_ID_ADMIN_COUNT);
    puts("Observations are not a coherent snapshot. File mtimes do not reliably\n"
         "indicate mmap write activity. Old mtimes are not evidence of dead writers.");
    if (fflush(stdout) != 0 || ferror(stdout)) {
        fputs("error: inspection output failed\n", stderr);
        return 2;
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--help")) { usage(argv[0]); return 0; }
    if (argc == 3) return list(argv[1], argv[2]);
    if (argc != 5 || strcmp(argv[3], "--unset")) { usage(argv[0]); return 2; }

    const char *id_text = argv[4];
    if (!*id_text || strspn(id_text, "0123456789") != strlen(id_text)) {
        fputs("error: expected an appender ID from 0 to 255\n", stderr); return 2;
    }
    errno = 0;
    unsigned long id = strtoul(id_text, NULL, 10);
    if (errno == ERANGE || id >= MMAPQ_APPENDER_ID_ADMIN_COUNT) {
        fputs("error: expected an appender ID from 0 to 255\n", stderr); return 2;
    }
    fputs(responsibility, stderr);
    int released = 0;
    int error = mmapq_appender_id_admin_release(argv[1], argv[2], (unsigned)id, &released);
    if (error) {
        fprintf(stderr, "release failed: %s (errno=%d)\n", strerror(error), error);
        if (released)
            fputs("The bit WAS cleared; writeback or cleanup failed. Do not blindly retry.\n", stderr);
        return 2;
    }
    printf("ID %lu: %s\n", id, released ? "released (was taken)" : "not taken (no-op)");
    if (fflush(stdout) != 0 || ferror(stdout)) {
        fputs("error: output failed; the release operation has already completed\n", stderr);
        return 2;
    }
    return released ? 0 : 1;
}
