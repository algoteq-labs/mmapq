/* Consumption smoke test: link and call into the installed mmapq library. */
#include <mmapq/mmapq.h>
#include <stdio.h>

int main(void)
{
    const char *v = mmapq_version();
    if (v == 0 || v[0] == '\0') {
        fprintf(stderr, "mmapq_version() returned nothing\n");
        return 1;
    }
    printf("mmapq consumer OK (version %s)\n", v);
    return 0;
}
