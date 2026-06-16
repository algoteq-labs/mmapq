#include <time.h>
#include <stdint.h>
#include <stdio.h>

static inline uint64_t monotonic_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

int main() {
    uint64_t start = monotonic_nanos();
    uint64_t count = 10000000;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t val = monotonic_nanos();
        (void)val;
    }
    uint64_t end = monotonic_nanos();
    printf("Clock overhead: %.2f ns\n", (double)(end - start) / count);
    return 0;
}
