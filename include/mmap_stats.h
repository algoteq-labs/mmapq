#ifndef MMAP_STATS_H
#define MMAP_STATS_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * mmap_stats_compare_u64 — comparison function for qsort.
 */
static inline int mmap_stats_compare_u64(const void *a, const void *b)
{
    uint64_t v1 = *(const uint64_t *)a;
    uint64_t v2 = *(const uint64_t *)b;
    return (v1 < v2) ? -1 : (v1 > v2);
}

/**
 * mmap_stats_print — print comprehensive latency statistics.
 *
 * results:   array of latency samples in nanoseconds.
 * count:     number of samples.
 * total_ns:  total duration of the measured period in nanoseconds.
 * label:     header for the statistics table.
 */
static inline void mmap_stats_print(
    uint64_t   *results, 
    int         count, 
    uint64_t    total_ns, 
    const char *label)
{
    if (count <= 0) return;

    qsort(results, (size_t)count, sizeof(uint64_t), mmap_stats_compare_u64);
    
    double sum = 0;
    for (int i = 0; i < count; i++) sum += (double)results[i];
    double avg = sum / count;
    double ops_per_sec = (total_ns > 0) ? ((double)count / ((double)total_ns / 1e9)) : 0;
    
    printf("\n%s\n", label ? label : "Statistics (nanoseconds):");
    printf("------------------------------------------\n");
    printf("Iterations: %10d\n", count);
    if (ops_per_sec > 0) {
        printf("Throughput: %10.2f ops/sec\n", ops_per_sec);
    }
    printf("Average:    %10.2f\n", avg);
    printf("Min (p0):   %10lu\n", results[0]);
    printf("Max (p100): %10lu\n", results[count - 1]);
    printf("------------------------------------------\n");
    printf("P50:        %10lu\n", results[(int)(count * 0.50)]);
    printf("P90:        %10lu\n", results[(int)(count * 0.90)]);
    printf("P95:        %10lu\n", results[(int)(count * 0.95)]);
    printf("P99:        %10lu\n", results[(int)(count * 0.99)]);
    printf("P99.9:      %10lu\n", results[(int)(count * 0.999)]);
    printf("P99.99:     %10lu\n", results[(int)(count * 0.9999)]);
    printf("P99.999:    %10lu\n", results[(int)(count * 0.99999)]);
    printf("------------------------------------------\n\n");
}

#endif /* MMAP_STATS_H */
