/* Test-only link wrappers. Failure injection is one-shot and never shipped. */
#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static int failure;
static int remaining;
void test_fail_operation(int operation) { failure = operation; remaining = 0; }
void test_fail_after(int operation, int calls) { failure = operation; remaining = calls; }
static int fail(int operation)
{
    if (failure != operation) return 0;
    if (remaining > 0) { remaining--; return 0; }
    failure = 0;
    errno = EIO;
    return 1;
}

int __real_msync(void *, size_t, int);
int __wrap_msync(void *address, size_t size, int flags)
{
    return fail(1) ? -1 : __real_msync(address, size, flags);
}
void *__real_mmap(void *, size_t, int, int, int, off_t);
void *__wrap_mmap(void *address, size_t size, int protection, int flags, int fd, off_t offset)
{
    return fail(2) ? MAP_FAILED : __real_mmap(address, size, protection, flags, fd, offset);
}
int __real_fstat(int, struct stat *);
int __wrap_fstat(int fd, struct stat *st)
{
    return fail(3) ? -1 : __real_fstat(fd, st);
}
int __real_fstatat(int, const char *, struct stat *, int);
int __wrap_fstatat(int fd, const char *name, struct stat *st, int flags)
{
    return fail(4) ? -1 : __real_fstatat(fd, name, st, flags);
}
struct dirent *__real_readdir(DIR *);
struct dirent *__wrap_readdir(DIR *stream)
{
    return fail(5) ? NULL : __real_readdir(stream);
}
DIR *__real_fdopendir(int);
DIR *__wrap_fdopendir(int fd) { return fail(6) ? NULL : __real_fdopendir(fd); }
int __real_munmap(void *, size_t);
int __wrap_munmap(void *address, size_t size)
{
    int result = __real_munmap(address, size);
    return fail(7) ? -1 : result;
}
int __real_close(int);
int __wrap_close(int fd)
{
    int result = __real_close(fd);
    return fail(8) ? -1 : result;
}

/* Simulate the core's acquisition CAS on a synthetic pool, independently of
 * the administration module's release code, for cross-process contention. */
int test_acquire_bit(const char *path, unsigned id)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    void *base = mmap(NULL, 64, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { close(fd); return -1; }
    _Atomic uint32_t *word = (_Atomic uint32_t *)base + id / 32;
    uint32_t mask = UINT32_C(1) << (id % 32);
    uint32_t current = atomic_load_explicit(word, memory_order_acquire);
    int acquired = 0;
    while (!(current & mask)) {
        if (atomic_compare_exchange_weak_explicit(word, &current, current | mask,
                memory_order_acq_rel, memory_order_acquire)) { acquired = 1; break; }
    }
    munmap(base, 64);
    close(fd);
    return acquired;
}
