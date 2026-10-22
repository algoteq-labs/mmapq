import ctypes as C
import errno
import multiprocessing as MP
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


BUILD = Path(os.environ["ADMIN_BUILD"])


class Facts(C.Structure):
    _fields_ = [("taken", C.c_int), ("has_meta", C.c_int),
                ("payload_files", C.c_uint64), ("payload_bytes", C.c_uint64),
                ("has_mtime", C.c_int), ("latest_mtime", C.c_long)]


def library(name):
    lib = C.CDLL(str(BUILD / name))
    lib.mmapq_appender_id_admin_inspect.argtypes = [C.c_char_p, C.c_char_p, C.POINTER(Facts)]
    lib.mmapq_appender_id_admin_release.argtypes = [C.c_char_p, C.c_char_p, C.c_uint, C.POINTER(C.c_int)]
    return lib


def contend(directory, barrier, lane, acquire, results):
    lib = library("test_admin.so")
    barrier.wait(timeout=10)
    if acquire:
        lib.test_acquire_bit.argtypes = [C.c_char_p, C.c_uint]
        result = lib.test_acquire_bit(os.fsencode(Path(directory) / "q/appender_id_pool"), lane)
        results.put((lane, 0, result))
    else:
        released = C.c_int()
        error = lib.mmapq_appender_id_admin_release(os.fsencode(directory), b"q", lane, C.byref(released))
        results.put((lane, error, released.value))


class AdminTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = library("libmmapq_appender_id_admin.so")
        cls.faults = library("test_admin.so")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="mmapq-id-admin-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.queue = self.root / "q"
        self.queue.mkdir()
        self.pool = self.queue / "appender_id_pool"
        self.pool.write_bytes(bytes(64))
        self.faults.test_fail_operation(0)
        self.addCleanup(self.faults.test_fail_operation, 0)

    def inspect(self, lib=None, queue=b"q"):
        facts = (Facts * 256)()
        C.memset(facts, 0xA5, C.sizeof(facts))
        error = (lib or self.lib).mmapq_appender_id_admin_inspect(os.fsencode(self.root), queue, facts)
        if error:
            self.assertEqual(bytes(facts), bytes(C.sizeof(facts)))
        return error, facts

    def release(self, lane, lib=None, queue=b"q"):
        released = C.c_int(99)
        error = (lib or self.lib).mmapq_appender_id_admin_release(
            os.fsencode(self.root), queue, lane, C.byref(released))
        return error, released.value

    def cli(self, *args):
        return subprocess.run([str(BUILD / "mmapq-ids"), str(self.root), "q", *args],
                              capture_output=True, text=True, timeout=10)

    def test_empty_inspection_is_read_only(self):
        before = self.pool.stat().st_mtime_ns
        error, facts = self.inspect()
        self.assertEqual(error, 0)
        self.assertEqual(bytes(facts), bytes(C.sizeof(facts)))
        self.assertEqual(self.pool.stat().st_mtime_ns, before)
        self.assertEqual(self.pool.read_bytes(), bytes(64))

    def test_all_256_ids_and_reserved_bytes(self):
        original = b"\xff" * 32 + bytes(range(32))
        self.pool.write_bytes(original)
        error, facts = self.inspect()
        self.assertEqual(error, 0)
        self.assertEqual([f.taken for f in facts], [1] * 256)
        for lane in range(256):
            with self.subTest(lane=lane):
                self.pool.write_bytes(original)
                self.assertEqual(self.release(lane), (0, 1))
                expected = bytearray(original)
                expected[lane // 8] &= ~(1 << (lane % 8))
                self.assertEqual(self.pool.read_bytes(), expected)
                self.assertEqual(self.release(lane), (0, 0))

    def test_missing_pool_is_not_created(self):
        self.pool.unlink()
        self.assertEqual(self.inspect()[0], errno.ENOENT)
        self.assertEqual(self.release(7), (errno.ENOENT, 0))
        self.assertFalse(self.pool.exists())

    def test_missing_directory_is_not_created(self):
        self.assertEqual(self.inspect(queue=b"missing")[0], errno.ENOENT)
        self.assertEqual(self.release(7, queue=b"missing"), (errno.ENOENT, 0))
        self.assertFalse((self.root / "missing").exists())

    def test_exact_pool_size_required(self):
        for size in (0, 1, 31, 32, 63, 65, 4096):
            with self.subTest(size=size):
                data = b"\xff" * size
                self.pool.write_bytes(data)
                self.assertEqual(self.inspect()[0], errno.EINVAL)
                self.assertEqual(self.release(7), (errno.EINVAL, 0))
                self.assertEqual(self.pool.read_bytes(), data)

    def test_pool_directory_fifo_and_symlink_rejected(self):
        self.pool.unlink()
        self.pool.mkdir()
        self.assertEqual(self.inspect()[0], errno.EINVAL)
        self.assertNotEqual(self.release(0)[0], 0)
        self.pool.rmdir()
        os.mkfifo(self.pool)
        self.assertEqual(self.inspect()[0], errno.EINVAL)
        self.assertEqual(self.release(0), (errno.EINVAL, 0))
        self.pool.unlink()
        other = self.root / "other"
        other.write_bytes(b"\xff" * 64)
        self.pool.symlink_to(other)
        self.assertEqual(self.inspect()[0], errno.ELOOP)
        self.assertEqual(self.release(0), (errno.ELOOP, 0))
        self.assertEqual(other.read_bytes(), b"\xff" * 64)

    def test_queue_symlink_rejected(self):
        (self.root / "alias").symlink_to(self.queue)
        self.assertNotEqual(self.inspect(queue=b"alias")[0], 0)
        self.assertNotEqual(self.release(0, queue=b"alias")[0], 0)

    def test_argument_validation(self):
        for queue in (None, b"", b".", b"..", b"q/../q"):
            self.assertEqual(self.inspect(queue=queue)[0], errno.EINVAL)
            self.assertEqual(self.release(0, queue=queue), (errno.EINVAL, 0))
        for lane in (256, 65536, 2**32 - 1):
            self.assertEqual(self.release(lane), (errno.EINVAL, 0))
        self.assertEqual(self.lib.mmapq_appender_id_admin_inspect(b"x", b"q", None), errno.EINVAL)
        self.assertEqual(self.lib.mmapq_appender_id_admin_release(b"x", b"q", 0, None), errno.EINVAL)
        facts = (Facts * 256)()
        released = C.c_int(99)
        self.assertEqual(self.lib.mmapq_appender_id_admin_inspect(None, b"q", facts), errno.EINVAL)
        self.assertEqual(self.lib.mmapq_appender_id_admin_release(None, b"q", 0, C.byref(released)), errno.EINVAL)
        self.assertEqual(released.value, 0)

    def test_exact_lane_names_sizes_and_mtimes(self):
        files = {"appender_meta_7": b"m" * 64, "payload_7_0": b"abc",
                 "payload_7_1": b"defgh", "payload_255_9223372036854775807": b"x"}
        for index, (name, data) in enumerate(files.items()):
            path = self.queue / name
            path.write_bytes(data)
            os.utime(path, (100 + index, 100 + index))
        for name in ("payload_7_2.backup", "appender_meta_8.bak", "payload_256_0",
                     "payload_-1_0", "payload_07_0", "payload_7_00", "payload_7_",
                     "payload_7_9223372036854775808", "payload_99999999999999999999999_0",
                     "payload_7_+2", "appender_meta_", "meta"):
            (self.queue / name).write_bytes(b"ignored")
        error, facts = self.inspect()
        self.assertEqual(error, 0)
        f = facts[7]
        self.assertEqual((f.has_meta, f.payload_files, f.payload_bytes, f.has_mtime, f.latest_mtime),
                         (1, 2, 8, 1, 102))
        self.assertEqual(facts[255].payload_files, 1)
        self.assertEqual(sum(f.payload_files for f in facts), 3)
        self.assertEqual(sum(f.has_meta for f in facts), 1)

    def test_matching_lane_nonregular_files_fail_inspection(self):
        lane = self.queue / "payload_1_0"
        lane.mkdir()
        self.assertEqual(self.inspect()[0], errno.EINVAL)
        lane.rmdir()
        lane.symlink_to(self.pool)
        self.assertEqual(self.inspect()[0], errno.EINVAL)

    def test_release_does_not_require_valid_lane_or_queue_meta(self):
        (self.queue / "payload_7_0").mkdir()
        (self.queue / "meta").write_bytes(b"damaged")
        self.pool.write_bytes(b"\x80" + bytes(63))
        self.assertEqual(self.release(7), (0, 1))

    def test_errors_do_not_return_partial_inspection(self):
        (self.queue / "payload_7_0").write_bytes(b"data")
        for operation in (2, 3, 4, 5, 6, 7, 8):
            with self.subTest(operation=operation):
                self.faults.test_fail_operation(operation)
                self.assertEqual(self.inspect(self.faults)[0], errno.EIO)

    def test_failed_mapping_does_not_release(self):
        self.pool.write_bytes(b"\xff" * 64)
        for operation in (2, 3, 8):
            self.faults.test_fail_operation(operation)
            self.assertEqual(self.release(7, self.faults), (errno.EIO, 0))
            self.assertEqual(self.pool.read_bytes(), b"\xff" * 64)

    def test_late_stat_failure_discards_preceding_facts(self):
        (self.queue / "payload_7_0").write_bytes(b"first")
        (self.queue / "payload_8_0").write_bytes(b"second")
        self.faults.test_fail_after(4, 1)
        self.assertEqual(self.inspect(self.faults)[0], errno.EIO)

    def test_writeback_and_unmap_failure_report_already_released(self):
        for operation in (1, 7):
            self.pool.write_bytes(b"\xff" * 64)
            self.faults.test_fail_operation(operation)
            self.assertEqual(self.release(7, self.faults), (errno.EIO, 1))
            self.assertEqual(self.pool.read_bytes(), b"\x7f" + b"\xff" * 63)

    def test_close_failure_after_cas_reports_already_released(self):
        self.pool.write_bytes(b"\xff" * 64)
        # Skip closing the base directory; fail closing the pool after CAS.
        self.faults.test_fail_after(8, 1)
        self.assertEqual(self.release(7, self.faults), (errno.EIO, 1))
        self.assertEqual(self.pool.read_bytes(), b"\x7f" + b"\xff" * 63)

    def test_noop_does_not_write_back(self):
        self.faults.test_fail_operation(1)
        self.assertEqual(self.release(7, self.faults), (0, 0))
        self.pool.write_bytes(b"\x80" + bytes(63))
        self.assertEqual(self.release(7, self.faults), (errno.EIO, 1))

    def test_error_paths_do_not_leak_descriptors_or_mappings(self):
        (self.queue / "payload_0_0").touch()
        before = len(os.listdir("/proc/self/fd"))
        for _ in range(10):
            for operation in (2, 3, 4, 5, 6, 7, 8):
                self.faults.test_fail_operation(operation)
                self.assertEqual(self.inspect(self.faults)[0], errno.EIO)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        self.assertNotIn(str(self.pool), Path("/proc/self/maps").read_text())

    def test_cross_process_neighbor_acquisitions_are_preserved(self):
        ctx = MP.get_context("fork")
        for _ in range(5):
            self.pool.write_bytes(b"\x55" + bytes(31) + b"\xa5" * 32)
            barrier = ctx.Barrier(8)
            results = ctx.Queue()
            workers = [ctx.Process(target=contend, args=(str(self.root), barrier, lane, lane % 2, results))
                       for lane in range(8)]
            try:
                for worker in workers:
                    worker.start()
                outcomes = [results.get(timeout=15) for _ in workers]
                self.assertEqual(sorted(outcomes), [(lane, 0, 1) for lane in range(8)])
                for worker in workers:
                    worker.join(10)
                    self.assertEqual(worker.exitcode, 0)
                self.assertEqual(self.pool.read_bytes(), b"\xaa" + bytes(31) + b"\xa5" * 32)
            finally:
                for worker in workers:
                    if worker.is_alive():
                        worker.kill()
                    if worker.pid:
                        worker.join()
                results.close()
                results.join_thread()

    def test_cli_list_and_release_exit_codes(self):
        self.pool.write_bytes(b"\x80" + bytes(63))
        result = self.cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1/256 IDs taken", result.stdout)
        self.assertIn("not evidence of dead writers", result.stdout)
        result = self.cli("--unset", "7")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("does not verify ownership", result.stderr)
        self.assertIn("released (was taken)", result.stdout)
        self.assertEqual(self.cli("--unset", "7").returncode, 1)

    def test_cli_rejects_invalid_ids_and_usage(self):
        for lane in ("", "-1", "+1", " 7", "7tail", "256", "9" * 100):
            result = self.cli("--unset", lane)
            self.assertEqual(result.returncode, 2)
        self.assertEqual(self.cli("--unknown").returncode, 2)
        self.assertEqual(self.pool.read_bytes(), bytes(64))
        result = subprocess.run([str(BUILD / "mmapq-ids"), "--help"], capture_output=True)
        self.assertEqual(result.returncode, 0)

    def test_cli_reports_output_failure(self):
        with open("/dev/full", "w") as output:
            result = subprocess.run([str(BUILD / "mmapq-ids"), str(self.root), "q"],
                                    stdout=output, stderr=subprocess.PIPE, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("output failed", result.stderr)

    def test_cli_output_failure_after_release_does_not_hide_mutation(self):
        self.pool.write_bytes(b"\x80" + bytes(63))
        with open("/dev/full", "w") as output:
            result = subprocess.run([str(BUILD / "mmapq-ids"), str(self.root), "q", "--unset", "7"],
                                    stdout=output, stderr=subprocess.PIPE, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("release operation has already completed", result.stderr)
        self.assertEqual(self.pool.read_bytes(), bytes(64))

    @unittest.skipIf(os.geteuid() == 0, "permission enforcement requires an unprivileged user")
    def test_read_only_inspection_and_permission_errors(self):
        self.pool.chmod(0o444)
        self.assertEqual(self.inspect()[0], 0)
        self.assertEqual(self.release(0), (errno.EACCES, 0))
        self.pool.chmod(0)
        self.assertEqual(self.inspect()[0], errno.EACCES)
        self.pool.chmod(0o600)


if __name__ == "__main__":
    unittest.main()
