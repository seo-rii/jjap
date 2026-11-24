"""Replay Kafka test server snapshots into a second test server through the Kafka protocol."""

import contextlib
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import quote


SCRIPT = Path(__file__).resolve().parents[1] / "kafka_server.py"


class KafkaMigrateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-kafka-migrate-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("source #?.sqlite3" if os.name != "nt" else "source #.sqlite3")

    def varint(self, value):
        return self.kafka["wire_uint"]((value << 1) ^ (value >> 63))

    def blob(self, value):
        return self.varint(-1) if value is None else self.varint(len(value)) + value

    def batch(self, records, compressed=False):
        """Encode (timestamp, key, value, headers) tuples as one magic 2 batch, as a producer sends it."""
        base = records[0][0]
        payload = b""
        for index, (stamp, key, value, headers) in enumerate(records):
            body = (b"\0" + self.varint(stamp - base) + self.varint(index) + self.blob(key) + self.blob(value)
                    + self.varint(len(headers)) + b"".join(self.blob(name.encode()) + self.blob(item) for name, item in headers))
            payload += self.varint(len(body)) + body
        if compressed:
            payload = gzip.compress(payload, mtime=0)
        body = struct.pack(">hiqqqhii", int(compressed), len(records) - 1, base, max(item[0] for item in records),
                           -1, -1, -1, len(records)) + payload
        return struct.pack(">qiibI", 0, len(body) + 9, -1, 2, self.kafka["crc32c"](body)) + body

    def raw(self, base, records, *, attributes=0, producer=(-1, -1, -1), last_delta=None, max_timestamp=None):
        """Encode a batch as a broker stores it: (offset delta, timestamp, key, value) records, any attribute bits."""
        first = min(stamp for _, stamp, _, _ in records) if records else 0
        payload = b""
        for delta, stamp, key, value in records:
            body = b"\0" + self.varint(stamp - first) + self.varint(delta) + self.blob(key) + self.blob(value) + self.varint(0)
            payload += self.varint(len(body)) + body
        if attributes & 7 == 1:
            payload = gzip.compress(payload, mtime=0)
        last_delta = records[-1][0] if last_delta is None else last_delta
        if max_timestamp is None:
            max_timestamp = max(stamp for _, stamp, _, _ in records)
        body = struct.pack(">hiqqqhii", attributes, last_delta, first, max_timestamp, *producer, len(records)) + payload
        return struct.pack(">qiibI", base, len(body) + 9, 0, 2, self.kafka["crc32c"](body)) + body

    def store_raw(self, path, topic, batches, end, start=0, configs=None):
        """Write broker-style batches into one new single-partition topic, bypassing producer validation."""
        store = self.kafka["Store"](str(path))
        try:
            with store.db:
                store.db.execute("BEGIN IMMEDIATE")
                store.add_topic(topic, 1, configs)
                for data in batches:
                    parsed, = self.kafka["read_batches"](data, client=False)
                    base = struct.unpack_from(">q", data)[0]
                    store.store_batch(topic, 0, base, base + parsed.last_delta, data, parsed.timestamps(), start)
                store.db.execute("UPDATE kafka_partitions SET next_offset = ?, log_start_offset = ? WHERE topic = ?",
                                 (end, start, topic))
        finally:
            store.db.close()

    def store(self, path, topics, batches=(), commits=()):
        store = self.kafka["Store"](str(path))
        try:
            created = store.create_topics([(item[0], item[1], 1, [], item[2] if len(item) > 2 else {}) for item in topics],
                                          False)
            self.assertEqual([error for _, error, _ in created], [0] * len(topics))
            for topic, partition, data in batches:
                (_, [(_, error, *_)]), = store.produce([(topic, [(partition, data)])], 0)
                self.assertEqual(error, 0)
            for group, topic, partition, offset, metadata in commits:
                (_, [(_, error)]), = store.commit_offsets(group, -1, "", [(topic, [(partition, offset, metadata)])])
                self.assertEqual(error, 0)
        finally:
            store.db.close()

    def build_source(self):
        self.store(self.source, [("events", 2), ("empty", 1), ("solo", 1)], [
            ("events", 0, self.batch([
                (1_000, None, b"first", [("trace", b"a"), ("null-header", None)]),
                (999, b"\x00\xffkey", None, []),
                (1_005, b"", b"", [("dup", b"1"), ("dup", b"2")]),
            ])),
            ("events", 0, self.batch([(2_000, b"zip", b"z" * 300, []), (2_001, "키".encode(), "값".encode(), [])], True)),
            ("events", 1, self.batch([(3_000, b"other", b"partition", [])])),
            ("solo", 0, self.batch([(4_000, None, None, [])])),
        ], [
            ("readers", "events", 0, 4, "checkpoint"),
            ("readers", "events", 1, 1, None),
            ("readers", "solo", 0, 0, ""),
            ("idle", "empty", 0, 0, "nothing yet"),
        ])

    @contextlib.contextmanager
    def target(self, name="target.sqlite3"):
        database = self.directory / name
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0", "--db-file", str(database)],
            cwd=self.directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            line = process.stdout.readline()
            match = re.search(r"127\.0\.0\.1:(\d+)", line)
            if match is None:
                self.fail(f"Kafka target did not start: {line}{process.stdout.read()}")
            yield int(match[1]), database
        finally:
            process.terminate()
            process.wait(timeout=10)
            process.stdout.close()

    def migrate(self, port, *args, check=True):
        result = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), "migrate", "--db-file", str(self.source),
             "--bootstrap-server", f"127.0.0.1:{port}", "--timeout", "5", *args],
            cwd=self.directory, text=True, capture_output=True, timeout=60,
        )
        if check and result.returncode:
            self.fail(f"migrate failed with {result.returncode}: {result.stderr}")
        return result

    def rows(self, database, sql):
        with contextlib.closing(sqlite3.connect(f"file:{quote(str(database))}?mode=ro", uri=True)) as connection:
            return connection.execute(sql).fetchall()

    def test_round_trip_preserves_batches_timestamps_and_group_offsets(self):
        self.build_source()
        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()
        offset_map = self.directory / "maps" / "offsets.json"
        with self.target() as (port, target):
            result = self.migrate(port, "--offset-map", str(offset_map))
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout, f"Migrated 7 records in 4 batches across 4 partitions of 3 topics and committed "
                                        f"4 offsets for 2 consumer groups to 127.0.0.1:{port}\n")
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)
        for sql in ("SELECT topic, partition, next_offset FROM kafka_partitions ORDER BY 1, 2",
                    "SELECT topic, partition, base_offset, last_offset, data FROM kafka_batches ORDER BY 1, 2, 4",
                    "SELECT * FROM kafka_timestamps ORDER BY 1, 2, 3",
                    "SELECT * FROM kafka_committed_offsets ORDER BY 1, 2, 3"):
            # Byte-identical batches keep keys, null values, headers, gzip payloads, and CreateTime timestamps.
            self.assertEqual(self.rows(target, sql), self.rows(self.source, sql), sql)
        self.assertEqual(self.rows(target, "SELECT timestamp FROM kafka_timestamps WHERE partition = 0 AND topic = 'events'"),
                         [(1_000,), (999,), (1_005,), (2_000,), (2_001,)])
        document = json.loads(offset_map.read_text(encoding="utf-8"))
        self.assertEqual(document["partitions"][1], {
            "topic": "events", "partition": 0, "source_end": 5, "target_start": 0, "target_end": 5, "records": 5,
            "ranges": [{"source_offset": 0, "target_offset": 0, "count": 5}], "error": None,
        })
        self.assertEqual([(item["group"], item["topic"], item["target_offset"], item["error"])
                          for item in document["group_offsets"]],
                         [("idle", "empty", 0, None), ("readers", "events", 4, None), ("readers", "events", 1, None),
                          ("readers", "solo", 0, None)])

    def test_existing_topics_are_checked_and_offsets_follow_the_target_log(self):
        self.build_source()
        target_file = self.directory / "target.sqlite3"
        prior = self.batch([(10, b"prior", b"record", []), (11, b"prior", b"record", [])])
        self.store(target_file, [("events", 2), ("solo", 3)], [("events", 0, prior)])
        with self.target() as (port, target):
            refused = self.migrate(port, check=False)
            self.assertEqual(refused.returncode, 1)
            self.assertIn("topic 'events': the target topic already holds 2 records", refused.stderr)
            self.assertIn("topic 'solo': the target topic has 3 partitions but the source has 1", refused.stderr)
            self.assertIn("group 'readers' topic 'events' partition 0: skipped", refused.stderr)
            self.assertTrue(refused.stderr.endswith(
                "Kafka migrate failed: 3 of 4 partitions and 1 of 2 consumer groups failed\n"), refused.stderr)
            self.assertEqual(self.rows(target, "SELECT group_id, topic, offset FROM kafka_committed_offsets ORDER BY 1"),
                             [("idle", "empty", 0)])
            self.assertEqual(self.rows(target, "SELECT next_offset FROM kafka_partitions WHERE topic = 'events'"),
                             [(2,), (0,)])

            offset_map = self.directory / "appended.json"
            self.migrate(port, "--topic", "events", "--topic", "events", "--allow-non-empty-topics",
                         "--offset-map", str(offset_map))
            self.assertEqual(self.rows(target, "SELECT topic, partition, offset, metadata FROM kafka_committed_offsets "
                                               "WHERE group_id = 'readers' ORDER BY 2, 3"),
                             [("events", 0, 6, "checkpoint"), ("events", 1, 1, None)])
            stored = self.rows(target, "SELECT base_offset, data FROM kafka_batches WHERE topic = 'events' AND partition = 0 "
                                       "ORDER BY base_offset")
            source = self.rows(self.source, "SELECT data FROM kafka_batches WHERE topic = 'events' AND partition = 0 "
                                            "ORDER BY base_offset")
            self.assertEqual([base for base, _ in stored], [0, 2, 5])
            self.assertEqual([data[12:] for _, data in stored[1:]], [data[12:] for data, in source])
        document = json.loads(offset_map.read_text(encoding="utf-8"))
        self.assertEqual([(item["partition"], item["target_start"], item["ranges"]) for item in document["partitions"]], [
            (0, 2, [{"source_offset": 0, "target_offset": 2, "count": 5}]),
            (1, 0, [{"source_offset": 0, "target_offset": 0, "count": 1}]),
        ])

    def test_active_target_group_is_reported(self):
        self.build_source()
        with self.target() as (port, target), socket.create_connection(("127.0.0.1", port), timeout=5) as member:
            wire_string, wire_array, wire_bytes = (self.kafka[name] for name in ("wire_string", "wire_array", "wire_bytes"))
            stream = member.makefile("rb")

            def call(api, version, body):
                frame = struct.pack(">hhi", api, version, 1) + wire_string("member") + body
                member.sendall(struct.pack(">i", len(frame)) + frame)
                return self.kafka["Reader"](stream.read(struct.unpack(">i", stream.read(4))[0]))

            joined = call(11, 1, wire_string("readers") + struct.pack(">ii", 30_000, 30_000) + wire_string("")
                          + wire_string("consumer") + wire_array([wire_string("range") + wire_bytes(b"")]))
            joined.i32()
            self.assertEqual(joined.i16(), 0)
            generation = joined.i32()
            joined.string(), joined.string()
            member_id = joined.string()
            synced = call(14, 1, wire_string("readers") + struct.pack(">i", generation) + wire_string(member_id)
                          + wire_array([wire_string(member_id) + wire_bytes(b"")]))
            synced.i32(), synced.i32()
            self.assertEqual(synced.i16(), 0)
            result = self.migrate(port, check=False)
            stream.close()
        # DescribeGroups reports the active group before anything is written.
        self.assertEqual((result.returncode, result.stdout), (1, ""))
        self.assertEqual(result.stderr, "Kafka migrate failed: consumer group 'readers' is Stable on the target with "
                                        "1 members; stop its consumers first\n")
        self.assertEqual(self.rows(target, "SELECT group_id FROM kafka_committed_offsets"), [])
        self.assertEqual(self.rows(target, "SELECT COUNT(*) FROM kafka_batches"), [(0,)])

    def test_requests_stay_bounded_and_ordered(self):
        values = [bytes([65 + index % 26]) * 700 for index in range(12)]
        self.store(self.source, [("bulk", 3)], [
            ("bulk", index % 3, self.batch([(index, None, value, [])])) for index, value in enumerate(values)
        ])
        migrate_main = self.kafka["migrate_main"]
        cluster = migrate_main.__globals__["KafkaCluster"]
        original = cluster.produce
        requests = []

        def produce(instance, address, topic, copies):
            requests.append([copy.partition for copy in copies])
            self.assertTrue(len(copies) == 1 or sum(len(copy.pending[2]) for copy in copies) <= 1600)
            return original(instance, address, topic, copies)

        output = io.StringIO()
        with self.target() as (port, target), patch.object(cluster, "produce", produce), \
                patch.dict(migrate_main.__globals__, {"MIGRATE_REQUEST_BYTES": 1600}), contextlib.redirect_stdout(output):
            self.assertEqual(migrate_main(["--db-file", str(self.source), "--bootstrap-server", f"127.0.0.1:{port}"]), 0)
            stored = self.rows(target, "SELECT partition, base_offset, data FROM kafka_batches ORDER BY partition, base_offset")
        self.assertTrue(output.getvalue().startswith("Migrated 12 records in 12 batches across 3 partitions"))
        # Two 770-byte batches fit the patched budget, so each round sends partitions 0 and 1 together, then 2.
        self.assertEqual(requests, [[0, 1], [2]] * 4)
        self.assertEqual([(partition, base) for partition, base, _ in stored],
                         [(partition, base) for partition in range(3) for base in range(4)])
        self.assertEqual([data[-701:-1] for _, _, data in stored],
                         [values[base * 3 + partition] for partition in range(3) for base in range(4)])

    def test_rejects_invalid_sources_and_existing_offset_maps(self):
        missing = self.directory / "missing.sqlite3"
        result = subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "migrate", "--db-file", str(missing),
                                 "--bootstrap-server", "127.0.0.1:9"], capture_output=True, text=True, timeout=15)
        self.assertEqual((result.returncode, result.stdout), (1, ""))
        self.assertTrue(result.stderr.startswith("Kafka migrate failed: Source database not found"), result.stderr)
        self.assertFalse(missing.exists())

        with contextlib.closing(sqlite3.connect(self.source)) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        result = self.migrate(9, check=False)
        self.assertEqual(result.stderr, "Kafka migrate failed: Expected a Kafka test server SQLite persistence file\n")

        self.source.unlink()
        self.build_source()
        existing = self.directory / "used.json"
        existing.write_text("preserve", encoding="utf-8")
        result = self.migrate(9, "--offset-map", str(existing), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Offset map already exists", result.stderr)
        self.assertEqual(existing.read_text(encoding="utf-8"), "preserve")

        unused = self.directory / "unused.json"
        result = self.migrate(9, "--topic", "nope", "--offset-map", str(unused), check=False)
        self.assertEqual(result.stderr, "Kafka migrate failed: Topic not found in the source: nope\n")
        with socket.socket() as closed:
            closed.bind(("127.0.0.1", 0))
            port = closed.getsockname()[1]
        started = time.monotonic()
        result = self.migrate(port, "--offset-map", str(unused), check=False)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith(f"Kafka migrate failed: 127.0.0.1:{port}: "), result.stderr)
        self.assertFalse(unused.exists())
        result = self.migrate(port, "--username", "user", check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--username and --password-file must be given together", result.stderr)


    def test_migrate_carries_topic_configs_and_dry_run_writes_nothing(self):
        self.store(self.source, [("events", 2), ("solo", 1, {"max.message.bytes": "5000", "segment.ms": "100000"})], [
            ("events", 0, self.batch([(1_000, b"k", b"v", []), (1_001, b"k", b"w", [])])),
            ("solo", 0, self.batch([(2_000, None, b"x", [])])),
        ], [("readers", "events", 0, 2, ""), ("readers", "solo", 0, 1, None)])
        with self.target() as (port, target):
            planned = self.migrate(port, "--dry-run")
            self.assertEqual(planned.stderr, "")
            self.assertEqual(planned.stdout.splitlines(), [
                "Kafka migrate: would create topic 'events' with 2 partitions",
                "Kafka migrate: would append 2 records in 1 batches to 'events' partition 0 from target offset 0",
                "Kafka migrate: would create topic 'solo' with 1 partitions and configs max.message.bytes=5000, segment.ms=100000",
                "Kafka migrate: would append 1 records in 1 batches to 'solo' partition 0 from target offset 0",
                "Kafka migrate: would commit offset 2 for consumer group 'readers' on 'events' partition 0",
                "Kafka migrate: would commit offset 1 for consumer group 'readers' on 'solo' partition 0",
                f"Dry run: would migrate 3 records in 2 batches across 3 partitions of 2 topics and commit 2 offsets "
                f"for 1 consumer groups to 127.0.0.1:{port}",
            ])
            self.assertEqual(self.rows(target, "SELECT COUNT(*) FROM kafka_topics"), [(0,)])
            self.migrate(port)
            self.assertEqual(self.rows(target, "SELECT * FROM kafka_topic_configs ORDER BY 1, 2"),
                             [("solo", "max.message.bytes", "5000"), ("solo", "segment.ms", "100000")])
            self.assertEqual(self.rows(target, "SELECT group_id, topic, offset FROM kafka_committed_offsets ORDER BY 2"),
                             [("readers", "events", 2), ("readers", "solo", 1)])

        other = self.directory / "other.sqlite3"
        self.store(other, [("solo", 1, {"max.message.bytes": "6000"})])
        with self.target("other.sqlite3") as (port, target):
            for args, outcome in ((["--dry-run"], "would fail"), (["--dry-run", "--topic", "solo"], "would fail"), ([], "failed")):
                refused = self.migrate(port, *args, check=False)
                self.assertEqual(refused.returncode, 1)
                self.assertIn("topic 'solo': the target topic has different configs: max.message.bytes=6000 (source 5000), "
                              "segment.ms=604800000 (source 100000); alter the target topic or delete it", refused.stderr)
                self.assertTrue(refused.stderr.endswith(f"consumer groups {outcome}\n"), refused.stderr)
            self.assertEqual(self.rows(target, "SELECT topic, COUNT(*) FROM kafka_batches GROUP BY 1"), [("events", 1)])
        result = self.migrate(9, "--dry-run", "--offset-map", str(self.directory / "map.json"), check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--offset-map records a real run; it cannot be combined with --dry-run", result.stderr)

    def test_migrate_rewrites_broker_written_batches(self):
        crc32c = self.kafka["crc32c"]
        self.store_raw(self.source, "log", [
            self.raw(0, [(0, 100, b"a", b"r0"), (1, 101, b"b", b"r1"), (2, 102, b"c", b"r2")], producer=(7, 0, 0)),
            self.raw(3, [(0, 103, b"\0\0\0\1", b"\0\0\0\0\0\0")], attributes=48, producer=(7, 0, -1)),  # COMMIT marker
            self.raw(4, [(0, 104, b"d", b"r4"), (2, 106, b"f", b"r6")], attributes=17, producer=(8, 2, 5)),  # gzip, compacted
            self.raw(7, [(0, 107, b"g", b"r7"), (1, 108, b"h", b"r8")], attributes=8, max_timestamp=9_999),  # LogAppendTime
        ], end=9, start=1)
        store = self.kafka["Store"](str(self.source))
        try:
            for group, offset in (("readers", 5), ("readers-at-marker", 3), ("done", 9), ("early", 0)):
                with store.db:
                    store.db.execute("INSERT INTO kafka_committed_offsets VALUES (?, 'log', 0, ?, NULL)", (group, offset))
        finally:
            store.db.close()
        offset_map = self.directory / "offsets.json"
        with self.target() as (port, target):
            result = self.migrate(port, "--offset-map", str(offset_map), check=False)
            stored = self.rows(target, "SELECT base_offset, last_offset, data FROM kafka_batches ORDER BY base_offset")
            committed = self.rows(target, "SELECT group_id, offset FROM kafka_committed_offsets ORDER BY 1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Migrated 6 records in 3 batches across 1 partitions of 1 topics and committed 3 offsets", result.stdout)
        self.assertIn("group 'early' topic 'log' partition 0: offset 0 does not point into the source log, which spans "
                      "offsets 1 to 9", result.stderr)
        self.assertEqual([(base, last) for base, last, _ in stored], [(0, 1), (2, 3), (4, 5)])
        values = []
        for _, _, data in stored:
            parsed, = self.kafka["read_batches"](data, client=False)
            # Plain producer batches: no producer state, transaction, or LogAppendTime flags, and a valid CRC.
            self.assertEqual((parsed.attributes & ~7, parsed.producer_id, parsed.epoch, parsed.sequence), (0, -1, -1, -1))
            self.assertEqual(crc32c(data[21:]), struct.unpack_from(">I", data, 17)[0])
            records = data[61:] if parsed.attributes == 0 else gzip.decompress(data[61:])
            values += re.findall(rb"r\d", records)
            values.append(parsed.max_timestamp)
        self.assertEqual(values, [b"r1", b"r2", 102, b"r4", b"r6", 106, b"r7", b"r8", 108])
        self.assertEqual(committed, [("done", 6), ("readers", 3), ("readers-at-marker", 2)])
        document = json.loads(offset_map.read_text(encoding="utf-8"))
        self.assertEqual([(item["source_offset"], item["target_offset"], item["count"]) for item in document["partitions"][0]["ranges"]],
                         [(1, 0, 2), (4, 2, 1), (6, 3, 3)])

    def import_source(self):
        """A source broker file holding everything import must carry, including broker-written batches."""
        self.store(self.source, [("events", 2, {"retention.ms": "-1", "max.message.bytes": "9000"}), ("solo", 1)], [
            ("events", 0, self.batch([(1_000, b"k", b"first", [("trace", b"a")]), (1_001, None, None, [])])),
            ("events", 0, self.batch([(2_000, b"zip", b"z" * 300, [])], True)),
            ("events", 1, self.batch([(3_000, b"other", b"partition", [])])),
        ], [("readers", "events", 0, 3, "checkpoint"), ("readers", "events", 1, 1, None), ("idle", "solo", 0, 0, "")])
        store = self.kafka["Store"](str(self.source))
        try:
            (_, [(_, low, error)]), = store.delete_records([("events", [(0, 1)])])
            self.assertEqual((low, error), (1, 0))
            producer = store.init_producer_id()
            stamp = int(time.time() * 1000)
            idempotent = bytearray(self.batch([(stamp, b"p", b"idempotent", [])]))
            struct.pack_into(">qhi", idempotent, 43, producer, 0, 0)
            struct.pack_into(">I", idempotent, 17, self.kafka["crc32c"](bytes(idempotent[21:])))
            (_, [(_, error, *_)]), = store.produce([("solo", [(0, bytes(idempotent))])], 0)
            self.assertEqual(error, 0)
        finally:
            store.db.close()
        self.store_raw(self.source.with_name("raw.sqlite3"), "txn", [
            self.raw(0, [(0, 100, None, b"committed")], attributes=16, producer=(9, 0, 0)),
            self.raw(1, [(0, 101, b"\0\0\0\1", b"\0\0\0\0\0\0")], attributes=48, producer=(9, 0, -1)),
            self.raw(2, [(0, 102, None, b"aborted"), (1, 103, None, b"aborted")], attributes=16, producer=(9, 0, 1)),
            self.raw(4, [(0, 104, b"\0\0\0\0", b"\0\0\0\0\0\0")], attributes=48, producer=(9, 0, -1)),
            self.raw(5, [(1, 106, b"kept", b"after a gap")], producer=(10, 0, 0)),
            self.raw(7, [(0, 107, None, b"opaque lz4 payload")], attributes=3),
        ], end=8, start=0, configs={"cleanup.policy": "compact"})
        with contextlib.closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("ATTACH DATABASE ? AS raw", (str(self.source.with_name("raw.sqlite3")),))
            for table in ("kafka_topics", "kafka_partitions", "kafka_batches", "kafka_timestamps", "kafka_topic_configs"):
                connection.execute(f"INSERT INTO {table} SELECT * FROM raw.{table}")

    def import_into(self, port, database, *args, aborted=()):
        """Run import in this process, adding aborted transactions to the source's Fetch answers like Kafka does."""
        import_main = self.kafka["import_main"]
        cluster = import_main.__globals__["KafkaCluster"]
        original = cluster.fetch

        def fetch(instance, address, topic, partition, offset, max_bytes, wait_ms):
            error, watermark, stable, _, records = original(instance, address, topic, partition, offset, max_bytes, wait_ms)
            return error, watermark, stable, list(aborted) if topic == "txn" else [], records

        output, errors = io.StringIO(), io.StringIO()
        with patch.object(cluster, "fetch", fetch), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            try:
                code = import_main(["--db-file", str(database), "--bootstrap-server", f"127.0.0.1:{port}", "--timeout", "3",
                                    *args])
            except SystemExit as exc:
                code = exc.code
        return code, output.getvalue(), errors.getvalue()

    def test_import_round_trip_keeps_offsets_batches_configs_and_groups(self):
        self.import_source()
        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()
        copy = self.directory / "copy" / "imported.sqlite3"
        offset_map = self.directory / "imported.json"
        with self.target(self.source.name) as (port, _):
            code, output, errors = self.import_into(port, copy, "--offset-map", str(offset_map), aborted=[(9, 2)])
        self.assertEqual(code, 0, errors)
        self.assertEqual(errors, "Kafka import: warning: topic 'txn' partition 0: skipped 2 transaction markers and 2 records "
                                 "of aborted transactions; their offsets stay unused\n")
        self.assertEqual(output, f"Imported 8 records in 7 batches across 4 partitions of 3 topics and 3 committed offsets of "
                                 f"2 consumer groups from 127.0.0.1:{port} into {copy}\n")
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)
        self.assertEqual(sorted(path.name for path in copy.parent.iterdir()), ["imported.sqlite3"])
        for sql in ("SELECT * FROM kafka_topics ORDER BY 1",
                    "SELECT topic, partition, next_offset, log_start_offset FROM kafka_partitions ORDER BY 1, 2",
                    "SELECT * FROM kafka_topic_configs ORDER BY 1, 2",
                    "SELECT * FROM kafka_committed_offsets ORDER BY 1, 2, 3",
                    "SELECT * FROM kafka_batches WHERE topic != 'txn' ORDER BY 1, 2, 4",
                    "SELECT * FROM kafka_timestamps WHERE topic != 'txn' ORDER BY 1, 2, 3"):
            self.assertEqual(self.rows(copy, sql), self.rows(self.source, sql), sql)
        # Markers and the aborted transaction leave gaps; every kept batch keeps its source offset and bytes.
        self.assertEqual(self.rows(copy, "SELECT base_offset, last_offset, data FROM kafka_batches WHERE topic = 'txn' ORDER BY 2"),
                         self.rows(self.source, "SELECT base_offset, last_offset, data FROM kafka_batches "
                                                "WHERE topic = 'txn' AND base_offset IN (0, 5, 7) ORDER BY 2"))
        self.assertEqual(self.rows(copy, "SELECT offset, timestamp FROM kafka_timestamps WHERE topic = 'txn' ORDER BY 1"),
                         [(0, 100), (6, 106), (7, 107)])
        self.assertEqual(self.rows(copy, "SELECT value FROM kafka_counters"), [(11,)])
        document = json.loads(offset_map.read_text(encoding="utf-8"))
        txn = next(item for item in document["partitions"] if item["topic"] == "txn")
        self.assertEqual((txn["source_start"], txn["source_end"], txn["skipped_transaction_markers"],
                          txn["skipped_aborted_records"], txn["ranges"]),
                         (0, 8, 2, 2, [{"source_offset": 0, "target_offset": 0, "count": 1},
                                       {"source_offset": 6, "target_offset": 6, "count": 2}]))
        self.assertEqual(document["group_offsets"][0], {"group": "idle", "topic": "solo", "partition": 0, "source_offset": 0,
                                                        "target_offset": 0, "metadata": ""})

        # The imported file serves the same answers as its source.
        def answers(port):
            cluster = self.kafka["KafkaCluster"]([("127.0.0.1", port)], 5, None)
            try:
                result = {}
                for topic, count in (("events", 2), ("solo", 1), ("txn", 1)):
                    leaders = cluster.leaders(topic, count)
                    starts = cluster.list_offsets(topic, count, leaders, -2)
                    result[topic] = starts, cluster.list_offsets(topic, count, leaders, -1), [
                        cluster.fetch(leaders[index], topic, index, starts[index], 1 << 20, 0)[4] for index in range(count)]
                return result, cluster.describe_configs(["events"]), cluster.offset_fetch("readers")
            finally:
                cluster.close()

        with self.target(self.source.name) as (port, _):
            source_answers = answers(port)
        with self.target(str(copy)) as (port, _):
            copy_answers = answers(port)
        source_answers[0]["txn"] = None
        copy_answers[0]["txn"] = None
        self.assertEqual(copy_answers, source_answers)
        self.assertEqual(copy_answers[2], {("events", 0): (3, "checkpoint"), ("events", 1): (1, None)})

    def test_import_refuses_existing_files_and_leaves_nothing_behind(self):
        self.import_source()
        existing = self.directory / "existing.sqlite3"
        existing.write_bytes(b"keep")
        fresh = self.directory / "fresh.sqlite3"
        with self.target(self.source.name) as (port, _):
            self.assertEqual(self.import_into(port, existing)[0::2],
                             (1, f"Kafka import failed: Destination already exists: {existing}\n"))
            self.assertEqual(self.import_into(port, fresh, "--topic", "nope")[0::2],
                             (1, "Kafka import failed: Topic not found in the source cluster: nope\n"))
            self.assertEqual(self.import_into(port, fresh, "--group", "readers", "--topic", "solo")[:2], (0, (
                f"Imported 1 records in 1 batches across 1 partitions of 1 topics and 0 committed offsets of 0 consumer "
                f"groups from 127.0.0.1:{port} into {fresh}\n")))
            self.assertEqual(self.rows(fresh, "SELECT name FROM kafka_topics"), [("solo",)])
            with contextlib.closing(socket.socket()) as closed:
                closed.bind(("127.0.0.1", 0))
                unused = closed.getsockname()[1]
            code, _, errors = self.import_into(unused, self.directory / "unreachable.sqlite3")
            self.assertEqual(code, 1)
            self.assertTrue(errors.startswith(f"Kafka import failed: 127.0.0.1:{unused}: "), errors)
        self.assertEqual(existing.read_bytes(), b"keep")
        self.assertEqual(sorted(path.name for path in self.directory.iterdir() if path.name.startswith(".")), [])
        result = subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "import", "--db-file", str(fresh),
                                 "--bootstrap-server", "127.0.0.1:9"], capture_output=True, text=True, timeout=30)
        self.assertEqual((result.returncode, result.stdout), (1, ""))
        self.assertEqual(result.stderr, f"Kafka import failed: Destination already exists: {fresh}\n")


if __name__ == "__main__":
    unittest.main()
