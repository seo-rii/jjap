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

    def store(self, path, topics, batches=(), commits=()):
        store = self.kafka["Store"](str(path))
        try:
            created = store.create_topics([(name, count, 1, [], []) for name, count in topics], False)
            self.assertEqual([error for _, error in created], [0] * len(topics))
            for topic, partition, data in batches:
                (_, [(_, error, _)]), = store.produce([(topic, [(partition, data)])], 0)
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
        self.assertEqual(result.returncode, 1)
        self.assertIn("group 'readers' topic 'events' partition 0: OffsetCommit failed with UNKNOWN_MEMBER_ID; "
                      "the group is active on the target, stop its consumers first", result.stderr)
        self.assertIn("0 of 4 partitions and 1 of 2 consumer groups failed", result.stderr)
        self.assertEqual(self.rows(target, "SELECT group_id FROM kafka_committed_offsets"), [("idle",)])

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


if __name__ == "__main__":
    unittest.main()
