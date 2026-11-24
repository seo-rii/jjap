"""Verify idempotent producers, topic configs, and admin APIs of the Kafka test broker through its TCP protocol."""

import contextlib
import gzip
import pathlib
import socket
import sqlite3
import struct
import tempfile
import time
import unittest

from kafka_server import Reader, compact_string, crc32c, wire_array, wire_bytes, wire_string, wire_uint
from test_servers import HOST, running_server


def varint(value):
    return wire_uint((value << 1) ^ (value >> 63))


def blob(value):
    return varint(-1) if value is None else varint(len(value)) + value


def batch(records, *, producer=(-1, -1, -1), compressed=False):
    """Encode (timestamp, key, value) tuples as one magic 2 batch with the given producer id, epoch, and sequence."""
    base = records[0][0]
    payload = b""
    for index, (stamp, key, value) in enumerate(records):
        body = b"\0" + varint(stamp - base) + varint(index) + blob(key) + blob(value) + varint(0)
        payload += varint(len(body)) + body
    if compressed:
        payload = gzip.compress(payload, mtime=0)
    body = struct.pack(">hiqqqhii", int(compressed), len(records) - 1, base, max(item[0] for item in records),
                       *producer, len(records)) + payload
    return struct.pack(">qiibI", 0, len(body) + 9, -1, 2, crc32c(body)) + body


def now_ms():
    return int(time.time() * 1000)


def subscription(*topics):
    return struct.pack(">h", 0) + wire_array(wire_string(topic) for topic in topics) + b"\xff" * 4


class Client:
    def __init__(self, connection, stream):
        self.connection = connection
        self.stream = stream
        self.correlation = 0

    def call(self, api, version, payload=b"", *, flexible=False):
        self.correlation += 1
        frame = struct.pack(">hhi", api, version, self.correlation) + wire_string("compat-test")
        frame += (b"\0" if flexible else b"") + payload
        self.connection.sendall(struct.pack(">i", len(frame)) + frame)
        reply = Reader(self.stream.read(struct.unpack(">i", self.stream.read(4))[0]))
        assert reply.i32() == self.correlation
        if flexible:
            reply.tagged_fields()
        return reply

    def init_producer_id(self, transactional_id=None):
        reply = self.call(22, 1, wire_string(transactional_id) + struct.pack(">i", 60000))
        result = reply.i32(), reply.i16(), reply.i64(), reply.i16()
        reply.finish()
        return result[1:]

    def produce(self, topic, partition, data, *, acks=1, transactional_id=None):
        """Produce v8; return (error, base offset, log append time, log start offset, error message)."""
        reply = self.call(0, 8, wire_string(transactional_id) + struct.pack(">hi", acks, 5000) + wire_array([
            wire_string(topic) + wire_array([struct.pack(">i", partition) + wire_bytes(data)])]))
        assert (reply.i32(), reply.string(), reply.i32(), reply.i32()) == (1, topic, 1, partition)
        error, base, append_time, log_start = reply.i16(), reply.i64(), reply.i64(), reply.i64()
        assert reply.array(lambda: (reply.i32(), reply.string(nullable=True))) == []
        message = reply.string(nullable=True)
        reply.i32()
        reply.finish()
        return error, base, append_time, log_start, message

    def create_topic(self, name, count=1, configs=(), *, validate=False, version=4):
        body = (wire_array([wire_string(name) + struct.pack(">ihi", count, 1 if count != -1 else -1, 0) + wire_array(
                    wire_string(key) + wire_string(value) for key, value in dict(configs).items())])
                + struct.pack(">i?", 5000, validate))
        reply = self.call(19, version, body)
        reply.i32()
        (result_name, error, message), = reply.array(lambda: (reply.string(), reply.i16(), reply.string(nullable=True)))
        reply.finish()
        assert result_name == name
        return error, message

    def offsets(self, topic, partition, timestamp):
        reply = self.call(2, 2, struct.pack(">ib", -1, 0) + wire_array([
            wire_string(topic) + wire_array([struct.pack(">iq", partition, timestamp)])]))
        reply.i32()
        (_, [(_, error, stamp, offset)]), = reply.array(lambda: (reply.string(), reply.array(
            lambda: (reply.i32(), reply.i16(), reply.i64(), reply.i64()))))
        reply.finish()
        return error, stamp, offset

    def fetch(self, topic, partition, offset):
        """Fetch v5; return (error, high watermark, log start offset, records)."""
        reply = self.call(1, 5, struct.pack(">iiiib", -1, 0, 0, 1 << 20, 0) + wire_array([
            wire_string(topic) + wire_array([struct.pack(">iqqi", partition, offset, -1, 1 << 20)])]))
        reply.i32()
        assert (reply.i32(), reply.string(), reply.i32(), reply.i32()) == (1, topic, 1, partition)
        error, watermark, _, log_start = reply.i16(), reply.i64(), reply.i64(), reply.i64()
        assert reply.i32() == -1
        records = reply.bytes()
        reply.finish()
        return error, watermark, log_start, records

    def describe_configs(self, resources, *, version=3, synonyms=True):
        body = wire_array(struct.pack(">b", kind) + wire_string(name) + (
            struct.pack(">i", -1) if keys is None else wire_array(wire_string(key) for key in keys))
            for kind, name, keys in resources)
        reply = self.call(32, version, body + (struct.pack(">?", synonyms) if version >= 1 else b"")
                          + (b"\0" if version >= 3 else b""))
        reply.i32()

        def entry():
            name, value, read_only = reply.string(), reply.string(nullable=True), reply.boolean()
            source = reply.i8()
            assert reply.boolean() is False
            others = reply.array(lambda: (reply.string(), reply.string(nullable=True), reply.i8())) if version >= 1 else None
            kind = (reply.i8(), reply.string(nullable=True))[0] if version >= 3 else None
            return name, (value, read_only, source, others, kind)

        result = reply.array(lambda: (reply.i16(), reply.string(nullable=True), reply.i8(), reply.string(),
                                      dict(reply.array(entry))))
        reply.finish()
        return result

    def alter(self, topic, changes, *, incremental=False, validate=False):
        body = wire_array([struct.pack(">b", 2) + wire_string(topic) + wire_array(
            wire_string(name) + (struct.pack(">b", operation) if incremental else b"") + wire_string(value)
            for name, operation, value in changes)]) + struct.pack(">?", validate)
        reply = self.call(44 if incremental else 33, 0, body)
        reply.i32()
        (error, message, kind, name), = reply.array(lambda: (reply.i16(), reply.string(nullable=True), reply.i8(), reply.string()))
        reply.finish()
        assert (kind, name) == (2, topic)
        return error, message

    def commit(self, group, topic, partition, offset):
        reply = self.call(8, 2, wire_string(group) + struct.pack(">i", -1) + wire_string("") + struct.pack(">q", -1)
                          + wire_array([wire_string(topic) + wire_array([struct.pack(">iq", partition, offset)
                                                                         + wire_string("note")])]))
        (_, [(_, error)]), = reply.array(lambda: (reply.string(), reply.array(lambda: (reply.i32(), reply.i16()))))
        reply.finish()
        return error

    def committed(self, group):
        reply = self.call(9, 2, wire_string(group) + struct.pack(">i", -1))
        topics = reply.array(lambda: (reply.string(), reply.array(
            lambda: (reply.i32(), reply.i64(), reply.string(nullable=True), reply.i16()))))
        reply.i16()
        reply.finish()
        return {(topic, partition): offset for topic, items in topics for partition, offset, _, _ in items}

    def simple(self, api, version, body, parse):
        reply = self.call(api, version, body)
        result = parse(reply)
        reply.finish()
        return result


@contextlib.contextmanager
def client(port):
    with socket.create_connection((HOST, port), timeout=5) as connection:
        with connection.makefile("rb") as stream:
            yield Client(connection, stream)


class KafkaCompatTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-kafka-compat-")
        self.addCleanup(temporary.cleanup)
        self.directory = pathlib.Path(temporary.name)
        self.database = self.directory / "kafka.sqlite3"

    def server(self, *args):
        return running_server("kafka_server.py", self.directory, "--db-file", str(self.database), *args)

    def test_idempotent_producer_sequences_survive_restart(self):
        stamp = now_ms()
        with self.server() as port, client(port) as broker:
            self.assertEqual(broker.create_topic("events"), (0, None))
            error, producer, epoch = broker.init_producer_id()
            self.assertEqual((error, epoch), (0, 0))
            first = batch([(stamp, b"a", b"1"), (stamp, b"b", b"2")], producer=(producer, 0, 0))
            self.assertEqual(broker.produce("events", 0, first, acks=-1)[:4], (0, 0, -1, 0))
            # A retried batch is acknowledged with its original offset and is not appended again.
            self.assertEqual(broker.produce("events", 0, first)[:2], (0, 0))
            self.assertEqual(broker.offsets("events", 0, -1), (0, -1, 2))
            gap = broker.produce("events", 0, batch([(stamp, b"c", b"3")], producer=(producer, 0, 5)))
            self.assertEqual(gap[:2], (45, -1))
            self.assertIn("Out of order sequence number for producer", gap[4])
            for sequence in range(2, 8):
                self.assertEqual(broker.produce("events", 0, batch([(stamp, None, b"%d" % sequence)],
                                                                    producer=(producer, 0, sequence)))[:2], (0, sequence))
            # Only the last five batches are kept, so the first one is no longer recognized as a retry.
            self.assertEqual(broker.produce("events", 0, first)[0], 45)
            retry = batch([(stamp, None, b"3")], producer=(producer, 0, 3))
            self.assertEqual(broker.produce("events", 0, retry)[:2], (0, 3))
            self.assertEqual(broker.produce("events", 0, batch([(stamp, None, b"x")], producer=(producer, 1, 4)))[0], 45)
            self.assertEqual(broker.produce("events", 0, batch([(stamp, None, b"x")], producer=(producer, 1, 0)))[:2], (0, 8))
            fenced = broker.produce("events", 0, batch([(stamp, None, b"y")], producer=(producer, 0, 8)))
            self.assertEqual(fenced[0], 47)
            self.assertIn("smaller than the last seen epoch 1", fenced[4])
            stored = broker.fetch("events", 0, 8)[3]
            self.assertEqual(struct.unpack_from(">qhi", stored, 43), (producer, 1, 0))  # producer fields are kept

        with self.server() as port, client(port) as broker:
            self.assertEqual(broker.produce("events", 0, retry)[0], 47)
            self.assertEqual(broker.produce("events", 0, batch([(stamp, None, b"x")], producer=(producer, 1, 0)))[:2], (0, 8))
            self.assertEqual(broker.produce("events", 0, batch([(stamp, None, b"z")], producer=(producer, 1, 1)))[:2], (0, 9))
            self.assertEqual(broker.init_producer_id(), (0, producer + 1, 0))
            self.assertEqual(broker.init_producer_id("tx"), (53, -1, -1))
            reply = broker.call(10, 1, wire_string("tx") + b"\x01")
            self.assertEqual((reply.i32(), reply.i16(), reply.string(nullable=True)),
                             (0, 42, "Transactions are not supported by the jjap Kafka test broker"))
            refused = broker.produce("events", 0, batch([(stamp, None, b"t")]), transactional_id="tx")
            self.assertEqual(refused[0], 53)
            marked = bytearray(batch([(stamp, None, b"t")], producer=(producer, 1, 2)))
            struct.pack_into(">h", marked, 21, 16)
            struct.pack_into(">I", marked, 17, crc32c(bytes(marked[21:])))
            self.assertEqual(broker.produce("events", 0, bytes(marked))[0], 87)

    def test_topic_configs_are_validated_described_and_altered(self):
        with self.server() as port, client(port) as broker:
            self.assertEqual(broker.create_topic("bad", configs={"foo.bar": "1"}), (40, "Unknown topic config name: foo.bar"))
            for key, value, message in [
                ("retention.ms", "abc", "Invalid value abc for configuration retention.ms: Not a number of type LONG"),
                ("retention.ms", "-5", "Invalid value -5 for configuration retention.ms: Value must be at least -1"),
                ("cleanup.policy", "compacted", "Invalid value compacted for configuration cleanup.policy: "
                                                "String must be one of: compact, delete"),
                ("segment.bytes", "100", "Invalid value 100 for configuration segment.bytes: Value must be at least 1048576"),
                ("message.timestamp.type", "Foo", "Invalid value Foo for configuration message.timestamp.type: "
                                                  "String must be one of: CreateTime, LogAppendTime"),
                ("min.cleanable.dirty.ratio", "2", "Invalid value 2.0 for configuration min.cleanable.dirty.ratio: "
                                                   "Value must be no more than 1"),
                ("compression.type", "brotli", "Invalid value brotli for configuration compression.type: "
                                               "String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer"),
                ("compression.gzip.level", "12", "Invalid value 12 for configuration compression.gzip.level: "
                                                 "Value must be between 1 and 9 or equal to -1"),
                ("unclean.leader.election.enable", "maybe", "Invalid value maybe for configuration "
                                                            "unclean.leader.election.enable: Expected value to be either true or false"),
            ]:
                self.assertEqual(broker.create_topic("bad", configs={key: value}), (40, message))
            self.assertEqual(broker.create_topic("defaults", -1), (0, None))
            self.assertEqual(broker.create_topic("old-defaults", -1, version=3)[0], 37)
            self.assertEqual(broker.create_topic("events", 2, {"retention.ms": "60000", "cleanup.policy": "compact,delete"}),
                             (0, None))
            (error, _, kind, name, configs), = broker.describe_configs([(2, "events", None)])
            self.assertEqual((error, kind, name, len(configs)), (0, 2, "events", 37))
            self.assertEqual(configs["retention.ms"], ("60000", False, 1, [("retention.ms", "60000", 1),
                                                                           ("log.retention.ms", "-1", 5)], 5))
            self.assertEqual(configs["max.message.bytes"], ("1048588", False, 5, [("message.max.bytes", "1048588", 5)], 3))
            self.assertEqual(configs["cleanup.policy"][:3], ("compact,delete", False, 1))
            (_, _, _, _, legacy), = broker.describe_configs([(2, "events", ["retention.ms", "segment.ms"])], version=0)
            self.assertEqual(legacy, {"retention.ms": ("60000", False, 0, None, None),
                                      "segment.ms": ("604800000", False, 1, None, None)})
            missing, = broker.describe_configs([(2, "nope", None)])
            self.assertEqual(missing[:2], (3, "Topic nope does not exist."))
            (error, _, _, _, brokers), = broker.describe_configs([(4, "0", ["node.id", "log.retention.ms"])])
            self.assertEqual((error, brokers["node.id"][:3], brokers["log.retention.ms"][:3]),
                             (0, ("0", True, 4), ("-1", True, 5)))

            self.assertEqual(broker.alter("events", [("retention.ms", 1, None), ("cleanup.policy", 2, "delete,compact"),
                                                     ("max.message.bytes", 0, "5000")], incremental=True), (0, None))
            self.assertEqual(broker.alter("events", [("cleanup.policy", 3, "compact")], incremental=True), (0, None))
            self.assertEqual(broker.alter("events", [("retention.ms", 2, "5")], incremental=True),
                             (40, "Can't APPEND to key retention.ms because its type is not LIST."))
            self.assertEqual(broker.alter("events", [("segment.ms", 0, "0")], incremental=True),
                             (40, "Invalid value 0 for configuration segment.ms: Value must be at least 1"))
            self.assertEqual(broker.alter("events", [("retention.bytes", 0, "100")], incremental=True, validate=True), (0, None))
            self.assertEqual(broker.alter("nope", [("retention.ms", 0, "1")], incremental=True)[0], 3)
            self.assertEqual(broker.alter("events", [("message.timestamp.type", 0, "LogAppendTime")]), (0, None))

        with self.server() as port, client(port) as broker:
            (_, _, _, _, configs), = broker.describe_configs([(2, "events", None)], version=1)
            overridden = {name: value for name, (value, _, source, _, _) in configs.items() if source == 1}
            # AlterConfigs replaces the whole set of overrides.
            self.assertEqual(overridden, {"message.timestamp.type": "LogAppendTime"})

    def test_enforced_topic_configs(self):
        stamp = now_ms()
        with self.server() as port, client(port) as broker:
            broker.create_topic("small", configs={"max.message.bytes": "200"})
            broker.create_topic("stamped", configs={"message.timestamp.type": "LogAppendTime"})
            broker.create_topic("keyed", configs={"cleanup.policy": "compact"})
            broker.create_topic("replicated", configs={"min.insync.replicas": "2"})
            broker.create_topic("events")
            large = broker.produce("small", 0, batch([(stamp, b"k", b"x" * 300)]))
            self.assertEqual(large[0], 10)
            self.assertIn("max.message.bytes (200)", large[4])
            self.assertEqual(broker.produce("small", 0, batch([(stamp, b"k", b"x" * 20)]))[0], 0)

            before = now_ms()
            error, base, append_time, _, _ = broker.produce("stamped", 0, batch([(1000, b"k", b"v"), (1001, b"k", b"w")]))
            self.assertEqual((error, base), (0, 0))
            self.assertTrue(before <= append_time <= now_ms())
            data = broker.fetch("stamped", 0, 0)[3]
            attributes, _, _, max_timestamp = struct.unpack_from(">hiqq", data, 21)
            self.assertEqual((attributes, max_timestamp), (8, append_time))
            self.assertEqual(crc32c(data[21:]), struct.unpack_from(">I", data, 17)[0])
            self.assertEqual(broker.offsets("stamped", 0, append_time), (0, append_time, 0))
            # ListOffsets v7 (flexible) answers the max-timestamp spec with the first record holding that timestamp.
            reply = broker.call(2, 7, struct.pack(">ib", -1, 0) + wire_uint(2) + compact_string("stamped") + wire_uint(2)
                                + struct.pack(">iiq", 0, -1, -3) + b"\0\0\0", flexible=True)
            self.assertEqual((reply.i32(), reply.unsigned_varint(), reply.string(compact=True), reply.unsigned_varint()),
                             (0, 2, "stamped", 2))
            self.assertEqual((reply.i32(), reply.i16(), reply.i64(), reply.i64(), reply.i32()), (0, 0, append_time, 0, 0))
            reply.tagged_fields()
            reply.tagged_fields()
            reply.tagged_fields()
            reply.finish()

            keyless = broker.produce("keyed", 0, batch([(stamp, b"k", b"v"), (stamp, None, b"v")]))
            self.assertEqual(keyless[0], 87)
            self.assertEqual(keyless[4], "Compacted topic cannot accept message without key in topic partition keyed-0.")
            self.assertEqual(broker.produce("replicated", 0, batch([(stamp, b"k", b"v")]), acks=-1)[0], 19)
            self.assertEqual(broker.produce("replicated", 0, batch([(stamp, b"k", b"v")]), acks=1)[0], 0)
            future = broker.produce("events", 0, batch([(stamp + 7_200_000, None, b"v")]))
            self.assertEqual(future[0], 32)
            self.assertIn("is out of range", future[4])

    def test_retention_and_delete_records_move_the_log_start(self):
        stamp = now_ms()
        with self.server() as port, client(port) as broker:
            broker.create_topic("sized", configs={"retention.bytes": "250"})
            for index in range(4):
                self.assertEqual(broker.produce("sized", 0, batch([(stamp, b"k", b"%d" % index * 50)]))[:2], (0, index))
            # Like Kafka with log segments, a 120-byte head batch goes once the rest still holds 250 bytes.
            self.assertEqual(broker.offsets("sized", 0, -2), (0, -1, 1))
            self.assertEqual(broker.fetch("sized", 0, 0)[:3], (1, 4, -1))
            self.assertEqual(broker.fetch("sized", 0, 1)[:3], (0, 4, 1))
            self.assertEqual(broker.produce("sized", 0, batch([(stamp, b"k", b"%d" % 4 * 50)]))[3], 2)

            broker.create_topic("aged")
            broker.produce("aged", 0, batch([(1000, b"k", b"old")]))
            broker.produce("aged", 0, batch([(stamp, b"k", b"new")]))
            self.assertEqual(broker.alter("aged", [("retention.ms", 0, "3600000")], incremental=True), (0, None))
            self.assertEqual(broker.offsets("aged", 0, -2), (0, -1, 1))

            broker.create_topic("trimmed")
            for index in range(3):
                broker.produce("trimmed", 0, batch([(stamp, None, b"a"), (stamp, None, b"b")]))
            def delete(offset, topic="trimmed"):
                reply = broker.call(21, 1, wire_array([wire_string(topic) + wire_array([struct.pack(">iq", 0, offset)])])
                                    + struct.pack(">i", 5000))
                reply.i32()
                (_, [(_, low, error)]), = reply.array(lambda: (reply.string(), reply.array(
                    lambda: (reply.i32(), reply.i64(), reply.i16()))))
                reply.finish()
                return low, error

            self.assertEqual(delete(3), (3, 0))
            self.assertEqual(delete(1), (3, 0))
            self.assertEqual(delete(7), (-1, 1))
            self.assertEqual(broker.offsets("trimmed", 0, -2), (0, -1, 3))
            # The batch that holds the new log start stays, as a log segment would.
            self.assertEqual(struct.unpack_from(">q", broker.fetch("trimmed", 0, 3)[3])[0], 2)
            self.assertEqual(delete(-1), (6, 0))
            self.assertEqual(broker.fetch("trimmed", 0, 6)[:4], (0, 6, 6, b""))
            broker.create_topic("compacted", configs={"cleanup.policy": "compact"})
            self.assertEqual(delete(0, "compacted"), (-1, 44))

        with self.server() as port, client(port) as broker:
            self.assertEqual(broker.offsets("trimmed", 0, -2), (0, -1, 6))
            self.assertEqual(broker.offsets("sized", 0, -2), (0, -1, 2))

    def test_time_retention_runs_in_the_background(self):
        with self.server() as port, client(port) as broker:
            broker.create_topic("aged", configs={"retention.ms": "1000"})
            broker.produce("aged", 0, batch([(now_ms(), b"k", b"v")]))
            self.assertEqual(broker.offsets("aged", 0, -2), (0, -1, 0))
            deadline = time.monotonic() + 8
            while broker.offsets("aged", 0, -2)[2] != 1:
                self.assertLess(time.monotonic(), deadline, "retention.ms did not delete the expired batch")
                time.sleep(0.2)
            self.assertEqual(broker.offsets("aged", 0, -1), (0, -1, 1))

    def test_topic_and_partition_admin(self):
        with self.server() as port, client(port) as broker:
            broker.create_topic("events", 2)
            broker.produce("events", 1, batch([(now_ms(), b"k", b"v")]))
            self.assertEqual(broker.commit("readers", "events", 1, 1), 0)
            grow = lambda topic, count, assignments=None, validate=False: broker.simple(37, 1, wire_array([
                wire_string(topic) + struct.pack(">i", count) + (struct.pack(">i", -1) if assignments is None else wire_array(
                    wire_array(struct.pack(">i", node) for node in nodes) for nodes in assignments))])
                + struct.pack(">i?", 5000, validate),
                lambda reply: (reply.i32(), reply.array(lambda: (reply.string(), reply.i16(), reply.string(nullable=True)))))[1][0][1:]
            self.assertEqual(grow("events", 3, validate=True), (0, None))
            self.assertEqual(grow("events", 4, [[0], [0]]), (0, None))
            self.assertEqual(grow("events", 4), (37, "Topic already has 4 partition(s)."))
            self.assertEqual(grow("events", 3), (37, "The topic events currently has 4 partition(s); 3 would not be an increase."))
            self.assertEqual(grow("events", 5, [[1]])[0], 39)
            self.assertEqual(grow("nope", 2), (3, "The topic 'nope' does not exist."))
            self.assertEqual(broker.offsets("events", 3, -1), (0, -1, 0))

            delete = lambda *names: broker.simple(20, 3, wire_array(wire_string(name) for name in names) + struct.pack(">i", 5000),
                                                  lambda reply: (reply.i32(), reply.array(lambda: (reply.string(), reply.i16())))[1])
            self.assertEqual(delete("events", "nope"), [("events", 0), ("nope", 3)])
            self.assertEqual(broker.offsets("events", 0, -1)[0], 3)
            self.assertEqual(broker.committed("readers"), {})
            self.assertEqual(broker.create_topic("events"), (0, None))
            self.assertEqual(broker.offsets("events", 0, -1), (0, -1, 0))

    def test_group_admin(self):
        with self.server() as port, client(port) as member, client(port) as admin:
            admin.create_topic("events")
            admin.create_topic("other")
            joined = member.call(11, 1, wire_string("active") + struct.pack(">ii", 10000, 10000) + wire_string("")
                                 + wire_string("consumer") + wire_array([wire_string("range") + wire_bytes(subscription("events"))]))
            error, generation, _, _, member_id = joined.i16(), joined.i32(), joined.string(), joined.string(), joined.string()
            joined.read(joined.remaining)
            self.assertEqual(error, 0)
            synced = member.call(14, 1, wire_string("active") + struct.pack(">i", generation) + wire_string(member_id)
                                 + wire_array([wire_string(member_id) + wire_bytes(b"assignment")]))
            self.assertEqual((synced.i32(), synced.i16(), synced.bytes()), (0, 0, b"assignment"))
            for topic in ("events", "other"):
                self.assertEqual(admin.commit("active", topic, 0, 0), 25)  # standalone commits need an empty group
            self.assertEqual(admin.commit("idle", "events", 0, 0), 0)
            self.assertEqual(admin.commit("idle", "other", 0, 0), 0)

            listed = admin.simple(16, 2, b"", lambda reply: (reply.i32(), reply.i16(), reply.array(
                lambda: (reply.string(), reply.string()))))
            self.assertEqual(listed, (0, 0, [("active", "consumer"), ("idle", "")]))
            flexible = admin.call(16, 5, wire_uint(2) + compact_string("stable") + wire_uint(2) + compact_string("Classic")
                                  + b"\0", flexible=True)
            self.assertEqual((flexible.i32(), flexible.i16(), flexible.unsigned_varint()), (0, 0, 2))
            self.assertEqual([flexible.string(compact=True) for _ in range(4)], ["active", "consumer", "Stable", "classic"])
            flexible.tagged_fields()
            flexible.tagged_fields()
            flexible.finish()

            def describe(*names):
                reply = admin.call(15, 4, wire_array(wire_string(name) for name in names) + b"\x01")
                reply.i32()
                groups = reply.array(lambda: (reply.i16(), reply.string(), reply.string(), reply.string(), reply.string(),
                                              reply.array(lambda: (reply.string(), reply.string(nullable=True), reply.string(),
                                                                   reply.string(), reply.bytes(), reply.bytes())), reply.i32()))
                reply.finish()
                return groups

            active, idle, dead = describe("active", "idle", "unknown")
            self.assertEqual(active[:5], (0, "active", "Stable", "consumer", "range"))
            self.assertEqual(active[5], [(member_id, None, "compat-test", "/127.0.0.1", subscription("events"), b"assignment")])
            self.assertEqual(idle[:6], (0, "idle", "Empty", "", "", []))
            self.assertEqual(dead[:6], (0, "unknown", "Dead", "", "", []))

            def delete_offsets(group, topic):
                reply = admin.call(47, 0, wire_string(group) + wire_array([wire_string(topic) + wire_array([struct.pack(">i", 0)])]))
                result = reply.i16(), reply.i32(), reply.array(lambda: (reply.string(), reply.array(lambda: (reply.i32(), reply.i16()))))
                reply.finish()
                return result[0], result[2]

            self.assertEqual(delete_offsets("active", "events"), (0, [("events", [(0, 86)])]))
            self.assertEqual(delete_offsets("idle", "events"), (0, [("events", [(0, 0)])]))
            self.assertEqual(delete_offsets("unknown", "events"), (69, []))
            self.assertEqual(admin.committed("idle"), {("other", 0): 0})

            delete = lambda *names: admin.simple(42, 1, wire_array(wire_string(name) for name in names),
                                                 lambda reply: (reply.i32(), reply.array(lambda: (reply.string(), reply.i16())))[1])
            self.assertEqual(delete("active", "idle", "unknown", ""), [("active", 68), ("idle", 0), ("unknown", 69), ("", 24)])
            self.assertEqual(admin.committed("idle"), {})
            self.assertEqual(describe("idle")[0][2], "Dead")

    def test_older_database_files_still_open(self):
        with contextlib.closing(sqlite3.connect(self.database)) as connection, connection:
            connection.executescript("""
                CREATE TABLE kafka_cluster (id TEXT NOT NULL);
                INSERT INTO kafka_cluster VALUES ('legacy');
                CREATE TABLE kafka_topics (name TEXT PRIMARY KEY);
                CREATE TABLE kafka_partitions (
                    topic TEXT REFERENCES kafka_topics(name) ON DELETE CASCADE,
                    partition INTEGER, next_offset INTEGER NOT NULL, PRIMARY KEY (topic, partition)
                );
                CREATE TABLE kafka_batches (
                    topic TEXT NOT NULL, partition INTEGER NOT NULL, base_offset INTEGER NOT NULL,
                    last_offset INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (topic, partition, last_offset),
                    FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
                );
                CREATE TABLE kafka_timestamps (
                    topic TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL, timestamp INTEGER NOT NULL,
                    PRIMARY KEY (topic, partition, offset),
                    FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
                );
                CREATE TABLE kafka_committed_offsets (
                    group_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL,
                    metadata TEXT, PRIMARY KEY (group_id, topic, partition),
                    FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
                );
                INSERT INTO kafka_topics VALUES ('events');
                INSERT INTO kafka_partitions VALUES ('events', 0, 0);
            """)
        with self.server() as port, client(port) as broker:
            self.assertEqual(broker.produce("events", 0, batch([(now_ms(), None, b"v")]))[:4], (0, 0, -1, 0))
            self.assertEqual(broker.fetch("events", 0, 0)[:3], (0, 1, 0))


if __name__ == "__main__":
    unittest.main()
