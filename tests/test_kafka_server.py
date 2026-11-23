"""Verify the standalone Kafka broker through its TCP protocol."""

import contextlib
import gzip
import pathlib
import socket
import struct
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from kafka_server import Reader, crc32c, wire_array, wire_bytes, wire_string, wire_uint
from test_servers import HOST, running_server


class Peer:
    def __init__(self, connection, stream):
        self.connection = connection
        self.stream = stream
        self.correlation = 0

    def send(self, api, version, payload=b"", header_tags=b"\0"):
        self.correlation += 1
        frame = struct.pack(">hhi", api, version, self.correlation) + wire_string("jjap-test")
        if api == 18 and version >= 3:
            frame += header_tags
        frame += payload
        self.connection.sendall(struct.pack(">i", len(frame)) + frame)

    def receive(self):
        size = struct.unpack(">i", self.stream.read(4))[0]
        reader = Reader(self.stream.read(size))
        if reader.i32() != self.correlation:
            raise AssertionError("Kafka response correlation does not match the request")
        return reader

    def request(self, api, version, payload=b"", **options):
        self.send(api, version, payload, **options)
        return self.receive()


@contextlib.contextmanager
def peer(port):
    with socket.create_connection((HOST, port), timeout=4) as connection:
        with connection.makefile("rb") as stream:
            yield Peer(connection, stream)


def signed_varint(value):
    return wire_uint((value << 1) ^ (value >> 63))


def record_batch(values, *, compressed=False, timestamp=1000, base_offset=0):
    records = bytearray()
    for index, value in enumerate(values):
        record = (b"\0" + signed_varint(index) * 2 + signed_varint(2) + b"\x00\xff"
                  + signed_varint(-1 if value is None else len(value)) + (value or b"")
                  + signed_varint(1) + signed_varint(3) + b"tag" + signed_varint(1) + b"\xff")
        records.extend(signed_varint(len(record)) + record)
    body = struct.pack(">hiqqqhii", int(compressed), len(values) - 1, timestamp, timestamp + len(values) - 1,
                       -1, -1, -1, len(values))
    body += gzip.compress(records, mtime=0) if compressed else records
    return struct.pack(">qiibI", base_offset, len(body) + 9, -1, 2, crc32c(body)) + body


def topic_partitions(topic, partitions):
    return wire_array([wire_string(topic) + wire_array(partitions)])


def produce_request(topic, partition, data, acks=1):
    return wire_string(None) + struct.pack(">hi", acks, 10000) + topic_partitions(topic, [struct.pack(">i", partition) + wire_bytes(data)])


def fetch_request(topic, partition, offset, *, min_bytes=0, wait_ms=0, max_partition=1_000_000):
    return struct.pack(">iiiib", -1, wait_ms, min_bytes, 1_000_000, 0) + topic_partitions(
        topic, [struct.pack(">iqqi", partition, offset, -1, max_partition)],
    )


def join_request(group, member, *, protocol="range", session=3000, rebalance=5000):
    return (wire_string(group) + struct.pack(">ii", session, rebalance) + wire_string(member) + wire_string("consumer")
            + wire_array([wire_string(protocol) + wire_bytes(b"topic subscription")]))


def join_response(reader):
    error, generation = reader.i16(), reader.i32()
    protocol, leader, member = reader.string(), reader.string(), reader.string()
    members = reader.array(lambda: (reader.string(), reader.bytes()))
    reader.finish()
    return error, generation, protocol, leader, member, members


class KafkaServerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-kafka-")
        self.addCleanup(temporary.cleanup)
        self.directory = pathlib.Path(temporary.name)

    def server(self, *args):
        return running_server("kafka_server.py", self.directory, "--db-file", str(self.directory / "kafka.sqlite3"), *args)

    def create_topic(self, client, topic, count=1, replicas=1, validate=False, error=0):
        body = (wire_array([wire_string(topic) + struct.pack(">ihii", count, replicas, 0, 0)])
                + struct.pack(">i?", 10000, validate))
        result = client.request(19, 3, body)
        self.assertEqual(result.i32(), 0)
        items = result.array(lambda: (result.string(), result.i16(), result.string(nullable=True)))
        self.assertEqual([(name, code) for name, code, _ in items], [(topic, error)])
        result.finish()

    def metadata(self, client, names=None, auto_create=False):
        topics = struct.pack(">i", -1) if names is None else wire_array(wire_string(name) for name in names)
        response = client.request(3, 5, topics + struct.pack(">?", auto_create))
        self.assertEqual(response.i32(), 0)
        brokers = response.array(lambda: (response.i32(), response.string(), response.i32(), response.string(nullable=True)))
        self.assertEqual([(node, host, rack) for node, host, _, rack in brokers], [(0, HOST, None)])
        cluster_id = response.string()
        self.assertEqual(response.i32(), 0)

        def read_topic():
            error, name, internal = response.i16(), response.string(), response.boolean()
            self.assertFalse(internal)

            def read_partition():
                error, partition, leader = response.i16(), response.i32(), response.i32()
                self.assertEqual((error, leader), (0, 0))
                self.assertEqual(response.array(response.i32), [0])
                self.assertEqual(response.array(response.i32), [0])
                self.assertEqual(response.array(response.i32), [])
                return partition

            return name, error, response.array(read_partition)

        topics = response.array(read_topic)
        response.finish()
        return cluster_id, topics

    def produce(self, client, topic, partition, data, *, offset=0, error=0):
        reply = client.request(0, 5, produce_request(topic, partition, data))
        self.assertEqual((reply.i32(), reply.string(), reply.i32()), (1, topic, 1))
        self.assertEqual((reply.i32(), reply.i16(), reply.i64()), (partition, error, offset if not error else -1))
        self.assertEqual((reply.i64(), reply.i64(), reply.i32()), (-1, 0 if not error else -1, 0))
        reply.finish()

    def fetch_response(self, reply, topic, partition, watermark, *, error=0):
        self.assertEqual((reply.i32(), reply.i32(), reply.string(), reply.i32()), (0, 1, topic, 1))
        self.assertEqual((reply.i32(), reply.i16(), reply.i64()), (partition, error, watermark))
        self.assertEqual((reply.i64(), reply.i64(), reply.i32()), (-1 if error else watermark, -1 if error else 0, -1))
        result = reply.bytes()
        reply.finish()
        return result

    def list_offset(self, client, topic, partition, timestamp):
        result = client.request(2, 2, struct.pack(">ib", -1, 0) + topic_partitions(topic, [struct.pack(">iq", partition, timestamp)]))
        self.assertEqual((result.i32(), result.i32(), result.string(), result.i32()), (0, 1, topic, 1))
        self.assertEqual((result.i32(), result.i16()), (partition, 0))
        timestamp, offset = result.i64(), result.i64()
        result.finish()
        return timestamp, offset

    def commit(self, client, group, topic, generation, member, offset, *, error=0):
        body = (wire_string(group) + struct.pack(">i", generation) + wire_string(member) + struct.pack(">q", -1)
                + topic_partitions(topic, [struct.pack(">iq", 0, offset) + wire_string("checkpoint")]))
        result = client.request(8, 3, body)
        self.assertEqual((result.i32(), result.i32(), result.string(), result.i32()), (0, 1, topic, 1))
        self.assertEqual((result.i32(), result.i16()), (0, error))
        result.finish()

    def offsets(self, client, group):
        result = client.request(9, 2, wire_string(group) + struct.pack(">i", -1))
        offsets = result.array(lambda: (result.string(), result.array(
            lambda: (result.i32(), result.i64(), result.string(nullable=True), result.i16())
        )))
        self.assertEqual(result.i16(), 0)
        result.finish()
        return offsets

    def heartbeat(self, client, group, generation, member):
        response = client.request(12, 1, wire_string(group) + struct.pack(">i", generation) + wire_string(member))
        self.assertEqual(response.i32(), 0)
        error = response.i16()
        response.finish()
        return error

    def sync(self, client, group, generation, member, assignments):
        body = wire_string(group) + struct.pack(">i", generation) + wire_string(member) + wire_array(
            wire_string(identifier) + wire_bytes(assignment) for identifier, assignment in assignments.items()
        )
        response = client.request(14, 1, body)
        self.assertEqual(response.i32(), 0)
        self.assertEqual(response.i16(), 0)
        assignment = response.bytes()
        response.finish()
        return assignment

    def test_version_negotiation_and_topic_metadata(self):
        with self.server("--auto-create-topics") as port, peer(port) as client:
            # An unknown request-header tag is intentionally present in the flexible version.
            hello = client.request(18, 4, b"\x05test\x041.0\x00", header_tags=b"\x01\x08\x01\xff")
            self.assertEqual(hello.i16(), 0)
            advertised = {}
            for _ in range(hello.unsigned_varint() - 1):
                key, first, last = hello.i16(), hello.i16(), hello.i16()
                advertised[key] = (first, last)
                hello.tagged_fields()
            self.assertEqual(hello.i32(), 0)
            hello.tagged_fields()
            hello.finish()
            self.assertEqual(advertised, {0: (3, 5), 1: (4, 5), 2: (1, 2), 3: (0, 5), 8: (0, 3),
                                         9: (0, 2), 10: (0, 1), 11: (0, 1), 12: (0, 1), 13: (0, 1),
                                         14: (0, 1), 18: (0, 4), 19: (0, 3)})
            fallback = client.request(18, 55)
            self.assertEqual(fallback.i16(), 35)
            self.assertEqual(dict(fallback.array(lambda: (fallback.i16(), (fallback.i16(), fallback.i16())))), advertised)
            fallback.finish()

            self.create_topic(client, "events", count=2, validate=True)
            self.assertEqual(self.metadata(client)[1], [])
            self.create_topic(client, "events", count=2)
            self.create_topic(client, "events", count=2, error=36)
            self.create_topic(client, "unsupported-replicas", replicas=2, error=38)
            self.assertEqual(self.metadata(client, ["auto"])[1], [("auto", 3, [])])
            self.assertEqual(self.metadata(client, ["auto"], auto_create=True)[1], [("auto", 0, [0])])
            self.assertEqual(self.metadata(client)[1], [("auto", 0, [0]), ("events", 0, [0, 1])])

    def test_batches_offsets_and_restart(self):
        self.assertEqual(crc32c(b"123456789"), 0xE3069283)
        first = record_batch([b"binary\x00\xff", None])
        second = record_batch([b"gzip", b"gzip"], compressed=True, timestamp=2000)
        damaged = first[:-1] + bytes([first[-1] ^ 1])
        with self.server() as port, peer(port) as client:
            self.create_topic(client, "events", count=2)
            cluster_id, _ = self.metadata(client)
            self.produce(client, "events", 0, first)
            self.produce(client, "events", 0, damaged, error=2)
            self.produce(client, "events", 0, second, offset=2)
            self.produce(client, "events", 1, first)
            self.produce(client, "events", 0, first, offset=4)
            self.assertEqual(self.list_offset(client, "events", 0, -2), (-1, 0))
            self.assertEqual(self.list_offset(client, "events", 0, -1), (-1, 6))
            self.assertEqual(self.list_offset(client, "events", 0, 2001), (2001, 3))
            self.assertEqual(self.list_offset(client, "events", 1, -1), (-1, 2))
            unread = self.fetch_response(client.request(1, 5, fetch_request("events", 0, 7)), "events", 0, 6, error=1)
            self.assertEqual(unread, b"")
            self.commit(client, "reader", "events", -1, "", 4)

        with self.server() as port, peer(port) as client:
            self.assertEqual(self.metadata(client), (cluster_id, [("events", 0, [0, 1])]))
            self.assertEqual(self.offsets(client, "reader"), [("events", [(0, 4, "checkpoint", 0)])])
            response = client.request(1, 5, fetch_request("events", 0, 3, max_partition=1))
            recovered = self.fetch_response(response, "events", 0, 6)
            self.assertEqual(struct.unpack_from(">q", recovered)[0], 2)
            self.assertEqual(recovered[16:], second[16:])
            self.produce(client, "events", 0, first, offset=6)

    def test_long_poll_and_no_ack_produce(self):
        payload = record_batch([b"later"])
        with self.server() as port, peer(port) as consumer, peer(port) as producer:
            self.create_topic(producer, "wake")
            consumer.send(1, 5, fetch_request("wake", 0, 0, min_bytes=1, wait_ms=2000))
            with ThreadPoolExecutor(max_workers=1) as workers:
                receiving = workers.submit(consumer.receive)
                time.sleep(0.1)
                self.assertFalse(receiving.done())
                producer.send(0, 5, produce_request("wake", 0, payload, acks=0))
                self.assertEqual(self.list_offset(producer, "wake", 0, -1), (-1, 1))
                response = receiving.result(timeout=2)
            self.assertEqual(self.fetch_response(response, "wake", 0, 1)[16:], payload[16:])

    def test_classic_rebalance_and_generation_checked_commits(self):
        with self.server() as port, peer(port) as first, peer(port) as second:
            self.create_topic(first, "events", count=2)
            error, generation, protocol, leader, member, members = join_response(first.request(11, 1, join_request("readers", "")))
            self.assertEqual((error, generation, protocol, leader), (0, 1, "range", member))
            self.assertEqual(members, [(member, b"topic subscription")])
            self.assertEqual(self.sync(first, "readers", generation, member, {member: b"both"}), b"both")
            self.assertEqual(self.heartbeat(first, "readers", generation, member), 0)
            self.commit(first, "readers", "events", generation, member, 4)

            second.send(11, 1, join_request("readers", ""))
            deadline = time.monotonic() + 2
            while self.heartbeat(first, "readers", generation, member) != 27:
                if time.monotonic() >= deadline:
                    self.fail("second member did not start a rebalance")
                time.sleep(0.01)
            error, new_generation, _, leader, _, members = join_response(first.request(11, 1, join_request("readers", member)))
            self.assertEqual((error, new_generation, leader), (0, generation + 1, member))
            other_error, other_generation, _, other_leader, other_member, other_payload = join_response(second.receive())
            self.assertEqual((other_error, other_generation, other_leader, other_payload), (0, new_generation, member, []))
            self.assertEqual({identifier for identifier, _ in members}, {member, other_member})
            self.assertEqual(self.sync(first, "readers", new_generation, member, {member: b"p0", other_member: b"p1"}), b"p0")
            self.assertEqual(self.sync(second, "readers", new_generation, other_member, {}), b"p1")
            self.commit(first, "readers", "events", generation, member, 9, error=22)
            self.assertEqual(self.offsets(first, "readers"), [("events", [(0, 4, "checkpoint", 0)])])
            self.commit(second, "readers", "events", new_generation, other_member, 8)
            leave = second.request(13, 1, wire_string("readers") + wire_string(other_member))
            self.assertEqual((leave.i32(), leave.i16()), (0, 0))
            leave.finish()
            self.assertEqual(self.heartbeat(first, "readers", new_generation, member), 27)
            self.assertEqual(self.heartbeat(second, "readers", new_generation, other_member), 25)
            self.assertEqual(self.offsets(first, "readers"), [("events", [(0, 8, "checkpoint", 0)])])
