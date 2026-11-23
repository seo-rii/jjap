#!/usr/bin/env python3
"""Single-node Kafka protocol subset for local integration tests."""

from __future__ import annotations

import argparse
import gzip
import io
import re
import socketserver
import sqlite3
import struct
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path


BROKER_ID = 0
MAX_FRAME_BYTES = 32 * 1024 * 1024
MAX_BATCH_BYTES = 8 * 1024 * 1024
MAX_FETCH_BYTES = 16 * 1024 * 1024
MAX_EXPANDED_BYTES = 32 * 1024 * 1024
MAX_BATCH_RECORDS = 100_000
MAX_ARRAY_ITEMS = 16_384
MAX_PARTITIONS = 1024
MAX_FETCH_WAIT_MS = 30_000
MAX_GROUP_MEMBERS = 128
MAX_GROUPS = 1024
MAX_GROUP_TIMEOUT_MS = 60_000
MAX_MEMBER_DATA_BYTES = 64 * 1024
TOPIC_NAME = re.compile(r"[A-Za-z0-9._-]{1,249}\Z")
BATCH_HEADER = struct.Struct(">qiibIhiqqqhii")

NONE = 0
OFFSET_OUT_OF_RANGE = 1
CORRUPT_MESSAGE = 2
UNKNOWN_TOPIC_OR_PARTITION = 3
RECORD_TOO_LARGE = 10
INVALID_TOPIC = 17
INVALID_REQUIRED_ACKS = 21
ILLEGAL_GENERATION = 22
INCONSISTENT_GROUP_PROTOCOL = 23
INVALID_GROUP_ID = 24
UNKNOWN_MEMBER_ID = 25
INVALID_SESSION_TIMEOUT = 26
REBALANCE_IN_PROGRESS = 27
UNSUPPORTED_VERSION = 35
TOPIC_ALREADY_EXISTS = 36
INVALID_PARTITIONS = 37
INVALID_REPLICATION_FACTOR = 38
INVALID_REPLICA_ASSIGNMENT = 39
INVALID_CONFIG = 40
INVALID_REQUEST = 42
UNSUPPORTED_COMPRESSION = 76
GROUP_MAX_SIZE_REACHED = 81

APIS = {
    0: (3, 5, "produce"),
    1: (4, 5, "fetch"),
    2: (1, 2, "list_offsets"),
    3: (0, 5, "metadata"),
    8: (0, 3, "offset_commit"),
    9: (0, 2, "offset_fetch"),
    10: (0, 1, "find_coordinator"),
    11: (0, 1, "join_group"),
    12: (0, 1, "heartbeat"),
    13: (0, 1, "leave_group"),
    14: (0, 1, "sync_group"),
    18: (0, 4, "api_versions"),
    19: (0, 3, "create_topics"),
}


class WireError(ValueError):
    """The request frame does not match its wire schema."""


class BatchError(ValueError):
    def __init__(self, code):
        self.code = code


class Reader:
    def __init__(self, data):
        self.data = memoryview(data)
        self.position = 0

    @property
    def remaining(self):
        return len(self.data) - self.position

    def read(self, size):
        if not 0 <= size <= self.remaining:
            raise WireError("truncated or invalid-length field")
        start = self.position
        self.position += size
        return self.data[start:self.position]

    def i8(self):
        return struct.unpack(">b", self.read(1))[0]

    def i16(self):
        return struct.unpack(">h", self.read(2))[0]

    def i32(self):
        return struct.unpack(">i", self.read(4))[0]

    def i64(self):
        return struct.unpack(">q", self.read(8))[0]

    def boolean(self):
        flag = self.i8()
        if flag not in (0, 1):
            raise WireError("invalid boolean")
        return flag == 1

    def string(self, *, nullable=False, compact=False):
        length = self.unsigned_varint() - 1 if compact else self.i16()
        if length == -1 and nullable:
            return None
        try:
            return self.read(length).tobytes().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WireError("invalid UTF-8 string") from exc

    def bytes(self, *, nullable=False):
        length = self.i32()
        if length == -1 and nullable:
            return None
        return self.read(length).tobytes()

    def array(self, read_item, *, nullable=False):
        count = self.i32()
        if count == -1 and nullable:
            return None
        if not 0 <= count <= MAX_ARRAY_ITEMS:
            raise WireError("invalid array size")
        return [read_item() for _ in range(count)]

    def unsigned_varint(self, width=32):
        value = 0
        for shift in range(0, width, 7):
            byte = self.read(1)[0]
            value |= (byte & 127) << shift
            if byte < 128 and value < 1 << width:
                return value
            if byte < 128:
                break
        raise WireError("invalid variable-length integer")

    def varint(self, width=32):
        value = self.unsigned_varint(width)
        return (value >> 1) ^ -(value & 1)

    def tagged_fields(self):
        count = self.unsigned_varint()
        if count > MAX_ARRAY_ITEMS:
            raise WireError("invalid count of tagged fields")
        previous = -1
        for _ in range(count):
            tag = self.unsigned_varint()
            if tag <= previous:
                raise WireError("tagged fields are out of order")
            previous = tag
            self.read(self.unsigned_varint())

    def finish(self):
        if self.remaining:
            raise WireError("trailing bytes in request")


def wire_string(value):
    if value is None:
        return b"\xff\xff"
    data = value.encode("utf-8")
    return struct.pack(">h", len(data)) + data


def wire_bytes(value):
    if value is None:
        return b"\xff\xff\xff\xff"
    return struct.pack(">i", len(value)) + value


def wire_array(items):
    values = tuple(items)
    return struct.pack(">i", len(values)) + b"".join(values)


def wire_uint(value):
    encoded = bytearray()
    while value >= 128:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def crc32c_table():
    table = []
    for byte in range(256):
        for _ in range(8):
            byte = (byte >> 1) ^ (0x82F63B78 if byte & 1 else 0)
        table.append(byte)
    return tuple(table)


CRC32C_TABLE = crc32c_table()


def crc32c(data):
    current = 0xFFFFFFFF
    for byte in data:
        current = CRC32C_TABLE[(current ^ byte) & 255] ^ (current >> 8)
    return current ^ 0xFFFFFFFF


def record_timestamps(records, count, base_timestamp):
    reader = Reader(records)
    timestamps = []
    for index in range(count):
        record = Reader(reader.read(reader.varint()))
        if record.i8() != 0:
            raise WireError("unsupported record attributes")
        stamp = base_timestamp + record.varint(64)
        if not 0 <= stamp <= 2**63 - 1 or record.varint() != index:
            raise WireError("invalid record timestamp or offset")
        timestamps.append(stamp)
        for _ in range(2):
            length = record.varint()
            if length != -1:
                record.read(length)
        headers = record.varint()
        if not 0 <= headers <= MAX_ARRAY_ITEMS:
            raise WireError("invalid header count")
        for _ in range(headers):
            record.read(record.varint()).tobytes().decode("utf-8")
            length = record.varint()
            if length != -1:
                record.read(length)
        record.finish()
    reader.finish()
    return timestamps


def read_batches(data):
    if not data:
        raise BatchError(CORRUPT_MESSAGE)
    reader = Reader(data)
    expanded_bytes = 0
    total_records = 0
    batches = []
    try:
        while reader.remaining:
            start = reader.position
            base_offset, length = reader.i64(), reader.i32()
            if length + 12 > MAX_BATCH_BYTES:
                raise BatchError(RECORD_TOO_LARGE)
            reader.read(length)
            batch = reader.data[start:reader.position]
            if len(batch) < BATCH_HEADER.size:
                raise BatchError(CORRUPT_MESSAGE)
            (_, _, _, magic, crc, attributes, last_delta, base_ts, max_ts,
             producer_id, epoch, sequence, count) = BATCH_HEADER.unpack_from(batch)
            if (base_offset != 0 or magic != 2 or attributes & ~7 or count < 1 or last_delta != count - 1
                    or producer_id != -1 or epoch != -1 or sequence != -1):
                raise BatchError(INVALID_REQUEST)
            total_records += count
            if total_records > MAX_BATCH_RECORDS:
                raise BatchError(RECORD_TOO_LARGE)
            if crc32c(batch[21:]) != crc:
                raise BatchError(CORRUPT_MESSAGE)
            codec = attributes & 7
            records = batch[BATCH_HEADER.size:]
            if codec == 1:
                with gzip.GzipFile(fileobj=io.BytesIO(records)) as stream:
                    records = stream.read(MAX_EXPANDED_BYTES - expanded_bytes + 1)
            elif codec != 0:
                raise BatchError(UNSUPPORTED_COMPRESSION)
            expanded_bytes += len(records)
            if expanded_bytes > MAX_EXPANDED_BYTES:
                raise BatchError(RECORD_TOO_LARGE)
            stamps = record_timestamps(records, count, base_ts)
            if max(stamps) != max_ts:
                raise BatchError(CORRUPT_MESSAGE)
            batches.append((batch, stamps))
    except (WireError, UnicodeError, OSError, EOFError, zlib.error) as exc:
        raise BatchError(CORRUPT_MESSAGE) from exc
    return batches


def valid_topic(name):
    return bool(TOPIC_NAME.fullmatch(name)) and name not in (".", "..")


@dataclass
class GroupMember:
    protocols: dict[str, bytes]
    session_seconds: float
    rebalance_seconds: float
    last_seen: float


@dataclass
class Group:
    members: dict[str, GroupMember] = field(default_factory=dict)
    joined: set[str] = field(default_factory=set)
    assignments: dict[str, bytes] = field(default_factory=dict)
    generation: int = 0
    state: str = "joining"
    leader: str = ""
    protocol: str = ""
    deadline: float = 0.0

    def begin_rebalance(self, now):
        self.generation += 1
        self.state = "joining"
        self.joined.clear()
        self.assignments.clear()
        self.deadline = now + max(member.rebalance_seconds for member in self.members.values())

    def finish_join(self, now):
        self.leader = next(iter(self.members))
        common = set.intersection(*(set(member.protocols) for member in self.members.values()))
        votes = {name: 0 for name in self.members[self.leader].protocols if name in common}
        for member in self.members.values():
            votes[next(name for name in member.protocols if name in common)] += 1
        self.protocol = max(votes, key=votes.get)
        self.state = "syncing"
        self.deadline = now + max(member.rebalance_seconds for member in self.members.values())


class Store:
    def __init__(self, db_file):
        self.changed = threading.Condition(threading.RLock())
        self.groups = {}
        self.db = sqlite3.connect(db_file, timeout=15, check_same_thread=False)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS kafka_cluster (id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS kafka_topics (name TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS kafka_partitions (
                topic TEXT REFERENCES kafka_topics(name) ON DELETE CASCADE,
                partition INTEGER, next_offset INTEGER NOT NULL, PRIMARY KEY (topic, partition)
            );
            CREATE TABLE IF NOT EXISTS kafka_batches (
                topic TEXT NOT NULL, partition INTEGER NOT NULL, base_offset INTEGER NOT NULL,
                last_offset INTEGER NOT NULL, data BLOB NOT NULL,
                PRIMARY KEY (topic, partition, last_offset),
                FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS kafka_timestamps (
                topic TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL, timestamp INTEGER NOT NULL,
                PRIMARY KEY (topic, partition, offset),
                FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS kafka_committed_offsets (
                group_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL,
                metadata TEXT, PRIMARY KEY (group_id, topic, partition),
                FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
            );
        """)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            cluster = self.db.execute("SELECT id FROM kafka_cluster").fetchone()
            self.cluster_id = cluster[0] if cluster else uuid.uuid4().hex
            if not cluster:
                self.db.execute("INSERT INTO kafka_cluster VALUES (?)", (self.cluster_id,))

    def offset(self, topic, partition):
        row = self.db.execute("SELECT next_offset FROM kafka_partitions WHERE topic = ? AND partition = ?",
                              (topic, partition)).fetchone()
        return row[0] if row else None

    def add_topic(self, name, count):
        self.db.execute("INSERT INTO kafka_topics VALUES (?)", (name,))
        self.db.executemany("INSERT INTO kafka_partitions VALUES (?, ?, 0)", ((name, p) for p in range(count)))

    def create_topics(self, topics, validate_only):
        result = []
        seen = set()
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for name, partitions, replicas, assignments, configs in topics:
                error = NONE
                if not valid_topic(name):
                    error = INVALID_TOPIC
                elif name in seen:
                    error = INVALID_REQUEST
                elif self.offset(name, 0) is not None:
                    error = TOPIC_ALREADY_EXISTS
                elif configs:
                    error = INVALID_CONFIG
                elif assignments:
                    assigned = {partition for partition, _ in assignments}
                    if (partitions != -1 or replicas != -1 or len(assignments) > MAX_PARTITIONS
                            or sorted(assigned) != list(range(len(assignments)))
                            or any(nodes != [BROKER_ID] for _, nodes in assignments)):
                        error = INVALID_REPLICA_ASSIGNMENT
                    partitions = len(assignments)
                elif not 1 <= partitions <= MAX_PARTITIONS:
                    error = INVALID_PARTITIONS
                elif replicas != 1:
                    error = INVALID_REPLICATION_FACTOR
                if not error and not validate_only:
                    self.add_topic(name, partitions)
                seen.add(name)
                result.append((name, error))
        return result

    def metadata_topics(self, requested, auto_create):
        with self.changed, self.db:
            if requested is None:
                requested = [row[0] for row in self.db.execute("SELECT name FROM kafka_topics ORDER BY name")]
            elif auto_create:
                self.db.execute("BEGIN IMMEDIATE")
            topic_details = []
            for name in requested:
                if not valid_topic(name):
                    topic_details.append((name, INVALID_TOPIC, ()))
                    continue
                partitions = [row[0] for row in self.db.execute(
                    "SELECT partition FROM kafka_partitions WHERE topic = ? ORDER BY partition", (name,))]
                if not partitions and auto_create:
                    self.add_topic(name, 1)
                    partitions = [0]
                topic_details.append((name, NONE if partitions else UNKNOWN_TOPIC_OR_PARTITION, partitions))
            return topic_details

    def produce(self, topics, request_error):
        result = []
        with self.changed:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                seen = set()
                for topic, partitions in topics:
                    replies = []
                    result.append((topic, replies))
                    for partition, data in partitions:
                        key = (topic, partition)
                        offset = self.offset(*key)
                        error = request_error
                        if key in seen:
                            error = INVALID_REQUEST
                        elif not error and offset is None:
                            error = UNKNOWN_TOPIC_OR_PARTITION
                        seen.add(key)
                        if not error:
                            try:
                                self.append_batches(topic, partition, offset, read_batches(data))
                            except BatchError as exc:
                                error = exc.code
                        replies.append((partition, error, offset if not error else -1))
            self.changed.notify_all()
        return result

    def append_batches(self, topic, partition, offset, batches):
        total = sum(len(stamps) for _, stamps in batches)
        if offset + total > 2**63 - 1:
            raise BatchError(RECORD_TOO_LARGE)
        for batch, stamps in batches:
            last_offset = offset + len(stamps) - 1
            # Neither the broker-assigned offsets nor the leader epoch are covered by CRC32C.
            data = struct.pack(">qiI", offset, len(batch) - 12, 0) + batch[16:].tobytes()
            self.db.execute("INSERT INTO kafka_batches VALUES (?, ?, ?, ?, ?)",
                            (topic, partition, offset, last_offset, data))
            self.db.executemany("INSERT INTO kafka_timestamps VALUES (?, ?, ?, ?)",
                                ((topic, partition, offset + i, stamp) for i, stamp in enumerate(stamps)))
            offset = last_offset + 1
        self.db.execute("UPDATE kafka_partitions SET next_offset = ? WHERE topic = ? AND partition = ?",
                        (offset, topic, partition))

    def fetch(self, topics, min_bytes, max_bytes, wait_ms, request_error):
        deadline = time.monotonic() + wait_ms / 1000
        with self.changed:
            while True:
                result, total, has_error = self.fetch_snapshot(topics, max_bytes, request_error)
                remaining = deadline - time.monotonic()
                if total >= min_bytes or has_error or remaining <= 0 or not topics:
                    return result
                self.changed.wait(remaining)

    def fetch_snapshot(self, topics, max_bytes, request_error):
        result = []
        total = 0
        has_error = False
        for topic, partitions in topics:
            replies = []
            result.append((topic, replies))
            for partition, start, max_partition_bytes in partitions:
                watermark = self.offset(topic, partition)
                data = bytearray()
                if request_error or max_partition_bytes < 0:
                    error = request_error or INVALID_REQUEST
                elif watermark is None:
                    error = UNKNOWN_TOPIC_OR_PARTITION
                elif not 0 <= start <= watermark:
                    error = OFFSET_OUT_OF_RANGE
                else:
                    error = NONE
                    if total < max_bytes or total == 0:
                        for (batch,) in self.db.execute("""
                            SELECT data FROM kafka_batches WHERE topic = ? AND partition = ? AND last_offset >= ?
                            ORDER BY last_offset
                        """, (topic, partition, start)):
                            if total and (total + len(batch) > max_bytes or len(data) + len(batch) > max_partition_bytes):
                                break
                            data.extend(batch)
                            total += len(batch)
                has_error = has_error or bool(error)
                replies.append((partition, error, watermark if watermark is not None else -1, bytes(data)))
        return result, total, has_error

    def list_offsets(self, topics, request_error):
        with self.changed:
            result = []
            for topic, partitions in topics:
                replies = []
                result.append((topic, replies))
                for partition, stamp in partitions:
                    watermark = self.offset(topic, partition)
                    error, timestamp, offset = request_error, -1, -1
                    if not error:
                        if watermark is None:
                            error = UNKNOWN_TOPIC_OR_PARTITION
                        elif stamp == -2:
                            offset = 0
                        elif stamp == -1:
                            offset = watermark
                        elif stamp < 0:
                            error = INVALID_REQUEST
                        else:
                            match = self.db.execute("""
                                SELECT timestamp, offset FROM kafka_timestamps
                                WHERE topic = ? AND partition = ? AND timestamp >= ? ORDER BY offset LIMIT 1
                            """, (topic, partition, stamp)).fetchone()
                            if match is not None:
                                timestamp, offset = match
                    replies.append((partition, error, timestamp, offset))
            return result

    def commit_offsets(self, group, generation, member, topics):
        result = []
        with self.changed, self.db:
            group_error = self.check_membership(group, generation, member)
            self.db.execute("BEGIN IMMEDIATE")
            for topic, partitions in topics:
                replies = []
                result.append((topic, replies))
                for partition, offset, metadata in partitions:
                    error = group_error
                    if not error and self.offset(topic, partition) is None:
                        error = UNKNOWN_TOPIC_OR_PARTITION
                    if not error and offset < 0:
                        error = INVALID_REQUEST
                    if not error:
                        self.db.execute("""
                            INSERT INTO kafka_committed_offsets VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(group_id, topic, partition) DO UPDATE SET
                                offset = excluded.offset, metadata = excluded.metadata
                        """, (group, topic, partition, offset, metadata))
                    replies.append((partition, error))
        return result

    def check_membership(self, group_id, generation, member_id):
        if not group_id:
            return INVALID_GROUP_ID
        self.maintain_groups(time.monotonic())
        group = self.groups.get(group_id)
        if group is None and generation == -1 and not member_id:
            return NONE
        if group is None or member_id not in group.members:
            return UNKNOWN_MEMBER_ID
        if group.state != "stable":
            return REBALANCE_IN_PROGRESS
        return NONE if generation == group.generation else ILLEGAL_GENERATION

    # Callers hold the condition while changing a group or reading its assignments.
    def maintain_groups(self, now):
        updated = False
        for group_id, group in list(self.groups.items()):
            if group.state == "syncing":
                if now >= group.deadline:
                    del self.groups[group_id]
                    updated = True
                continue
            expired = [member_id for member_id, member in group.members.items()
                       if (group.state == "stable" or member_id not in group.joined)
                       and (now >= member.last_seen + member.session_seconds
                            or group.state == "joining" and now >= group.deadline)]
            for member_id in expired:
                del group.members[member_id]
            if not group.members:
                del self.groups[group_id]
                updated = True
                continue
            if expired and group.state == "stable":
                group.begin_rebalance(now)
                updated = True
            elif group.state == "joining" and group.joined == group.members.keys():
                group.finish_join(now)
                updated = True
        if updated:
            self.changed.notify_all()

    @staticmethod
    def join_error(error, member_id):
        return error, -1, "", "", member_id, ()

    def join_group(self, group_id, member_id, session_ms, rebalance_ms, protocol_type, protocols):
        if not group_id:
            return self.join_error(INVALID_GROUP_ID, member_id)
        if not 1 <= session_ms <= MAX_GROUP_TIMEOUT_MS or rebalance_ms < 1:
            return self.join_error(INVALID_SESSION_TIMEOUT, member_id)
        if (protocol_type != "consumer" or not protocols or len(protocols) > 128
                or len(dict(protocols)) != len(protocols) or any(not name for name, _ in protocols)
                or sum(len(name.encode("utf-8")) + len(metadata) for name, metadata in protocols) > MAX_MEMBER_DATA_BYTES):
            return self.join_error(INCONSISTENT_GROUP_PROTOCOL, member_id)
        rebalance_seconds = min(rebalance_ms, MAX_GROUP_TIMEOUT_MS) / 1000
        with self.changed:
            now = time.monotonic()
            self.maintain_groups(now)
            group = self.groups.get(group_id)
            if member_id and (group is None or member_id not in group.members):
                return self.join_error(UNKNOWN_MEMBER_ID, member_id)
            if group is None:
                if len(self.groups) >= MAX_GROUPS:
                    return self.join_error(GROUP_MAX_SIZE_REACHED, member_id)
                group = self.groups[group_id] = Group()
            new_member = not member_id
            if new_member and len(group.members) >= MAX_GROUP_MEMBERS:
                return self.join_error(GROUP_MAX_SIZE_REACHED, member_id)

            preferences = dict(protocols)
            common = set(preferences)
            for identifier, other in group.members.items():
                if identifier != member_id:
                    common.intersection_update(other.protocols)
            if not common:
                return self.join_error(INCONSISTENT_GROUP_PROTOCOL, member_id)
            if new_member:
                member_id = uuid.uuid4().hex
            resume_join = (not new_member and group.state == "syncing" and member_id in group.joined
                           and group.members[member_id].protocols == preferences)
            group.members[member_id] = GroupMember(preferences, session_ms / 1000, rebalance_seconds, now)
            if not group.generation or (group.state != "joining" and not resume_join):
                group.begin_rebalance(now)
                self.changed.notify_all()
            group.joined.add(member_id)
            group.deadline = max(group.deadline, now + rebalance_seconds)
            generation = group.generation
            while True:
                self.maintain_groups(time.monotonic())
                if self.groups.get(group_id) is not group or member_id not in group.members:
                    return self.join_error(UNKNOWN_MEMBER_ID, member_id)
                if group.generation != generation:
                    return self.join_error(REBALANCE_IN_PROGRESS, member_id)
                if group.state != "joining":
                    metadata = [(identifier, member.protocols[group.protocol]) for identifier, member in group.members.items()
                                if member_id == group.leader]
                    return NONE, generation, group.protocol, group.leader, member_id, metadata
                self.changed.wait(0.1)

    def sync_group(self, group_id, generation, member_id, assignments):
        with self.changed:
            self.maintain_groups(time.monotonic())
            group = self.groups.get(group_id)
            while True:
                if not group_id:
                    return INVALID_GROUP_ID, b""
                if group is None or self.groups.get(group_id) is not group or member_id not in group.members:
                    return UNKNOWN_MEMBER_ID, b""
                if group.state == "joining":
                    return REBALANCE_IN_PROGRESS, b""
                if generation != group.generation:
                    return ILLEGAL_GENERATION, b""
                if group.state == "syncing" and member_id == group.leader:
                    assigned = dict(assignments)
                    if (len(assigned) != len(assignments) or assigned.keys() != group.members.keys()
                            or any(len(assignment) > MAX_MEMBER_DATA_BYTES for assignment in assigned.values())):
                        return INVALID_REQUEST, b""
                    group.assignments = assigned
                    group.state = "stable"
                    now = time.monotonic()
                    for member in group.members.values():
                        member.last_seen = now
                    self.changed.notify_all()
                if group.state == "stable":
                    return NONE, group.assignments[member_id]
                if assignments:
                    return INVALID_REQUEST, b""
                self.changed.wait(0.1)
                self.maintain_groups(time.monotonic())

    def heartbeat(self, group_id, generation, member_id):
        with self.changed:
            error = self.check_membership(group_id, generation, member_id)
            group = self.groups.get(group_id)
            if group is None or member_id not in group.members:
                return error or UNKNOWN_MEMBER_ID
            if error in (NONE, REBALANCE_IN_PROGRESS):
                group.members[member_id].last_seen = time.monotonic()
            return error

    def leave_group(self, group_id, member_id):
        with self.changed:
            if not group_id:
                return INVALID_GROUP_ID
            now = time.monotonic()
            self.maintain_groups(now)
            group = self.groups.get(group_id)
            if group is None or member_id not in group.members:
                return UNKNOWN_MEMBER_ID
            del group.members[member_id]
            group.joined.discard(member_id)
            if not group.members:
                del self.groups[group_id]
            elif group.state != "joining":
                group.begin_rebalance(now)
            self.changed.notify_all()
            return NONE

    def fetch_offsets(self, group, topics):
        result = []
        group_error = NONE if group else INVALID_GROUP_ID
        with self.changed:
            if topics is None:
                rows = self.db.execute("""
                    SELECT topic, partition, offset, metadata FROM kafka_committed_offsets
                    WHERE group_id = ? ORDER BY topic, partition
                """, (group,))
                result_by_topic = {}
                for topic, partition, offset, metadata in rows:
                    result_by_topic.setdefault(topic, []).append((partition, offset, metadata, group_error))
                result = list(result_by_topic.items())
            else:
                for topic, partitions in topics:
                    replies = []
                    result.append((topic, replies))
                    for partition in partitions:
                        error = group_error
                        row = self.db.execute("""
                            SELECT offset, metadata FROM kafka_committed_offsets
                            WHERE group_id = ? AND topic = ? AND partition = ?
                        """, (group, topic, partition)).fetchone()
                        if not error and row is None and self.offset(topic, partition) is None:
                            error = UNKNOWN_TOPIC_OR_PARTITION
                        offset, metadata = row if row else (-1, "")
                        replies.append((partition, offset, metadata, error))
        return result, group_error


class Broker:
    def __init__(self, store, host, port, auto_create):
        self.store = store
        self.location = struct.pack(">i", BROKER_ID) + wire_string(host) + struct.pack(">i", port)
        self.auto_create = auto_create

    def request(self, frame):
        reader = Reader(frame)
        api_key, version, correlation = reader.i16(), reader.i16(), reader.i32()
        reader.string(nullable=True)  # client_id
        api = APIS.get(api_key)
        if api is None or not api[0] <= version <= api[1]:
            if api_key == 18:
                return struct.pack(">i", correlation) + self.version_response(0, UNSUPPORTED_VERSION)
            raise WireError(f"unsupported API {api_key} version {version}")
        if api_key == 18 and version >= 3:
            reader.tagged_fields()
        response = getattr(self, api[2])(version, reader)
        return struct.pack(">i", correlation) + response if response is not None else None

    def version_response(self, version, error=NONE):
        keys = sorted(APIS.items())
        if version >= 3:
            versions = wire_uint(len(keys) + 1) + b"".join(
                struct.pack(">hhhB", key, minimum, maximum, 0) for key, (minimum, maximum, _) in keys
            )
        else:
            versions = wire_array(struct.pack(">hhh", key, minimum, maximum)
                                  for key, (minimum, maximum, _) in keys)
        return (struct.pack(">h", error) + versions + (b"\0" * 4 if version >= 1 else b"")
                + (b"\0" if version >= 3 else b""))

    def api_versions(self, version, reader):
        if version >= 3:
            reader.string(compact=True)  # client_software_name
            reader.string(compact=True)  # client_software_version
            reader.tagged_fields()
        reader.finish()
        return self.version_response(version)

    def metadata(self, version, reader):
        topics = reader.array(reader.string, nullable=version >= 1)
        if version == 0 and not topics:
            topics = None
        auto = reader.boolean() if version >= 4 else True
        reader.finish()
        details = self.store.metadata_topics(topics, auto and self.auto_create)
        headers = []
        if version >= 3:
            headers.append(b"\0" * 4)  # throttle_time_ms
        headers.append(wire_array([self.location + (wire_string(None) if version >= 1 else b"")]))
        if version >= 2:
            headers.append(wire_string(self.store.cluster_id))
        if version >= 1:
            headers.append(struct.pack(">i", BROKER_ID))  # controller_id
        replicas = struct.pack(">iiii", 1, BROKER_ID, 1, BROKER_ID)

        def topic_metadata(name, error, partitions):
            prefix = struct.pack(">h", error) + wire_string(name) + (b"\0" if version >= 1 else b"")
            return prefix + wire_array(
                struct.pack(">hii", NONE, partition, BROKER_ID) + replicas + (b"\0" * 4 if version >= 5 else b"")
                for partition in partitions
            )

        return b"".join(headers) + wire_array(topic_metadata(*topic) for topic in details)

    def create_topics(self, version, reader):
        def parse_topic():
            name, partitions, replicas = reader.string(), reader.i32(), reader.i16()
            assignments = reader.array(lambda: (reader.i32(), reader.array(reader.i32)))
            configs = reader.array(lambda: (reader.string(), reader.string(nullable=True)))
            return name, partitions, replicas, assignments, configs

        topics = reader.array(parse_topic)
        timeout_ms = reader.i32()
        validate_only = reader.boolean() if version >= 1 else False
        reader.finish()
        errors = self.store.create_topics(topics, validate_only) if timeout_ms >= 0 else [
            (name, INVALID_REQUEST) for name, *_ in topics
        ]
        messages = {
            INVALID_TOPIC: "Invalid topic name", INVALID_PARTITIONS: "Invalid partition count",
            INVALID_REPLICATION_FACTOR: "This broker has a single replica",
            TOPIC_ALREADY_EXISTS: "Topic already exists", INVALID_REQUEST: "Invalid topic creation request",
            INVALID_REPLICA_ASSIGNMENT: "Assignments must use the single local broker",
            INVALID_CONFIG: "Topic configuration overrides are unsupported",
        }
        return (b"\0" * 4 if version >= 2 else b"") + wire_array(
            wire_string(name) + struct.pack(">h", error) + (wire_string(messages.get(error)) if version >= 1 else b"")
            for name, error in errors
        )

    def produce(self, version, reader):
        transactional_id = reader.string(nullable=True)
        acks, timeout_ms = reader.i16(), reader.i32()
        topics = reader.array(lambda: (reader.string(), reader.array(lambda: (reader.i32(), reader.bytes(nullable=True)))))
        reader.finish()
        error = NONE
        if acks not in (-1, 0, 1):
            error = INVALID_REQUIRED_ACKS
        elif transactional_id is not None or timeout_ms < 0:
            error = INVALID_REQUEST
        result = self.store.produce(topics, error)
        if acks == 0:
            if any(item_error for _, partitions in result for _, item_error, _ in partitions):
                raise WireError("unacknowledged append failed")
            return None
        return wire_array(wire_string(name) + wire_array(
            struct.pack(">ihqq", partition, item_error, offset, -1) +
            (struct.pack(">q", -1 if item_error else 0) if version >= 5 else b"")
            for partition, item_error, offset in partitions
        ) for name, partitions in result) + b"\0" * 4

    def fetch(self, version, reader):
        replica, wait_ms, min_bytes, max_bytes = reader.i32(), reader.i32(), reader.i32(), reader.i32()
        isolation = reader.i8()

        def read_partition():
            partition, offset = reader.i32(), reader.i64()
            if version >= 5:
                reader.i64()  # log_start_offset used by replica fetchers
            return partition, offset, reader.i32()

        topics = reader.array(lambda: (reader.string(), reader.array(read_partition)))
        reader.finish()
        request_error = NONE if replica == -1 and min(wait_ms, min_bytes, max_bytes) >= 0 and isolation in (0, 1) else INVALID_REQUEST
        max_bytes = min(MAX_FETCH_BYTES, max_bytes)
        result = self.store.fetch(topics, min_bytes, max_bytes, min(wait_ms, MAX_FETCH_WAIT_MS), request_error)

        def partition_response(partition, error, watermark, batch):
            return (struct.pack(">ihqq", partition, error, watermark, -1 if error else watermark)
                    + (struct.pack(">q", -1 if error else 0) if version >= 5 else b"")
                    + b"\xff" * 4 + wire_bytes(batch))  # null aborted_transactions, then records

        return b"\0" * 4 + wire_array(wire_string(name) + wire_array(
            partition_response(*partition) for partition in partitions
        ) for name, partitions in result)

    def list_offsets(self, version, reader):
        replica = reader.i32()
        isolation = reader.i8() if version >= 2 else 0
        topics = reader.array(lambda: (reader.string(), reader.array(lambda: (reader.i32(), reader.i64()))))
        reader.finish()
        error = NONE if replica == -1 and isolation in (0, 1) else INVALID_REQUEST
        result = self.store.list_offsets(topics, error)
        return (b"\0" * 4 if version >= 2 else b"") + wire_array(wire_string(name) + wire_array(
            struct.pack(">ihqq", *partition) for partition in partitions
        ) for name, partitions in result)

    def find_coordinator(self, version, reader):
        group = reader.string()
        key_type = reader.i8() if version >= 1 else 0
        reader.finish()
        error = NONE if group else INVALID_GROUP_ID
        if key_type != 0:
            error = INVALID_REQUEST
        result = (b"\0" * 4 if version >= 1 else b"") + struct.pack(">h", error)
        if version >= 1:
            result += wire_string(None if not error else "Only nonempty classic consumer groups are supported")
        return result + (self.location if not error else struct.pack(">ihi", -1, 0, -1))

    def join_group(self, version, reader):
        group, session_ms = reader.string(), reader.i32()
        rebalance_ms = reader.i32() if version >= 1 else session_ms
        member_id, protocol_type = reader.string(), reader.string()
        protocols = reader.array(lambda: (reader.string(), reader.bytes()))
        reader.finish()
        error, generation, protocol, leader, member, members = self.store.join_group(
            group, member_id, session_ms, rebalance_ms, protocol_type, protocols,
        )
        return (struct.pack(">hi", error, generation) + wire_string(protocol) + wire_string(leader)
                + wire_string(member) + wire_array(wire_string(item) + wire_bytes(metadata) for item, metadata in members))

    def sync_group(self, version, reader):
        group, generation, member = reader.string(), reader.i32(), reader.string()
        assignments = reader.array(lambda: (reader.string(), reader.bytes()))
        reader.finish()
        error, assignment = self.store.sync_group(group, generation, member, assignments)
        return (b"\0" * 4 if version >= 1 else b"") + struct.pack(">h", error) + wire_bytes(assignment)

    def heartbeat(self, version, reader):
        group, generation, member = reader.string(), reader.i32(), reader.string()
        reader.finish()
        error = self.store.heartbeat(group, generation, member)
        return (b"\0" * 4 if version >= 1 else b"") + struct.pack(">h", error)

    def leave_group(self, version, reader):
        group, member = reader.string(), reader.string()
        reader.finish()
        error = self.store.leave_group(group, member)
        return (b"\0" * 4 if version >= 1 else b"") + struct.pack(">h", error)

    def offset_commit(self, version, reader):
        group = reader.string()
        generation, member = (reader.i32(), reader.string()) if version >= 1 else (-1, "")
        if version >= 2:
            reader.i64()  # retention_time_ms; offsets have no local expiration

        def read_partition():
            partition, offset = reader.i32(), reader.i64()
            if version == 1:
                reader.i64()  # timestamp
            return partition, offset, reader.string(nullable=True)

        topics = reader.array(lambda: (reader.string(), reader.array(read_partition)))
        reader.finish()
        result = self.store.commit_offsets(group, generation, member, topics)
        return (b"\0" * 4 if version >= 3 else b"") + wire_array(wire_string(name) + wire_array(
            struct.pack(">ih", *partition) for partition in partitions
        ) for name, partitions in result)

    def offset_fetch(self, version, reader):
        group = reader.string()
        topics = reader.array(lambda: (reader.string(), reader.array(reader.i32)), nullable=version >= 2)
        reader.finish()
        result, group_error = self.store.fetch_offsets(group, topics)
        return wire_array(wire_string(name) + wire_array(
            struct.pack(">iq", partition, offset) + wire_string(metadata) + struct.pack(">h", error)
            for partition, offset, metadata, error in partitions
        ) for name, partitions in result) + (struct.pack(">h", group_error) if version >= 2 else b"")


class Handler(socketserver.StreamRequestHandler):
    timeout = 60

    def handle(self):
        try:
            while header := self.rfile.read(4):
                if len(header) != 4:
                    raise WireError("truncated request size")
                size = struct.unpack(">i", header)[0]
                if not 10 <= size <= MAX_FRAME_BYTES:
                    raise WireError("invalid request size")
                frame = self.rfile.read(size)
                if len(frame) != size:
                    raise WireError("incomplete request frame")
                response = self.server.broker.request(frame)
                if response is None:
                    continue
                if len(response) > MAX_FRAME_BYTES:
                    raise WireError("response exceeds maximum frame size")
                self.wfile.write(struct.pack(">i", len(response)) + response)
        except (WireError, OSError, sqlite3.Error, RecursionError) as exc:
            if self.server.verbose:
                print(f"Kafka connection closed: {exc}", flush=True)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def service_actions(self):
        with self.broker.store.changed:
            self.broker.store.maintain_groups(time.monotonic())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", default=9092, type=int, help="listen port, 0 chooses a free port (default: %(default)s)")
    parser.add_argument("--advertised-host", help="hostname or IPv4 address reported in Kafka metadata")
    parser.add_argument("--db-file", type=Path, help="SQLite path for persistent partitions and offsets")
    parser.add_argument("--auto-create-topics", action="store_true", help="allow clients to create topics through Metadata")
    parser.add_argument("--verbose", action="store_true", help="log connection failures")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    advertised_host = args.advertised_host or ("127.0.0.1" if args.host in ("", "0.0.0.0") else args.host)
    if not 0 < len(advertised_host.encode("utf-8")) <= 255:
        parser.error("--advertised-host must be at most 255 bytes")
    try:
        store = Store(args.db_file or ":memory:")
        try:
            with Server((args.host, args.port), Handler) as server:
                port = server.server_address[1]
                server.broker = Broker(store, advertised_host, port, args.auto_create_topics)
                server.verbose = args.verbose
                print(f"Kafka test server listening on {args.host}:{port}", flush=True)
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
        finally:
            with store.changed:
                store.db.close()
    except (OSError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
