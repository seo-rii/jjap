#!/usr/bin/env python3
"""Single-node Kafka protocol subset for local integration tests."""

from __future__ import annotations

import argparse
import bisect
import contextlib
import errno
import gzip
import io
import json
import os
import re
import socket
import socketserver
import sqlite3
import struct
import sys
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote


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
INT_MAX, LONG_MAX = 2**31 - 1, 2**63 - 1
RETENTION_INTERVAL = 1.0
PRODUCER_BATCHES_KEPT = 5  # Kafka's ProducerStateEntry.NUM_BATCHES_TO_RETAIN

NONE = 0
OFFSET_OUT_OF_RANGE = 1
CORRUPT_MESSAGE = 2
UNKNOWN_TOPIC_OR_PARTITION = 3
RECORD_TOO_LARGE = 10
INVALID_TOPIC = 17
NOT_ENOUGH_REPLICAS = 19
INVALID_REQUIRED_ACKS = 21
ILLEGAL_GENERATION = 22
INCONSISTENT_GROUP_PROTOCOL = 23
INVALID_GROUP_ID = 24
UNKNOWN_MEMBER_ID = 25
INVALID_SESSION_TIMEOUT = 26
REBALANCE_IN_PROGRESS = 27
INVALID_TIMESTAMP = 32
UNSUPPORTED_VERSION = 35
TOPIC_ALREADY_EXISTS = 36
INVALID_PARTITIONS = 37
INVALID_REPLICATION_FACTOR = 38
INVALID_REPLICA_ASSIGNMENT = 39
INVALID_CONFIG = 40
INVALID_REQUEST = 42
POLICY_VIOLATION = 44
OUT_OF_ORDER_SEQUENCE_NUMBER = 45
INVALID_PRODUCER_EPOCH = 47
TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
GROUP_NOT_EMPTY = 68
GROUP_ID_NOT_FOUND = 69
UNSUPPORTED_COMPRESSION = 76
GROUP_MAX_SIZE_REACHED = 81
GROUP_SUBSCRIBED_TO_TOPIC = 86
INVALID_RECORD = 87
NO_TRANSACTIONS = "Transactions are not supported by the jjap Kafka test broker"

APIS = {
    0: (3, 8, "produce"),
    1: (4, 5, "fetch"),
    2: (1, 7, "list_offsets"),
    3: (0, 5, "metadata"),
    8: (0, 3, "offset_commit"),
    9: (0, 2, "offset_fetch"),
    10: (0, 1, "find_coordinator"),
    11: (0, 1, "join_group"),
    12: (0, 1, "heartbeat"),
    13: (0, 1, "leave_group"),
    14: (0, 1, "sync_group"),
    15: (0, 4, "describe_groups"),
    16: (0, 5, "list_groups"),
    18: (0, 4, "api_versions"),
    19: (0, 4, "create_topics"),
    20: (0, 3, "delete_topics"),
    21: (0, 1, "delete_records"),
    22: (0, 1, "init_producer_id"),
    32: (0, 3, "describe_configs"),
    33: (0, 1, "alter_configs"),
    37: (0, 1, "create_partitions"),
    42: (0, 1, "delete_groups"),
    44: (0, 0, "incremental_alter_configs"),
    47: (0, 0, "offset_delete"),
}
FLEXIBLE_FROM = {2: 6, 16: 3, 18: 3}  # first version with tagged fields, for the APIs above that reach one

# Kafka 4.4 topic configs: name: (type, default, rule, broker synonym). A rule is a minimum, a (low, high) range,
# the allowed values, or the name of a special check.
TOPIC_CONFIGS = {
    "cleanup.policy": ("LIST", "delete", ("compact", "delete"), "log.cleanup.policy"),
    "compression.gzip.level": ("INT", "-1", "gzip", "compression.gzip.level"),
    "compression.lz4.level": ("INT", "9", (1, 17), "compression.lz4.level"),
    "compression.type": ("STRING", "producer", ("uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"),
                         "compression.type"),
    "compression.zstd.level": ("INT", "3", (-131072, 22), "compression.zstd.level"),
    "delete.retention.ms": ("LONG", "86400000", 0, "log.cleaner.delete.retention.ms"),
    "errors.deadletterqueue.group.enable": ("BOOLEAN", "false", None, None),
    "file.delete.delay.ms": ("LONG", "60000", 0, "log.segment.delete.delay.ms"),
    "flush.messages": ("LONG", str(LONG_MAX), 1, "log.flush.interval.messages"),
    "flush.ms": ("LONG", str(LONG_MAX), 0, None),
    "follower.replication.throttled.replicas": ("LIST", "", "replicas", None),
    "index.interval.bytes": ("INT", "4096", 0, "log.index.interval.bytes"),
    "leader.replication.throttled.replicas": ("LIST", "", "replicas", None),
    "local.retention.bytes": ("LONG", "-2", -2, "log.local.retention.bytes"),
    "local.retention.ms": ("LONG", "-2", -2, "log.local.retention.ms"),
    "max.compaction.lag.ms": ("LONG", str(LONG_MAX), 1, "log.cleaner.max.compaction.lag.ms"),
    "max.decompressed.message.bytes": ("INT", "2147483639", (1, 2147483639), "max.decompressed.message.bytes"),
    "max.message.bytes": ("INT", "1048588", 0, "message.max.bytes"),
    "message.timestamp.after.max.ms": ("LONG", "3600000", 0, "log.message.timestamp.after.max.ms"),
    "message.timestamp.before.max.ms": ("LONG", str(LONG_MAX), 0, "log.message.timestamp.before.max.ms"),
    "message.timestamp.type": ("STRING", "CreateTime", ("CreateTime", "LogAppendTime"), "log.message.timestamp.type"),
    "min.cleanable.dirty.ratio": ("DOUBLE", "0.5", (0, 1), "log.cleaner.min.cleanable.ratio"),
    "min.compaction.lag.ms": ("LONG", "0", 0, "log.cleaner.min.compaction.lag.ms"),
    "min.insync.replicas": ("INT", "1", 1, "min.insync.replicas"),
    "preallocate": ("BOOLEAN", "false", None, "log.preallocate"),
    "remote.copy.lag.bytes": ("LONG", "-1", -1, "log.remote.copy.lag.bytes"),
    "remote.copy.lag.ms": ("LONG", "0", -1, "log.remote.copy.lag.ms"),
    "remote.log.copy.disable": ("BOOLEAN", "false", None, None),
    "remote.log.delete.on.disable": ("BOOLEAN", "false", None, None),
    "remote.storage.enable": ("BOOLEAN", "false", None, None),
    "retention.bytes": ("LONG", "-1", None, "log.retention.bytes"),
    # Kafka keeps records for 7 days by default; this broker keeps them until a topic sets a retention limit.
    "retention.ms": ("LONG", "-1", -1, "log.retention.ms"),
    "segment.bytes": ("INT", "1073741824", 1024 * 1024, "log.segment.bytes"),
    "segment.index.bytes": ("INT", "10485760", 4, "log.index.size.max.bytes"),
    "segment.jitter.ms": ("LONG", "0", 0, None),
    "segment.ms": ("LONG", "604800000", 1, None),
    "unclean.leader.election.enable": ("BOOLEAN", "false", None, "unclean.leader.election.enable"),
}
CONFIG_TYPES = {"BOOLEAN": 1, "STRING": 2, "INT": 3, "LONG": 5, "DOUBLE": 6, "LIST": 7}
TOPIC_RESOURCE, BROKER_RESOURCE = 2, 4
DYNAMIC_TOPIC_CONFIG, STATIC_BROKER_CONFIG, DEFAULT_CONFIG = 1, 4, 5


class WireError(ValueError):
    """The request frame does not match its wire schema."""


class BatchError(ValueError):
    def __init__(self, code, message=None):
        self.code = code
        self.message = message


class ConfigError(ValueError):
    pass


def parse_config(name, value):
    """Parse a topic config value the way Kafka's ConfigDef does, raising ConfigError with Kafka's message."""
    kind, _, rule, _ = TOPIC_CONFIGS[name]
    text = value.strip()
    invalid = f"Invalid value {text} for configuration {name}: "
    if kind in ("INT", "LONG"):
        bits = 31 if kind == "INT" else 63
        if not re.fullmatch(r"[+-]?\d+", text) or not -2**bits <= int(text) < 2**bits:
            raise ConfigError(f"{invalid}Not a number of type {kind}")
        parsed = int(text)
    elif kind == "DOUBLE":
        try:
            parsed = float(text)
        except ValueError:
            raise ConfigError(f"{invalid}Not a number of type {kind}") from None
    elif kind == "BOOLEAN":
        if text.lower() not in ("true", "false"):
            raise ConfigError(f"{invalid}Expected value to be either true or false")
        parsed = text.lower() == "true"
    else:
        parsed = ([item.strip() for item in text.split(",")] if text else []) if kind == "LIST" else text
    shown = f"Invalid value {parsed} for configuration {name}: "
    if isinstance(rule, int) and not isinstance(parsed, bool) and parsed < rule:
        raise ConfigError(f"{shown}Value must be at least {rule}")
    if isinstance(rule, tuple) and isinstance(rule[0], int):
        if parsed < rule[0]:
            raise ConfigError(f"{shown}Value must be no less than {rule[0]}")
        if parsed > rule[1]:
            raise ConfigError(f"{shown}Value must be no more than {rule[1]}")
    if isinstance(rule, tuple) and isinstance(rule[0], str):
        for item in parsed if kind == "LIST" else [parsed]:
            if item not in rule:
                raise ConfigError(f"Invalid value {item} for configuration {name}: String must be one of: {', '.join(rule)}")
    if rule == "gzip" and parsed != -1 and not 1 <= parsed <= 9:
        raise ConfigError(f"{shown}Value must be between 1 and 9 or equal to -1")
    if rule == "replicas" and parsed != ["*"] and not all(re.fullmatch(r"\d+:\d+", item) for item in parsed):
        raise ConfigError(f"{invalid}Must be the literal '*' or a list of replicas in the format "
                          "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId],...")
    return parsed


def check_topic_configs(configs):
    """Validate a complete set of topic overrides as Kafka's LogConfig.validate does and return them parsed."""
    unknown = [name for name in configs if name not in TOPIC_CONFIGS]
    if unknown:
        raise ConfigError(f"Unknown topic config name: {', '.join(unknown)}")
    missing = [name for name, value in configs.items() if value is None]
    if missing:
        raise ConfigError(f"Null value not supported for topic configs: {','.join(missing)}")
    parsed = {name: parse_config(name, value) for name, value in configs.items()}
    effective = {name: parsed.get(name, parse_config(name, TOPIC_CONFIGS[name][1])) for name in TOPIC_CONFIGS}
    if effective["min.compaction.lag.ms"] > effective["max.compaction.lag.ms"]:
        raise ConfigError(f"conflict topic config setting min.compaction.lag.ms ({effective['min.compaction.lag.ms']}) "
                          f"> max.compaction.lag.ms ({effective['max.compaction.lag.ms']})")
    if effective["remote.storage.enable"]:
        raise ConfigError("Tiered Storage functionality is disabled in the broker. "
                          "Topic cannot be configured with remote log storage.")
    return parsed


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

    def array(self, read_item, *, nullable=False, compact=False):
        count = self.unsigned_varint() - 1 if compact else self.i32()
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


def compact_string(value):
    if value is None:
        return b"\0"
    data = value.encode("utf-8")
    return wire_uint(len(data) + 1) + data


def compact_array(items):
    values = tuple(items)
    return wire_uint(len(values) + 1) + b"".join(values)


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


def read_records(records, count, base_timestamp, last_delta, contiguous):
    """Return (offset delta, timestamp, key is null) for each record of a decompressed batch."""
    reader = Reader(records)
    entries = []
    previous = -1
    for index in range(count):
        record = Reader(reader.read(reader.varint()))
        if record.i8() != 0:
            raise WireError("unsupported record attributes")
        stamp = base_timestamp + record.varint(64)
        delta = record.varint()
        if not (0 if contiguous else -1) <= stamp <= LONG_MAX or not (
                delta == index if contiguous else previous < delta <= last_delta):
            raise WireError("invalid record timestamp or offset")
        previous = delta
        key = record.varint()
        if key != -1:
            record.read(key)
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
        entries.append((delta, stamp, key == -1))
    reader.finish()
    return entries


@dataclass
class Batch:
    data: memoryview  # the whole batch, from its base offset field
    attributes: int
    last_delta: int
    max_timestamp: int
    producer_id: int
    epoch: int
    sequence: int
    count: int
    records: list | None  # read_records() entries, or None when the codec is not readable here

    @property
    def last_sequence(self):
        return (self.sequence + self.last_delta) % (INT_MAX + 1)

    def timestamps(self):
        """Per-record timestamps for offset lookups; batch-wide when the broker stamped it or its codec is opaque."""
        if self.records is None:
            return [(0, self.max_timestamp)]
        if self.attributes & 8:
            return [(delta, self.max_timestamp) for delta, _, _ in self.records]
        return [(delta, stamp) for delta, stamp, _ in self.records]


def read_batches(data, client=True):
    """Split a magic 2 record set. Client batches must be a producer's; others may come from a broker's log."""
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
            if magic != 2 or not 0 <= count <= last_delta + 1 or (client and (
                    base_offset != 0 or count < 1 or last_delta != count - 1)):
                raise BatchError(INVALID_REQUEST)
            if client and attributes & 8:
                raise BatchError(INVALID_TIMESTAMP, "Producer should not set timestamp type to LogAppendTime.")
            if client and attributes & ~7:
                raise BatchError(INVALID_RECORD, NO_TRANSACTIONS if attributes & 16
                                 else "Clients may not set broker-only batch attributes")
            if client and not (producer_id == epoch == sequence == -1 or min(producer_id, epoch, sequence) >= 0):
                raise BatchError(INVALID_RECORD, "Invalid producer id, epoch, or sequence number")
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
                if client:
                    raise BatchError(UNSUPPORTED_COMPRESSION)
                records = None
            entries = None
            if records is not None:
                expanded_bytes += len(records)
                if expanded_bytes > MAX_EXPANDED_BYTES:
                    raise BatchError(RECORD_TOO_LARGE)
                entries = read_records(records, count, base_ts, last_delta, client)
                if client and max(stamp for _, stamp, _ in entries) != max_ts:
                    raise BatchError(CORRUPT_MESSAGE)
            batches.append(Batch(batch, attributes, last_delta, max_ts, producer_id, epoch, sequence, count, entries))
    except (WireError, UnicodeError, OSError, EOFError, zlib.error) as exc:
        raise BatchError(CORRUPT_MESSAGE) from exc
    return batches


def restamp(batch, *, attributes=None, max_timestamp=None, producer=None):
    """Rewrite header fields that the CRC covers and recompute the CRC."""
    data = bytearray(batch)
    if attributes is not None:
        struct.pack_into(">h", data, 21, attributes)
    if max_timestamp is not None:
        struct.pack_into(">q", data, 35, max_timestamp)
    if producer is not None:
        struct.pack_into(">qhi", data, 43, *producer)
    struct.pack_into(">I", data, 17, crc32c(memoryview(data)[21:]))
    return data


def valid_topic(name):
    return bool(TOPIC_NAME.fullmatch(name)) and name not in (".", "..")


def subscribed_topics(metadata):
    """Topics in a ConsumerProtocolSubscription, or None when the metadata is not one."""
    try:
        reader = Reader(metadata)
        reader.i16()  # version
        return set(reader.array(reader.string))
    except WireError:
        return None


@dataclass
class GroupMember:
    protocols: dict[str, bytes]
    session_seconds: float
    rebalance_seconds: float
    last_seen: float
    client_id: str = ""
    client_host: str = ""


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
    GROUP_STATES = {"joining": "PreparingRebalance", "syncing": "CompletingRebalance", "stable": "Stable"}
    ENFORCED = ("cleanup.policy", "max.message.bytes", "message.timestamp.after.max.ms", "message.timestamp.before.max.ms",
                "message.timestamp.type", "min.insync.replicas", "retention.bytes", "retention.ms")

    def __init__(self, db_file):
        self.changed = threading.Condition(threading.RLock())
        self.groups = {}
        self.group_types = {}  # protocol types of groups that had members, as Kafka reports them once they are empty
        self.settings_cache = {}
        self.next_retention = 0.0
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
            CREATE TABLE IF NOT EXISTS kafka_topic_configs (
                topic TEXT NOT NULL REFERENCES kafka_topics(name) ON DELETE CASCADE, name TEXT NOT NULL,
                value TEXT NOT NULL, PRIMARY KEY (topic, name)
            );
            CREATE TABLE IF NOT EXISTS kafka_producers (
                topic TEXT NOT NULL, partition INTEGER NOT NULL, producer_id INTEGER NOT NULL, epoch INTEGER NOT NULL,
                batches TEXT NOT NULL, PRIMARY KEY (topic, partition, producer_id),
                FOREIGN KEY (topic, partition) REFERENCES kafka_partitions(topic, partition) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS kafka_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        """)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if "log_start_offset" not in {row[1] for row in self.db.execute("PRAGMA table_info(kafka_partitions)")}:
                self.db.execute("ALTER TABLE kafka_partitions ADD COLUMN log_start_offset INTEGER NOT NULL DEFAULT 0")
            cluster = self.db.execute("SELECT id FROM kafka_cluster").fetchone()
            self.cluster_id = cluster[0] if cluster else uuid.uuid4().hex
            if not cluster:
                self.db.execute("INSERT INTO kafka_cluster VALUES (?)", (self.cluster_id,))

    def offset(self, topic, partition):
        row = self.partition(topic, partition)
        return row[0] if row else None

    def partition(self, topic, partition):
        """Return (next offset, log start offset), or None for an unknown partition."""
        return self.db.execute("SELECT next_offset, log_start_offset FROM kafka_partitions WHERE topic = ? AND partition = ?",
                               (topic, partition)).fetchone()

    def partition_count(self, topic):
        return self.db.execute("SELECT COUNT(*) FROM kafka_partitions WHERE topic = ?", (topic,)).fetchone()[0]

    def overrides(self, topic):
        return dict(self.db.execute("SELECT name, value FROM kafka_topic_configs WHERE topic = ?", (topic,)))

    def settings(self, topic):
        settings = self.settings_cache.get(topic)
        if settings is None:
            overrides = self.overrides(topic)
            settings = self.settings_cache[topic] = {
                name: parse_config(name, overrides.get(name, TOPIC_CONFIGS[name][1])) for name in self.ENFORCED}
        return settings

    def add_topic(self, name, count, configs=None):
        self.db.execute("INSERT INTO kafka_topics VALUES (?)", (name,))
        self.add_partitions(name, 0, count)
        self.db.executemany("INSERT INTO kafka_topic_configs VALUES (?, ?, ?)",
                            ((name, key, value) for key, value in (configs or {}).items()))
        self.settings_cache.pop(name, None)

    def add_partitions(self, topic, first, count):
        self.db.executemany("INSERT INTO kafka_partitions (topic, partition, next_offset) VALUES (?, ?, 0)",
                            ((topic, index) for index in range(first, count)))

    def create_topics(self, topics, validate_only):
        result = []
        seen = set()
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for name, partitions, replicas, assignments, configs in topics:
                error, message = NONE, None
                if not valid_topic(name):
                    error = INVALID_TOPIC
                elif name in seen:
                    error = INVALID_REQUEST
                elif self.offset(name, 0) is not None:
                    error, message = TOPIC_ALREADY_EXISTS, f"Topic '{name}' already exists."
                elif assignments:
                    assigned = {partition for partition, _ in assignments}
                    if (partitions != -1 or replicas != -1 or len(assignments) > MAX_PARTITIONS
                            or sorted(assigned) != list(range(len(assignments)))
                            or any(nodes != [BROKER_ID] for _, nodes in assignments)):
                        error = INVALID_REPLICA_ASSIGNMENT
                    partitions = len(assignments)
                elif partitions != -1 and not 1 <= partitions <= MAX_PARTITIONS:
                    error = INVALID_PARTITIONS
                elif replicas not in (-1, 1):
                    error = INVALID_REPLICATION_FACTOR
                if not error:
                    try:
                        check_topic_configs(dict(configs))
                    except ConfigError as exc:
                        error, message = INVALID_CONFIG, str(exc)
                if not error and not validate_only:
                    self.add_topic(name, 1 if partitions == -1 else partitions, dict(configs))
                seen.add(name)
                result.append((name, error, message))
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

    def produce(self, topics, request_error, acks=1):
        """Append each partition's record set; replies are (partition, error, base offset, append time, log start, message)."""
        result = []
        now = int(time.time() * 1000)
        with self.changed:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                seen = set()
                for topic, partitions in topics:
                    replies = []
                    result.append((topic, replies))
                    for partition, data in partitions:
                        key = (topic, partition)
                        state = self.partition(*key)
                        error, message, reply = request_error, None, (-1, -1, -1)
                        if key in seen:
                            error = INVALID_REQUEST
                        elif not error and state is None:
                            error = UNKNOWN_TOPIC_OR_PARTITION
                        seen.add(key)
                        if not error:
                            try:
                                reply = self.append(topic, partition, state, read_batches(data), acks, now)
                            except BatchError as exc:
                                error, message = exc.code, exc.message
                        replies.append((partition, error, *reply, message))
            self.changed.notify_all()
        return result

    def append(self, topic, partition, state, batches, acks, now):
        """Validate client batches like Kafka's LogValidator and producer state checks, then store them."""
        end, start = state
        settings = self.settings(topic)
        name = f"{topic}-{partition}"
        if acks == -1 and settings["min.insync.replicas"] > 1:
            raise BatchError(NOT_ENOUGH_REPLICAS, f"The size of the current ISR [{BROKER_ID}] is insufficient to satisfy "
                                                  f"the min.isr requirement of {settings['min.insync.replicas']} for partition {name}")
        log_append = settings["message.timestamp.type"] == "LogAppendTime"
        compacted = "compact" in settings["cleanup.policy"]
        earliest = now - settings["message.timestamp.before.max.ms"]
        latest = now + settings["message.timestamp.after.max.ms"]
        for batch in batches:
            if len(batch.data) > settings["max.message.bytes"]:
                raise BatchError(RECORD_TOO_LARGE, f"The batch of {len(batch.data)} bytes is larger than max.message.bytes "
                                                   f"({settings['max.message.bytes']}) of topic {topic}")
            for delta, stamp, null_key in batch.records:
                if compacted and null_key:
                    raise BatchError(INVALID_RECORD, f"Compacted topic cannot accept message without key in topic partition {name}.")
                if not log_append and not earliest <= stamp <= latest:
                    raise BatchError(INVALID_TIMESTAMP, f"Timestamp {stamp} of message with offset {delta} is out of range. "
                                                        f"The timestamp should be within [{earliest}, {latest}]")
        persisted, updated, placed, position = {}, {}, [], end
        for batch in batches:
            pid = batch.producer_id
            if pid >= 0:
                if pid not in persisted:
                    row = self.db.execute("SELECT epoch, batches FROM kafka_producers WHERE topic = ? AND partition = ? "
                                          "AND producer_id = ?", (topic, partition, pid)).fetchone()
                    persisted[pid] = updated[pid] = (row[0], json.loads(row[1])) if row else (-1, [])
                epoch, history = persisted[pid]
                if batch.epoch == epoch:
                    for first, last, base, _, append_time in history:
                        if (first, last) == (batch.sequence, batch.last_sequence):
                            return base, append_time, start  # a retried batch: report where it was written
                epoch, history = updated[pid]
                where = f"producer {pid} at offset {position} in partition {name}"
                if batch.epoch < epoch:
                    raise BatchError(INVALID_PRODUCER_EPOCH, f"Epoch of {where} is {batch.epoch}, "
                                                             f"which is smaller than the last seen epoch {epoch}")
                if batch.epoch != epoch:
                    if batch.sequence and epoch != -1:
                        raise BatchError(OUT_OF_ORDER_SEQUENCE_NUMBER, f"Invalid sequence number for new epoch of {where}: "
                                         f"{batch.epoch} (request epoch), {batch.sequence} (seq. number), {epoch} (current producer epoch)")
                    history = []
                elif history and batch.sequence != (history[-1][1] + 1) % (INT_MAX + 1):
                    raise BatchError(OUT_OF_ORDER_SEQUENCE_NUMBER, f"Out of order sequence number for {where}: {batch.sequence} "
                                     f"(incoming seq. number), {history[-1][1]} (current end sequence number)")
                record = [batch.sequence, batch.last_sequence, position, position + batch.last_delta, now if log_append else -1]
                updated[pid] = batch.epoch, (history + [record])[-PRODUCER_BATCHES_KEPT:]
            placed.append((batch, position))
            position += batch.last_delta + 1
        if position - 1 > LONG_MAX:
            raise BatchError(RECORD_TOO_LARGE)
        for batch, offset in placed:
            data, stamps = batch.data, batch.timestamps()
            if log_append:
                data = restamp(data, attributes=batch.attributes | 8, max_timestamp=now)
                stamps = [(delta, now) for delta, _ in stamps]
            self.store_batch(topic, partition, offset, offset + batch.last_delta, data, stamps)
        self.db.executemany("""
            INSERT INTO kafka_producers VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(topic, partition, producer_id) DO UPDATE SET epoch = excluded.epoch, batches = excluded.batches
        """, ((topic, partition, pid, epoch, json.dumps(history)) for pid, (epoch, history) in updated.items()))
        self.db.execute("UPDATE kafka_partitions SET next_offset = ? WHERE topic = ? AND partition = ?",
                        (position, topic, partition))
        start = self.retain(topic, partition, now) or start
        return end, now if log_append else -1, start

    def store_batch(self, topic, partition, base, last, batch, stamps, log_start=0):
        # Neither the broker-assigned offsets nor the leader epoch are covered by CRC32C.
        data = struct.pack(">qiI", base, len(batch) - 12, 0) + bytes(batch[16:])
        self.db.execute("INSERT INTO kafka_batches VALUES (?, ?, ?, ?, ?)", (topic, partition, base, last, data))
        self.db.executemany("INSERT INTO kafka_timestamps VALUES (?, ?, ?, ?)", (
            (topic, partition, base + delta, stamp) for delta, stamp in stamps if base + delta >= log_start))

    def truncate_head(self, topic, partition, start):
        self.db.execute("DELETE FROM kafka_batches WHERE topic = ? AND partition = ? AND last_offset < ?", (topic, partition, start))
        self.db.execute("DELETE FROM kafka_timestamps WHERE topic = ? AND partition = ? AND offset < ?", (topic, partition, start))
        self.db.execute("UPDATE kafka_partitions SET log_start_offset = ? WHERE topic = ? AND partition = ?",
                        (start, topic, partition))

    def retain(self, topic, partition, now):
        """Delete head batches past retention.ms or retention.bytes, a batch standing in for a log segment."""
        settings = self.settings(topic)
        limit_ms, limit_bytes = settings["retention.ms"], settings["retention.bytes"]
        if "delete" not in settings["cleanup.policy"] or limit_ms < 0 and limit_bytes < 0:
            return None
        end, start = self.partition(topic, partition)
        size = self.db.execute("SELECT COALESCE(SUM(length(data)), 0) FROM kafka_batches WHERE topic = ? AND partition = ?",
                               (topic, partition)).fetchone()[0]
        deleted, new_start = False, end
        for base, length, stamp in self.db.execute("""
            SELECT base_offset, length(data), substr(data, 36, 8) FROM kafka_batches
            WHERE topic = ? AND partition = ? ORDER BY last_offset
        """, (topic, partition)).fetchall():
            if not (0 <= limit_ms < now - struct.unpack(">q", stamp)[0] or 0 <= limit_bytes <= size - length):
                new_start = base
                break
            size -= length
            deleted = True
        if not deleted or max(start, new_start) <= start:
            return None
        self.truncate_head(topic, partition, max(start, new_start))
        return max(start, new_start)

    def maintain_logs(self, now):
        if now < self.next_retention:
            return
        self.next_retention = now + RETENTION_INTERVAL
        wall = int(time.time() * 1000)
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            moved = False
            for topic, partition in self.db.execute("""
                SELECT topic, partition FROM kafka_partitions WHERE topic IN (
                    SELECT topic FROM kafka_topic_configs WHERE name IN ('retention.ms', 'retention.bytes'))
            """).fetchall():
                moved = self.retain(topic, partition, wall) is not None or moved
            if moved:
                self.changed.notify_all()

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
                watermark, log_start = self.partition(topic, partition) or (None, -1)
                data = bytearray()
                if request_error or max_partition_bytes < 0:
                    error = request_error or INVALID_REQUEST
                elif watermark is None:
                    error = UNKNOWN_TOPIC_OR_PARTITION
                elif not log_start <= start <= watermark:
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
                replies.append((partition, error, watermark if watermark is not None else -1, log_start, bytes(data)))
        return result, total, has_error

    def list_offsets(self, topics, request_error):
        with self.changed:
            result = []
            for topic, partitions in topics:
                replies = []
                result.append((topic, replies))
                for partition, stamp in partitions:
                    watermark, log_start = self.partition(topic, partition) or (None, -1)
                    error, timestamp, offset = request_error, -1, -1
                    if not error:
                        if watermark is None:
                            error = UNKNOWN_TOPIC_OR_PARTITION
                        elif stamp == -2:
                            offset = log_start
                        elif stamp == -1:
                            offset = watermark
                        elif stamp == -3:  # the record with the largest timestamp
                            timestamp, offset = self.db.execute("""
                                SELECT timestamp, offset FROM kafka_timestamps WHERE topic = ? AND partition = ?
                                ORDER BY timestamp DESC, offset LIMIT 1
                            """, (topic, partition)).fetchone() or (-1, -1)
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

    def join_group(self, group_id, member_id, session_ms, rebalance_ms, protocol_type, protocols, client=("", "")):
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
            group.members[member_id] = GroupMember(preferences, session_ms / 1000, rebalance_seconds, now, *client)
            self.group_types[group_id] = protocol_type
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


    def has_offsets(self, group_id):
        return self.db.execute("SELECT 1 FROM kafka_committed_offsets WHERE group_id = ? LIMIT 1", (group_id,)).fetchone()

    def list_groups(self):
        with self.changed:
            self.maintain_groups(time.monotonic())
            names = set(self.groups).union(group for group, in self.db.execute(
                "SELECT DISTINCT group_id FROM kafka_committed_offsets"))
            return [(name, self.group_types.get(name, ""),
                     self.GROUP_STATES[self.groups[name].state] if name in self.groups else "Empty") for name in sorted(names)]

    def describe_groups(self, names):
        """Describe classic groups as (error, id, state, protocol type, protocol, members)."""
        result = []
        with self.changed:
            self.maintain_groups(time.monotonic())
            for name in names:
                group = self.groups.get(name)
                if group is None:
                    empty = bool(name) and self.has_offsets(name)
                    result.append((NONE if name else INVALID_GROUP_ID, name, "Empty" if empty else "Dead",
                                   self.group_types.get(name, "") if empty else "", "", []))
                    continue
                stable = group.state == "stable"
                result.append((NONE, name, self.GROUP_STATES[group.state], self.group_types.get(name, "consumer"),
                               group.protocol if stable else "", [
                                   (member_id, member.client_id, member.client_host,
                                    member.protocols[group.protocol] if stable else b"", group.assignments.get(member_id, b""))
                                   for member_id, member in group.members.items()]))
        return result

    def delete_groups(self, names):
        result = []
        with self.changed, self.db:
            self.maintain_groups(time.monotonic())
            self.db.execute("BEGIN IMMEDIATE")
            for name in names:
                if not name:
                    error = INVALID_GROUP_ID
                elif name in self.groups:
                    error = GROUP_NOT_EMPTY
                elif self.db.execute("DELETE FROM kafka_committed_offsets WHERE group_id = ?", (name,)).rowcount:
                    error = NONE
                    self.group_types.pop(name, None)
                else:
                    error = GROUP_ID_NOT_FOUND
                result.append((name, error))
        return result

    def delete_offsets(self, group_id, topics):
        with self.changed, self.db:
            self.maintain_groups(time.monotonic())
            group = self.groups.get(group_id)
            if not group_id:
                return INVALID_GROUP_ID, []
            if group is None and not self.has_offsets(group_id):
                return GROUP_ID_NOT_FOUND, []
            subscribed = set()
            for member in group.members.values() if group else ():
                topics_of_member = subscribed_topics(member.protocols.get(group.protocol) or next(iter(member.protocols.values())))
                subscribed = None if topics_of_member is None or subscribed is None else subscribed | topics_of_member
            self.db.execute("BEGIN IMMEDIATE")
            result = []
            for topic, partitions in topics:
                replies = []
                for partition in partitions:
                    error = GROUP_SUBSCRIBED_TO_TOPIC if subscribed is None or topic in subscribed else NONE
                    if not error:
                        self.db.execute("DELETE FROM kafka_committed_offsets WHERE group_id = ? AND topic = ? AND partition = ?",
                                        (group_id, topic, partition))
                    replies.append((partition, error))
                result.append((topic, replies))
            return NONE, result

    def delete_topics(self, names):
        result = []
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for name in names:
                deleted = self.db.execute("DELETE FROM kafka_topics WHERE name = ?", (name,)).rowcount
                self.settings_cache.pop(name, None)
                result.append((name, NONE if deleted else UNKNOWN_TOPIC_OR_PARTITION))
            self.changed.notify_all()
        return result

    def create_partitions(self, topics, validate_only):
        result = []
        seen = set()
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for name, count, assignments in topics:
                current = self.partition_count(name)
                error, message = NONE, None
                if name in seen:
                    error, message = INVALID_REQUEST, "Duplicate topic name."
                elif not current:
                    error, message = UNKNOWN_TOPIC_OR_PARTITION, f"The topic '{name}' does not exist."
                elif count == current:
                    error, message = INVALID_PARTITIONS, f"Topic already has {current} partition(s)."
                elif count < current:
                    error, message = INVALID_PARTITIONS, (f"The topic {name} currently has {current} partition(s); "
                                                          f"{count} would not be an increase.")
                elif count > MAX_PARTITIONS:
                    error, message = INVALID_PARTITIONS, f"This broker allows at most {MAX_PARTITIONS} partitions per topic."
                elif assignments is not None and (len(assignments) != count - current
                                                  or any(nodes != [BROKER_ID] for nodes in assignments)):
                    error, message = INVALID_REPLICA_ASSIGNMENT, "Assignments must name the single local broker for each new partition."
                if not error and not validate_only:
                    self.add_partitions(name, current, count)
                seen.add(name)
                result.append((name, error, message))
        return result

    def delete_records(self, topics):
        result = []
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for topic, partitions in topics:
                replies = []
                result.append((topic, replies))
                for partition, offset in partitions:
                    state = self.partition(topic, partition)
                    error, low = NONE, -1
                    if state is None:
                        error = UNKNOWN_TOPIC_OR_PARTITION
                    elif "delete" not in self.settings(topic)["cleanup.policy"]:
                        error = POLICY_VIOLATION
                    elif not (offset == -1 or 0 <= offset <= state[0]):
                        error = OFFSET_OUT_OF_RANGE
                    else:
                        low = max(state[1], state[0] if offset == -1 else offset)
                        if low > state[1]:
                            self.truncate_head(topic, partition, low)
                    replies.append((partition, low, error))
            self.changed.notify_all()
        return result

    def init_producer_id(self):
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute("SELECT value FROM kafka_counters WHERE name = 'producer_id'").fetchone()
            producer_id = row[0] if row else 0
            self.db.execute("INSERT INTO kafka_counters VALUES ('producer_id', ?) "
                            "ON CONFLICT(name) DO UPDATE SET value = excluded.value", (producer_id + 1,))
        return producer_id

    def topic_configs(self, topic, keys):
        """Describe a topic's configs as (name, value, source, synonyms, type), or None for an unknown topic."""
        with self.changed:
            if self.offset(topic, 0) is None:
                return None
            overrides = self.overrides(topic)
        entries = []
        for name, (kind, default, _, synonym) in sorted(TOPIC_CONFIGS.items()):
            if keys is not None and name not in keys:
                continue
            synonyms = [(synonym, default, DEFAULT_CONFIG)] if synonym else []
            if name in overrides:
                entries.append((name, overrides[name], DYNAMIC_TOPIC_CONFIG,
                                [(name, overrides[name], DYNAMIC_TOPIC_CONFIG)] + synonyms, kind))
            else:
                entries.append((name, default, DEFAULT_CONFIG, synonyms, kind))
        return entries

    def alter_topic_configs(self, topic, changes, incremental, validate_only):
        """Apply AlterConfigs (replace all) or IncrementalAlterConfigs operations; return (error, message)."""
        with self.changed, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if self.offset(topic, 0) is None:
                return UNKNOWN_TOPIC_OR_PARTITION, f"The topic '{topic}' does not exist."
            names = [name for name, _, _ in changes]
            if len(set(names)) != len(names):
                return INVALID_REQUEST, "Error due to duplicate config keys"
            configs = self.overrides(topic) if incremental else {}
            for name, operation, value in changes:
                if not incremental or operation == 0:
                    configs[name] = value
                elif operation == 1:
                    configs.pop(name, None)
                elif operation in (2, 3):
                    verb = "APPEND to" if operation == 2 else "SUBTRACT from"
                    if TOPIC_CONFIGS.get(name, ("",))[0] != "LIST":
                        return INVALID_CONFIG, f"Can't {verb} key {name} because its type is not LIST."
                    items = [item.strip() for item in configs.get(name, TOPIC_CONFIGS[name][1]).split(",") if item.strip()]
                    given = [item.strip() for item in (value or "").split(",") if item.strip()]
                    items = items + [item for item in given if item not in items] if operation == 2 else [
                        item for item in items if item not in given]
                    configs[name] = ",".join(items)
                else:
                    return INVALID_REQUEST, f"Unknown alter configs operation {operation}"
            try:
                check_topic_configs(configs)
            except ConfigError as exc:
                return INVALID_CONFIG, str(exc)
            if not validate_only:
                self.db.execute("DELETE FROM kafka_topic_configs WHERE topic = ?", (topic,))
                self.db.executemany("INSERT INTO kafka_topic_configs VALUES (?, ?, ?)",
                                    ((topic, name, value) for name, value in configs.items()))
                self.settings_cache.pop(topic, None)
                now = int(time.time() * 1000)
                for partition in range(self.partition_count(topic)):
                    self.retain(topic, partition, now)
                self.changed.notify_all()
        return NONE, None

class Broker:
    def __init__(self, store, host, port, auto_create, listen_host=None):
        self.store = store
        self.location = struct.pack(">i", BROKER_ID) + wire_string(host) + struct.pack(">i", port)
        self.auto_create = auto_create
        self.client = threading.local()
        static = {"node.id": str(BROKER_ID), "broker.id": str(BROKER_ID), "process.roles": "broker,controller",
                  "advertised.listeners": f"PLAINTEXT://{host}:{port}",
                  "listeners": f"PLAINTEXT://{listen_host or host}:{port}",
                  "auto.create.topics.enable": str(bool(auto_create)).lower(), "offsets.topic.replication.factor": "1",
                  "transaction.state.log.replication.factor": "1"}
        defaults = {"num.partitions": "1", "default.replication.factor": "1", "delete.topic.enable": "true",
                    "group.initial.rebalance.delay.ms": "0"}
        defaults.update((synonym, default) for _, default, _, synonym in TOPIC_CONFIGS.values() if synonym)
        self.broker_configs = sorted([(name, value, STATIC_BROKER_CONFIG) for name, value in static.items()]
                                     + [(name, value, DEFAULT_CONFIG) for name, value in defaults.items()])

    def request(self, frame, peer=""):
        reader = Reader(frame)
        api_key, version, correlation = reader.i16(), reader.i16(), reader.i32()
        self.client.id, self.client.host = reader.string(nullable=True) or "", f"/{peer}"
        api = APIS.get(api_key)
        if api is None or not api[0] <= version <= api[1]:
            if api_key == 18:
                return struct.pack(">i", correlation) + self.version_response(0, UNSUPPORTED_VERSION)
            raise WireError(f"unsupported API {api_key} version {version}")
        flexible = version >= FLEXIBLE_FROM.get(api_key, 1 << 15)
        if flexible:
            reader.tagged_fields()
        response = getattr(self, api[2])(version, reader)
        if response is None:
            return None
        # ApiVersions keeps the old response header so that any client can parse its error.
        return struct.pack(">i", correlation) + (b"\0" if flexible and api_key != 18 else b"") + response

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

    @staticmethod
    def throttle(version, since=1):
        return b"\0" * 4 if version >= since else b""

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
        if version < 4:  # broker defaults for the partition count and replication factor arrived in v4
            topics = [(name, count if count != -1 or assignments else 0, replicas if replicas != -1 or assignments else 0,
                       assignments, configs) for name, count, replicas, assignments, configs in topics]
        errors = self.store.create_topics(topics, validate_only) if timeout_ms >= 0 else [
            (name, INVALID_REQUEST, None) for name, *_ in topics
        ]
        messages = {
            INVALID_TOPIC: "Invalid topic name", INVALID_PARTITIONS: "Invalid partition count",
            INVALID_REPLICATION_FACTOR: "This broker has a single replica", INVALID_REQUEST: "Invalid topic creation request",
            INVALID_REPLICA_ASSIGNMENT: "Assignments must use the single local broker",
        }
        return self.throttle(version, 2) + wire_array(
            wire_string(name) + struct.pack(">h", error)
            + (wire_string(message or messages.get(error)) if version >= 1 else b"")
            for name, error, message in errors
        )

    def produce(self, version, reader):
        transactional_id = reader.string(nullable=True)
        acks, timeout_ms = reader.i16(), reader.i32()
        topics = reader.array(lambda: (reader.string(), reader.array(lambda: (reader.i32(), reader.bytes(nullable=True)))))
        reader.finish()
        error = NONE
        if acks not in (-1, 0, 1):
            error = INVALID_REQUIRED_ACKS
        elif transactional_id is not None:
            error = TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        elif timeout_ms < 0:
            error = INVALID_REQUEST
        result = self.store.produce(topics, error, acks)
        if acks == 0:
            if any(item[1] for _, partitions in result for item in partitions):
                raise WireError("unacknowledged append failed")
            return None

        def partition_response(partition, item_error, offset, append_time, log_start, message):
            if item_error == TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                message = NO_TRANSACTIONS
            return (struct.pack(">ihqq", partition, item_error, offset, append_time)
                    + (struct.pack(">q", log_start) if version >= 5 else b"")
                    + (wire_array([]) + wire_string(message) if version >= 8 else b""))

        return wire_array(wire_string(name) + wire_array(
            partition_response(*item) for item in partitions) for name, partitions in result) + b"\0" * 4

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

        def partition_response(partition, error, watermark, log_start, batch):
            return (struct.pack(">ihqq", partition, error, watermark, -1 if error else watermark)
                    + (struct.pack(">q", -1 if error else log_start) if version >= 5 else b"")
                    + b"\xff" * 4 + wire_bytes(batch))  # null aborted_transactions, then records

        return b"\0" * 4 + wire_array(wire_string(name) + wire_array(
            partition_response(*partition) for partition in partitions
        ) for name, partitions in result)

    def list_offsets(self, version, reader):
        flexible = version >= 6

        def read_partition():
            partition = reader.i32()
            if version >= 4:
                reader.i32()  # current_leader_epoch
            stamp = reader.i64()
            if flexible:
                reader.tagged_fields()
            return partition, stamp

        def read_topic():
            name = reader.string(compact=flexible)
            partitions = reader.array(read_partition, compact=flexible)
            if flexible:
                reader.tagged_fields()
            return name, partitions

        replica = reader.i32()
        isolation = reader.i8() if version >= 2 else 0
        topics = reader.array(read_topic, compact=flexible)
        if flexible:
            reader.tagged_fields()
        reader.finish()
        error = NONE if replica == -1 and isolation in (0, 1) else INVALID_REQUEST
        result = self.store.list_offsets(topics, error)
        items = compact_array if flexible else wire_array
        return self.throttle(version, 2) + items(
            (compact_string(name) if flexible else wire_string(name)) + items(
                struct.pack(">ihqq", *partition) + (struct.pack(">i", 0 if partition[3] >= 0 else -1) if version >= 4 else b"")
                + (b"\0" if flexible else b"") for partition in partitions) + (b"\0" if flexible else b"")
            for name, partitions in result) + (b"\0" if flexible else b"")

    def find_coordinator(self, version, reader):
        group = reader.string()
        key_type = reader.i8() if version >= 1 else 0
        reader.finish()
        error, message = (NONE, None) if group else (INVALID_GROUP_ID, "Only nonempty classic consumer groups are supported")
        if key_type == 1:  # clients treat this as fatal and show the message, unlike a retriable or authorization error
            error, message = INVALID_REQUEST, NO_TRANSACTIONS
        elif key_type != 0:
            error, message = INVALID_REQUEST, "Only consumer group coordinators are supported"
        result = self.throttle(version) + struct.pack(">h", error)
        if version >= 1:
            result += wire_string(message)
        return result + (self.location if not error else struct.pack(">ihi", -1, 0, -1))

    def init_producer_id(self, version, reader):
        transactional_id = reader.string(nullable=True)
        reader.i32()  # transaction_timeout_ms
        reader.finish()
        if transactional_id is not None:
            return struct.pack(">ihqh", 0, TRANSACTIONAL_ID_AUTHORIZATION_FAILED, -1, -1)
        return struct.pack(">ihqh", 0, NONE, self.store.init_producer_id(), 0)

    def join_group(self, version, reader):
        group, session_ms = reader.string(), reader.i32()
        rebalance_ms = reader.i32() if version >= 1 else session_ms
        member_id, protocol_type = reader.string(), reader.string()
        protocols = reader.array(lambda: (reader.string(), reader.bytes()))
        reader.finish()
        error, generation, protocol, leader, member, members = self.store.join_group(
            group, member_id, session_ms, rebalance_ms, protocol_type, protocols, (self.client.id, self.client.host),
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

    def list_groups(self, version, reader):
        flexible = version >= 3
        def names():
            return {item.lower() for item in reader.array(lambda: reader.string(compact=True), compact=True)}

        states = names() if version >= 4 else set()
        types = names() if version >= 5 else set()
        if flexible:
            reader.tagged_fields()
        reader.finish()
        groups = [group for group in self.store.list_groups()
                  if (not states or group[2].lower() in states) and (not types or "classic" in types)]
        if not flexible:
            return self.throttle(version) + struct.pack(">h", NONE) + wire_array(
                wire_string(name) + wire_string(kind) for name, kind, _ in groups)
        return self.throttle(version) + struct.pack(">h", NONE) + compact_array(
            compact_string(name) + compact_string(kind) + (compact_string(state) if version >= 4 else b"")
            + (compact_string("classic") if version >= 5 else b"") + b"\0" for name, kind, state in groups) + b"\0"

    def describe_groups(self, version, reader):
        names = reader.array(reader.string)
        operations = reader.boolean() if version >= 3 else False
        reader.finish()

        def described(error, name, state, kind, protocol, members):
            return (struct.pack(">h", error) + wire_string(name) + wire_string(state) + wire_string(kind)
                    + wire_string(protocol) + wire_array(
                        wire_string(member) + (wire_string(None) if version >= 4 else b"") + wire_string(client)
                        + wire_string(host) + wire_bytes(metadata) + wire_bytes(assignment)
                        for member, client, host, metadata, assignment in members)
                    # READ, DELETE, and DESCRIBE, the operations Kafka authorizes on a group
                    + (struct.pack(">i", 0b101001000 if operations else -2**31) if version >= 3 else b""))

        return self.throttle(version) + wire_array(described(*group) for group in self.store.describe_groups(names))

    def delete_groups(self, version, reader):
        names = reader.array(reader.string)
        reader.finish()
        return self.throttle(version, 0) + wire_array(
            wire_string(name) + struct.pack(">h", error) for name, error in self.store.delete_groups(names))

    def offset_delete(self, version, reader):
        group = reader.string()
        topics = reader.array(lambda: (reader.string(), reader.array(reader.i32)))
        reader.finish()
        error, result = self.store.delete_offsets(group, topics)
        return struct.pack(">hi", error, 0) + wire_array(wire_string(name) + wire_array(
            struct.pack(">ih", *partition) for partition in partitions) for name, partitions in result)

    def delete_topics(self, version, reader):
        names = reader.array(reader.string)
        reader.i32()  # timeout_ms; deletion is immediate
        reader.finish()
        return self.throttle(version) + wire_array(
            wire_string(name) + struct.pack(">h", error) for name, error in self.store.delete_topics(names))

    def create_partitions(self, version, reader):
        topics = reader.array(lambda: (reader.string(), reader.i32(),
                                       reader.array(lambda: reader.array(reader.i32), nullable=True)))
        timeout_ms = reader.i32()
        validate_only = reader.boolean()
        reader.finish()
        result = self.store.create_partitions(topics, validate_only) if timeout_ms >= 0 else [
            (name, INVALID_REQUEST, "Invalid timeout") for name, *_ in topics]
        return self.throttle(version, 0) + wire_array(
            wire_string(name) + struct.pack(">h", error) + wire_string(message) for name, error, message in result)

    def delete_records(self, version, reader):
        topics = reader.array(lambda: (reader.string(), reader.array(lambda: (reader.i32(), reader.i64()))))
        reader.i32()  # timeout_ms
        reader.finish()
        return self.throttle(version, 0) + wire_array(wire_string(name) + wire_array(
            struct.pack(">iqh", *partition) for partition in partitions) for name, partitions in self.store.delete_records(topics))

    def describe_configs(self, version, reader):
        resources = reader.array(lambda: (reader.i8(), reader.string(), reader.array(reader.string, nullable=True)))
        synonyms = reader.boolean() if version >= 1 else False
        if version >= 3:
            reader.boolean()  # include_documentation; no documentation is available here
        reader.finish()

        def entry(name, value, source, others, kind, read_only=False):
            return (wire_string(name) + wire_string(value) + struct.pack(">?", read_only)
                    + (struct.pack(">b", source) if version >= 1 else struct.pack(">?", source != DYNAMIC_TOPIC_CONFIG))
                    + b"\0" + (wire_array(wire_string(item) + wire_string(text) + struct.pack(">b", origin)
                                         for item, text, origin in (others if synonyms else ())) if version >= 1 else b"")
                    + (struct.pack(">b", CONFIG_TYPES.get(kind, 0)) + wire_string(None) if version >= 3 else b""))

        def result(kind, name, keys):
            error, message, entries = NONE, None, []
            if kind == TOPIC_RESOURCE:
                described = self.store.topic_configs(name, keys)
                if described is None:
                    error, message = UNKNOWN_TOPIC_OR_PARTITION, f"Topic {name} does not exist."
                entries = [entry(*item) for item in described or ()]
            elif kind == BROKER_RESOURCE and name == str(BROKER_ID):
                entries = [entry(item, value, source, [(item, value, source)], "STRING", True)
                           for item, value, source in self.broker_configs if keys is None or item in keys]
            elif kind != BROKER_RESOURCE or name:
                error, message = INVALID_REQUEST, (f"Unexpected broker id, expected {BROKER_ID} or empty string, but received {name}"
                                                   if kind == BROKER_RESOURCE else f"Unsupported resource type {kind}")
            return (struct.pack(">h", error) + wire_string(message) + struct.pack(">b", kind) + wire_string(name)
                    + wire_array(entries))

        return self.throttle(version, 0) + wire_array(result(*resource) for resource in resources)

    def alter(self, version, reader, incremental):
        def read_config():
            name = reader.string()
            operation = reader.i8() if incremental else 0
            return name, operation, reader.string(nullable=True)

        resources = reader.array(lambda: (reader.i8(), reader.string(), reader.array(read_config)))
        validate_only = reader.boolean()
        reader.finish()

        def result(kind, name, changes):
            if kind == TOPIC_RESOURCE:
                error, message = self.store.alter_topic_configs(name, changes, incremental, validate_only)
            elif kind == BROKER_RESOURCE:
                error, message = INVALID_REQUEST, "Broker configs are read-only on the jjap Kafka test broker"
            else:
                error, message = INVALID_REQUEST, f"Unsupported resource type {kind}"
            return struct.pack(">h", error) + wire_string(message) + struct.pack(">b", kind) + wire_string(name)

        return self.throttle(version, 0) + wire_array(result(*resource) for resource in resources)

    def alter_configs(self, version, reader):
        return self.alter(version, reader, False)

    def incremental_alter_configs(self, version, reader):
        return self.alter(version, reader, True)


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
                response = self.server.broker.request(frame, self.client_address[0])
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
        self.broker.store.maintain_logs(time.monotonic())


# Replay into a running Kafka cluster


MIGRATE_CLIENT = "jjap-kafka-migrate"
MIGRATE_REQUEST_BYTES = 1024 * 1024
MIGRATE_APIS = {  # api_key: (name, lowest and highest version this client speaks)
    0: ("Produce", 3, 8), 1: ("Fetch", 4, 11), 2: ("ListOffsets", 2, 5), 3: ("Metadata", 4, 8), 8: ("OffsetCommit", 2, 7),
    9: ("OffsetFetch", 2, 5), 10: ("FindCoordinator", 0, 2), 15: ("DescribeGroups", 0, 4), 16: ("ListGroups", 0, 2),
    17: ("SaslHandshake", 1, 1), 18: ("ApiVersions", 0, 0), 19: ("CreateTopics", 2, 4), 32: ("DescribeConfigs", 1, 3),
    36: ("SaslAuthenticate", 0, 1), 69: ("ConsumerGroupDescribe", 0, 1),
}
FLEXIBLE_APIS = {69}  # every other version above predates tagged fields
KAFKA_ERRORS = {
    1: "OFFSET_OUT_OF_RANGE", 2: "CORRUPT_MESSAGE", 3: "UNKNOWN_TOPIC_OR_PARTITION", 5: "LEADER_NOT_AVAILABLE",
    6: "NOT_LEADER_OR_FOLLOWER", 7: "REQUEST_TIMED_OUT", 9: "REPLICA_NOT_AVAILABLE", 10: "MESSAGE_TOO_LARGE",
    12: "OFFSET_METADATA_TOO_LARGE", 14: "COORDINATOR_LOAD_IN_PROGRESS", 15: "COORDINATOR_NOT_AVAILABLE",
    16: "NOT_COORDINATOR", 17: "INVALID_TOPIC_EXCEPTION", 18: "RECORD_LIST_TOO_LARGE", 19: "NOT_ENOUGH_REPLICAS",
    20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND", 22: "ILLEGAL_GENERATION", 24: "INVALID_GROUP_ID", 25: "UNKNOWN_MEMBER_ID",
    27: "REBALANCE_IN_PROGRESS", 29: "TOPIC_AUTHORIZATION_FAILED", 30: "GROUP_AUTHORIZATION_FAILED",
    31: "CLUSTER_AUTHORIZATION_FAILED", 32: "INVALID_TIMESTAMP", 33: "UNSUPPORTED_SASL_MECHANISM",
    34: "ILLEGAL_SASL_STATE", 35: "UNSUPPORTED_VERSION", 36: "TOPIC_ALREADY_EXISTS", 37: "INVALID_PARTITIONS", 38: "INVALID_REPLICATION_FACTOR",
    40: "INVALID_CONFIG", 41: "NOT_CONTROLLER", 42: "INVALID_REQUEST", 44: "POLICY_VIOLATION",
    45: "OUT_OF_ORDER_SEQUENCE_NUMBER", 47: "INVALID_PRODUCER_EPOCH", 53: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
    58: "SASL_AUTHENTICATION_FAILED", 69: "GROUP_ID_NOT_FOUND", 76: "UNSUPPORTED_COMPRESSION_TYPE",
    87: "INVALID_RECORD", 113: "STALE_MEMBER_EPOCH",
}
# The broker rejected these before appending, so resending the same batch cannot duplicate records.
LEADER_MOVED = {3, 5, 6, 19}
COORDINATOR_MOVED = {14, 15, 16}
GROUP_ACTIVE = {22, 25, 27, 113}


def error_name(code):
    return KAFKA_ERRORS.get(code, f"error code {code}")


def broker_address(text):
    host, colon, port = text.strip().rpartition(":")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if not colon or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError(f"Invalid bootstrap server {text.strip()!r}; expected HOST:PORT")
    return host, int(port)


def show_address(address):
    host, port = address
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class MigrationError(Exception):
    """The target cluster rejected a step, or the connection to it failed."""


class UnsupportedApi(MigrationError):
    pass


class KafkaConnection:
    def __init__(self, address, timeout, credentials):
        self.name = show_address(address)
        self.correlation = 0
        self.ranges = {18: (0, 0)}
        self.socket = socket.create_connection(address, timeout=timeout)
        self.stream = self.socket.makefile("rb")
        try:
            error, ranges = self.request(18, lambda version: b"", lambda reply, version: (
                reply.i16(), reply.array(lambda: (reply.i16(), (reply.i16(), reply.i16())))))
            if error:
                raise MigrationError(f"{self.name} rejected ApiVersions with {error_name(error)}")
            self.ranges = dict(ranges)
            if credentials:
                self.authenticate(*credentials)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.stream.close()
        self.socket.close()

    def read(self, size):
        data = self.stream.read(size)
        if len(data) != size:
            raise MigrationError(f"{self.name} closed the connection")
        return data

    def request(self, api, body, parse):
        name, lowest, highest = MIGRATE_APIS[api]
        offered = self.ranges.get(api)
        if offered is None or max(lowest, offered[0]) > min(highest, offered[1]):
            raise UnsupportedApi(f"{self.name} does not support {name} versions {lowest}-{highest}")
        version = min(highest, offered[1])
        self.correlation += 1
        frame = (struct.pack(">hhi", api, version, self.correlation) + wire_string(MIGRATE_CLIENT)
                 + (b"\0" if api in FLEXIBLE_APIS else b"") + body(version))
        self.socket.sendall(struct.pack(">i", len(frame)) + frame)
        size = struct.unpack(">i", self.read(4))[0]
        if not 4 <= size <= MAX_FRAME_BYTES:
            raise MigrationError(f"{self.name} sent an invalid {name} response size")
        reply = Reader(self.read(size))
        try:
            if reply.i32() != self.correlation:
                raise MigrationError(f"{self.name} answered {name} out of order")
            if api in FLEXIBLE_APIS:
                reply.tagged_fields()
            result = parse(reply, version)
            reply.finish()
        except WireError as exc:
            raise MigrationError(f"{self.name} sent a malformed {name} v{version} response: {exc}") from exc
        return result

    def authenticate(self, username, password):
        error, mechanisms = self.request(17, lambda version: wire_string("PLAIN"),
                                         lambda reply, version: (reply.i16(), reply.array(reply.string)))
        if error:
            offered = ", ".join(mechanisms) or "none"
            raise MigrationError(f"{self.name} refused SASL/PLAIN with {error_name(error)} (mechanisms offered: {offered})")
        token = b"\0" + username.encode("utf-8") + b"\0" + password.encode("utf-8")

        def parse(reply, version):
            result = reply.i16(), reply.string(nullable=True), reply.bytes()
            if version >= 1:
                reply.i64()  # session_lifetime_ms
            return result

        error, message, _ = self.request(36, lambda version: wire_bytes(token), parse)
        if error:
            raise MigrationError(f"{self.name} rejected the SASL/PLAIN login: {message or error_name(error)}")


class KafkaCluster:
    def __init__(self, bootstrap, timeout, credentials):
        self.bootstrap = bootstrap
        self.timeout = timeout
        self.credentials = credentials
        self.connections = {}
        self.brokers = {}

    def close(self):
        for connection in self.connections.values():
            connection.close()
        self.connections.clear()

    def request(self, address, api, body, parse):
        try:
            connection = self.connections.get(address)
            if connection is None:
                # Socket reads outlast the broker-side request timeout so its own error arrives first.
                connection = self.connections[address] = KafkaConnection(address, self.timeout + 10, self.credentials)
            return connection.request(api, body, parse)
        except (OSError, MigrationError) as exc:
            stale = self.connections.pop(address, None)
            if stale is not None:
                stale.close()
            if isinstance(exc, MigrationError):
                raise
            raise MigrationError(f"{show_address(address)}: {exc}") from exc

    def any_request(self, api, body, parse):
        failures = []
        for address in dict.fromkeys([*self.bootstrap, *self.brokers.values()]):
            try:
                return self.request(address, api, body, parse)
            except MigrationError as exc:
                failures.append(str(exc))
        raise MigrationError("; ".join(failures))

    def metadata(self, topics):
        """Describe the named topics, or every topic for None."""
        def body(version):
            names = struct.pack(">i", -1) if topics is None else wire_array(wire_string(name) for name in topics)
            return names + b"\0" * (3 if version >= 8 else 1)

        def parse(reply, version):
            if version >= 3:
                reply.i32()  # throttle_time_ms
            brokers = reply.array(lambda: (reply.i32(), (reply.string(), reply.i32()), reply.string(nullable=True)))
            reply.string(nullable=True)  # cluster_id
            reply.i32()  # controller_id

            def partition():
                error, index, leader = reply.i16(), reply.i32(), reply.i32()
                if version >= 7:
                    reply.i32()  # leader_epoch
                for _ in range(3 if version >= 5 else 2):
                    reply.array(reply.i32)
                return index, (error, leader)

            def topic():
                error, name, internal = reply.i16(), reply.string(), reply.boolean()
                partitions = dict(reply.array(partition))
                if version >= 8:
                    reply.i32()  # topic_authorized_operations
                return name, (error, internal, partitions)

            topics = dict(reply.array(topic))
            if version >= 8:
                reply.i32()  # cluster_authorized_operations
            return {node: address for node, address, _ in brokers}, topics

        self.brokers, result = self.any_request(3, body, parse)
        return result

    def create_topic(self, name, count, replication, configs):
        def body(version):
            factor = replication if replication is not None else -1 if version >= 4 else 1
            return (wire_array([wire_string(name) + struct.pack(">ihi", count, factor, 0) + wire_array(
                        wire_string(key) + wire_string(value) for key, value in sorted(configs.items()))])
                    + struct.pack(">i?", int(self.timeout * 1000), False))

        def parse(reply, version):
            reply.i32()  # throttle_time_ms
            return reply.array(lambda: (reply.string(), reply.i16(), reply.string(nullable=True)))

        results = self.any_request(19, body, parse)
        if [item[0] for item in results] != [name]:
            raise MigrationError("CreateTopics answered for a different topic")
        return results[0][1:]

    def leaders(self, topic, count):
        deadline = time.monotonic() + self.timeout
        while True:
            error, _, partitions = self.metadata([topic]).get(topic, (UNKNOWN_TOPIC_OR_PARTITION, False, {}))
            leaders = {index: self.brokers[leader] for index, (code, leader) in partitions.items()
                       if code in (NONE, 9) and leader in self.brokers}
            if not error and all(index in leaders for index in range(count)):
                return leaders
            if time.monotonic() >= deadline:
                reason = error_name(error) if error else "some partitions have no leader"
                raise MigrationError(f"partition leaders were not ready within {self.timeout:g} seconds ({reason})")
            time.sleep(0.2)

    def describe_configs(self, topics):
        """Return {topic: (error, message, {name: (value, source)})} for the topics' configs."""
        def body(version):
            return (wire_array(struct.pack(">b", TOPIC_RESOURCE) + wire_string(name) + struct.pack(">i", -1) for name in topics)
                    + b"\0" + (b"\0" if version >= 3 else b""))

        def parse(reply, version):
            reply.i32()  # throttle_time_ms

            def entry():
                name, value, _, source, _ = reply.string(), reply.string(nullable=True), reply.boolean(), reply.i8(), reply.boolean()
                reply.array(lambda: (reply.string(), reply.string(nullable=True), reply.i8()))
                if version >= 3:
                    reply.i8(), reply.string(nullable=True)  # config_type, documentation
                return name, (value, source)

            def result():
                error, message, _, name = reply.i16(), reply.string(nullable=True), reply.i8(), reply.string()
                return name, (error, message, dict(reply.array(entry)))

            return dict(reply.array(result))

        return self.any_request(32, body, parse)

    def list_groups(self):
        def parse(reply, version):
            if version >= 1:
                reply.i32()  # throttle_time_ms
            return reply.i16(), reply.array(lambda: (reply.string(), reply.string()))

        groups = {}
        for address in dict.fromkeys(self.brokers.values()):  # each broker lists the groups it coordinates
            error, items = self.request(address, 16, lambda version: b"", parse)
            if error:
                raise MigrationError(f"ListGroups failed on {show_address(address)} with {error_name(error)}")
            groups.update(items)
        return groups

    def offset_fetch(self, group):
        """Return every committed offset of a group as {(topic, partition): (offset, metadata)}."""
        def parse(reply, version):
            if version >= 3:
                reply.i32()  # throttle_time_ms

            def partition():
                index, offset = reply.i32(), reply.i64()
                if version >= 5:
                    reply.i32()  # committed_leader_epoch
                return index, offset, reply.string(nullable=True), reply.i16()

            topics = reply.array(lambda: (reply.string(), reply.array(partition)))
            return reply.i16(), topics

        deadline = time.monotonic() + self.timeout
        while True:
            error, topics = self.request(self.coordinator(group), 9, lambda version: wire_string(group)
                                         + struct.pack(">i", -1), parse)
            if error in COORDINATOR_MOVED and time.monotonic() < deadline:
                time.sleep(0.2)
                continue
            if error:
                raise MigrationError(f"OffsetFetch failed for consumer group {group!r} with {error_name(error)}")
            result = {}
            for topic, partitions in topics:
                for index, offset, metadata, code in partitions:
                    if code:
                        raise MigrationError(f"OffsetFetch failed for consumer group {group!r} topic {topic!r} "
                                             f"partition {index} with {error_name(code)}")
                    if offset >= 0:
                        result[topic, index] = offset, metadata
            return result

    def fetch(self, address, topic, partition, offset, max_bytes, wait_ms):
        """Fetch read_committed data; return (error, high watermark, last stable offset, aborted txns, records)."""
        def body(version):
            request = struct.pack(">iiiib", -1, wait_ms, 1, max_bytes, 1)
            if version >= 7:
                request += struct.pack(">ii", 0, -1)  # no fetch session
            request += wire_array([wire_string(topic) + wire_array([
                struct.pack(">i", partition) + (struct.pack(">i", -1) if version >= 9 else b"") + struct.pack(">q", offset)
                + (struct.pack(">q", -1) if version >= 5 else b"") + struct.pack(">i", max_bytes)])])
            if version >= 7:
                request += wire_array([])  # forgotten_topics_data
            return request + (wire_string("") if version >= 11 else b"")

        def parse(reply, version):
            reply.i32()  # throttle_time_ms
            if version >= 7:
                error = reply.i16()
                reply.i32()  # session_id
                if error:
                    raise MigrationError(f"Fetch failed with {error_name(error)}")

            def item():
                index, error, watermark, stable = reply.i32(), reply.i16(), reply.i64(), reply.i64()
                if version >= 5:
                    reply.i64()  # log_start_offset
                aborted = reply.array(lambda: (reply.i64(), reply.i64()), nullable=True) or []
                if version >= 11:
                    reply.i32()  # preferred_read_replica
                return index, (error, watermark, stable, aborted, reply.bytes(nullable=True) or b"")

            replies = {(name, index): value for name, items in reply.array(lambda: (reply.string(), reply.array(item)))
                       for index, value in items}
            if (topic, partition) not in replies:
                raise MigrationError("Fetch answered for a different partition")
            return replies[topic, partition]

        return self.request(address, 1, body, parse)

    def list_offsets(self, topic, count, leaders, timestamp, isolation=0):
        def body(members):
            return lambda version: struct.pack(">ib", -1, isolation) + wire_array([
                wire_string(topic) + wire_array(struct.pack(">i", partition) + (b"\xff" * 4 if version >= 4 else b"")
                                                + struct.pack(">q", timestamp) for partition in members)])

        def parse(reply, version):
            if version >= 2:
                reply.i32()  # throttle_time_ms

            def partition():
                index, error, _, offset = reply.i32(), reply.i16(), reply.i64(), reply.i64()
                if version >= 4:
                    reply.i32()  # leader_epoch
                return index, (error, offset)

            return {index: item for _, items in reply.array(lambda: (reply.string(), reply.array(partition)))
                    for index, item in items}

        offsets, deadline = {}, time.monotonic() + self.timeout
        while len(offsets) < count:
            moved = False
            for address, members in by_leader(leaders, [index for index in range(count) if index not in offsets]):
                replies = self.request(address, 2, body(members), parse)
                for index in members:
                    error, offset = replies.get(index, (None, -1))
                    if error == NONE:
                        offsets[index] = offset
                    elif error in LEADER_MOVED and time.monotonic() < deadline:
                        moved = True
                    else:
                        raise MigrationError(f"partition {index}: ListOffsets failed with "
                                             f"{'no answer' if error is None else error_name(error)}")
            if moved:
                time.sleep(0.2)
                leaders = self.leaders(topic, count)
        return offsets

    def produce(self, address, topic, copies):
        def body(version):
            return (wire_string(None) + struct.pack(">hi", -1, int(self.timeout * 1000)) + wire_array([
                wire_string(topic) + wire_array(struct.pack(">i", copy.partition) + wire_bytes(copy.pending[2])
                                                for copy in copies)]))

        def parse(reply, version):
            def partition():
                index, error, base, append_time = reply.i32(), reply.i16(), reply.i64(), reply.i64()
                if version >= 5:
                    reply.i64()  # log_start_offset
                details = []
                if version >= 8:
                    details = [message for _, message in reply.array(lambda: (reply.i32(), reply.string(nullable=True)))]
                    details.append(reply.string(nullable=True))
                return index, (error, base, append_time, "; ".join(item for item in details if item))

            replies = {index: item for _, items in reply.array(lambda: (reply.string(), reply.array(partition)))
                       for index, item in items}
            reply.i32()  # throttle_time_ms
            return replies

        return self.request(address, 0, body, parse)

    def coordinator(self, group):
        def body(version):
            return wire_string(group) + (b"\0" if version >= 1 else b"")

        def parse(reply, version):
            if version >= 1:
                reply.i32()  # throttle_time_ms
            error = reply.i16()
            message = reply.string(nullable=True) if version >= 1 else None
            return error, message, reply.i32(), (reply.string(), reply.i32())

        deadline = time.monotonic() + self.timeout
        while True:
            error, message, _, address = self.any_request(10, body, parse)
            if error == NONE:
                return address
            if error not in COORDINATOR_MOVED or time.monotonic() >= deadline:
                raise MigrationError(f"FindCoordinator failed with {error_name(error)}" + (f": {message}" if message else ""))
            time.sleep(0.2)

    def group_activity(self, group):
        """Return the group's state and member count, or None when the coordinator cannot describe it."""
        def classic(reply, version):
            if version >= 1:
                reply.i32()  # throttle_time_ms

            def described():
                error, _, state = reply.i16(), reply.string(), reply.string()
                reply.string(), reply.string()  # protocol_type, protocol_data
                members = reply.array(lambda: (reply.string(), reply.string(nullable=True) if version >= 4 else None,
                                               reply.string(), reply.string(), reply.bytes(), reply.bytes()))
                if version >= 3:
                    reply.i32()  # authorized_operations
                return error, state, len(members)

            return reply.array(described)

        def consumer(reply, version):
            reply.i32()  # throttle_time_ms
            if reply.unsigned_varint() != 2:
                raise WireError("expected one group")
            error = reply.i16()
            reply.string(nullable=True, compact=True)  # error_message
            reply.string(compact=True)  # group_id
            state = reply.string(compact=True)
            reply.i32(), reply.i32(), reply.string(compact=True)  # group and assignment epochs, assignor
            members = reply.unsigned_varint() - 1
            reply.read(reply.remaining)  # member details are not needed
            return [(error, state, members)]

        encoded = group.encode("utf-8")
        result = self.describe(group, 15, lambda version: wire_array([wire_string(group)])
                               + (b"\0" if version >= 3 else b""), classic)
        if result is None or result == ("Dead", 0):
            # Kafka reports a group that uses the KIP-848 consumer protocol as Dead to classic DescribeGroups.
            result = self.describe(group, 69, lambda version: wire_uint(2) + wire_uint(len(encoded) + 1) + encoded
                                   + b"\0\0", consumer)
        return result

    def describe(self, group, api, body, parse):
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                described = self.request(self.coordinator(group), api, body, parse)
            except UnsupportedApi:
                return None
            if len(described) != 1:
                raise MigrationError(f"{MIGRATE_APIS[api][0]} answered for a different group")
            error, state, members = described[0]
            if error == NONE:
                return state, members
            if error == 69:  # GROUP_ID_NOT_FOUND: no group of this protocol type
                return None
            if error not in COORDINATOR_MOVED or time.monotonic() >= deadline:
                raise MigrationError(f"{MIGRATE_APIS[api][0]} failed for consumer group {group!r} with {error_name(error)}")
            time.sleep(0.2)

    def commit(self, address, group, offsets):
        def body(version):
            header = wire_string(group) + struct.pack(">i", -1) + wire_string("")  # a standalone commit
            if version >= 7:
                header += wire_string(None)  # group_instance_id
            if version <= 4:
                header += struct.pack(">q", -1)  # retention_time_ms: the broker default
            return header + wire_array(wire_string(topic) + wire_array(
                struct.pack(">iq", partition, offset) + (b"\xff" * 4 if version >= 6 else b"") + wire_string(metadata)
                for partition, offset, metadata in items
            ) for topic, items in offsets.items())

        def parse(reply, version):
            if version >= 3:
                reply.i32()  # throttle_time_ms
            return {(topic, index): error for topic, items in reply.array(
                lambda: (reply.string(), reply.array(lambda: (reply.i32(), reply.i16())))) for index, error in items}

        return self.request(address, 8, body, parse)


def by_leader(leaders, items, key=lambda item: item):
    routes = {}
    for item in items:
        routes.setdefault(leaders[key(item)], []).append(item)
    return routes.items()


class KafkaSnapshot:
    """One read transaction over a Kafka test server SQLite file."""

    TABLES = {"kafka_cluster", "kafka_topics", "kafka_partitions", "kafka_batches", "kafka_timestamps",
              "kafka_committed_offsets"}

    def __init__(self, path):
        if not path.is_file():
            raise ValueError(f"Source database not found: {path}")
        self.db = sqlite3.connect(f"file:{quote(str(path.absolute()))}?mode=ro", uri=True, isolation_level=None)
        try:
            self.db.execute("BEGIN")
            tables = {name for name, in self.db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            if not self.TABLES <= tables:
                raise ValueError("Expected a Kafka test server SQLite persistence file")
            columns = {row[1] for row in self.db.execute("PRAGMA table_info(kafka_partitions)")}
            self.topics = {name: [] for name, in self.db.execute("SELECT name FROM kafka_topics ORDER BY name")}
            self.starts = {name: [] for name in self.topics}
            self.configs = {name: {} for name in self.topics}
            for topic, partition, end, first in self.db.execute(
                    f"SELECT topic, partition, next_offset, {'log_start_offset' if 'log_start_offset' in columns else 0} "
                    "FROM kafka_partitions ORDER BY topic, partition"):
                ends = self.topics[topic]
                if partition != len(ends):
                    raise ValueError(f"Source topic {topic!r} has non-contiguous partition numbers")
                ends.append(end)
                self.starts[topic].append(first)
            if "kafka_topic_configs" in tables:
                for topic, name, value in self.db.execute("SELECT topic, name, value FROM kafka_topic_configs ORDER BY 1, 2"):
                    self.configs[topic][name] = value
            self.groups = {}
            for group, topic, partition, offset, metadata in self.db.execute("""
                SELECT group_id, topic, partition, offset, metadata FROM kafka_committed_offsets
                ORDER BY group_id, topic, partition
            """):
                self.groups.setdefault(group, []).append((topic, partition, offset, metadata))
        except BaseException:
            self.db.close()
            raise

    def batches(self, topic, partition):
        return iter(self.db.execute("""
            SELECT base_offset, last_offset, data FROM kafka_batches WHERE topic = ? AND partition = ? ORDER BY last_offset
        """, (topic, partition)))


CODEC_NAMES = {0: "uncompressed", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}


def wire_varint(value):
    return wire_uint((value << 1) ^ (value >> 63))


def renumber(records, count, keep):
    """Re-encode the kept records (by offset delta) with consecutive offset deltas, as a producer writes them."""
    reader, output = Reader(records), bytearray()
    index = 0
    for _ in range(count):
        record = Reader(reader.read(reader.varint()))
        attributes = record.read(1)
        start = record.position
        record.varint(64)  # timestamp delta, relative to the unchanged base timestamp
        stamp = record.data[start:record.position]
        if record.varint() in keep:
            body = bytes(attributes) + bytes(stamp) + wire_varint(index) + bytes(record.read(record.remaining))
            output += wire_varint(len(body)) + body
            index += 1
    return bytes(output)


def producer_batch(data, log_start=0):
    """Turn a stored batch into one that a plain producer could send, with the source offset delta of each record.

    Broker-written batches (imported ones) may carry producer state, transaction or LogAppendTime flags, offset gaps
    left by compaction, and records below the log start offset. Control and empty batches return None.
    """
    batch = bytes(8) + data[8:12] + b"\xff" * 4 + data[16:]
    base = struct.unpack_from(">q", data)[0]
    parsed, = read_batches(batch, client=False)
    *_, base_ts, _, _, _, _, count = BATCH_HEADER.unpack_from(batch)
    codec = parsed.attributes & 7
    if parsed.attributes & 32 or not count:
        return None, []
    contiguous = count == parsed.last_delta + 1 and base >= log_start
    if parsed.records is None:
        if not contiguous:
            raise ValueError(f"has offset gaps or records below the log start offset in a {CODEC_NAMES[codec]} payload, "
                             "which migrate cannot rewrite")
        deltas = list(range(count))
        if parsed.attributes == codec and parsed.producer_id == -1:
            return batch, deltas
        return bytes(restamp(batch, attributes=codec, producer=(-1, -1, -1))), deltas
    kept = [(delta, stamp) for delta, stamp, _ in parsed.records if base + delta >= log_start]
    if not kept:
        return None, []
    deltas, max_ts = [delta for delta, _ in kept], max(stamp for _, stamp in kept)
    if contiguous:
        if parsed.attributes == codec and parsed.producer_id == -1:
            return batch, deltas
        return bytes(restamp(batch, attributes=codec, max_timestamp=max_ts, producer=(-1, -1, -1))), deltas
    payload = batch[BATCH_HEADER.size:]
    if codec == 1:
        payload = gzip.compress(renumber(gzip.decompress(payload), count, set(deltas)), mtime=0)
    else:
        payload = renumber(payload, count, set(deltas))
    body = struct.pack(">hiqqqhii", codec, len(kept) - 1, base_ts, max_ts, -1, -1, -1, len(kept)) + payload
    return struct.pack(">qiibI", 0, len(body) + 9, -1, 2, crc32c(body)) + body, deltas


@dataclass
class PartitionCopy:
    partition: int
    source_end: int
    target_start: int
    rows: object = None
    pending: tuple | None = None
    ranges: list = field(default_factory=list)
    records: int = 0
    batches: int = 0
    stalled: float | None = None
    error: str | None = None
    log_start: int = 0

    def __post_init__(self):
        self.target_end = self.target_start

    def load(self):
        self.pending = None
        while self.error is None:
            row = next(self.rows, None)
            if row is None:
                return
            base, _, data = row
            try:
                batch, deltas = producer_batch(data, self.log_start)
            except BatchError:
                self.error = f"the source batch at offset {base} is corrupt"
            except ValueError as exc:
                self.error = f"the source batch at offset {base} {exc}"
            else:
                if batch is not None:
                    self.pending = base, [base + delta for delta in deltas], batch
                    return

    def advance(self, target):
        _, sources, _ = self.pending
        for index, source in enumerate(sources):
            last = self.ranges[-1] if self.ranges else None
            if last and last[0] + last[2] == source and last[1] + last[2] == target + index:
                last[2] += 1
            else:
                self.ranges.append([source, target + index, 1])
        self.target_end = target + len(sources)
        self.records += len(sources)
        self.batches += 1
        self.stalled = None
        self.load()

    def translate(self, offset):
        """Map a source offset to the target offset of the same record, or of the next record after a gap."""
        if not self.log_start <= offset <= self.source_end:
            return None
        index = bisect.bisect_right(self.ranges, offset, key=lambda item: item[0]) - 1
        if index >= 0 and offset < self.ranges[index][0] + self.ranges[index][2]:
            source, target, _ = self.ranges[index]
            return target + offset - source
        return self.ranges[index + 1][1] if index + 1 < len(self.ranges) else self.target_end


def replay_topic(cluster, topic, copies, leaders):
    """Send each partition's batches in order, one batch per partition per Produce request."""
    log_append_time = False
    active = [copy for copy in copies if copy.pending]
    while active:
        moved = False
        for address, members in by_leader(leaders, active, key=lambda copy: copy.partition):
            while members:
                chunk, size = [], 0
                while members and (not chunk or size + len(members[0].pending[2]) <= MIGRATE_REQUEST_BYTES):
                    chunk.append(members.pop(0))
                    size += len(chunk[-1].pending[2])
                try:
                    replies = cluster.produce(address, topic, chunk)
                except MigrationError as exc:
                    for copy in chunk:
                        copy.error = f"{exc} (the last batch may or may not have been written)"
                    continue
                for copy in chunk:
                    error, base, append_time, detail = replies.get(copy.partition, (None, -1, -1, ""))
                    if error == NONE and base >= 0:
                        log_append_time = log_append_time or append_time != -1
                        copy.advance(base)
                    elif error in LEADER_MOVED:
                        copy.stalled = copy.stalled or time.monotonic()
                        if time.monotonic() - copy.stalled < cluster.timeout:
                            moved = True
                        else:
                            copy.error = f"Produce kept failing with {error_name(error)} for {cluster.timeout:g} seconds"
                    else:
                        hint = "; raise max.message.bytes on the target topic" if error in (10, 18) else ""
                        reason = "no answer" if error is None else error_name(error)
                        copy.error = (f"Produce failed at source offset {copy.pending[0]} with {reason}"
                                      + (f": {detail}" if detail else "") + hint)
        active = [copy for copy in active if copy.pending and copy.error is None]
        if moved and active:
            time.sleep(0.2)
            try:
                leaders = cluster.leaders(topic, len(copies))
            except MigrationError as exc:
                for copy in active:
                    copy.error = str(exc)
                break
    return log_append_time


def same_config(name, left, right):
    if left is None or right is None:
        return left == right
    try:
        left, right = parse_config(name, left), parse_config(name, right)
    except (KeyError, ConfigError):
        return left.strip() == right.strip()
    return sorted(left) == sorted(right) if isinstance(left, list) else left == right


def show_configs(configs):
    return ", ".join(f"{name}={value}" for name, value in sorted(configs.items()))


def migrate_topic(cluster, snapshot, topic, options, plan):
    ends, configs = snapshot.topics[topic], snapshot.configs[topic]
    count = len(ends)
    error, internal, partitions = cluster.metadata([topic]).get(topic, (UNKNOWN_TOPIC_OR_PARTITION, False, {}))
    created = error == UNKNOWN_TOPIC_OR_PARTITION
    if created and options.dry_run:
        plan.append(f"would create topic {topic!r} with {count} partitions"
                    + (f" and configs {show_configs(configs)}" if configs else ""))
    elif created:
        error, message = cluster.create_topic(topic, count, options.replication_factor, configs)
        created = error != TOPIC_ALREADY_EXISTS
        if error not in (NONE, TOPIC_ALREADY_EXISTS):
            raise MigrationError(f"CreateTopics failed with {error_name(error)}" + (f": {message}" if message else ""))
        if not created:
            error, internal, partitions = cluster.metadata([topic]).get(topic, (UNKNOWN_TOPIC_OR_PARTITION, False, {}))
    if not created:
        if error not in (NONE, 5):
            raise MigrationError(f"Metadata failed with {error_name(error)}")
        if internal:
            raise MigrationError("the target topic is an internal Kafka topic")
        if sorted(partitions) != list(range(count)):
            raise MigrationError(f"the target topic has {len(partitions)} partitions but the source has {count}")
        if configs:
            error, message, entries = cluster.describe_configs([topic]).get(topic, (UNKNOWN_TOPIC_OR_PARTITION, None, {}))
            if error:
                raise MigrationError(f"DescribeConfigs failed with {error_name(error)}" + (f": {message}" if message else ""))
            conflicts = [f"{name}={entries[name][0]} (source {value})" if name in entries else f"no {name} (source {value})"
                         for name, value in configs.items() if not same_config(name, entries.get(name, (None,))[0], value)]
            if conflicts:
                raise MigrationError(f"the target topic has different configs: {', '.join(conflicts)}; "
                                     "alter the target topic or delete it")
    if created and options.dry_run:
        latest = [0] * count
    else:
        leaders = cluster.leaders(topic, count)
        earliest = cluster.list_offsets(topic, count, leaders, -2)
        latest = cluster.list_offsets(topic, count, leaders, -1)
        held = sum(latest[index] - earliest[index] for index in range(count))
        if held and not options.allow_non_empty_topics:
            raise MigrationError(f"the target topic already holds {held} records; delete it or pass --allow-non-empty-topics")
    copies = [PartitionCopy(index, ends[index], latest[index], snapshot.batches(topic, index), log_start=start)
              for index, start in enumerate(snapshot.starts[topic])]
    for copy in copies:
        copy.load()
    if options.dry_run:
        for copy in copies:
            while copy.pending:
                copy.advance(copy.target_end)
            if copy.records and copy.error is None:
                plan.append(f"would append {copy.records} records in {copy.batches} batches to {topic!r} partition "
                            f"{copy.partition} from target offset {copy.target_start}")
    elif replay_topic(cluster, topic, copies, leaders):
        print(f"Kafka migrate: warning: topic {topic!r} uses LogAppendTime on the target, "
              "so the broker replaced the source CreateTime timestamps", file=sys.stderr)
    return copies


def migrate_groups(cluster, groups, copies, dry_run, plan):
    """Translate committed offsets through the offset map and commit them as standalone (generation -1) commits."""
    results = []
    for group, rows in groups.items():
        entries, outcome = {}, []
        for topic, partition, offset, metadata in rows:
            if topic not in copies:
                continue
            copy = copies[topic][partition]
            target = copy.translate(offset) if copy.error is None else None
            item = {"group": group, "topic": topic, "partition": partition, "source_offset": offset,
                    "target_offset": target, "metadata": metadata, "error": None}
            if copy.error is not None:
                item["error"] = "skipped because the partition was not fully migrated"
            elif target is None:
                item["error"] = (f"offset {offset} does not point into the source log, which spans offsets "
                                 f"{copy.log_start} to {copy.source_end}")
            else:
                entries[topic, partition] = item
            outcome.append(item)
        if not outcome:
            continue
        if dry_run:
            plan += [f"would commit offset {item['target_offset']} for consumer group {group!r} on {topic!r} partition "
                     f"{partition}" for (topic, partition), item in sorted(entries.items())]
            entries = {}
        deadline = time.monotonic() + cluster.timeout
        try:
            while entries:
                address = cluster.coordinator(group)
                offsets = {}
                for (topic, partition), item in sorted(entries.items()):
                    offsets.setdefault(topic, []).append((partition, item["target_offset"], item["metadata"]))
                replies = cluster.commit(address, group, offsets)
                retry = {}
                for key, item in entries.items():
                    error = replies.get(key)
                    if error in COORDINATOR_MOVED and time.monotonic() < deadline:
                        retry[key] = item
                    elif error != NONE:
                        hint = "; the group is active on the target, stop its consumers first" if error in GROUP_ACTIVE else ""
                        item["error"] = f"OffsetCommit failed with {'no answer' if error is None else error_name(error)}{hint}"
                entries = retry
                if entries:
                    time.sleep(0.2)
        except MigrationError as exc:
            for item in entries.values():
                item["error"] = str(exc)
        results.append((group, outcome))
    return results


def read_credentials(args):
    if args.username is None:
        return None
    return args.username, args.password_file.expanduser().read_text(encoding="utf-8").split("\n", 1)[0].rstrip("\r")


def bootstrap_servers(text):
    bootstrap = [broker_address(item) for item in text.split(",") if item.strip()]
    if not bootstrap:
        raise ValueError("--bootstrap-server is empty")
    return bootstrap


def add_connection_arguments(parser):
    parser.add_argument("--username", help="SASL/PLAIN user name (plaintext transport)")
    parser.add_argument("--password-file", type=Path, help="file whose first line is the SASL/PLAIN password")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="seconds to wait for leaders, coordinators, and acknowledgements (default: %(default)g)")


def check_connection_arguments(parser, args):
    if (args.username is None) != (args.password_file is None):
        parser.error("--username and --password-file must be given together")
    if not 0 < args.timeout <= 3600:
        parser.error("--timeout must be between 0 and 3600 seconds")


def migrate_main(argv):
    parser = argparse.ArgumentParser(
        prog="kafka_server.py migrate",
        description="Replay a Kafka test server SQLite snapshot into a running Kafka cluster.",
    )
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--bootstrap-server", required=True, help="target broker HOST:PORT; separate several with commas")
    parser.add_argument("--topic", action="append", help="migrate only this topic; repeat for more (default: all)")
    parser.add_argument("--replication-factor", type=int, help="replication factor of created topics (default: the broker's)")
    parser.add_argument("--offset-map", type=Path, help="new JSON file that receives the source-to-target offset map")
    parser.add_argument("--allow-non-empty-topics", action="store_true",
                        help="append to existing target topics that already hold records")
    parser.add_argument("--dry-run", action="store_true",
                        help="inspect the source and the target and print the plan without writing to the target")
    add_connection_arguments(parser)
    args = parser.parse_args(argv)
    if args.replication_factor is not None and not 1 <= args.replication_factor <= 32767:
        parser.error("--replication-factor must be between 1 and 32767")
    if args.dry_run and args.offset_map is not None:
        parser.error("--offset-map records a real run; it cannot be combined with --dry-run")
    check_connection_arguments(parser, args)
    plan = []
    try:
        bootstrap = bootstrap_servers(args.bootstrap_server)
        credentials = read_credentials(args)
        snapshot = KafkaSnapshot(args.db_file.expanduser())
        cluster = KafkaCluster(bootstrap, args.timeout, credentials)
        offset_map = None
        try:
            if args.offset_map is not None:
                path = args.offset_map.expanduser().absolute()
                if os.path.lexists(path):
                    raise FileExistsError(f"Offset map already exists: {path}")
                path.parent.mkdir(parents=True, exist_ok=True)
                offset_map = path.open("x", encoding="utf-8")  # reserved now so a late write cannot fail on access
            selected = list(dict.fromkeys(args.topic or snapshot.topics))
            missing = [name for name in selected if name not in snapshot.topics]
            if missing:
                raise ValueError(f"Topic not found in the source: {', '.join(missing)}")
            cluster.metadata([])
            for group, rows in snapshot.groups.items():
                if any(row[0] in selected for row in rows):
                    activity = cluster.group_activity(group)
                    if activity and (activity[1] or activity[0] not in ("Empty", "Dead")):
                        raise MigrationError(f"consumer group {group!r} is {activity[0]} on the target with "
                                             f"{activity[1]} members; stop its consumers first")
            copies, failures = {}, []
            for topic in selected:
                ends = snapshot.topics[topic]
                try:
                    copies[topic] = migrate_topic(cluster, snapshot, topic, args, plan)
                except MigrationError as exc:
                    copies[topic] = [PartitionCopy(index, end, None, error=str(exc)) for index, end in enumerate(ends)]
                    failures.append(f"topic {topic!r}: {exc}")
                    continue
                failures += [f"topic {topic!r} partition {copy.partition}: {copy.error}"
                             for copy in copies[topic] if copy.error is not None]
            groups = migrate_groups(cluster, snapshot.groups, copies, args.dry_run, plan)
            failures += [f"group {group!r} topic {item['topic']!r} partition {item['partition']}: {item['error']}"
                         for group, items in groups for item in items if item["error"] is not None]
            if offset_map is not None:
                json.dump({
                    "bootstrap_server": args.bootstrap_server,
                    "partitions": [{
                        "topic": topic, "partition": copy.partition, "source_end": copy.source_end,
                        "target_start": copy.target_start, "target_end": copy.target_end, "records": copy.records,
                        "ranges": [{"source_offset": source, "target_offset": target, "count": count}
                                   for source, target, count in copy.ranges],
                        "error": copy.error,
                    } for topic, items in copies.items() for copy in items],
                    "group_offsets": [item for _, items in groups for item in items],
                }, offset_map, indent=2)
                offset_map.write("\n")
                offset_map.close()
                offset_map = None
        finally:
            cluster.close()
            snapshot.db.close()
            if offset_map is not None:
                offset_map.close()
                os.unlink(offset_map.name)
    except (OSError, ValueError, sqlite3.Error, MigrationError) as exc:
        parser.exit(1, f"Kafka migrate failed: {exc}\n")
    for line in plan:
        print(f"Kafka migrate: {line}")
    partitions = [copy for items in copies.values() for copy in items]
    committed = sum(item["error"] is None for _, items in groups for item in items)
    action = "Dry run: would migrate" if args.dry_run else "Migrated"
    print(f"{action} {sum(copy.records for copy in partitions)} records in {sum(copy.batches for copy in partitions)} "
          f"batches across {len(partitions)} partitions of {len(copies)} topics and "
          f"{'commit' if args.dry_run else 'committed'} {committed} offsets "
          f"for {len(groups)} consumer groups to {args.bootstrap_server}", flush=True)
    if failures:
        for failure in failures:
            print(f"Kafka migrate: {failure}", file=sys.stderr)
        failed_partitions = sum(copy.error is not None for copy in partitions)
        failed_groups = sum(any(item["error"] is not None for item in items) for _, items in groups)
        parser.exit(1, f"Kafka migrate failed: {failed_partitions} of {len(partitions)} partitions and "
                       f"{failed_groups} of {len(groups)} consumer groups "
                       f"{'would fail' if args.dry_run else 'failed'}\n")
    return 0


# Capture a running Kafka cluster


@dataclass
class PartitionImport:
    topic: str
    partition: int
    start: int
    end: int
    records: int = 0
    batches: int = 0
    markers: int = 0
    aborted: int = 0
    ranges: list = field(default_factory=list)

    def cover(self, base, deltas):
        for delta in deltas:
            last = self.ranges[-1] if self.ranges else None
            if last and last[0] + last[2] == base + delta:
                last[2] += 1
            else:
                self.ranges.append([base + delta, base + delta, 1])


def split_batches(records, where):
    """Return the complete batches of a fetched record set; a broker may cut the last one short."""
    position, batches = 0, []
    while len(records) - position >= 12:
        base, length = struct.unpack_from(">qi", records, position)
        if length < BATCH_HEADER.size - 12:
            raise MigrationError(f"{where}: the batch at offset {base} has an invalid length")
        if position + 12 + length > len(records):
            break
        batches.append((base, memoryview(records)[position:position + 12 + length]))
        position += 12 + length
    return batches


def import_partition(cluster, store, item, leaders, count, timeout):
    """Copy one partition's batches between its log start and high watermark, keeping their source offsets."""
    offset, pending, aborting = item.start, [], set()
    max_bytes, deadline, producer_ids = 1024 * 1024, time.monotonic() + timeout, set()
    where = f"topic {item.topic!r} partition {item.partition}"
    while offset < item.end:
        error, _, stable, aborted, records = cluster.fetch(leaders[item.partition], item.topic, item.partition, offset,
                                                           max_bytes, 500)
        if error in LEADER_MOVED and time.monotonic() < deadline:
            time.sleep(0.2)
            leaders = cluster.leaders(item.topic, count)
            continue
        if error == OFFSET_OUT_OF_RANGE:
            raise MigrationError(f"{where}: offset {offset} is no longer in the source log, which changed during the import")
        if error:
            raise MigrationError(f"{where}: Fetch failed with {error_name(error)}")
        pending = sorted(set(pending).union((first, producer) for producer, first in aborted))
        progressed = False
        for base, batch in split_batches(records, where):
            try:
                parsed, = read_batches(batch, client=False)
            except BatchError as exc:
                magic = batch[16] if len(batch) > 16 else None
                reason = f"uses message format v{magic}" if magic in (0, 1) else "is corrupt or too large for this server"
                raise MigrationError(f"{where}: the batch at offset {base} {reason}") from exc
            last = base + parsed.last_delta
            if last < offset:
                continue
            if base >= item.end:
                offset = item.end
                break
            while pending and pending[0][0] <= last:
                aborting.add(pending.pop(0)[1])
            if parsed.attributes & 32:
                item.markers += 1
                aborting.discard(parsed.producer_id)
            elif parsed.attributes & 16 and parsed.producer_id in aborting:
                item.aborted += parsed.count
            else:
                store.store_batch(item.topic, item.partition, base, last, batch, parsed.timestamps(), item.start)
                deltas = [delta for delta, _, _ in parsed.records] if parsed.records is not None else range(parsed.last_delta + 1)
                item.cover(base, deltas)
                item.records += parsed.count
                item.batches += 1
                producer_ids.add(parsed.producer_id)
            offset, progressed = last + 1, True
        if progressed:
            deadline = time.monotonic() + timeout
            continue
        if records and max_bytes < MAX_FETCH_BYTES:
            max_bytes *= 2  # the next batch is larger than the partition fetch size
        elif time.monotonic() >= deadline:
            if stable <= offset:
                raise MigrationError(f"{where} has an open transaction: its last stable offset {stable} is below the "
                                     f"high watermark {item.end}; commit or abort it and retry")
            raise MigrationError(f"{where}: no records arrived at offset {offset} within {timeout:g} seconds")
    store.db.execute("UPDATE kafka_partitions SET next_offset = ?, log_start_offset = ? WHERE topic = ? AND partition = ?",
                     (item.end, item.start, item.topic, item.partition))
    return max(producer_ids, default=-1)


def import_configs(topic, entries, warn):
    """Keep the topic-level overrides, the configs that DescribeConfigs reports as DYNAMIC_TOPIC_CONFIG."""
    kept = {}
    for name, (value, source) in sorted(entries.items()):
        if source != DYNAMIC_TOPIC_CONFIG or value is None:
            continue
        try:
            parse_config(name, value)
        except KeyError:
            warn(f"topic {topic!r}: skipped config {name}={value}, which this server does not know")
            continue
        except ConfigError as exc:
            warn(f"topic {topic!r}: skipped config {name}={value}: {exc}")
            continue
        kept[name] = value
    try:
        check_topic_configs(kept)
    except ConfigError as exc:
        raise ValueError(f"topic {topic!r} has configs this server cannot represent: {exc}") from None
    return kept


def import_cluster(cluster, store, args, warn):
    described = cluster.metadata(None)
    if args.topic:
        selected = list(dict.fromkeys(args.topic))
        missing = [name for name in selected if name not in described]
        if missing:
            raise ValueError(f"Topic not found in the source cluster: {', '.join(missing)}")
    else:
        selected = sorted(name for name, (_, internal, _) in described.items() if not internal)
    invalid = [name for name in selected if not valid_topic(name)]
    if invalid:
        raise ValueError(f"Topic names this server cannot represent: {', '.join(invalid)}")
    configs = cluster.describe_configs(selected) if selected else {}
    imports, max_producer = [], -1
    for topic in selected:
        error, message, entries = configs.get(topic, (UNKNOWN_TOPIC_OR_PARTITION, None, {}))
        if error:
            raise MigrationError(f"DescribeConfigs failed for topic {topic!r} with {error_name(error)}"
                                 + (f": {message}" if message else ""))
        count = len(described[topic][2])
        if not 0 < count <= MAX_PARTITIONS or sorted(described[topic][2]) != list(range(count)):
            raise MigrationError(f"topic {topic!r} has {count} partitions, which this server cannot represent")
        leaders = cluster.leaders(topic, count)
        starts = cluster.list_offsets(topic, count, leaders, -2)
        ends = cluster.list_offsets(topic, count, leaders, -1)
        with store.db:
            store.db.execute("BEGIN IMMEDIATE")
            store.add_topic(topic, count, import_configs(topic, entries, warn))
            for index in range(count):
                item = PartitionImport(topic, index, starts[index], ends[index])
                max_producer = max(max_producer, import_partition(cluster, store, item, leaders, count, cluster.timeout))
                imports.append(item)
                if item.markers or item.aborted:
                    warn(f"topic {item.topic!r} partition {index}: skipped {item.markers} transaction markers and "
                         f"{item.aborted} records of aborted transactions; their offsets stay unused")
    groups = list(dict.fromkeys(args.group)) if args.group else sorted(cluster.list_groups())
    committed, offsets_of = [], {}
    ends = {(item.topic, item.partition): item.end for item in imports}
    for group in groups:
        try:
            offsets = cluster.offset_fetch(group)
        except MigrationError as exc:
            if args.group:
                raise
            warn(f"skipped consumer group {group!r}: {exc}")
            continue
        kept = {key: value for key, value in offsets.items() if key in ends}
        if len(kept) < len(offsets) and not args.topic:  # offsets of internal or deleted topics
            warn(f"consumer group {group!r}: skipped {len(offsets) - len(kept)} committed offsets of topics that were "
                 "not imported")
        for (topic, partition), (offset, metadata) in sorted(kept.items()):
            if offset > ends[topic, partition]:
                warn(f"consumer group {group!r} committed offset {offset} on {topic!r} partition {partition}, past the "
                     f"imported end {ends[topic, partition]}")
            committed.append({"group": group, "topic": topic, "partition": partition, "source_offset": offset,
                              "target_offset": offset, "metadata": metadata})
        offsets_of[group] = len(kept)
    with store.db:
        store.db.execute("BEGIN IMMEDIATE")
        store.db.executemany("INSERT INTO kafka_committed_offsets VALUES (?, ?, ?, ?, ?)", (
            (item["group"], item["topic"], item["partition"], item["source_offset"], item["metadata"]) for item in committed))
        if max_producer >= 0:
            # Keep producer ids handed out by this server apart from the imported batches' ids.
            store.db.execute("INSERT INTO kafka_counters VALUES ('producer_id', ?)", (max_producer + 1,))
    return selected, imports, committed, sum(1 for count in offsets_of.values() if count)


def publish_file(source, target):
    """Give a finished file its final name without replacing anything that is already there."""
    try:
        os.link(source, target)
    except FileExistsError:
        raise FileExistsError(f"Destination already exists: {target}") from None
    except OSError as exc:
        if exc.errno not in (errno.EPERM, errno.EACCES, errno.EXDEV, errno.EMLINK, errno.ENOSYS, errno.ENOTSUP,
                             errno.EOPNOTSUPP):
            raise
        if os.path.lexists(target):  # without hard links, a checked rename is the closest equivalent
            raise FileExistsError(f"Destination already exists: {target}") from None
        os.rename(source, target)
    else:
        os.unlink(source)


def import_main(argv):
    parser = argparse.ArgumentParser(
        prog="kafka_server.py import",
        description="Copy topics, record batches, topic configs, and committed group offsets from a running Kafka "
                    "cluster into a new Kafka test server SQLite file.",
    )
    parser.add_argument("--db-file", type=Path, required=True, help="new SQLite persistence file to create")
    parser.add_argument("--bootstrap-server", required=True, help="source broker HOST:PORT; separate several with commas")
    parser.add_argument("--topic", action="append", help="import only this topic; repeat for more (default: all "
                                                         "non-internal topics)")
    parser.add_argument("--group", action="append", help="import the committed offsets of only this consumer group; "
                                                         "repeat for more (default: every group)")
    parser.add_argument("--offset-map", type=Path, help="new JSON file that receives the per-partition offset report")
    add_connection_arguments(parser)
    args = parser.parse_args(argv)
    check_connection_arguments(parser, args)
    warnings = []

    def warn(message):
        warnings.append(message)
        print(f"Kafka import: warning: {message}", file=sys.stderr, flush=True)

    target = args.db_file.expanduser().absolute()
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex[:12]}.tmp")
    try:
        bootstrap = bootstrap_servers(args.bootstrap_server)
        credentials = read_credentials(args)
        if os.path.lexists(target):
            raise FileExistsError(f"Destination already exists: {target}")
        offset_map = None
        if args.offset_map is not None:
            map_path = args.offset_map.expanduser().absolute()
            if os.path.lexists(map_path):
                raise FileExistsError(f"Offset map already exists: {map_path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        cluster = KafkaCluster(bootstrap, args.timeout, credentials)
        store = None
        try:
            store = Store(str(temporary))
            topics, imports, committed, groups = import_cluster(cluster, store, args, warn)
            store.db.close()
            store = None
            for suffix in ("-wal", "-shm", "-journal"):
                leftover = Path(f"{temporary}{suffix}")
                if leftover.exists() and suffix != "-shm":
                    raise OSError(f"SQLite left {leftover.name} behind; the new file is incomplete")
            if args.offset_map is not None:
                map_path.parent.mkdir(parents=True, exist_ok=True)
                offset_map = map_path.open("x", encoding="utf-8")
                json.dump({
                    "bootstrap_server": args.bootstrap_server,
                    "offsets": "Imported batches keep their source offsets, so every target offset equals its source offset.",
                    "partitions": [{
                        "topic": item.topic, "partition": item.partition, "source_start": item.start,
                        "source_end": item.end, "target_start": item.start, "target_end": item.end,
                        "records": item.records, "batches": item.batches, "skipped_transaction_markers": item.markers,
                        "skipped_aborted_records": item.aborted,
                        "ranges": [{"source_offset": source, "target_offset": target_offset, "count": count}
                                   for source, target_offset, count in item.ranges],
                    } for item in imports],
                    "group_offsets": committed,
                }, offset_map, indent=2)
                offset_map.write("\n")
                offset_map.close()
            publish_file(temporary, target)
        except BaseException:
            if offset_map is not None:
                offset_map.close()
                os.unlink(offset_map.name)
            raise
        finally:
            cluster.close()
            if store is not None:
                store.db.close()
            for suffix in ("", "-wal", "-shm", "-journal"):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(f"{temporary}{suffix}")
    except (OSError, ValueError, sqlite3.Error, MigrationError) as exc:
        parser.exit(1, f"Kafka import failed: {exc}\n")
    print(f"Imported {sum(item.records for item in imports)} records in {sum(item.batches for item in imports)} batches "
          f"across {len(imports)} partitions of {len(topics)} topics and {len(committed)} committed offsets of "
          f"{groups} consumer groups from {args.bootstrap_server} into {target}", flush=True)
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] in (["migrate"], ["import"]):
        try:
            return (migrate_main if argv[0] == "migrate" else import_main)(argv[1:])
        except KeyboardInterrupt:
            return 130
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
                server.broker = Broker(store, advertised_host, port, args.auto_create_topics, args.host)
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
