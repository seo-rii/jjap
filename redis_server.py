#!/usr/bin/env python3
"""A single-file, standard-library Redis-compatible test server for Python 3.10+.

Run: python3 redis_server.py --port 6379
Use --port 0 for an ephemeral port. Data is in memory by default. With
--db-file, SAVE and orderly shutdown write an atomic JSON snapshot.
Set REDIS_TEST_PASSWORD or use --password-file to require AUTH.

The server implements RESP2/RESP3 with logical databases. Use --cluster for
Redis Cluster clients that need to discover a single primary with all slots.
The snapshot format is specific to this server. Multi-node clustering,
replication, Sentinel, Lua/functions, modules, and ACL management are outside
its compatibility scope.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import copy
import hmac
import json
import math
import os
import queue
import random
import re
import select
import signal
import socket
import socketserver
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

MAX_BULK = 64 * 1024 * 1024
MAX_ARGUMENTS = 65_536
MAX_LINE = 64 * 1024
MAX_PENDING_REPLIES = 1024
MAX_INT = (1 << 63) - 1
MIN_INT = -(1 << 63)
SLOT_COUNT = 16384
INTEGER = re.compile(rb"(?:0|-?[1-9][0-9]*)\Z")
WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
SYNTAX = "ERR syntax error"


class RedisError(Exception):
    """An error reply. Errors in EXEC are returned alongside other replies."""


class ProtocolError(RedisError):
    """A malformed frame that leaves the socket out of sync."""


class Simple(bytes):
    """A RESP simple string."""


class Push(tuple):
    """A RESP3 push message, represented as an array in RESP2."""


OK = Simple(b"OK")
NULL_ARRAY = object()
SENT = object()


def integer(value: bytes, *, minimum: int = MIN_INT, maximum: int = MAX_INT) -> int:
    if len(value) > 20 or not INTEGER.fullmatch(value):
        raise RedisError("ERR value is not an integer or out of range")
    number = int(value)
    if not minimum <= number <= maximum:
        raise RedisError("ERR value is not an integer or out of range")
    return number


def number(value: bytes) -> float:
    try:
        if value.strip() != value or b"_" in value:
            raise ValueError
        parsed = float(value)
    except (OverflowError, ValueError) as error:
        raise RedisError("ERR value is not a valid float") from error
    if math.isnan(parsed):
        raise RedisError("ERR value is not a valid float")
    return parsed


def format_score(score: float) -> bytes:
    if math.isinf(score):
        return b"-inf" if score < 0 else b"inf"
    text = repr(score)
    if text.endswith(".0"):
        text = text[:-2]
    return ("0" if text == "-0" else text).encode("ascii")


def encode(value: object, protocol: int) -> bytes:
    if isinstance(value, RedisError):
        text = str(value).replace("\r", " ").replace("\n", " ")
        return b"-" + text.encode("utf-8") + b"\r\n"
    if isinstance(value, Simple):
        return b"+" + value + b"\r\n"
    if value is None or value is NULL_ARRAY:
        if protocol == 3:
            return b"_\r\n"
        return b"*-1\r\n" if value is NULL_ARRAY else b"$-1\r\n"
    if isinstance(value, bytes):
        return b"$%d\r\n" % len(value) + value + b"\r\n"
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, float):
        score = format_score(value)
        return b"," + score + b"\r\n" if protocol == 3 else encode(score, protocol)
    if isinstance(value, dict):
        items = [item for pair in value.items() for item in pair]
        prefix, length = (b"%", len(value)) if protocol == 3 else (b"*", len(items))
        return prefix + b"%d\r\n" % length + b"".join(encode(item, protocol) for item in items)
    if isinstance(value, (tuple, list, set)):
        prefix = b"*"
        if protocol == 3 and isinstance(value, (set, Push)):
            prefix = b"~" if isinstance(value, set) else b">"
        return prefix + b"%d\r\n" % len(value) + b"".join(encode(item, protocol) for item in value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def read_line(source) -> bytes:
    line = source.readline(MAX_LINE + 1)
    if len(line) > MAX_LINE or not line.endswith(b"\r\n"):
        raise ProtocolError("ERR Protocol error: expected CRLF")
    return line[:-2]


def read_command(source) -> list[bytes] | None:
    first = source.read(1)
    if not first:
        return None
    if first != b"*":
        if first == b"\n":
            return []
        line = first + source.readline(MAX_LINE)
        if len(line) > MAX_LINE or not line.endswith(b"\n"):
            raise ProtocolError("ERR Protocol error: invalid inline request")
        try:
            return split_inline(line.removesuffix(b"\n").removesuffix(b"\r"))
        except ValueError as error:
            raise ProtocolError("ERR Protocol error: invalid inline quoting") from error
    try:
        count = integer(read_line(source), minimum=1, maximum=MAX_ARGUMENTS)
        items = []
        total = 0
        for _ in range(count):
            if source.read(1) != b"$":
                raise ProtocolError("ERR Protocol error: expected bulk string")
            size = integer(read_line(source), minimum=0, maximum=MAX_BULK)
            total += size
            if total > MAX_BULK:
                raise ProtocolError("ERR Protocol error: request is too large")
            argument = source.read(size)
            if len(argument) != size or source.read(2) != b"\r\n":
                raise ProtocolError("ERR Protocol error: invalid bulk string")
            items.append(argument)
        return items
    except ProtocolError:
        raise
    except RedisError as error:
        raise ProtocolError("ERR Protocol error: invalid length") from error


def split_inline(line: bytes) -> list[bytes]:
    # Keep the inline protocol binary-safe, including hexadecimal escapes.
    args = []
    position = 0
    while position < len(line):
        while position < len(line) and line[position:position + 1].isspace():
            position += 1
        if position == len(line):
            break
        current = bytearray()
        quote = 0
        while position < len(line):
            char = line[position]
            position += 1
            if quote:
                if char == quote:
                    quote = 0
                    if position < len(line) and not line[position:position + 1].isspace():
                        raise ValueError
                    break
                if char == 92 and position < len(line):
                    escaped = line[position]
                    if quote == 34:
                        position += 1
                        if escaped == 120 and position + 2 <= len(line):
                            digits = line[position:position + 2]
                            if re.fullmatch(rb"[0-9a-fA-F]{2}", digits):
                                current.append(int(digits, 16))
                                position += 2
                                continue
                        char = {110: 10, 114: 13, 116: 9, 98: 8, 97: 7}.get(escaped, escaped)
                    elif escaped == 39:
                        char = escaped
                        position += 1
                current.append(char)
            elif bytes((char,)).isspace():
                break
            elif char in (34, 39):
                quote = char
            else:
                current.append(char)
        if quote:
            raise ValueError
        args.append(bytes(current))
    return args


def milliseconds() -> int:
    return time.time_ns() // 1_000_000


def index_range(length: int, start: int, stop: int) -> slice:
    if start < 0:
        start = max(0, length + start)
    if stop < 0:
        stop += length
    return slice(start, max(0, stop + 1))


def position(length: int, index: int) -> int:
    return index if index >= 0 else length + index


def glob_pattern(pattern: bytes) -> re.Pattern[bytes]:
    """Compile Redis-style byte glob syntax, including escapes and negated classes."""
    fragments = []
    index = 0
    while index < len(pattern):
        char = pattern[index:index + 1]
        index += 1
        if char == b"*":
            if fragments[-1:] != [b".*"]:
                fragments.append(b".*")
        elif char == b"?":
            fragments.append(b".")
        elif char == b"\\" and index < len(pattern):
            fragments.append(re.escape(pattern[index:index + 1]))
            index += 1
        elif char == b"[":
            negate = pattern[index:index + 1] == b"^"
            index += int(negate)
            alternatives = []
            while index < len(pattern) and pattern[index] != 93:
                if pattern[index] == 92 and index + 1 < len(pattern):
                    index += 1
                    alternatives.append(re.escape(pattern[index:index + 1]))
                elif index + 2 < len(pattern) and pattern[index + 1] == 45:
                    lower, upper = sorted((pattern[index], pattern[index + 2]))
                    alternatives.extend((re.escape(bytes((lower,))), b"-", re.escape(bytes((upper,)))))
                    index += 2
                else:
                    alternatives.append(re.escape(pattern[index:index + 1]))
                index += 1
            index += 1
            if alternatives:
                fragments.append(b"[" + (b"^" if negate else b"") + b"".join(alternatives) + b"]")
            else:
                fragments.append(b"." if negate else b"(?!)")
        else:
            fragments.append(re.escape(char))
    return re.compile(b"".join(fragments), re.DOTALL)


def matches(pattern: re.Pattern[bytes] | None, subject: bytes) -> bool:
    return pattern is None or pattern.fullmatch(subject) is not None


def choose(items, count: int):
    values = list(items)
    if count < 0:
        if count < -MAX_ARGUMENTS:
            raise RedisError("ERR requested reply is too large")
        return random.choices(values, k=-count) if values else []
    return random.sample(values, min(count, len(values)))


@dataclass
class Entry:
    kind: bytes
    value: object
    expires: int | None = None


@dataclass(eq=False)
class Session:
    id: int
    authenticated: bool
    protocol: int = 2
    database: int = 0
    name: bytes | None = None
    lib_name: bytes | None = None
    lib_version: bytes | None = None
    pending: list[list[bytes]] | None = None
    broken_transaction: bool = False
    executing: bool = False
    watched: set[tuple[int, bytes]] = field(default_factory=set)
    watch_dirty: bool = False
    subscriptions: set[bytes] = field(default_factory=set)
    patterns: dict[bytes, re.Pattern[bytes]] = field(default_factory=dict)
    closing: bool = False
    outbox: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=MAX_PENDING_REPLIES))
    writable: threading.Event = field(default_factory=threading.Event)
    connection: socket.socket | None = None

    def send(self, value: object) -> None:
        if self.closing:
            return
        try:
            self.outbox.put_nowait(encode(value, self.protocol))
        except queue.Full:
            self.closing = True
            if self.connection is not None:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def dead(self) -> bool:
        if self.closing:
            return True
        if self.connection is None:
            return False
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable and not self.connection.recv(1, socket.MSG_PEEK))
        except OSError:
            return True


COMMANDS = {}


def command(names: str, minimum: int, maximum: int | None = None, *, exact: bool = False):
    """Register command names and arity once, including validation while queuing MULTI."""
    if exact:
        maximum = minimum

    def register(function):
        for name in names.encode("ascii").split():
            assert name not in COMMANDS, name
            COMMANDS[name] = (function, minimum, maximum)
        return function

    return register


class Store:
    def __init__(self, databases: int, password: bytes | None, snapshot: Path | None, cluster: bool):
        self.databases = [{} for _ in range(databases)]
        self.password = password
        self.snapshot = snapshot
        self.cluster = cluster
        self.node_id = uuid.uuid4().hex.encode() + uuid.uuid4().hex[:8].encode()
        self.endpoint: tuple[bytes, int] = (b"", 0)
        self.changed = threading.Condition(threading.RLock())
        self.sessions: set[Session] = set()
        self.watchers: dict[tuple[int, bytes], set[Session]] = {}
        self.next_client_id = 1
        self.stop = False
        self.started = int(time.time())
        self.last_save = self.started
        self.processed = 0
        if snapshot is not None and snapshot.exists():
            self.load()

    def connect(self, connection: socket.socket) -> Session:
        with self.changed:
            session = Session(self.next_client_id, self.password is None, connection=connection)
            self.next_client_id += 1
            self.sessions.add(session)
            return session

    def disconnect(self, session: Session) -> None:
        with self.changed:
            self.unwatch(session)
            self.sessions.discard(session)

    def unwatch(self, session: Session) -> None:
        for key in session.watched:
            watchers = self.watchers[key]
            watchers.discard(session)
            if not watchers:
                del self.watchers[key]
        session.watched.clear()
        session.watch_dirty = False

    def touch(self, database: int, key: bytes) -> None:
        for watcher in self.watchers.get((database, key), ()):
            watcher.watch_dirty = True
        self.changed.notify_all()

    def get(self, database: int, key: bytes, kind: bytes | None = None) -> Entry | None:
        data = self.databases[database]
        entry = data.get(key)
        if entry is not None and entry.expires is not None and entry.expires <= milliseconds():
            del data[key]
            self.touch(database, key)
            entry = None
        if entry is not None and kind is not None and kind != entry.kind:
            raise RedisError(WRONG_TYPE)
        return entry

    def put(self, database: int, key: bytes, entry: Entry) -> None:
        self.databases[database][key] = entry
        self.touch(database, key)

    def delete(self, database: int, key: bytes) -> int:
        if self.get(database, key) is None:
            return 0
        del self.databases[database][key]
        self.touch(database, key)
        return 1

    def live_keys(self, database: int) -> list[bytes]:
        return [key for key in list(self.databases[database]) if self.get(database, key) is not None]

    def value(self, session: Session, key: bytes, kind: bytes, default=None):
        entry = self.get(session.database, key, kind)
        return default if entry is None else entry.value

    def update(self, session: Session, key: bytes, kind: bytes, value, *, remove_empty=False) -> None:
        data = self.databases[session.database]
        existing = data.get(key)
        if remove_empty and not value:
            if existing is not None:
                del data[key]
                self.touch(session.database, key)
        else:
            self.put(session.database, key, Entry(kind, value, existing.expires if existing else None))

    def execute(self, session: Session, request: list[bytes]):
        name, *args = request
        name = name.upper()
        try:
            if not session.authenticated and name not in {b"AUTH", b"HELLO", b"QUIT", b"RESET"}:
                raise RedisError("NOAUTH Authentication required.")
            allowed = {b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"PING", b"QUIT", b"RESET"}
            if session.protocol == 2 and (session.subscriptions or session.patterns) and name not in allowed:
                raise RedisError("ERR command is unavailable while subscribed in RESP2 mode")
            definition = COMMANDS.get(name)
            if definition is None:
                raise RedisError(f"ERR unknown command '{name[:128].decode('utf-8', 'replace').lower()}'")
            function, minimum, maximum = definition
            if len(args) < minimum or (maximum is not None and len(args) > maximum):
                raise RedisError(f"ERR wrong number of arguments for '{name.decode('ascii').lower()}' command")
            if self.cluster and name not in PUBSUB_COMMANDS:
                check_slots(command_keys(name, args))
            if session.pending is not None and name not in {b"MULTI", b"EXEC", b"DISCARD", b"WATCH", b"QUIT", b"RESET"}:
                if name in {b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE", b"HELLO"}:
                    raise RedisError("ERR Command not allowed inside a transaction")
                session.pending.append(request)
                return Simple(b"QUEUED")
            self.processed += 1
            return function(self, session, name, args)
        except RedisError:
            if session.pending is not None and name not in {b"MULTI", b"WATCH"}:
                session.broken_transaction = True
            raise

    def save(self) -> None:
        if self.snapshot is None:
            self.last_save = int(time.time())
            return
        data = []
        for database in range(len(self.databases)):
            rows = []
            for key in self.live_keys(database):
                entry = self.databases[database][key]
                rows.append([to_base64(key), entry.kind.decode(), entry.expires, pack_entry(entry)])
            data.append(rows)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.snapshot.parent, delete=False
            ) as handle:
                temporary = Path(handle.name)
                json.dump({"version": 1, "databases": data}, handle, separators=(",", ":"), allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.snapshot)
        except OSError as error:
            raise RedisError(f"ERR snapshot write failed: {error.strerror}") from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        self.last_save = int(time.time())

    def load(self) -> None:
        with self.snapshot.open(encoding="utf-8") as handle:
            snapshot = json.load(handle)
        if snapshot["version"] != 1:
            raise ValueError("unsupported snapshot version")
        databases = snapshot["databases"]
        if len(databases) > len(self.databases):
            raise ValueError("snapshot contains more databases than configured")
        now = milliseconds()
        for database, rows in zip(self.databases, databases):
            for key, kind, expires, value in rows:
                if expires is None or expires > now:
                    redis_type = kind.encode("ascii")
                    database[from_base64(key)] = Entry(redis_type, unpack_entry(redis_type, value), expires)
        self.last_save = int(self.snapshot.stat().st_mtime)

    @command("PING", 0, 1)
    def ping(self, session, name, args):
        if session.protocol == 2 and (session.subscriptions or session.patterns):
            return [b"pong", args[0] if args else b""]
        return args[0] if args else Simple(b"PONG")

    @command("ECHO", 1, exact=True)
    def echo(self, session, name, args):
        return args[0]

    @command("QUIT", 0, exact=True)
    def quit(self, session, name, args):
        session.send(OK)
        session.closing = True
        return SENT

    @command("RESET", 0, exact=True)
    def reset(self, session, name, args):
        self.unwatch(session)
        session.protocol = 2
        session.database = 0
        session.authenticated = self.password is None
        session.name = session.lib_name = session.lib_version = None
        session.pending = None
        session.broken_transaction = False
        session.subscriptions.clear()
        session.patterns.clear()
        return Simple(b"RESET")

    def authenticate(self, username: bytes, password: bytes) -> None:
        if self.password is None:
            raise RedisError("ERR AUTH called without any password configured for the default user")
        if username != b"default" or not hmac.compare_digest(self.password, password):
            raise RedisError("WRONGPASS invalid username-password pair or user is disabled.")

    @command("AUTH", 1, 2)
    def auth(self, session, name, args):
        self.authenticate(*(args if len(args) == 2 else [b"default", args[0]]))
        session.authenticated = True
        return OK

    @command("HELLO", 0)
    def hello(self, session, name, args):
        protocol = integer(args[0]) if args else session.protocol
        if protocol not in (2, 3):
            raise RedisError("NOPROTO unsupported protocol version")
        authenticated = session.authenticated
        client_name = session.name
        offset = 1
        while offset < len(args):
            option = args[offset].upper()
            if option == b"AUTH" and offset + 2 < len(args):
                self.authenticate(args[offset + 1], args[offset + 2])
                authenticated = True
                offset += 3
            elif option == b"SETNAME" and offset + 1 < len(args):
                client_name = validated_name(args[offset + 1])
                offset += 2
            else:
                raise RedisError(SYNTAX)
        if not authenticated:
            raise RedisError("NOAUTH HELLO must be called with the client already authenticated, or with AUTH")
        session.protocol = protocol
        session.authenticated = authenticated
        session.name = client_name
        return {
            b"server": b"redis", b"version": b"7.2.0", b"proto": protocol,
            b"id": session.id, b"mode": b"cluster" if self.cluster else b"standalone",
            b"role": b"master", b"modules": [],
        }

    @command("CLIENT", 1)
    def client(self, session, name, args):
        subcommand, *params = args
        subcommand = subcommand.upper()
        if subcommand == b"ID" and not params:
            return session.id
        if subcommand == b"GETNAME" and not params:
            return session.name
        if subcommand == b"SETNAME" and len(params) == 1:
            session.name = validated_name(params[0])
            return OK
        if subcommand == b"SETINFO" and len(params) == 2:
            key, value = params
            if key.upper() == b"LIB-NAME":
                session.lib_name = validated_name(value)
            elif key.upper() == b"LIB-VER":
                session.lib_version = validated_name(value)
            else:
                raise RedisError(SYNTAX)
            return OK
        if subcommand == b"LIST" and not params:
            return b"".join(client_line(item) for item in sorted(self.sessions, key=lambda item: item.id))
        if subcommand == b"INFO" and not params:
            return client_line(session)
        raise RedisError(SYNTAX)

    @command("SELECT", 1, exact=True)
    def select(self, session, name, args):
        database = integer(args[0])
        if self.cluster and database != 0:
            raise RedisError("ERR SELECT is not allowed in cluster mode")
        if not 0 <= database < len(self.databases):
            raise RedisError("ERR DB index is out of range")
        session.database = database
        return OK

    @command("TIME", 0, exact=True)
    def time(self, session, name, args):
        seconds, nanos = divmod(time.time_ns(), 1_000_000_000)
        return [str(seconds).encode(), str(nanos // 1000).encode()]

    @command("SAVE BGSAVE LASTSAVE", 0, exact=True)
    def save_command(self, session, name, args):
        if name == b"LASTSAVE":
            return self.last_save
        self.save()
        return OK

    @command("INFO", 0)
    def info(self, session, name, args):
        redis_mode = b"cluster" if self.cluster else b"standalone"
        sections = {
            b"server": [b"redis_version:7.2.0", b"redis_mode:" + redis_mode, b"uptime_in_seconds:%d" % (int(time.time()) - self.started)],
            b"clients": [b"connected_clients:%d" % len(self.sessions)],
            b"stats": [b"total_commands_processed:%d" % self.processed],
            b"persistence": [b"rdb_last_save_time:%d" % self.last_save, b"rdb_last_bgsave_status:ok", b"loading:0", b"aof_enabled:0"],
            b"replication": [b"role:master", b"connected_slaves:0"],
            b"cluster": [b"cluster_enabled:%d" % self.cluster],
            b"keyspace": [],
        }
        for db in range(len(self.databases)):
            keys = self.live_keys(db)
            if keys:
                expires = sum(self.databases[db][key].expires is not None for key in keys)
                sections[b"keyspace"].append(b"db%d:keys=%d,expires=%d,avg_ttl=0" % (db, len(keys), expires))
        requested = {arg.lower() for arg in args}
        return b"\r\n".join(
            b"# " + key.title() + b"\r\n" + b"\r\n".join(values) + b"\r\n"
            for key, values in sections.items()
            if not requested or requested & {b"all", b"default", b"everything", key}
        )

    @command("ROLE", 0, exact=True)
    def role(self, session, name, args):
        return [b"master", 0, []]

    @command("READONLY READWRITE ASKING", 0, exact=True)
    def cluster_connection(self, session, name, args):
        if not self.cluster:
            raise RedisError("ERR This instance has cluster support disabled")
        return OK

    @command("CLUSTER", 1, 3)
    def cluster_command(self, session, name, args):
        if not self.cluster:
            raise RedisError("ERR This instance has cluster support disabled")
        operation, *options = args
        operation = operation.upper()
        host, port = self.endpoint
        if operation == b"SLOTS" and not options:
            return [[0, SLOT_COUNT - 1, [host, port, self.node_id, {}]]]
        if operation == b"SHARDS" and not options:
            node = {
                b"id": self.node_id, b"port": port, b"ip": host, b"endpoint": host,
                b"role": b"master", b"replication-offset": 0, b"health": b"online",
            }
            return [{b"slots": [0, SLOT_COUNT - 1], b"nodes": [node]}]
        if operation == b"NODES" and not options:
            return b"%s %s:%d@0 myself,master - 0 0 1 connected 0-%d\n" % (
                self.node_id, host, port, SLOT_COUNT - 1,
            )
        if operation == b"INFO" and not options:
            return (
                b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\n"
                b"cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\n"
                b"cluster_size:1\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\n"
            )
        if operation in {b"MYID", b"MYSHARDID"} and not options:
            return self.node_id
        if operation == b"KEYSLOT" and len(options) == 1:
            return key_slot(options[0])
        if operation in {b"COUNTKEYSINSLOT", b"GETKEYSINSLOT"} and options:
            slot = integer(options[0], minimum=0, maximum=SLOT_COUNT - 1)
            if len(options) == 1 and operation == b"COUNTKEYSINSLOT":
                return sum(key_slot(key) == slot for key in self.live_keys(0))
            if len(options) == 2 and operation == b"GETKEYSINSLOT":
                count = integer(options[1], minimum=0)
                return [key for key in self.live_keys(0) if key_slot(key) == slot][:count]
        raise RedisError(SYNTAX)

    @command("CONFIG", 2)
    def config(self, session, name, args):
        if args[0].upper() != b"GET":
            raise RedisError(SYNTAX)
        values = {
            b"databases": str(len(self.databases)).encode(), b"save": b"", b"appendonly": b"no",
            b"maxmemory": b"0", b"maxmemory-policy": b"noeviction",
        }
        patterns = [glob_pattern(pattern.lower()) for pattern in args[1:]]
        return {key: value for key, value in values.items() if any(matches(pattern, key) for pattern in patterns)}

    @command("COMMAND", 0)
    def command_info(self, session, name, args):
        if args and args[0].upper() == b"COUNT" and len(args) == 1:
            return len(COMMANDS)
        if args and args[0].upper() == b"LIST" and len(args) == 1:
            return [key.lower() for key in sorted(COMMANDS)]
        if args and args[0].upper() == b"GETKEYS" and len(args) > 1:
            command_name = args[1].upper()
            if command_name not in COMMANDS:
                raise RedisError("ERR Invalid command specified")
            keys = command_keys(command_name, args[2:])
            if not keys:
                raise RedisError("ERR The command has no key arguments")
            return keys
        if args and args[0].upper() != b"INFO":
            raise RedisError(SYNTAX)
        commands = [key.upper() for key in args[1:]] if len(args) > 1 else sorted(COMMANDS)
        result = []
        for key in commands:
            definition = COMMANDS.get(key)
            if definition is None:
                result.append(None)
                continue
            _, minimum, maximum = definition
            arity = minimum + 1 if minimum == maximum else -(minimum + 1)
            first, last, step = key_positions(key)
            flags = []
            if key in MOVABLE_KEY_COMMANDS:
                flags.append(b"movablekeys")
            if key in PUBSUB_COMMANDS:
                flags.append(b"pubsub")
            result.append([key.lower(), arity, flags, first, last, step, [], [], [], []])
        return result

    @command("MULTI", 0, exact=True)
    def multi(self, session, name, args):
        if session.pending is not None:
            raise RedisError("ERR MULTI calls can not be nested")
        session.pending = []
        session.broken_transaction = False
        return OK

    @command("DISCARD EXEC", 0, exact=True)
    def finish_transaction(self, session, name, args):
        pending = session.pending
        if pending is None:
            raise RedisError(f"ERR {name.decode()} without MULTI")
        session.pending = None
        dirty_transaction = session.broken_transaction
        session.broken_transaction = False
        watched_keys = [key for _, key in session.watched]
        for database, key in session.watched:
            self.get(database, key)
        watch_dirty = session.watch_dirty
        self.unwatch(session)
        if name == b"DISCARD":
            return OK
        if dirty_transaction:
            raise RedisError("EXECABORT Transaction discarded because of previous errors.")
        if watch_dirty:
            return NULL_ARRAY
        if self.cluster:
            keys = [key for request in pending for key in command_keys(request[0].upper(), request[1:])]
            check_slots([*watched_keys, *keys])
        result = []
        session.executing = True
        try:
            for request in pending:
                try:
                    result.append(self.execute(session, request))
                except RedisError as error:
                    result.append(error)
        finally:
            session.executing = False
        return result

    @command("WATCH", 1)
    def watch(self, session, name, args):
        if session.pending is not None:
            raise RedisError("ERR WATCH inside MULTI is not allowed")
        for key in args:
            self.get(session.database, key)
            item = (session.database, key)
            session.watched.add(item)
            self.watchers.setdefault(item, set()).add(session)
        return OK

    @command("UNWATCH", 0, exact=True)
    def unwatch_command(self, session, name, args):
        self.unwatch(session)
        return OK

    def wait_for(self, session, timeout, read, *, null=None):
        deadline = time.monotonic() + timeout if timeout else None
        while True:
            result = read()
            if result is not None:
                return result
            if session.executing or self.stop or session.dead():
                return null
            remaining = deadline - time.monotonic() if deadline is not None else 0.25
            if remaining <= 0:
                return null
            self.changed.wait(min(remaining, 0.25))

    @command("DEL UNLINK EXISTS TOUCH", 1)
    def keys_operation(self, session, name, args):
        if name in {b"DEL", b"UNLINK"}:
            return sum(self.delete(session.database, key) for key in args)
        return sum(self.get(session.database, key) is not None for key in args)

    @command("TYPE", 1, exact=True)
    def type(self, session, name, args):
        entry = self.get(session.database, args[0])
        return Simple(entry.kind if entry else b"none")

    @command("RANDOMKEY DBSIZE", 0, exact=True)
    def dbsize(self, session, name, args):
        keys = self.live_keys(session.database)
        if name == b"DBSIZE":
            return len(keys)
        return random.choice(keys) if keys else None

    @command("KEYS", 1, exact=True)
    def keys(self, session, name, args):
        pattern = glob_pattern(args[0])
        return [key for key in self.live_keys(session.database) if matches(pattern, key)]

    @command("FLUSHDB FLUSHALL", 0, 1)
    def flush(self, session, name, args):
        if args and args[0].upper() not in {b"ASYNC", b"SYNC"}:
            raise RedisError(SYNTAX)
        selected = range(len(self.databases)) if name == b"FLUSHALL" else [session.database]
        for database in selected:
            for key in self.live_keys(database):
                self.delete(database, key)
        return OK

    @command("RENAME RENAMENX", 2, exact=True)
    def rename(self, session, name, args):
        source, target = args
        entry = self.get(session.database, source)
        if entry is None:
            raise RedisError("ERR no such key")
        if name == b"RENAMENX" and self.get(session.database, target) is not None:
            return 0
        if source != target:
            self.delete(session.database, source)
            self.put(session.database, target, entry)
        return 1 if name == b"RENAMENX" else OK

    @command("COPY", 2, 5)
    def copy_key(self, session, name, args):
        source, target, *options = args
        database = session.database
        replace = False
        while options:
            option = options.pop(0).upper()
            if option == b"DB" and options:
                database = integer(options.pop(0), minimum=0, maximum=len(self.databases) - 1)
            elif option == b"REPLACE":
                replace = True
            else:
                raise RedisError(SYNTAX)
        if (session.database, source) == (database, target):
            raise RedisError("ERR source and destination objects are the same")
        entry = self.get(session.database, source)
        if entry is None or (not replace and self.get(database, target) is not None):
            return 0
        self.put(database, target, copy.deepcopy(entry))
        return 1

    @command("MOVE", 2, exact=True)
    def move(self, session, name, args):
        key, target = args
        database = integer(target, minimum=0, maximum=len(self.databases) - 1)
        entry = self.get(session.database, key)
        if entry is None or self.get(database, key) is not None:
            return 0
        self.delete(session.database, key)
        self.put(database, key, entry)
        return 1

    @command("EXPIRE PEXPIRE EXPIREAT PEXPIREAT", 2, 4)
    def expire(self, session, name, args):
        key, duration, *options = args
        amount = integer(duration)
        if name in {b"EXPIRE", b"EXPIREAT"}:
            amount *= 1000
        absolute = amount + milliseconds() if name in {b"EXPIRE", b"PEXPIRE"} else amount
        if not MIN_INT <= absolute <= MAX_INT:
            raise RedisError("ERR invalid expire time")
        flags = {option.upper() for option in options}
        if not flags <= {b"NX", b"XX", b"GT", b"LT"} or (b"NX" in flags and len(flags) > 1) or {b"GT", b"LT"} <= flags:
            raise RedisError(SYNTAX)
        entry = self.get(session.database, key)
        if entry is None:
            return 0
        existing = entry.expires
        if b"NX" in flags and existing is not None or b"XX" in flags and existing is None:
            return 0
        if b"GT" in flags and (existing is None or absolute <= existing):
            return 0
        if b"LT" in flags and existing is not None and absolute >= existing:
            return 0
        if absolute <= milliseconds():
            return self.delete(session.database, key)
        entry.expires = absolute
        self.touch(session.database, key)
        return 1

    @command("TTL PTTL EXPIRETIME PEXPIRETIME PERSIST", 1, exact=True)
    def ttl(self, session, name, args):
        entry = self.get(session.database, args[0])
        if name == b"PERSIST":
            if entry is None or entry.expires is None:
                return 0
            entry.expires = None
            self.touch(session.database, args[0])
            return 1
        if entry is None:
            return -2
        if entry.expires is None:
            return -1
        if name == b"PEXPIRETIME":
            return entry.expires
        if name == b"EXPIRETIME":
            return entry.expires // 1000
        duration = max(0, entry.expires - milliseconds())
        return (duration + 500) // 1000 if name == b"TTL" else duration

    @command("SCAN SSCAN HSCAN ZSCAN", 1)
    def scan(self, session, name, args):
        kind = {b"SSCAN": b"set", b"HSCAN": b"hash", b"ZSCAN": b"zset"}.get(name)
        if kind:
            if len(args) < 2:
                raise RedisError("ERR scan cursor is required")
            key, raw_offset, *options = args
        else:
            raw_offset, *options = args
        offset = integer(raw_offset, minimum=0)
        count = 10
        pattern = None
        key_type = None
        no_values = False
        while options:
            option = options.pop(0).upper()
            if option == b"NOVALUES" and kind == b"hash":
                no_values = True
            elif options and option == b"MATCH":
                pattern = glob_pattern(options.pop(0))
            elif options and option == b"COUNT":
                count = integer(options.pop(0), minimum=1)
            elif options and option == b"TYPE" and kind is None:
                key_type = options.pop(0).lower()
            else:
                raise RedisError(SYNTAX)
        values = self.value(session, key, kind, {}) if kind else self.databases[session.database]
        all_keys = sorted(values if kind else self.live_keys(session.database))
        batch = []
        for item in all_keys[offset:offset + count]:
            if not matches(pattern, item) or (key_type is not None and values[item].kind != key_type):
                continue
            batch.append(item)
            if kind == b"hash" and not no_values:
                batch.append(values[item])
            elif kind == b"zset":
                batch.append(format_score(values[item]))
        next_offset = offset + count if offset + count < len(all_keys) else 0
        return [str(next_offset).encode(), batch]

    @command("GET GETDEL STRLEN", 1, exact=True)
    def get_string(self, session, name, args):
        key = args[0]
        value = self.value(session, key, b"string")
        if name == b"GETDEL":
            self.delete(session.database, key)
        return len(value or b"") if name == b"STRLEN" else value

    @command("SET", 2)
    def set_string(self, session, name, args):
        key, value, *options = args
        condition = None
        expires = None
        keep_ttl = get = False
        while options:
            option = options.pop(0).upper()
            if option in {b"NX", b"XX"} and condition in {None, option}:
                condition = option
            elif option == b"GET":
                get = True
            elif option == b"KEEPTTL" and expires is None:
                keep_ttl = True
            elif option in EXPIRATION_MODES and options and not keep_ttl and expires is None:
                expires = expiry_time(option, options.pop(0))
            else:
                raise RedisError(SYNTAX)
        old = self.get(session.database, key, b"string" if get else None)
        previous = old.value if get and old else None
        if (condition == b"NX" and old is not None) or (condition == b"XX" and old is None):
            return previous
        if keep_ttl and old:
            expires = old.expires
        if expires is not None and expires <= milliseconds():
            self.delete(session.database, key)
        else:
            self.put(session.database, key, Entry(b"string", value, expires))
        return previous if get else OK

    @command("SETEX PSETEX", 3, exact=True)
    def setex(self, session, name, args):
        key, duration, value = args
        return self.set_string(session, b"SET", [key, value, b"EX" if name == b"SETEX" else b"PX", duration])

    @command("SETNX GETSET", 2, exact=True)
    def setnx_getset(self, session, name, args):
        if name == b"GETSET":
            return self.set_string(session, b"SET", [*args, b"GET"])
        return int(self.set_string(session, b"SET", [*args, b"NX"]) is not None)

    @command("GETEX", 1, 3)
    def getex(self, session, name, args):
        key, *options = args
        option = options[0].upper() if options else None
        if option in EXPIRATION_MODES and len(options) == 2:
            expires = expiry_time(option, options[1])
        elif not options or (option == b"PERSIST" and len(options) == 1):
            expires = None
        else:
            raise RedisError(SYNTAX)
        entry = self.get(session.database, key, b"string")
        if entry is None:
            return None
        if options:
            if expires is not None and expires <= milliseconds():
                self.delete(session.database, key)
            elif entry.expires != expires:
                entry.expires = expires
                self.touch(session.database, key)
        return entry.value

    @command("MGET", 1)
    def mget(self, session, name, args):
        entries = [self.get(session.database, key) for key in args]
        return [entry.value if entry is not None and entry.kind == b"string" else None for entry in entries]

    @command("MSET MSETNX", 2)
    def mset(self, session, name, args):
        items = pairs(args)
        if name == b"MSETNX" and any(self.get(session.database, key) is not None for key, _ in items):
            return 0
        for key, value in items:
            self.put(session.database, key, Entry(b"string", value))
        return 1 if name == b"MSETNX" else OK

    @command("INCR DECR", 1, exact=True)
    @command("INCRBY DECRBY INCRBYFLOAT", 2, exact=True)
    def increment(self, session, name, args):
        key = args[0]
        previous = self.value(session, key, b"string", b"0")
        if name == b"INCRBYFLOAT":
            result = number(previous) + number(args[1])
            if not math.isfinite(result):
                raise RedisError("ERR increment would produce NaN or Infinity")
            value = format_score(result)
            self.update(session, key, b"string", value)
            return value
        amount = integer(args[1]) if len(args) == 2 else 1
        result = integer(previous) + (-amount if name in {b"DECR", b"DECRBY"} else amount)
        if not MIN_INT <= result <= MAX_INT:
            raise RedisError("ERR increment or decrement would overflow")
        self.update(session, key, b"string", str(result).encode())
        return result

    @command("APPEND", 2, exact=True)
    @command("GETRANGE SUBSTR SETRANGE", 3, exact=True)
    def range_string(self, session, name, args):
        key = args[0]
        value = self.value(session, key, b"string", b"")
        if name in {b"GETRANGE", b"SUBSTR"}:
            selected = index_range(len(value), integer(args[1]), integer(args[2]))
            return value[selected]
        if name == b"APPEND":
            offset = len(value)
            data = args[1]
        else:
            offset = integer(args[1], minimum=0)
            data = args[2]
            if not data:
                return len(value)
        limit = offset + len(data)
        if limit > MAX_BULK:
            raise RedisError("ERR string exceeds this server's size limit")
        stored = value[:offset].ljust(offset, b"\0") + data + value[limit:]
        self.update(session, key, b"string", stored)
        return len(stored)

    @command("GETBIT", 2, exact=True)
    @command("SETBIT", 3, exact=True)
    def bit(self, session, name, args):
        key, bit_offset = args[:2]
        byte_offset, bit = divmod(integer(bit_offset, minimum=0, maximum=MAX_BULK * 8 - 1), 8)
        value = self.value(session, key, b"string", b"")
        mask = 1 << (7 - bit)
        current = int(len(value) > byte_offset and bool(value[byte_offset] & mask))
        if name == b"SETBIT":
            desired = integer(args[2], minimum=0, maximum=1)
            data = bytearray(value)
            if byte_offset >= len(data):
                data.extend(b"\0" * (byte_offset + 1 - len(data)))
            data[byte_offset] = (data[byte_offset] | mask) if desired else (data[byte_offset] & ~mask)
            self.update(session, key, b"string", bytes(data))
        return current

    @command("BITCOUNT", 1, 4)
    def bitcount(self, session, name, args):
        key, *options = args
        value = self.value(session, key, b"string", b"")
        if not options:
            return sum(byte.bit_count() for byte in value)
        if len(options) == 1 or (len(options) == 3 and options[2].upper() not in {b"BIT", b"BYTE"}):
            raise RedisError(SYNTAX)
        in_bits = len(options) == 3 and options[2].upper() == b"BIT"
        start, stop = map(integer, options[:2])
        if not in_bits:
            return sum(byte.bit_count() for byte in value[index_range(len(value), start, stop)])
        selected = index_range(len(value) * 8, start, stop)
        start, stop, _ = selected.indices(len(value) * 8)
        return sum((value[index // 8] >> (7 - index % 8)) & 1 for index in range(start, stop))

    @command("HSET HMSET", 3)
    @command("HSETNX", 3, exact=True)
    def hset(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"hash", {})
        items = pairs(options)
        if name == b"HSETNX" and items[0][0] in values:
            return 0
        added = 0
        for attribute, value in items:
            added += attribute not in values
            values[attribute] = value
        self.update(session, key, b"hash", values)
        return OK if name == b"HMSET" else added

    @command("HGET HEXISTS HSTRLEN", 2, exact=True)
    @command("HMGET HDEL", 2)
    @command("HGETALL HKEYS HVALS HLEN", 1, exact=True)
    def hash_operation(self, session, name, args):
        key, *fields = args
        values = self.value(session, key, b"hash", {})
        if name == b"HGET":
            return values.get(fields[0])
        if name == b"HEXISTS":
            return int(fields[0] in values)
        if name == b"HSTRLEN":
            return len(values.get(fields[0], b""))
        if name == b"HMGET":
            return [values.get(field) for field in fields]
        if name == b"HGETALL":
            return values.copy()
        if name == b"HKEYS":
            return list(values)
        if name == b"HVALS":
            return list(values.values())
        if name == b"HLEN":
            return len(values)
        removed = set(fields).intersection(values)
        if removed:
            for field in removed:
                del values[field]
            self.update(session, key, b"hash", values, remove_empty=True)
        return len(removed)

    @command("HINCRBY HINCRBYFLOAT", 3, exact=True)
    def hincrby(self, session, name, args):
        key, field, amount = args
        values = self.value(session, key, b"hash", {})
        previous = values.get(field, b"0")
        if name == b"HINCRBY":
            result = integer(previous) + integer(amount)
            if not MIN_INT <= result <= MAX_INT:
                raise RedisError("ERR increment or decrement would overflow")
            stored = str(result).encode()
        else:
            score = number(previous) + number(amount)
            if not math.isfinite(score):
                raise RedisError("ERR increment would produce NaN or Infinity")
            result = stored = format_score(score)
        values[field] = stored
        self.update(session, key, b"hash", values)
        return result

    @command("HRANDFIELD", 1, 3)
    def hrandfield(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"hash", {})
        if not options:
            return random.choice(list(values)) if values else None
        fields = choose(values, integer(options[0]))
        if len(options) == 1:
            return fields
        if options[1].upper() != b"WITHVALUES":
            raise RedisError(SYNTAX)
        return score_pairs([(field, values[field]) for field in fields], session.protocol)

    @command("LPUSH RPUSH LPUSHX RPUSHX", 2)
    def push(self, session, name, args):
        key, *items = args
        values = self.value(session, key, b"list", [])
        if name.endswith(b"X") and not values:
            return 0
        if name.startswith(b"L"):
            values[0:0] = reversed(items)
        else:
            values.extend(items)
        self.update(session, key, b"list", values)
        return len(values)

    def list_pop(self, session, key, left, count=None):
        values = self.value(session, key, b"list", [])
        if not values:
            return NULL_ARRAY if count is not None else None
        if count is None:
            result = values.pop(0 if left else -1)
        elif count == 0:
            return []
        elif left:
            result = values[:count]
            del values[:count]
        else:
            result = values[-count:][::-1]
            del values[-count:]
        self.update(session, key, b"list", values, remove_empty=True)
        return result

    @command("LPOP RPOP", 1, 2)
    def pop(self, session, name, args):
        count = integer(args[1], minimum=0) if len(args) == 2 else None
        return self.list_pop(session, args[0], name == b"LPOP", count)

    @command("BLPOP BRPOP", 2)
    def bpop(self, session, name, args):
        timeout = timeout_seconds(args[-1])

        def pop_next():
            for key in args[:-1]:
                result = self.list_pop(session, key, name == b"BLPOP")
                if result is not None:
                    return [key, result]
            return None

        return self.wait_for(session, timeout, pop_next, null=NULL_ARRAY)

    @command("LLEN", 1, exact=True)
    @command("LINDEX", 2, exact=True)
    @command("LRANGE LTRIM LSET LREM", 3, exact=True)
    @command("LINSERT", 4, exact=True)
    def list_operation(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"list", [])
        if name == b"LLEN":
            return len(values)
        if name in {b"LINDEX", b"LSET"}:
            index = position(len(values), integer(options[0]))
            if name == b"LINDEX":
                return values[index] if 0 <= index < len(values) else None
            if not values:
                raise RedisError("ERR no such key")
            if not 0 <= index < len(values):
                raise RedisError("ERR index out of range")
            values[index] = options[1]
            result = OK
        elif name in {b"LRANGE", b"LTRIM"}:
            selected = index_range(len(values), integer(options[0]), integer(options[1]))
            if name == b"LRANGE":
                return values[selected]
            values = values[selected]
            result = OK
        elif name == b"LINSERT":
            where, pivot, item = options
            direction = where.upper()
            if direction not in {b"BEFORE", b"AFTER"}:
                raise RedisError(SYNTAX)
            if not values:
                return 0
            try:
                index = values.index(pivot)
            except ValueError:
                return -1
            values.insert(index + (direction == b"AFTER"), item)
            result = len(values)
        else:
            count = integer(options[0])
            item = options[1]
            indices = [index for index, value in enumerate(values) if value == item]
            if count != 0:
                indices = indices[:count] if count > 0 else indices[count:]
            for index in reversed(indices):
                del values[index]
            result = len(indices)
            if not result:
                return 0
        self.update(session, key, b"list", values, remove_empty=True)
        return result

    @command("LPOS", 2, 8)
    def lpos(self, session, name, args):
        key, element, *options = args
        rank, count, maxlen = 1, None, 0
        for option, value in pairs(options):
            mode = option.upper()
            if mode == b"RANK":
                rank = integer(value)
                if rank == 0:
                    raise RedisError("ERR RANK can't be zero")
            elif mode == b"COUNT":
                count = integer(value, minimum=0)
            elif mode == b"MAXLEN":
                maxlen = integer(value, minimum=0)
            else:
                raise RedisError(SYNTAX)
        values = self.value(session, key, b"list", [])
        indices = range(len(values)) if rank > 0 else range(len(values) - 1, -1, -1)
        matches_found = [index for index in indices[:maxlen or None] if values[index] == element][abs(rank) - 1:]
        if count is None:
            return matches_found[0] if matches_found else None
        return matches_found[:count or None]

    @command("RPOPLPUSH", 2, exact=True)
    @command("BRPOPLPUSH", 3, exact=True)
    @command("LMOVE", 4, exact=True)
    @command("BLMOVE", 5, exact=True)
    def lmove(self, session, name, args):
        source, target, *options = args
        if name in {b"RPOPLPUSH", b"BRPOPLPUSH"}:
            options = [b"RIGHT", b"LEFT", *options]
        pop_side, push_side, *timeout = options
        pop_side, push_side = pop_side.upper(), push_side.upper()
        if pop_side not in {b"LEFT", b"RIGHT"} or push_side not in {b"LEFT", b"RIGHT"}:
            raise RedisError(SYNTAX)

        def move_one():
            source_values = self.value(session, source, b"list", [])
            if not source_values:
                return None
            target_values = self.value(session, target, b"list", [])
            result = source_values.pop(0 if pop_side == b"LEFT" else -1)
            if push_side == b"LEFT":
                target_values.insert(0, result)
            else:
                target_values.append(result)
            self.update(session, source, b"list", source_values, remove_empty=True)
            if source != target:
                self.update(session, target, b"list", target_values)
            return result

        if timeout:
            return self.wait_for(session, timeout_seconds(timeout[0]), move_one)
        return move_one()

    @command("SADD SREM", 2)
    @command("SMEMBERS SCARD", 1, exact=True)
    @command("SISMEMBER", 2, exact=True)
    @command("SMISMEMBER", 2)
    @command("SPOP SRANDMEMBER", 1, 2)
    def set_operation(self, session, name, args):
        key, *members = args
        values = self.value(session, key, b"set", set())
        if name == b"SMEMBERS":
            return values.copy()
        if name == b"SCARD":
            return len(values)
        if name == b"SISMEMBER":
            return int(members[0] in values)
        if name == b"SMISMEMBER":
            return [int(member in values) for member in members]
        if name in {b"SPOP", b"SRANDMEMBER"}:
            count = integer(members[0]) if members else 1
            if name == b"SPOP" and count < 0:
                raise RedisError("ERR value is out of range")
            result = choose(values, count)
            if name == b"SPOP" and result:
                values.difference_update(result)
                self.update(session, key, b"set", values, remove_empty=True)
            return result if members else (result[0] if result else None)
        changed = set(members).difference(values) if name == b"SADD" else set(members).intersection(values)
        if changed:
            if name == b"SADD":
                values.update(changed)
            else:
                values.difference_update(changed)
            self.update(session, key, b"set", values, remove_empty=True)
        return len(changed)

    @command("SUNION SINTER SDIFF", 1)
    @command("SUNIONSTORE SINTERSTORE SDIFFSTORE", 2)
    def set_combine(self, session, name, args):
        store = name.endswith(b"STORE")
        if store:
            target, *args = args
            name = name[:-5]
        sets = [self.value(session, key, b"set", set()) for key in args]
        first, *others = sets
        operation = {b"SUNION": set.union, b"SINTER": set.intersection, b"SDIFF": set.difference}[name]
        result = operation(first, *others)
        if not store:
            return result
        if result:
            self.put(session.database, target, Entry(b"set", result))
        else:
            self.delete(session.database, target)
        return len(result)

    @command("SINTERCARD", 2)
    def sintercard(self, session, name, args):
        count = integer(args[0], minimum=1)
        keys, remainder = args[1:count + 1], args[count + 1:]
        if len(keys) != count or (remainder and (len(remainder) != 2 or remainder[0].upper() != b"LIMIT")):
            raise RedisError(SYNTAX)
        limit = integer(remainder[1], minimum=0) if remainder else 0
        result = len(self.set_combine(session, b"SINTER", keys))
        return min(result, limit) if limit else result

    @command("SMOVE", 3, exact=True)
    def smove(self, session, name, args):
        source, target, member = args
        previous = self.value(session, source, b"set", set())
        if member not in previous:
            return 0
        next_values = self.value(session, target, b"set", set())
        if source != target:
            previous.remove(member)
            next_values.add(member)
            self.update(session, source, b"set", previous, remove_empty=True)
            self.update(session, target, b"set", next_values)
        return 1

    @command("ZADD", 3)
    def zadd(self, session, name, args):
        key, *items = args
        flags, items = read_flags(items, {b"NX", b"XX", b"GT", b"LT", b"CH", b"INCR"})
        if (b"NX" in flags and flags & {b"XX", b"GT", b"LT"}) or {b"GT", b"LT"} <= flags:
            raise RedisError("ERR incompatible ZADD options")
        scores = [(number(score), member) for score, member in pairs(items)]
        if not scores or (b"INCR" in flags and len(scores) != 1):
            raise RedisError(SYNTAX)
        original = self.value(session, key, b"zset", {})
        values = original.copy()
        changed = added = 0
        new_score = None
        for score, member in scores:
            exists = member in values
            if (exists and b"NX" in flags) or (not exists and b"XX" in flags):
                continue
            old_score = values.get(member, 0.0)
            if b"INCR" in flags:
                score += old_score
                if math.isnan(score):
                    raise RedisError("ERR resulting score is not a number (NaN)")
            if exists and ((b"GT" in flags and score <= old_score) or (b"LT" in flags and score >= old_score)):
                continue
            if not exists or old_score != score:
                added += not exists
                changed += 1
                values[member] = score
            new_score = score
        if changed:
            self.update(session, key, b"zset", values)
        return new_score if b"INCR" in flags else (changed if b"CH" in flags else added)

    @command("ZINCRBY", 3, exact=True)
    def zincrby(self, session, name, args):
        return self.zadd(session, b"ZADD", [args[0], b"INCR", *args[1:]])

    @command("ZCARD", 1, exact=True)
    @command("ZSCORE", 2, exact=True)
    @command("ZMSCORE", 2)
    @command("ZRANK ZREVRANK", 2, 3)
    @command("ZCOUNT ZLEXCOUNT", 3, exact=True)
    def zquery(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"zset", {})
        if name == b"ZCARD":
            return len(values)
        if name == b"ZSCORE":
            return values.get(options[0])
        if name == b"ZMSCORE":
            return [values.get(member) for member in options]
        if name in {b"ZCOUNT", b"ZLEXCOUNT"}:
            by_lex = name == b"ZLEXCOUNT"
            return len(sorted_range(values, options[0], options[1], mode=b"BYLEX" if by_lex else b"BYSCORE"))
        member = options[0]
        with_score = len(options) == 2 and options[1].upper() == b"WITHSCORE"
        if len(options) == 2 and not with_score:
            raise RedisError(SYNTAX)
        if member not in values:
            return NULL_ARRAY if with_score else None
        rank = next(index for index, pair in enumerate(sorted_set(values, name == b"ZREVRANK")) if pair[0] == member)
        return [rank, values[member]] if with_score else rank

    @command("ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX", 3)
    @command("ZRANGESTORE", 4)
    def zrange(self, session, name, args):
        if name == b"ZRANGESTORE":
            target, *args = args
        key, start, stop, *options = args
        values = self.value(session, key, b"zset", {})
        reverse = name.startswith(b"ZREV")
        mode = b"BYSCORE" if name.endswith(b"BYSCORE") else b"BYLEX" if name.endswith(b"BYLEX") else None
        offset, count = 0, None
        with_scores = False
        while options:
            option = options.pop(0).upper()
            if option == b"REV" and name in {b"ZRANGE", b"ZRANGESTORE"}:
                reverse = True
            elif option in {b"BYSCORE", b"BYLEX"} and mode is None and name in {b"ZRANGE", b"ZRANGESTORE"}:
                mode = option
            elif option == b"WITHSCORES" and name != b"ZRANGESTORE":
                with_scores = True
            elif option == b"LIMIT" and len(options) >= 2:
                offset = integer(options.pop(0))
                count = integer(options.pop(0))
            else:
                raise RedisError(SYNTAX)
        if (count is not None and mode is None) or (with_scores and mode == b"BYLEX"):
            raise RedisError(SYNTAX)
        result = sorted_range(values, start, stop, mode=mode, reverse=reverse)
        if count is not None:
            result = [] if offset < 0 else result[offset:None if count < 0 else offset + count]
        if name == b"ZRANGESTORE":
            if result:
                self.put(session.database, target, Entry(b"zset", dict(result)))
            else:
                self.delete(session.database, target)
            return len(result)
        return score_pairs(result, session.protocol) if with_scores else [member for member, _ in result]

    @command("ZREM", 2)
    @command("ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX", 3, exact=True)
    def zrem(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"zset", {})
        if name == b"ZREM":
            members = set(options).intersection(values)
        else:
            mode = b"BYSCORE" if name.endswith(b"BYSCORE") else b"BYLEX" if name.endswith(b"BYLEX") else None
            members = [member for member, _ in sorted_range(values, options[0], options[1], mode=mode)]
        if members:
            for member in members:
                del values[member]
            self.update(session, key, b"zset", values, remove_empty=True)
        return len(members)

    @command("ZRANDMEMBER", 1, 3)
    def zrandmember(self, session, name, args):
        key, *options = args
        values = self.value(session, key, b"zset", {})
        if not options:
            return random.choice(list(values)) if values else None
        members = choose(values, integer(options[0]))
        if len(options) == 1:
            return members
        if options[1].upper() != b"WITHSCORES":
            raise RedisError(SYNTAX)
        return score_pairs([(member, values[member]) for member in members], session.protocol)

    def zpop_entries(self, session, key, reverse, count):
        values = self.value(session, key, b"zset", {})
        result = sorted_set(values, reverse)[:count]
        if result:
            for member, _ in result:
                del values[member]
            self.update(session, key, b"zset", values, remove_empty=True)
        return result

    @command("ZPOPMIN ZPOPMAX", 1, 2)
    def zpop(self, session, name, args):
        count = integer(args[1], minimum=0) if len(args) == 2 else 1
        result = self.zpop_entries(session, args[0], name == b"ZPOPMAX", count)
        if len(args) == 1 and session.protocol == 3:
            return [item for pair in result for item in pair]
        return score_pairs(result, session.protocol)

    @command("BZPOPMIN BZPOPMAX", 2)
    def bzpop(self, session, name, args):
        timeout = timeout_seconds(args[-1])

        def pop_next():
            for key in args[:-1]:
                result = self.zpop_entries(session, key, name == b"BZPOPMAX", 1)
                if result:
                    return [key, *result[0]]
            return None

        return self.wait_for(session, timeout, pop_next, null=NULL_ARRAY)

    @command("ZUNION ZINTER ZDIFF ZINTERCARD", 2)
    @command("ZUNIONSTORE ZINTERSTORE ZDIFFSTORE", 3)
    def zcombine(self, session, name, args):
        store = name.endswith(b"STORE")
        if store:
            target, *args = args
            name = name[:-5]
        count = integer(args[0], minimum=1)
        keys, options = args[1:count + 1], args[count + 1:]
        if len(keys) != count:
            raise RedisError(SYNTAX)
        weights = [1.0] * count
        aggregate = b"SUM"
        with_scores = False
        limit = 0
        while options:
            option = options.pop(0).upper()
            if option == b"WITHSCORES" and not store and name != b"ZINTERCARD":
                with_scores = True
            elif option == b"WEIGHTS" and name in {b"ZUNION", b"ZINTER"} and len(options) >= count:
                weights = [number(value) for value in options[:count]]
                del options[:count]
            elif option == b"AGGREGATE" and name in {b"ZUNION", b"ZINTER"} and options:
                aggregate = options.pop(0).upper()
                if aggregate not in {b"SUM", b"MIN", b"MAX"}:
                    raise RedisError(SYNTAX)
            elif option == b"LIMIT" and name == b"ZINTERCARD" and options:
                limit = integer(options.pop(0), minimum=0)
            else:
                raise RedisError(SYNTAX)
        sets = []
        for key in keys:
            item = self.get(session.database, key)
            if item is None:
                sets.append({})
            elif item.kind == b"set":
                sets.append(dict.fromkeys(item.value, 1.0))
            elif item.kind == b"zset":
                sets.append(item.value)
            else:
                raise RedisError(WRONG_TYPE)
        first, *others = sets
        if name == b"ZUNION":
            members = set(first).union(*others)
        elif name in {b"ZINTER", b"ZINTERCARD"}:
            members = set(first).intersection(*others)
        else:
            members = set(first).difference(*others)
        if name == b"ZINTERCARD":
            return min(limit, len(members)) if limit else len(members)
        combined = {}
        for member in members:
            if name == b"ZDIFF":
                combined[member] = first[member]
                continue
            scores = []
            for values, weight in zip(sets, weights):
                if member in values:
                    score = values[member] * weight
                    scores.append(0.0 if math.isnan(score) else score)
            if aggregate == b"SUM":
                total = sum(scores)
                combined[member] = 0.0 if math.isnan(total) else total
            else:
                combined[member] = (min if aggregate == b"MIN" else max)(scores)
        if store:
            if combined:
                self.put(session.database, target, Entry(b"zset", combined))
            else:
                self.delete(session.database, target)
            return len(combined)
        result = sorted_set(combined)
        return score_pairs(result, session.protocol) if with_scores else [member for member, _ in result]

    @command("SUBSCRIBE PSUBSCRIBE", 1)
    @command("UNSUBSCRIBE PUNSUBSCRIBE", 0)
    def pubsub_subscription(self, session, name, args):
        subscribe = name in {b"SUBSCRIBE", b"PSUBSCRIBE"}
        pattern = name.startswith(b"P")
        subscription = session.patterns if pattern else session.subscriptions
        if not args:
            args = list(subscription) or [None]
        for channel in args:
            if subscribe:
                if pattern:
                    session.patterns[channel] = glob_pattern(channel)
                else:
                    session.subscriptions.add(channel)
            elif pattern:
                session.patterns.pop(channel, None)
            else:
                session.subscriptions.discard(channel)
            count = len(session.patterns) + len(session.subscriptions)
            session.send(Push((name.lower(), channel, count)))
        return SENT

    @command("PUBLISH", 2, exact=True)
    def publish(self, session, name, args):
        channel, message = args
        delivered = 0
        for recipient in sorted(self.sessions, key=lambda item: item.id):
            if channel in recipient.subscriptions:
                recipient.send(Push((b"message", channel, message)))
                delivered += 1
            for pattern, compiled in recipient.patterns.items():
                if matches(compiled, channel):
                    recipient.send(Push((b"pmessage", pattern, channel, message)))
                    delivered += 1
        return delivered

    @command("PUBSUB", 1)
    def pubsub_info(self, session, name, args):
        subcommand, *params = args
        subcommand = subcommand.upper()
        if subcommand == b"NUMPAT" and not params:
            return len(set().union(*(set(item.patterns) for item in self.sessions)))
        if subcommand == b"NUMSUB":
            return [item for channel in params for item in (channel, sum(channel in member.subscriptions for member in self.sessions))]
        if subcommand == b"CHANNELS" and len(params) <= 1:
            pattern = glob_pattern(params[0]) if params else None
            channels = set().union(*(member.subscriptions for member in self.sessions))
            return [channel for channel in sorted(channels) if matches(pattern, channel)]
        raise RedisError(SYNTAX)


    @command("XADD", 4)
    def xadd(self, session, name, args):
        key, *options = args
        keep_existing = bool(options and options[0].upper() == b"NOMKSTREAM")
        if keep_existing:
            options.pop(0)
        trim = parse_stream_trim(options) if options and options[0].upper() in {b"MAXLEN", b"MINID"} else None
        if len(options) < 3 or len(options) % 2 == 0:
            raise RedisError(SYNTAX)
        requested, *values = options
        stream = self.value(session, key, b"stream")
        if stream is None:
            if keep_existing:
                return None
            stream = Stream()
        if requested == b"*":
            ms = max(milliseconds(), stream.last_id[0])
            identifier = (ms, stream.last_id[1] + 1 if ms == stream.last_id[0] else 0)
        elif requested.endswith(b"-*"):
            ms = integer(requested[:-2], minimum=0, maximum=MAX_STREAM_COMPONENT)
            identifier = (ms, stream.last_id[1] + 1 if ms == stream.last_id[0] else int(ms == 0))
        else:
            identifier = parse_stream_id(requested)
        if not ZERO_STREAM_ID < identifier <= MAX_STREAM_ID or identifier <= stream.last_id:
            raise RedisError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        stream.items[identifier] = values
        stream.last_id = identifier
        stream.entries_added += 1
        if trim is not None:
            trim_stream(stream, *trim)
        self.update(session, key, b"stream", stream)
        return stream_id_bytes(identifier)

    @command("XLEN", 1, exact=True)
    @command("XDEL", 2)
    @command("XTRIM", 3)
    def edit_stream(self, session, name, args):
        key, *options = args
        stream = self.value(session, key, b"stream")
        if name == b"XLEN":
            return len(stream.items) if stream is not None else 0
        if name == b"XDEL":
            identifiers = [parse_stream_id(raw) for raw in options]
            removed = 0
            if stream is not None:
                for identifier in identifiers:
                    if identifier in stream.items:
                        del stream.items[identifier]
                        removed += 1
        else:
            trim = parse_stream_trim(options)
            if options:
                raise RedisError(SYNTAX)
            removed = trim_stream(stream, *trim) if stream is not None else 0
        if removed:
            self.touch(session.database, key)
        return removed

    @command("XRANGE XREVRANGE", 3, 5)
    def xrange(self, session, name, args):
        key, start, stop, *options = args
        reverse = name == b"XREVRANGE"
        if reverse:
            start, stop = stop, start
        minimum, maximum = stream_bounds(start, stop)
        count = None
        if options:
            if len(options) != 2 or options[0].upper() != b"COUNT":
                raise RedisError(SYNTAX)
            count = max(0, integer(options[1]))
        stream = self.value(session, key, b"stream")
        if stream is None:
            return []
        return [
            [stream_id_bytes(identifier), stream.items[identifier]]
            for identifier in sorted(stream.items, reverse=reverse)
            if minimum <= identifier <= maximum
        ][:count]

    @command("XGROUP", 2)
    def xgroup(self, session, name, args):
        subcommand, key, *options = args
        subcommand = subcommand.upper()
        stream = self.value(session, key, b"stream")
        if not options:
            raise RedisError(SYNTAX)
        group_name, *options = options
        if subcommand in {b"CREATE", b"SETID"}:
            if not options:
                raise RedisError(SYNTAX)
            requested, *options = options
            create_stream = False
            entries_read = None
            while options:
                option = options.pop(0).upper()
                if option == b"MKSTREAM" and subcommand == b"CREATE":
                    create_stream = True
                elif option == b"ENTRIESREAD" and options:
                    entries_read = integer(options.pop(0), minimum=-1)
                    if entries_read == -1:
                        entries_read = None
                else:
                    raise RedisError(SYNTAX)
            if stream is None:
                if not create_stream:
                    raise RedisError("ERR The XGROUP subcommand requires the key to exist")
                stream = Stream()
            identifier = stream.last_id if requested == b"$" else parse_stream_id(requested)
            if subcommand == b"CREATE":
                if group_name in stream.groups:
                    raise RedisError("BUSYGROUP Consumer Group name already exists")
                stream.groups[group_name] = StreamGroup(identifier, entries_read=entries_read)
            else:
                group = require_group(stream, group_name)
                group.last_id = identifier
                group.entries_read = entries_read
            self.update(session, key, b"stream", stream)
            return OK
        if subcommand == b"DESTROY" and not options:
            if stream is None or group_name not in stream.groups:
                return 0
            del stream.groups[group_name]
            self.touch(session.database, key)
            return 1
        if subcommand in {b"CREATECONSUMER", b"DELCONSUMER"} and len(options) == 1:
            group = require_group(stream, group_name)
            consumer = options[0]
            if subcommand == b"CREATECONSUMER":
                if consumer in group.consumers:
                    return 0
                group.consumers[consumer] = milliseconds()
                self.touch(session.database, key)
                return 1
            removed = [identifier for identifier, state in group.pending.items() if state.consumer == consumer]
            for identifier in removed:
                del group.pending[identifier]
            if consumer in group.consumers:
                del group.consumers[consumer]
                self.touch(session.database, key)
            return len(removed)
        raise RedisError(SYNTAX)

    @command("XREAD", 3)
    @command("XREADGROUP", 6)
    def xread(self, session, name, args):
        group_name = consumer = None
        count = timeout = None
        noack = False
        if name == b"XREADGROUP":
            if args[0].upper() != b"GROUP":
                raise RedisError(SYNTAX)
            group_name, consumer = args[1:3]
            args = args[3:]
        while args and args[0].upper() != b"STREAMS":
            option, *args = args
            option = option.upper()
            if option == b"COUNT" and args:
                count = integer(args.pop(0), minimum=1)
            elif option == b"BLOCK" and args:
                timeout = integer(args.pop(0), minimum=0) / 1000
            elif option == b"NOACK" and group_name is not None:
                noack = True
            else:
                raise RedisError(SYNTAX)
        if len(args) < 3 or len(args) % 2 == 0:
            raise RedisError(SYNTAX)
        split = len(args) // 2
        keys, positions = args[1:split + 1], args[split + 1:]
        starts = {}
        for key, requested in zip(keys, positions):
            stream = self.value(session, key, b"stream")
            if group_name is not None:
                require_group(stream, group_name)
            if requested == b">" and group_name is not None:
                starts[key] = None
            elif requested == b"$" and group_name is None:
                starts[key] = stream.last_id if stream is not None else ZERO_STREAM_ID
            else:
                starts[key] = parse_stream_id(requested)

        def read_next():
            streams = [(key, self.value(session, key, b"stream"), start) for key, start in starts.items()]
            if group_name is not None:
                for _, stream, _ in streams:
                    require_group(stream, group_name)
            responses = {}
            for key, stream, start in streams:
                if stream is None:
                    continue
                group = stream.groups.get(group_name) if group_name is not None else None
                if group is not None:
                    items = self.read_group(session, key, stream, group, consumer, start, count, noack)
                else:
                    items = [
                        [stream_id_bytes(identifier), fields]
                        for identifier, fields in stream.items.items() if identifier > start
                    ][:count]
                if items or group is not None and start is not None:
                    responses[key] = items
            if not responses:
                return None
            return responses if session.protocol == 3 else [[key, items] for key, items in responses.items()]

        if timeout is not None and (group_name is None or any(start is None for start in starts.values())):
            return self.wait_for(session, timeout, read_next, null=NULL_ARRAY)
        result = read_next()
        return NULL_ARRAY if result is None else result

    def read_group(self, session, key, stream, group, consumer, start, count, noack):
        now = milliseconds()
        group.consumers[consumer] = now
        if start is not None:
            pending = [
                identifier for identifier, state in sorted(group.pending.items())
                if identifier > start and state.consumer == consumer
            ][:count]
            for identifier in pending:
                state = group.pending[identifier]
                state.delivered_at = now
                state.deliveries += 1
            self.touch(session.database, key)
            return [[stream_id_bytes(identifier), stream.items.get(identifier)] for identifier in pending]
        entries = [
            (identifier, fields) for identifier, fields in stream.items.items()
            if identifier > group.last_id
        ][:count]
        if entries:
            group.last_id = entries[-1][0]
            if group.entries_read is not None:
                group.entries_read += len(entries)
            if not noack:
                for identifier, _ in entries:
                    group.pending[identifier] = PendingMessage(consumer, now, 1)
            self.touch(session.database, key)
        return [[stream_id_bytes(identifier), fields] for identifier, fields in entries]

    @command("XACK", 3)
    def xack(self, session, name, args):
        key, group_name, *options = args
        ids = [parse_stream_id(item) for item in options]
        stream = self.value(session, key, b"stream")
        group = stream.groups.get(group_name) if stream is not None else None
        if group is None:
            return 0
        acknowledged = sum(group.pending.pop(identifier, None) is not None for identifier in ids)
        if acknowledged:
            self.touch(session.database, key)
        return acknowledged

    @command("XPENDING", 2, 8)
    def xpending(self, session, name, args):
        key, group_name, *options = args
        group = require_group(self.value(session, key, b"stream"), group_name)
        ids = sorted(group.pending)
        if not options:
            consumers = {}
            for state in group.pending.values():
                consumers[state.consumer] = consumers.get(state.consumer, 0) + 1
            return [
                len(ids), stream_id_bytes(ids[0]) if ids else None, stream_id_bytes(ids[-1]) if ids else None,
                [[consumer, str(total).encode()] for consumer, total in sorted(consumers.items())] if ids else NULL_ARRAY,
            ]
        idle_ms = 0
        if options[0].upper() == b"IDLE" and len(options) >= 2:
            idle_ms = integer(options[1], minimum=0)
            options = options[2:]
        if len(options) not in {3, 4}:
            raise RedisError(SYNTAX)
        minimum, maximum = stream_bounds(options[0], options[1])
        count = integer(options[2], minimum=0)
        consumer = options[3] if len(options) == 4 else None
        now = milliseconds()
        result = []
        for identifier in ids:
            state = group.pending[identifier]
            idle = max(0, now - state.delivered_at)
            if minimum <= identifier <= maximum and idle >= idle_ms and (consumer is None or consumer == state.consumer):
                result.append([stream_id_bytes(identifier), state.consumer, idle, state.deliveries])
        return result[:count]

    @command("XINFO", 2, 3)
    def xinfo(self, session, name, args):
        subcommand, key, *options = args
        subcommand = subcommand.upper()
        stream = self.value(session, key, b"stream")
        if stream is None:
            raise RedisError("ERR no such key")
        if subcommand == b"STREAM" and not options:
            first = next(iter(stream.items), None)
            last = next(reversed(stream.items), None)
            return {
                b"length": len(stream.items), b"last-generated-id": stream_id_bytes(stream.last_id),
                b"first-entry": [stream_id_bytes(first), stream.items[first]] if first is not None else None,
                b"last-entry": [stream_id_bytes(last), stream.items[last]] if last is not None else None,
                b"groups": len(stream.groups), b"entries-added": stream.entries_added,
            }
        if subcommand == b"GROUPS" and not options:
            return [
                {
                    b"name": group_name, b"consumers": len(group.consumers), b"pending": len(group.pending),
                    b"last-delivered-id": stream_id_bytes(group.last_id), b"entries-read": group.entries_read,
                    b"lag": (stream.entries_added - group.entries_read) if group.entries_read is not None else None,
                }
                for group_name, group in stream.groups.items()
            ]
        if subcommand == b"CONSUMERS" and len(options) == 1:
            group = require_group(stream, options[0])
            now = milliseconds()
            return [
                {b"name": consumer, b"pending": sum(state.consumer == consumer for state in group.pending.values()), b"idle": max(0, now - seen)}
                for consumer, seen in group.consumers.items()
            ]
        raise RedisError(SYNTAX)


MAX_STREAM_COMPONENT = (1 << 64) - 1
ZERO_STREAM_ID = (0, 0)
MAX_STREAM_ID = (MAX_STREAM_COMPONENT, MAX_STREAM_COMPONENT)


@dataclass
class PendingMessage:
    consumer: bytes
    delivered_at: int
    deliveries: int


@dataclass
class StreamGroup:
    last_id: tuple[int, int]
    entries_read: int | None = None
    consumers: dict[bytes, int] = field(default_factory=dict)
    pending: dict[tuple[int, int], PendingMessage] = field(default_factory=dict)


@dataclass
class Stream:
    items: dict[tuple[int, int], list[bytes]] = field(default_factory=dict)
    last_id: tuple[int, int] = ZERO_STREAM_ID
    entries_added: int = 0
    groups: dict[bytes, StreamGroup] = field(default_factory=dict)


def parse_stream_id(value, *, default_sequence=0):
    ms, separator, sequence = value.partition(b"-")
    try:
        return (
            integer(ms, minimum=0, maximum=MAX_STREAM_COMPONENT),
            integer(sequence, minimum=0, maximum=MAX_STREAM_COMPONENT) if separator else default_sequence,
        )
    except RedisError as error:
        raise RedisError("ERR Invalid stream ID specified as stream command argument") from error


def stream_id_bytes(identifier):
    return b"%d-%d" % identifier


def stream_bounds(start, stop):
    first_open, last_open = start.startswith(b"("), stop.startswith(b"(")
    if first_open:
        start = start[1:]
    if last_open:
        stop = stop[1:]
    minimum = ZERO_STREAM_ID if start == b"-" else parse_stream_id(start)
    maximum = MAX_STREAM_ID if stop == b"+" else parse_stream_id(stop, default_sequence=MAX_STREAM_COMPONENT)
    if first_open and minimum[1] == MAX_STREAM_COMPONENT:
        minimum = (minimum[0] + 1, 0)
    elif first_open:
        minimum = (minimum[0], minimum[1] + 1)
    if last_open and maximum[1] == 0:
        maximum = (maximum[0] - 1, MAX_STREAM_COMPONENT)
    elif last_open:
        maximum = (maximum[0], maximum[1] - 1)
    return minimum, maximum


def parse_stream_trim(options):
    mode = options.pop(0).upper()
    if mode not in {b"MAXLEN", b"MINID"} or not options:
        raise RedisError(SYNTAX)
    approx = options[0] == b"~"
    if options[0] in {b"=", b"~"}:
        options.pop(0)
    if not options:
        raise RedisError(SYNTAX)
    raw_threshold = options.pop(0)
    threshold = integer(raw_threshold, minimum=0) if mode == b"MAXLEN" else parse_stream_id(raw_threshold)
    limit = 0
    if approx and len(options) >= 2 and options[0].upper() == b"LIMIT":
        options.pop(0)
        limit = integer(options.pop(0), minimum=0)
    return mode, threshold, limit


def trim_stream(stream, mode, threshold, limit):
    if mode == b"MAXLEN":
        count = max(0, len(stream.items) - threshold)
        expired = list(stream.items)[:count]
    else:
        expired = [identifier for identifier in stream.items if identifier < threshold]
    if limit:
        expired = expired[:limit]
    for identifier in expired:
        del stream.items[identifier]
    return len(expired)


def require_group(stream, group_name):
    if stream is None or group_name not in stream.groups:
        raise RedisError("NOGROUP No such key or consumer group")
    return stream.groups[group_name]

PUBSUB_COMMANDS = {b"PUBLISH", b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE", b"PUBSUB"}
KEYLESS_COMMANDS = {
    b"PING", b"ECHO", b"QUIT", b"RESET", b"AUTH", b"HELLO", b"CLIENT", b"SELECT", b"TIME",
    b"SAVE", b"BGSAVE", b"LASTSAVE", b"INFO", b"ROLE", b"READONLY", b"READWRITE", b"ASKING",
    b"CLUSTER", b"CONFIG", b"COMMAND", b"MULTI", b"DISCARD", b"EXEC", b"UNWATCH", b"RANDOMKEY",
    b"DBSIZE", b"KEYS", b"FLUSHDB", b"FLUSHALL", b"SCAN",
} | PUBSUB_COMMANDS
MULTI_KEY_COMMANDS = {
    b"WATCH", b"DEL", b"UNLINK", b"EXISTS", b"TOUCH", b"MGET", b"SUNION", b"SINTER", b"SDIFF",
    b"SUNIONSTORE", b"SINTERSTORE", b"SDIFFSTORE",
}
TWO_KEY_COMMANDS = {
    b"COPY", b"RENAME", b"RENAMENX", b"RPOPLPUSH", b"BRPOPLPUSH", b"LMOVE", b"BLMOVE",
    b"SMOVE", b"ZRANGESTORE",
}
BLOCKING_POP_COMMANDS = {b"BLPOP", b"BRPOP", b"BZPOPMIN", b"BZPOPMAX"}
COUNTED_COMMANDS = {b"SINTERCARD", b"ZINTERCARD", b"ZUNION", b"ZINTER", b"ZDIFF"}
COUNTED_STORE_COMMANDS = {b"ZUNIONSTORE", b"ZINTERSTORE", b"ZDIFFSTORE"}
STREAM_READ_COMMANDS = {b"XREAD", b"XREADGROUP"}
MOVABLE_KEY_COMMANDS = COUNTED_COMMANDS | COUNTED_STORE_COMMANDS | STREAM_READ_COMMANDS


def key_positions(command_name):
    if command_name in KEYLESS_COMMANDS or command_name in MOVABLE_KEY_COMMANDS:
        return 0, 0, 0
    if command_name in MULTI_KEY_COMMANDS:
        return 1, -1, 1
    if command_name in TWO_KEY_COMMANDS:
        return 1, 2, 1
    if command_name in BLOCKING_POP_COMMANDS:
        return 1, -2, 1
    if command_name in {b"MSET", b"MSETNX"}:
        return 1, -1, 2
    if command_name in {b"XGROUP", b"XINFO"}:
        return 2, 2, 1
    return 1, 1, 1


def command_keys(command_name, args):
    if command_name in COUNTED_COMMANDS or command_name in COUNTED_STORE_COMMANDS:
        destination = int(command_name in COUNTED_STORE_COMMANDS)
        if len(args) <= destination:
            return []
        count = integer(args[destination], minimum=1)
        if count > len(args) - destination - 1:
            raise RedisError("ERR Invalid arguments specified for command")
        return [*args[:destination], *args[destination + 1:destination + 1 + count]]
    if command_name in STREAM_READ_COMMANDS:
        start = 3 if command_name == b"XREADGROUP" else 0
        for index in range(start, len(args)):
            if args[index].upper() == b"STREAMS":
                return args[index + 1:index + 1 + (len(args) - index - 1) // 2]
        return []
    first, last, step = key_positions(command_name)
    if not step:
        return []
    if last < 0:
        last += len(args) + 1
    return args[first - 1:last:step]


def key_slot(key: bytes) -> int:
    _, start, tagged = key.partition(b"{")
    inside, end, _ = tagged.partition(b"}")
    if start and end and inside:
        key = inside
    return binascii.crc_hqx(key, 0) % SLOT_COUNT


def check_slots(keys) -> None:
    if len(keys) > 1 and len({key_slot(key) for key in keys}) > 1:
        raise RedisError("CROSSSLOT Keys in request don't hash to the same slot")


EXPIRATION_MODES = {b"EX", b"PX", b"EXAT", b"PXAT"}


def expiry_time(option: bytes, raw: bytes) -> int:
    amount = integer(raw, minimum=1)
    if option in {b"EX", b"EXAT"}:
        amount *= 1000
    if option in {b"EX", b"PX"}:
        amount += milliseconds()
    if amount > MAX_INT:
        raise RedisError("ERR invalid expire time")
    return amount


def timeout_seconds(raw: bytes) -> float:
    timeout = number(raw)
    if not math.isfinite(timeout) or timeout < 0:
        raise RedisError("ERR timeout is not a non-negative finite number")
    return timeout


def validated_name(value: bytes) -> bytes | None:
    if any(byte < 33 or byte > 126 for byte in value):
        raise RedisError("ERR Client names cannot contain spaces, newlines or special characters.")
    return value or None


def client_line(session: Session) -> bytes:
    return b"id=%d name=%s db=%d resp=%d lib-name=%s lib-ver=%s\n" % (
        session.id, session.name or b"", session.database, session.protocol,
        session.lib_name or b"", session.lib_version or b"",
    )


def pairs(items):
    if len(items) % 2:
        raise RedisError(SYNTAX)
    return list(zip(items[::2], items[1::2]))


def read_flags(args, recognized):
    flags = set()
    index = 0
    while index < len(args) and args[index].upper() in recognized:
        flags.add(args[index].upper())
        index += 1
    return flags, args[index:]


def sorted_set(values, reverse=False):
    return sorted(values.items(), key=lambda pair: (pair[1], pair[0]), reverse=reverse)


def score_pairs(result, protocol):
    if protocol == 3:
        return [list(pair) for pair in result]
    return [part for pair in result for part in pair]


def bound(raw, lex):
    if lex and raw in {b"+", b"-"}:
        return (None, True, -1 if raw == b"-" else 1)
    if lex and raw[:1] not in {b"(", b"["}:
        raise RedisError("ERR min or max not valid string range item")
    exclusive = raw.startswith(b"(")
    value = raw[1:] if lex or exclusive else raw
    return (value if lex else number(value), exclusive, 0)


def sorted_range(values, start, stop, *, mode=None, reverse=False):
    result = sorted_set(values, reverse)
    if mode is None:
        return result[index_range(len(result), integer(start), integer(stop))]
    lex = mode == b"BYLEX"
    if reverse:
        start, stop = stop, start
    lower, lower_strict, lower_unbounded = bound(start, lex)
    upper, upper_strict, upper_unbounded = bound(stop, lex)
    if lower_unbounded == 1 or upper_unbounded == -1:
        return []
    selected = []
    for member, score in result:
        value = member if lex else score
        if not lower_unbounded and (value < lower or lower_strict and value == lower):
            continue
        if not upper_unbounded and (value > upper or upper_strict and value == upper):
            continue
        selected.append((member, score))
    return selected


def to_base64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def from_base64(value: str) -> bytes:
    return base64.b64decode(value, validate=True)


def pack_entry(entry: Entry):
    value = entry.value
    if entry.kind == b"string":
        return to_base64(value)
    if entry.kind in {b"list", b"set"}:
        return [to_base64(item) for item in value]
    if entry.kind == b"hash":
        return [[to_base64(key), to_base64(item)] for key, item in value.items()]
    if entry.kind == b"zset":
        return [[to_base64(member), format_score(score).decode("ascii")] for member, score in value.items()]
    if entry.kind == b"stream":
        return {
            "items": [[identifier, [to_base64(field) for field in fields]] for identifier, fields in value.items.items()],
            "last_id": value.last_id,
            "entries_added": value.entries_added,
            "groups": [
                [to_base64(name), group.last_id, group.entries_read,
                 [[to_base64(consumer), seen] for consumer, seen in group.consumers.items()],
                 [[identifier, to_base64(state.consumer), state.delivered_at, state.deliveries]
                  for identifier, state in group.pending.items()]]
                for name, group in value.groups.items()
            ],
        }
    raise ValueError(f"invalid entry kind: {entry.kind!r}")


def unpack_entry(kind, value):
    if kind == b"string":
        return from_base64(value)
    if kind == b"list":
        return [from_base64(item) for item in value]
    if kind == b"set":
        return {from_base64(item) for item in value}
    if kind == b"hash":
        return {from_base64(key): from_base64(item) for key, item in value}
    if kind == b"zset":
        return {from_base64(member): number(score.encode("ascii")) for member, score in value}
    if kind == b"stream":
        stream = Stream(
            items={tuple(identifier): [from_base64(field) for field in fields] for identifier, fields in value["items"]},
            last_id=tuple(value["last_id"]),
            entries_added=value["entries_added"],
        )
        for name, last_id, entries_read, consumers, pending in value["groups"]:
            stream.groups[from_base64(name)] = StreamGroup(
                tuple(last_id), entries_read,
                {from_base64(consumer): seen for consumer, seen in consumers},
                {tuple(identifier): PendingMessage(from_base64(consumer), delivered_at, deliveries)
                 for identifier, consumer, delivered_at, deliveries in pending},
            )
        return stream
    raise ValueError(f"invalid entry kind: {kind!r}")


class RedisHandler(socketserver.StreamRequestHandler):
    def handle(self):
        store = self.server.store
        session = store.connect(self.connection)

        def write_replies():
            try:
                while (reply := session.outbox.get()) is not None:
                    session.writable.set()
                    self.connection.sendall(reply)
            except OSError:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            finally:
                session.closing = True
                session.writable.set()

        writer = threading.Thread(target=write_replies, daemon=True)
        writer.start()
        try:
            while not session.closing:
                session.writable.clear()
                if session.outbox.qsize() >= MAX_PENDING_REPLIES - 1:
                    session.writable.wait(0.25)
                    continue
                try:
                    request = read_command(self.rfile)
                except ProtocolError as error:
                    session.send(error)
                    break
                if request is None:
                    break
                if not request:
                    continue
                with store.changed:
                    if store.stop or session.closing:
                        break
                    try:
                        reply = store.execute(session, request)
                    except RedisError as error:
                        reply = error
                    if reply is not SENT:
                        session.send(reply)
        except (ConnectionError, OSError):
            pass
        finally:
            store.disconnect(session)
            while writer.is_alive():
                try:
                    session.outbox.put(None, timeout=0.25)
                    break
                except queue.Full:
                    if session.closing:
                        break
            writer.join(timeout=1)


class RedisServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, store):
        self.store = store
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, RedisHandler)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6379, help="listen port, use 0 for an available port")
    parser.add_argument("--databases", type=int, default=16, help="number of logical databases (default: 16)")
    parser.add_argument("--cluster", action="store_true", help="advertise one primary serving all Redis Cluster slots")
    parser.add_argument("--db-file", type=Path, help="restore and save a JSON snapshot at this path")
    parser.add_argument("--password-file", type=Path, help="read the default user's password from this file")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if not 1 <= args.databases <= 1024:
        parser.error("--databases must be between 1 and 1024")
    try:
        password = os.environb.get(b"REDIS_TEST_PASSWORD") if os.supports_bytes_environ else None
        if not os.supports_bytes_environ and "REDIS_TEST_PASSWORD" in os.environ:
            password = os.environ["REDIS_TEST_PASSWORD"].encode("utf-8")
        if args.password_file is not None:
            password = args.password_file.read_bytes().rstrip(b"\r\n")
            if not password:
                parser.error("--password-file must contain a non-empty password")
        store = Store(1 if args.cluster else args.databases, password or None, args.db_file, args.cluster)
        server = RedisServer((args.host, args.port), store)
        host, port = server.server_address[:2]
        store.endpoint = (host.encode("ascii") if host not in {"0.0.0.0", "::"} else b"", port)
    except (OSError, ValueError, RedisError, KeyError, TypeError, binascii.Error) as error:
        print(f"Redis test server startup failed: {error}", file=sys.stderr)
        return 1

    def stop(_signal, _frame):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        with server:
            host, port = server.server_address[:2]
            printable_host = f"[{host}]" if ":" in host else host
            print(f"Redis test server listening on {printable_host}:{port}", flush=True)
            server.serve_forever()
            with store.changed:
                store.stop = True
                store.changed.notify_all()
                if args.db_file is not None:
                    store.save()
    except RedisError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
