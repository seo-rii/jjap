"""Verify Redis snapshot exports by parsing the RDB and RESP output independently."""

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "redis_server.py"
UNKNOWN = (1 << 64) - 1


def crc64(data):
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x95AC9329AC4BC9B5 if crc & 1 else 0)
    return crc


class Reader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def take(self, size):
        chunk = self.data[self.offset:self.offset + size]
        if len(chunk) != size:
            raise AssertionError("truncated RDB")
        self.offset += size
        return chunk

    def byte(self):
        return self.take(1)[0]

    def length(self):
        first = self.byte()
        if first >> 6 == 0:
            return first & 0x3F
        if first >> 6 == 1:
            return (first & 0x3F) << 8 | self.byte()
        if first in (0x80, 0x81):
            return int.from_bytes(self.take(4 if first == 0x80 else 8), "big")
        raise AssertionError(f"unexpected special encoding {first:#x}")

    def string(self):
        return self.take(self.length())

    def stream_id(self):
        return self.length(), self.length()


def raw_id(data):
    return int.from_bytes(data[:8], "big"), int.from_bytes(data[8:], "big")


def parse_listpack(data):
    if int.from_bytes(data[:4], "little") != len(data) or data[-1] != 0xFF:
        raise AssertionError("invalid listpack header or terminator")
    offset, items = 6, []
    while offset < len(data) - 1:
        start, first = offset, data[offset]
        if first < 0x80:
            value, offset = first, offset + 1
        elif first & 0xC0 == 0x80:
            size = first & 0x3F
            value, offset = data[offset + 1:offset + 1 + size], offset + 1 + size
        elif first & 0xE0 == 0xC0:
            value = (first & 0x1F) << 8 | data[offset + 1]
            value, offset = value - (1 << 13) if value >> 12 else value, offset + 2
        elif first & 0xF0 == 0xE0:
            size = (first & 0x0F) << 8 | data[offset + 1]
            value, offset = data[offset + 2:offset + 2 + size], offset + 2 + size
        elif first == 0xF0:
            size = int.from_bytes(data[offset + 1:offset + 5], "little")
            value, offset = data[offset + 5:offset + 5 + size], offset + 5 + size
        else:
            width = {0xF1: 2, 0xF2: 3, 0xF3: 4, 0xF4: 8}[first]
            value = int.from_bytes(data[offset + 1:offset + 1 + width], "little", signed=True)
            offset += 1 + width
        encoded = offset - start
        size = 1 if encoded <= 127 else 2 if encoded < 16383 else 3 if encoded < 2097151 else 4
        backlen, shift, position = 0, 0, offset + size - 1
        while True:
            backlen |= (data[position] & 127) << shift
            if not data[position] & 128:
                break
            shift, position = shift + 7, position - 1
        if (backlen, position) != (encoded, offset):
            raise AssertionError(f"bad backlen at {start}")
        offset += size
        items.append(value)
    if int.from_bytes(data[4:6], "little") != min(len(items), 0xFFFF):
        raise AssertionError("listpack element count mismatch")
    return items


def text(value):
    return str(value).encode() if isinstance(value, int) else value


def parse_stream(reader):
    entries, nodes = [], reader.length()
    for _ in range(nodes):
        master = raw_id(reader.string())
        items = parse_listpack(reader.string())
        count, deleted, width = items[:3]
        names, position = items[3:3 + width], 4 + width
        if items[3 + width] != 0 or deleted:
            raise AssertionError("unexpected master entry")
        for _ in range(count):
            flags, ms, seq = items[position:position + 3]
            position += 3
            if flags & 2:
                fields = [part for pair in zip(names, items[position:position + width]) for part in pair]
                position += width
                expected_count = width + 3
            else:
                size = items[position]
                fields = items[position + 1:position + 1 + 2 * size]
                position += 1 + 2 * size
                expected_count = 2 * size + 4
            if items[position] != expected_count:
                raise AssertionError("bad lp-count")
            position += 1
            entries.append((((master[0] + ms) % (1 << 64), (master[1] + seq) % (1 << 64)), list(map(text, fields))))
        if position != len(items):
            raise AssertionError("trailing listpack items")
    length, last, first, deleted, added = reader.length(), reader.stream_id(), reader.stream_id(), reader.stream_id(), reader.length()
    if length != len(entries):
        raise AssertionError("stream length mismatch")
    groups = {}
    for _ in range(reader.length()):
        name, group_last, read = reader.string(), reader.stream_id(), reader.length()
        pending = {}
        for _ in range(reader.length()):
            identifier = raw_id(reader.take(16))
            pending[identifier] = [None, int.from_bytes(reader.take(8), "little", signed=True), reader.length()]
        consumers = {}
        for _ in range(reader.length()):
            consumer = reader.string()
            consumers[consumer] = int.from_bytes(reader.take(8), "little", signed=True)
            for _ in range(reader.length()):
                pending[raw_id(reader.take(16))][0] = consumer
        groups[name] = {"last": group_last, "read": None if read == UNKNOWN else read, "consumers": consumers,
                        "pending": {key: tuple(value) for key, value in pending.items()}}
    return {"entries": entries, "last": last, "first": first, "deleted": deleted, "added": added, "groups": groups}


def parse_rdb(data):
    if data[:9] != b"REDIS0010" or crc64(data[:-8]) != int.from_bytes(data[-8:], "little"):
        raise AssertionError("bad RDB header or checksum")
    reader, aux, databases, database, expires = Reader(data[:-8]), {}, {}, None, None
    reader.offset = 9
    while (opcode := reader.byte()) != 0xFF:
        if opcode == 0xFA:
            name = reader.string()
            aux[name] = reader.string()
        elif opcode == 0xFE:
            database = databases.setdefault(reader.length(), {})
        elif opcode == 0xFB:
            database["sizes"] = (reader.length(), reader.length())
        elif opcode == 0xFC:
            expires = int.from_bytes(reader.take(8), "little", signed=True)
        else:
            key = reader.string()
            if opcode == 0:
                value = reader.string()
            elif opcode in (1, 2):
                value = [reader.string() for _ in range(reader.length())]
            elif opcode == 4:
                value = dict((reader.string(), reader.string()) for _ in range(reader.length()))
            elif opcode == 5:
                value = [(reader.string(), struct.unpack("<d", reader.take(8))[0]) for _ in range(reader.length())]
            elif opcode == 19:
                value = parse_stream(reader)
            else:
                raise AssertionError(f"unexpected type {opcode}")
            database[key] = (opcode, expires, value)
            expires = None
    if reader.offset != len(reader.data):
        raise AssertionError("data after EOF")
    return aux, databases


def parse_resp(data):
    commands, offset = [], 0
    while offset < len(data):
        header = re.compile(rb"\*(\d+)\r\n").match(data, offset)
        offset, arguments = header.end(), []
        for _ in range(int(header[1])):
            bulk = re.compile(rb"\$(\d+)\r\n").match(data, offset)
            end = bulk.end() + int(bulk[1])
            if data[end:end + 2] != b"\r\n":
                raise AssertionError("bad bulk terminator")
            arguments.append(data[bulk.end():end])
            offset = end + 2
        commands.append(arguments)
    return commands


def parse_id(value):
    ms, seq = value.split(b"-")
    return int(ms), int(seq)


def replay(commands):
    """Apply the exported commands with the Redis semantics they rely on."""
    databases, index = {}, 0
    for command in commands:
        name = command[0]
        if name == b"SELECT":
            index = int(command[1])
            continue
        data = databases.setdefault(index, {})
        key = command[2] if name == b"XGROUP" else command[1]
        if name == b"DEL":
            data.pop(key, None)
        elif name == b"SET":
            data[key] = ["string", None, command[2]]
        elif name in {b"RPUSH", b"SADD"}:
            data.setdefault(key, ["list" if name == b"RPUSH" else "set", None, []])[2].extend(command[2:])
        elif name in {b"HSET", b"ZADD"}:
            target = data.setdefault(key, ["hash" if name == b"HSET" else "zset", None, {}])[2]
            for first, second in zip(command[2::2], command[3::2]):
                target.update({first: second} if name == b"HSET" else {second: float(first)})
        elif name == b"PEXPIREAT":
            data[key][1] = int(command[2])
        else:
            stream = data.setdefault(key, ["stream", None, {
                "entries": {}, "last": (0, 0), "deleted": (0, 0), "added": 0, "groups": {}}])[2]
            if name == b"XADD" and command[2] == b"MAXLEN":
                stream["added"] += 1
            elif name == b"XADD":
                identifier = parse_id(command[2])
                assert identifier > stream["last"]
                stream["entries"][identifier] = command[3:]
                stream["last"], stream["added"] = identifier, stream["added"] + 1
            elif name == b"XSETID":
                assert command[3::2] == [b"ENTRIESADDED", b"MAXDELETEDID"]
                stream["last"], stream["added"] = parse_id(command[2]), int(command[4])
                stream["deleted"] = max(stream["deleted"], parse_id(command[6]))
                assert stream["added"] >= len(stream["entries"]) and stream["last"] >= max(stream["entries"], default=(0, 0))
            elif name == b"XGROUP" and command[1] == b"CREATE":
                assert command[5] == b"ENTRIESREAD"
                read = int(command[6])
                stream["groups"][command[3]] = {"last": parse_id(command[4]), "read": None if read == -1 else read,
                                                "consumers": {}, "pending": {}}
            elif name == b"XGROUP":
                assert command[1] == b"CREATECONSUMER"
                stream["groups"][command[3]]["consumers"][command[4]] = None
            elif name == b"XCLAIM":
                assert command[4] == b"0" and command[6::2] == [b"TIME", b"RETRYCOUNT", b"JUSTID"] and command[-1] == b"FORCE"
                identifier = parse_id(command[5])
                assert identifier in stream["entries"], "XCLAIM FORCE ignores missing entries"
                group = stream["groups"][command[2]]
                group["consumers"][command[3]] = None
                group["pending"][identifier] = (command[3], int(command[7]), int(command[9]))
            else:
                assert name == b"XDEL"
                for identifier in map(parse_id, command[2:]):
                    del stream["entries"][identifier]
                    stream["deleted"] = max(stream["deleted"], identifier)
    for data in databases.values():
        for kind, _, value in data.values():
            if kind == "stream":
                value["entries"] = sorted(value["entries"].items())
                value["first"] = value["entries"][0][0] if value["entries"] else (0, 0)
    return databases


class RedisExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-redis-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("origin #?.json" if os.name != "nt" else "origin #.json")

    def command(self, source, output, *args, check=True):
        return subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), "export", "--db-file", str(source),
             "--out", str(output), *args],
            cwd=self.directory, check=check, text=True, capture_output=True, timeout=30,
        )

    def fixture(self, databases=16):
        store = self.redis["Store"](databases, None, self.source, False)
        session = self.redis["Session"](1, True)

        def run(*args):
            with store.changed:
                return store.execute(session, [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args])

        run("SET", b"\x00bin\r\nkey", bytes(range(256)))
        run("SET", "ключ", "значение 🔑")
        run("SET", "", "")
        run("SET", "counter", "-12", "PX", 3_600_000)
        run("RPUSH", "list", "", "a", "123", b"\xff", *range(200))
        run("SADD", "ints", 3, 1, -2)
        run("SADD", "mixed", "", "b", b"\x00")
        run("HSET", "hash", "", "", "field", "value", "n", "7")
        run("PEXPIRE", "hash", 7_200_000)
        run("ZADD", "zset", "inf", "top", "-inf", "bottom", "1e308", "huge", "5e-324", "tiny", "0.1", "", "-3", "neg")
        for number in range(1, 151):
            fields = ["kind", "tick", "n", number] if number % 9 else ["other", number, "blob", "x" * 70 * number]
            run("XADD", "events", f"{1_700_000_000_000 + number // 2}-{number % 2}", *fields)
        run("XADD", "events", "18446744073709551615-18446744073709551615", "last", "entry")
        run("XDEL", "events", "1700000000010-0")
        run("XGROUP", "CREATE", "events", "workers", "0")
        run("XREADGROUP", "GROUP", "workers", "alice", "COUNT", 6, "STREAMS", "events", ">")
        run("XREADGROUP", "GROUP", "workers", "alice", "STREAMS", "events", "0")
        run("XACK", "events", "workers", "1700000000001-0")
        run("XDEL", "events", "1700000000002-0")
        run("XGROUP", "CREATECONSUMER", "events", "workers", "bob")
        run("XGROUP", "CREATE", "events", "late", "$")
        run("XGROUP", "CREATE", "events", b"\xffgroup", "1700000000020-0", "ENTRIESREAD", 41)
        run("XGROUP", "CREATE", "empty", "readers", "$", "MKSTREAM")
        run("SELECT", 3)
        run("SET", "other", "database")
        with store.changed:
            store.save()
        return store

    def expected(self, store, now=None):
        now = now or time.time_ns() // 1_000_000
        result = {}
        for index, data in enumerate(store.databases):
            for key, entry in data.items():
                if entry.expires is not None and entry.expires <= now:
                    continue
                value = entry.value
                if entry.kind == b"stream":
                    deleted = sorted({i for g in value.groups.values() for i in g.pending} - value.items.keys())
                    value = {
                        "entries": sorted((identifier, list(fields)) for identifier, fields in value.items.items()),
                        "last": value.last_id, "first": min(value.items, default=(0, 0)),
                        "deleted": deleted[-1] if deleted else (0, 0), "added": value.entries_added,
                        "groups": {name: {
                            "last": group.last_id, "read": group.entries_read, "consumers": dict(group.consumers),
                            "pending": {i: (s.consumer, s.delivered_at, s.deliveries) for i, s in group.pending.items()},
                        } for name, group in value.groups.items()},
                    }
                elif entry.kind == b"set":
                    value = sorted(value)
                result.setdefault(index, {})[key] = [entry.kind.decode(), entry.expires, value]
        return result

    def test_rdb_preserves_every_type_stream_groups_and_expiry(self):
        store = self.fixture()
        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()
        output = self.directory / "out" / "dump.rdb"
        result = self.command(self.source, output)
        self.assertIn("Exported 12 keys in 2 databases", result.stdout)
        aux, databases = parse_rdb(output.read_bytes())
        self.assertEqual((aux[b"redis-ver"], aux[b"redis-bits"]), (b"7.0.0", b"64"))
        self.assertEqual(databases[0].pop("sizes"), (11, 2))
        self.assertEqual(databases[3].pop("sizes"), (1, 0))
        kinds = {0: "string", 1: "list", 2: "set", 4: "hash", 5: "zset", 19: "stream"}
        actual = {}
        for index, data in databases.items():
            for key, (kind, expires, value) in data.items():
                if kind == 5:
                    self.assertEqual(value, sorted(value, key=lambda pair: (pair[1], pair[0]), reverse=True))
                    value = dict(value)
                actual.setdefault(index, {})[key] = [kinds[kind], expires, value]
        self.assertEqual(actual, self.expected(store))
        events = actual[0][b"events"][2]
        self.assertEqual(events["deleted"], (1_700_000_000_002, 0))
        self.assertEqual(events["groups"][b"workers"]["pending"][(1_700_000_000_000, 1)][2], 2)
        self.assertEqual((events["groups"][b"late"]["read"], events["groups"][b"\xffgroup"]["read"]), (None, 41))
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)

    def test_resp_rebuilds_the_same_state_with_binary_safe_commands(self):
        store = self.fixture()
        output = self.directory / "fixture.resp"
        result = self.command(self.source, output, "--format", "resp")
        self.assertIn("seen-time of 2 stream consumers when the commands run", result.stderr)
        commands = parse_resp(output.read_bytes())
        self.assertEqual(commands[:3], [[b"SELECT", b"0"], [b"DEL", b"\x00bin\r\nkey"], [b"SET", b"\x00bin\r\nkey", bytes(range(256))]])
        self.assertIn([b"XADD", b"events", b"1700000000002-0", b"", b""], commands)
        self.assertIn([b"XDEL", b"events", b"1700000000002-0"], commands)
        self.assertIn([b"XADD", b"empty", b"MAXLEN", b"0", b"0-1", b"x", b"y"], commands)
        self.assertIn([b"ZADD", b"zset", b"-inf", b"bottom", b"-3", b"neg", b"5e-324", b"tiny", b"0.1", b"",
                       b"1e+308", b"huge", b"inf", b"top"], commands)
        self.assertLessEqual(max(len(command) for command in commands if command[0] == b"RPUSH"), 66)
        actual = replay(commands)
        expected = self.expected(store)
        for data in expected.values():
            for item in data.values():
                if item[0] == "set":
                    item[2] = sorted(item[2])
                if item[0] == "stream":
                    for group in item[2]["groups"].values():
                        group["consumers"] = dict.fromkeys(group["consumers"])
        for data in actual.values():
            for item in data.values():
                if item[0] == "set":
                    item[2] = sorted(item[2])
        self.assertEqual(actual, expected)

    def test_listpack_encoding_matches_redis_rules(self):
        entry, split = self.redis["listpack_entry"], self.redis["stream_listpacks"]
        self.assertEqual(self.redis["crc64"](b"123456789"), 0xE9C6D914C4B8D9CA)
        self.assertEqual(entry(b"127"), b"\x7f\x01")
        self.assertEqual(entry(b"-1"), b"\xdf\xff\x02")
        self.assertEqual(entry(b"4096"), b"\xf1\x00\x10\x03")
        self.assertEqual(entry(b"-9223372036854775808"), b"\xf4" + (1 << 63).to_bytes(8, "little") + b"\x09")
        for literal in (b"01", b"-0", b"+1", b" 1", b"9223372036854775808"):
            self.assertEqual(entry(literal), bytes((0x80 | len(literal),)) + literal + bytes((len(literal) + 1,)))
        self.assertEqual(entry(b"x" * 64), b"\xe0\x40" + b"x" * 64 + b"\x42")
        self.assertEqual(entry(b"w" * 125), b"\xe0\x7d" + b"w" * 125 + b"\x7f")
        self.assertEqual(entry(b"w" * 126), b"\xe0\x7e" + b"w" * 126 + b"\x01\x80")
        self.assertEqual(entry(b"y" * 4096), b"\xf0\x00\x10\x00\x00" + b"y" * 4096 + b"\x20\x85")
        self.assertEqual(len(entry(b"z" * 16400)), 5 + 16400 + 3)
        nodes = split([((1, index), [b"f", b"v"]) for index in range(250)])
        self.assertEqual([parse_listpack(data)[0] for _, data in nodes], [100, 100, 50])
        top = (1 << 64) - 1
        nodes = split([((0, 1), [b"a", b"1"]), ((top, 0), [b"a", b"2", b"b", b""]), ((top, top), [b"a", b"x" * 5000])])
        self.assertEqual([master for master, _ in nodes], [(0, 1), (top, top)])
        items = parse_listpack(nodes[0][1])
        self.assertEqual(items[:9], [2, 0, 1, b"a", 0, 2, 0, 0, 1])
        self.assertEqual(items[10:14], [0, -1, -1, 2])

    def test_database_selection_expired_and_empty_entries(self):
        store = self.fixture(databases=20)
        with store.changed:
            store.databases[17][b"far"] = self.redis["Entry"](b"string", b"db17")
            store.save()
        snapshot = json.loads(self.source.read_text(encoding="utf-8"))
        encode = lambda value: base64.b64encode(value).decode("ascii")
        snapshot["databases"][3] += [[encode(b"expired"), "string", 1000, encode(b"x")], [encode(b"blank"), "list", None, []]]
        self.source.write_text(json.dumps(snapshot), encoding="utf-8")
        result = self.command(self.source, self.directory / "all.rdb")
        self.assertIn("skipped 1 expired keys", result.stdout)
        self.assertIn("skipped empty list b'blank' in database 3", result.stderr)
        self.assertIn("database 17 loads only into Redis configured with databases 18 or more", result.stderr)
        self.assertEqual(sorted(parse_rdb((self.directory / "all.rdb").read_bytes())[1]), [0, 3, 17])
        self.command(self.source, self.directory / "db3.resp", "--format", "resp", "--db", "3")
        commands = parse_resp((self.directory / "db3.resp").read_bytes())
        self.assertEqual(commands, [[b"SELECT", b"3"], [b"DEL", b"other"], [b"SET", b"other", b"database"]])
        result = self.command(self.source, self.directory / "none.rdb", "--db", "20", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Database 20 is not in the snapshot, which has 20 databases", result.stderr)

    def test_rejects_existing_destination_and_cleans_up_invalid_sources(self):
        self.fixture()
        output = self.directory / "used.rdb"
        output.write_text("preserve", encoding="utf-8")
        result = self.command(self.source, output, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Output file already exists", result.stderr)
        self.assertEqual(output.read_text(encoding="utf-8"), "preserve")

        stream_row = json.loads(self.source.read_text(encoding="utf-8"))
        row = next(row for row in stream_row["databases"][0] if row[1] == "stream" and row[3]["groups"])
        row[3]["groups"][0][4][0][1] = base64.b64encode(b"ghost").decode()
        cases = {
            "missing.json": (None, "Snapshot file not found"),
            "garbage.json": (b"REDIS0010\xff", "Expected a Redis test server JSON snapshot"),
            "version.json": (b'{"version": 2, "databases": []}', "Unsupported Redis test server snapshot version 2"),
            "row.json": (b'{"version": 1, "databases": [[["a2V5", "string", true, "dg=="]]]}', "invalid expiration time True"),
            "stream.json": (json.dumps(stream_row).encode(), "has unknown consumer b'ghost'"),
        }
        for name, (content, message) in cases.items():
            with self.subTest(name):
                source = self.directory / name
                if content is not None:
                    source.write_bytes(content)
                target = self.directory / f"{name}.rdb"
                result = self.command(source, target, check=False)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Redis export failed: "), result.stderr)
                self.assertIn(message, result.stderr)
                self.assertFalse(target.exists())
                self.assertEqual(list(self.directory.glob(".redis-export-*")), [])

    @unittest.skipUnless(shutil.which("redis-server") and hasattr(socket, "AF_UNIX"), "needs redis-server")
    def test_real_redis_loads_rdb_and_resp_identically(self):
        version = subprocess.run(["redis-server", "--version"], capture_output=True, text=True).stdout
        if int(re.search(r"v=(\d+)", version)[1]) < 7:
            self.skipTest("RDB version 10 needs Redis 7.0 or later")
        self.fixture()
        rdb, resp = self.directory / "redis-a" / "dump.rdb", self.directory / "fixture.resp"
        self.command(self.source, rdb)
        self.command(self.source, resp, "--format", "resp")
        clients = {}
        for name in ("redis-a", "redis-b"):
            folder = self.directory / name
            folder.mkdir(exist_ok=True)
            process = subprocess.Popen([
                "redis-server", "--port", "0", "--unixsocket", str(folder / "s"), "--dir", str(folder),
                "--save", "", "--appendonly", "no", "--sanitize-dump-payload", "yes",
                "--enable-debug-command", "yes", "--logfile", str(folder / "log"),
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.addCleanup(process.wait, 10)
            self.addCleanup(process.terminate)
            deadline = time.monotonic() + 10
            while not (folder / "s").exists():
                if process.poll() is not None:
                    self.fail(f"redis-server exited: {(folder / 'log').read_text(errors='replace')[-500:]}")
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.02)
            connection = socket.socket(socket.AF_UNIX)
            self.addCleanup(connection.close)
            connection.connect(str(folder / "s"))
            reader = connection.makefile("rb")
            self.addCleanup(reader.close)
            clients[name] = (connection, reader)

        def reply(reader):
            line = reader.readline()[:-2]
            if line[:1] == b"$":
                return None if line == b"$-1" else reader.read(int(line[1:]) + 2)[:-2]
            if line[:1] == b"*":
                return None if line == b"*-1" else [reply(reader) for _ in range(int(line[1:]))]
            if line[:1] == b"-" and not line.startswith(b"-LOADING"):
                raise AssertionError(line)
            return int(line[1:]) if line[:1] == b":" else line[1:]

        def call(name, *args):
            connection, reader = clients[name]
            parts = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
            connection.sendall(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts))
            return reply(reader)

        for name in clients:
            while (status := call(name, "PING")) != b"PONG":
                self.assertTrue(status.startswith(b"LOADING"), status)
                time.sleep(0.02)

        commands = parse_resp(resp.read_bytes())
        clients["redis-b"][0].sendall(resp.read_bytes())
        for _ in commands:
            reply(clients["redis-b"][1])
        for index in (0, 3):
            call("redis-a", "SELECT", index)
            call("redis-b", "SELECT", index)
            keys = sorted(call("redis-a", "KEYS", "*"))
            self.assertEqual(keys, sorted(call("redis-b", "KEYS", "*")))
            for key in keys:
                for probe in (("TYPE",), ("PEXPIRETIME",), ("DEBUG", "DIGEST-VALUE")):
                    self.assertEqual(call("redis-a", *probe, key), call("redis-b", *probe, key), (probe, key))
                if call("redis-a", "TYPE", key) == b"stream":
                    full = [call(name, "XINFO", "STREAM", key, "FULL", "COUNT", 0) for name in clients]
                    for info in full:
                        for group in info[info.index(b"groups") + 1]:
                            for consumer in group[group.index(b"consumers") + 1]:
                                for field in (b"seen-time", b"active-time"):
                                    if field in consumer:
                                        consumer[consumer.index(field) + 1] = None
                    self.assertEqual(full[0], full[1], key)
        self.assertEqual(call("redis-a", "DBSIZE"), 1)


if __name__ == "__main__":
    unittest.main()
