"""Verify Redis imports from RDB files, append-only files, and RESP commands built independently of the server."""

import base64
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
HOUR = 3_600_000
MODULE_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def crc64(data):
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x95AC9329AC4BC9B5 if crc & 1 else 0)
    return crc


def length(value):
    if value < 64:
        return bytes((value,))
    if value < 16384:
        return bytes((0x40 | value >> 8, value & 0xFF))
    return b"\x80" + value.to_bytes(4, "big") if value < 1 << 32 else b"\x81" + value.to_bytes(8, "big")


def string(value):
    return length(len(value)) + value


def lzf_string(data, period=None):
    """LZF-encode data as literal runs, or, for data that repeats its first period bytes, as back references."""
    if period is None:
        output = b"".join(bytes((len(data[n:n + 32]) - 1,)) + data[n:n + 32] for n in range(0, len(data), 32))
        return b"\xc3" + length(len(output)) + length(len(data)) + output
    output, position = bytearray((period - 1,)) + data[:period], period
    while position < len(data):
        size, distance = min(264, len(data) - position), period - 1
        if size < 2:
            output += b"\x00" + data[position:]
            break
        code = size - 2
        output += bytes((min(code, 7) << 5 | distance >> 8,)) + (bytes((code - 7,)) if code >= 7 else b"")
        output += bytes((distance & 0xFF,))
        position += size
    return b"\xc3" + length(len(output)) + length(len(data)) + bytes(output)


def ms(value):
    return value.to_bytes(8, "little", signed=True)


def raw_id(ms_part, seq):
    return ms_part.to_bytes(8, "big") + seq.to_bytes(8, "big")


def ziplist(items):
    entries, previous = [], 0
    for item in items:
        header = bytes((previous,)) if previous < 254 else b"\xfe" + previous.to_bytes(4, "little")
        if isinstance(item, int):
            if 0 <= item <= 12:
                encoded = bytes((0xF1 + item,))
            else:
                marker, width = next((m, w) for m, w in ((0xFE, 1), (0xC0, 2), (0xF0, 3), (0xD0, 4), (0xE0, 8))
                                     if -(1 << 8 * w - 1) <= item < 1 << 8 * w - 1)
                encoded = bytes((marker,)) + item.to_bytes(width, "little", signed=True)
        elif len(item) < 64:
            encoded = bytes((len(item),)) + item
        else:
            encoded = bytes((0x40 | len(item) >> 8, len(item) & 0xFF)) + item
        entries.append(header + encoded)
        previous = len(entries[-1])
    body = b"".join(entries)
    tail = 10 + len(body) - (len(entries[-1]) if entries else 0)
    return (11 + len(body)).to_bytes(4, "little") + tail.to_bytes(4, "little") + len(items).to_bytes(2, "little") + body + b"\xff"


def listpack(items):
    body = b""
    for item in items:
        if isinstance(item, int):
            if 0 <= item < 128:
                encoded = bytes((item,))
            elif -4096 <= item < 4096:
                encoded = bytes((0xC0 | (item & 0x1FFF) >> 8, item & 0xFF))
            else:
                marker, width = next((m, w) for m, w in ((0xF1, 2), (0xF2, 3), (0xF3, 4), (0xF4, 8))
                                     if -(1 << 8 * w - 1) <= item < 1 << 8 * w - 1)
                encoded = bytes((marker,)) + item.to_bytes(width, "little", signed=True)
        elif len(item) < 64:
            encoded = bytes((0x80 | len(item),)) + item
        elif len(item) < 4096:
            encoded = bytes((0xE0 | len(item) >> 8, len(item) & 0xFF)) + item
        else:
            encoded = b"\xf0" + len(item).to_bytes(4, "little") + item
        size = len(encoded)
        body += encoded + (bytes((size,)) if size < 128 else bytes((size >> 7, size & 127 | 128)))
    return (len(body) + 7).to_bytes(4, "little") + len(items).to_bytes(2, "little") + body + b"\xff"


def intset(width, values):
    return width.to_bytes(4, "little") + len(values).to_bytes(4, "little") + b"".join(
        value.to_bytes(width, "little", signed=True) for value in sorted(values))


def stream_node(master, master_fields, entries):
    """entries are (id, fields, deleted); fields that match the master fields use the SAMEFIELDS flag."""
    live = sum(not deleted for _, _, deleted in entries)
    items = [live, len(entries) - live, len(master_fields), *master_fields, 0]
    for identifier, fields, deleted in entries:
        same = fields[::2] == master_fields
        items += [int(deleted) | (2 if same else 0), identifier[0] - master[0], identifier[1] - master[1]]
        items += fields[1::2] if same else [len(fields) // 2, *fields]
        items.append(len(fields) // 2 + 3 if same else len(fields) + 4)
    return string(raw_id(*master)) + string(listpack(items))


def rdb(version, body, checksum=True):
    data = b"REDIS%04d" % version + body + b"\xff"
    return data + ((crc64(data) if checksum else 0).to_bytes(8, "little") if version >= 5 else b"")


def key(rdb_type, name, payload, expires=None):
    return (b"\xfc" + ms(expires) if expires is not None else b"") + bytes((rdb_type,)) + string(name) + payload


def module_id(name, version):
    value = 0
    for character in name:
        value = value << 6 | MODULE_CHARACTERS.index(character)
    return value << 10 | version


def resp(*arguments):
    parts = [item if isinstance(item, bytes) else str(item).encode() for item in arguments]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(part), part) for part in parts)


class Client:
    def __init__(self, port):
        self.connection = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.reader = self.connection.makefile("rb")

    def close(self):
        self.reader.close()
        self.connection.close()

    def reply(self):
        line = self.reader.readline()[:-2]
        kind, rest = line[:1], line[1:]
        if kind == b"$":
            return None if rest == b"-1" else self.reader.read(int(rest) + 2)[:-2]
        if kind == b"*":
            return None if rest == b"-1" else [self.reply() for _ in range(int(rest))]
        if kind == b"-":
            raise AssertionError(rest.decode())
        return int(rest) if kind == b":" else rest

    def __call__(self, *arguments):
        self.connection.sendall(resp(*arguments))
        return self.reply()


def walk(call, databases):
    """Read every key back through ordinary commands; consumer seen times are left out on purpose."""
    result = {}
    for database in range(databases):
        call("SELECT", database)
        for name in sorted(call("KEYS", "*")):
            kind = call("TYPE", name).decode()
            if kind == "string":
                value = call("GET", name)
            elif kind == "list":
                value = call("LRANGE", name, 0, -1)
            elif kind == "set":
                value = sorted(call("SMEMBERS", name))
            elif kind == "hash":
                value = sorted(zip(*[iter(call("HGETALL", name))] * 2))
            elif kind == "zset":
                items = call("ZRANGE", name, 0, -1, "WITHSCORES")
                value = [(member, float(score)) for member, score in zip(items[::2], items[1::2])]
            else:
                info = call("XINFO", "STREAM", name)
                groups = []
                for group in call("XINFO", "GROUPS", name):
                    group = dict(zip(group[::2], group[1::2]))
                    pending = call("XPENDING", name, group[b"name"], "-", "+", 1000)
                    consumers = [dict(zip(item[::2], item[1::2])) for item in call("XINFO", "CONSUMERS", name, group[b"name"])]
                    groups.append((group[b"name"], group[b"last-delivered-id"], group[b"entries-read"],
                                   [(item[0], item[1], item[3]) for item in pending],
                                   sorted((item[b"name"], item[b"pending"]) for item in consumers)))
                value = (call("XRANGE", name, "-", "+"), info[info.index(b"last-generated-id") + 1],
                         info[info.index(b"entries-added") + 1], sorted(groups))
            result[database, name] = (kind, call("PEXPIRETIME", name), value)
    return result


class RedisImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-redis-import-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    def run_script(self, *args, check=True):
        return subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)],
                              cwd=self.directory, check=check, text=True, capture_output=True, timeout=60)

    def import_file(self, source, target, *args, check=True):
        return self.run_script("import", "--db-file", target, "--in", source, *args, check=check)

    def write(self, name, data):
        path = self.directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def snapshot(self, path):
        """Decode a JSON snapshot into {(database, key): (kind, expires, value)}."""
        decode = base64.b64decode
        result = {}
        for database, rows in enumerate(json.loads(path.read_text(encoding="utf-8"))["databases"]):
            for name, kind, expires, value in rows:
                if kind == "string":
                    value = decode(value)
                elif kind == "list":
                    value = [decode(item) for item in value]
                elif kind == "set":
                    value = sorted(decode(item) for item in value)
                elif kind in ("hash", "zset"):
                    value = dict((decode(first), decode(second) if kind == "hash" else float(second)) for first, second in value)
                else:
                    value["items"] = [[tuple(identifier), [decode(field) for field in fields]] for identifier, fields in value["items"]]
                    value["groups"] = {decode(group[0]): {
                        "last": tuple(group[1]), "read": group[2],
                        "consumers": {decode(consumer): seen for consumer, seen in group[3]},
                        "pending": {tuple(identifier): (decode(consumer), delivered, count)
                                    for identifier, consumer, delivered, count in group[4]},
                    } for group in value["groups"]}
                    value["last_id"] = tuple(value["last_id"])
                result[database, decode(name)] = (kind, expires, value)
        return result

    def serve(self, db_file, databases=16):
        process = subprocess.Popen([sys.executable, "-I", "-S", "-B", str(self.script), "--port", "0", "--db-file", str(db_file),
                                    "--databases", str(databases)], cwd=self.directory, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        line = process.stdout.readline()
        self.assertIn("listening", line, process.stderr.read() if not line else line)
        client = Client(int(line.rsplit(":", 1)[1]))
        self.addCleanup(client.close)
        return client

    def fixture(self, path):
        store = self.redis["Store"](16, None, path, False)
        session = self.redis["Session"](1, True)

        def run(*args):
            with store.changed:
                return store.execute(session, [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args])

        run("SET", b"\x00bin\r\nkey", bytes(range(256)))
        run("SET", "counter", "-12", "PX", HOUR)
        run("SET", "compressible", "abcdefgh" * 300)
        run("RPUSH", "list", "", "a", "123", b"\xff", *range(300), "x" * 5000)
        run("SADD", "ints", 3, 1, -2, 9223372036854775807)
        run("SADD", "mixed", "", "b", b"\x00")
        run("HSET", "hash", "", "", "field", "value", "n", "7")
        run("PEXPIRE", "hash", 2 * HOUR)
        run("ZADD", "zset", "inf", "top", "-inf", "bottom", "1e308", "huge", "5e-324", "tiny", "0.1", "", "-3", "neg")
        for number in range(1, 151):
            fields = ["kind", "tick", "n", number] if number % 9 else ["other", number, "blob", "x" * 70 * number]
            run("XADD", "events", f"{1_700_000_000_000 + number // 2}-{number % 2}", *fields)
        run("XDEL", "events", "1700000000010-0")
        run("XGROUP", "CREATE", "events", "workers", "0")
        run("XREADGROUP", "GROUP", "workers", "alice", "COUNT", 6, "STREAMS", "events", ">")
        run("XACK", "events", "workers", "1700000000001-0")
        run("XDEL", "events", "1700000000002-0")
        run("XGROUP", "CREATECONSUMER", "events", "workers", "bob")
        run("XGROUP", "CREATE", "events", "late", "$", "ENTRIESREAD", 149)
        run("XGROUP", "CREATE", "empty", "readers", "$", "MKSTREAM")
        run("SELECT", 3)
        run("SET", "other", "database")
        with store.changed:
            store.save()
        return store

    def test_round_trip_through_rdb_and_resp_exports(self):
        origin = self.directory / "origin.json"
        self.fixture(origin)
        self.run_script("export", "--db-file", origin, "--out", self.directory / "dump.rdb")
        self.run_script("export", "--db-file", origin, "--out", self.directory / "dump.resp", "--format", "resp")
        result = self.import_file(self.directory / "dump.rdb", self.directory / "from-rdb.json")
        self.assertRegex(result.stdout, r"^Imported 11 keys in 2 databases from .*dump\.rdb \(RDB version 10\) to ")
        self.assertEqual(self.snapshot(self.directory / "from-rdb.json"), self.snapshot(origin))
        result = self.import_file(self.directory / "dump.resp", self.directory / "from-resp.json")
        self.assertIn("(append-only file: ", result.stdout)
        imported, expected = self.snapshot(self.directory / "from-resp.json"), self.snapshot(origin)
        for snapshot in (imported, expected):
            for group in snapshot[0, b"events"][2]["groups"].values():
                group["consumers"] = sorted(group["consumers"])
        self.assertEqual(imported, expected)
        self.assertEqual(len(json.loads((self.directory / "from-rdb.json").read_text())["databases"]), 4)
        original = walk(self.serve(origin), 4)
        self.assertEqual(walk(self.serve(self.directory / "from-rdb.json"), 4), original)
        self.assertEqual(walk(self.serve(self.directory / "from-resp.json"), 4), original)
        self.assertEqual(original[0, b"events"][2][3][1][3][0], (b"1700000000000-1", b"alice", 1))

    def test_redis_5_and_6_encodings(self):
        future = int(time.time()) + 86_400
        stream = (
            length(2)
            + stream_node((5, 0), [b"f", b"n"], [
                ((5, 0), [b"f", b"a", b"n", 1], False), ((5, 1), [b"f", b"b", b"n", 2], True),
                ((6, 0), [b"other", b"x" * 130], False), ((7, 3), [b"f", b"c", b"n", -70000], False)])
            + stream_node((9, 0), [b"g"], [((9, 0), [b"g", b"1"], False)])
            + length(4) + length(9) + length(0)
            + length(2)
            + string(b"workers") + length(6) + length(0) + length(2)
            + raw_id(5, 0) + ms(1_700_000_000_123) + length(3) + raw_id(5, 1) + ms(1_700_000_000_456) + length(1)
            + length(2) + string(b"alice") + ms(1_700_000_001_000) + length(2) + raw_id(5, 0) + raw_id(5, 1)
            + string(b"bob") + ms(1_700_000_002_000) + length(0)
            + string(b"first") + length(5) + length(0) + length(0) + length(0)
        )
        body = b"".join([
            b"\xfa" + string(b"redis-ver") + string(b"6.0.16"), b"\xfa" + string(b"lua") + string(b"return 1"),
            b"\xf7" + length(module_id("example-1", 2)) + length(2) + length(2)
            + length(2) + length(7) + length(5) + string(b"aux") + length(4) + ms(0) + length(0),
            b"\xfe" + length(0) + b"\xfb" + length(20) + length(2),
            key(0, b"s:int8", b"\xc0\x7f"), key(0, b"s:int16", b"\xc1" + (-300).to_bytes(2, "little", signed=True)),
            key(0, b"s:int32", b"\xc2" + (100_000).to_bytes(4, "little", signed=True)),
            key(0, b"s:lzf", lzf_string(b"0123456789" * 70 + b"012", 10)),
            b"\xfd" + future.to_bytes(4, "little") + key(0, b"s:seconds", string(b"later")),
            key(0, b"s:expired", string(b"gone"), expires=1_000),
            b"\xf8" + length(500) + b"\xf9\x07" + b"\x00\xc0\x07" + string(b"int key"),
            key(1, b"l:linked", length(2) + string(b"a") + b"\xc0\x05"),
            key(10, b"l:ziplist", string(ziplist([b"a", 0, 12, 13, -1, 300, -40000, 100_000, 1 << 40, b"y" * 70]))),
            key(14, b"l:quicklist", length(2) + string(ziplist([b"x", 1])) + lzf_string(ziplist([b"q" * 20] * 4))),
            key(9, b"h:zipmap", string(b"\x02\x03foo\x03\x02barzz\x05hello\x05\x00world\xff")),
            key(13, b"h:ziplist", string(ziplist([b"a", 1, b"b", b"two"]))),
            key(12, b"z:ziplist", string(ziplist([b"neg", -7, b"half", b"0.5", b"one", 1]))),
            key(3, b"z:strings", length(3) + string(b"a") + b"\x031.5" + string(b"top") + b"\xfe" + string(b"bottom") + b"\xff"),
            key(5, b"z:binary", length(1) + string(b"m") + struct.pack("<d", -2.25)),
            key(11, b"set:16", string(intset(2, [1, -5, 300]))), key(11, b"set:32", string(intset(4, [70_000]))),
            key(11, b"set:64", string(intset(8, [-(1 << 63), 1 << 40]))),
            key(2, b"set:plain", length(2) + string(b"x") + b"\xc0\x09"),
            key(4, b"h:plain", length(1) + string(b"k") + string(b"v")),
            key(15, b"x:v1", stream),
            b"\xfe" + length(2) + key(0, b"db2", string(b"two")),
        ])
        source = self.write("redis6.rdb", rdb(9, body))
        result = self.import_file(source, self.directory / "redis6.json")
        self.assertIn("(RDB version 9)", result.stdout)
        self.assertIn("skipped 1 expired keys", result.stdout)
        self.assertIn("skipped auxiliary data of module types example-1", result.stderr)
        data = self.snapshot(self.directory / "redis6.json")
        values = {name: value for (database, name), (_, _, value) in data.items()}
        self.assertEqual((values[b"s:int8"], values[b"s:int16"], values[b"s:int32"]), (b"127", b"-300", b"100000"))
        self.assertEqual(values[b"s:lzf"], b"0123456789" * 70 + b"012")
        self.assertEqual(data[0, b"s:seconds"][1], future * 1000)
        self.assertEqual(values[b"7"], b"int key")
        self.assertEqual(values[b"l:linked"], [b"a", b"5"])
        self.assertEqual(values[b"l:ziplist"], [b"a", b"0", b"12", b"13", b"-1", b"300", b"-40000", b"100000",
                                                b"%d" % (1 << 40), b"y" * 70])
        self.assertEqual(values[b"l:quicklist"], [b"x", b"1"] + [b"q" * 20] * 4)
        self.assertEqual(values[b"h:zipmap"], {b"foo": b"bar", b"hello": b"world"})
        self.assertEqual(values[b"h:ziplist"], {b"a": b"1", b"b": b"two"})
        self.assertEqual(values[b"z:ziplist"], {b"one": 1.0, b"half": 0.5, b"neg": -7.0})
        self.assertEqual(values[b"z:strings"], {b"a": 1.5, b"top": float("inf"), b"bottom": float("-inf")})
        self.assertEqual(values[b"z:binary"], {b"m": -2.25})
        self.assertEqual((values[b"set:16"], values[b"set:32"]), ([b"-5", b"1", b"300"], [b"70000"]))
        self.assertEqual(values[b"set:64"], sorted([b"%d" % -(1 << 63), b"%d" % (1 << 40)]))
        self.assertEqual((values[b"set:plain"], values[b"h:plain"]), ([b"9", b"x"], {b"k": b"v"}))
        stream_value = values[b"x:v1"]
        self.assertEqual(stream_value["items"], [[(5, 0), [b"f", b"a", b"n", b"1"]], [(6, 0), [b"other", b"x" * 130]],
                                                 [(7, 3), [b"f", b"c", b"n", b"-70000"]], [(9, 0), [b"g", b"1"]]])
        self.assertEqual((stream_value["last_id"], stream_value["entries_added"]), ((9, 0), 4))
        workers, first = stream_value["groups"][b"workers"], stream_value["groups"][b"first"]
        self.assertEqual(workers["pending"], {(5, 0): (b"alice", 1_700_000_000_123, 3), (5, 1): (b"alice", 1_700_000_000_456, 1)})
        self.assertEqual(workers["consumers"], {b"alice": 1_700_000_001_000, b"bob": 1_700_000_002_000})
        # Without stored counters, entries-read is estimated like Redis: unknown for 6-0, exact for the first entry.
        self.assertEqual((workers["read"], first["read"], first["last"]), (None, 1, (5, 0)))
        self.assertEqual(data[2, b"db2"][2], b"two")
        client = self.serve(self.directory / "redis6.json")
        self.assertEqual(client("ZSCORE", "z:strings", "top"), b"inf")
        self.assertEqual(client("XPENDING", "x:v1", "workers")[0], 2)

    def test_redis_7_2_through_8_4_encodings(self):
        group = (string(b"g") + length(3) + length(0) + length(2) + length(1)
                 + raw_id(3, 0) + ms(1_000) + length(2)
                 + length(1) + string(b"c") + ms(2_000) + ms(3_000) + length(1) + raw_id(3, 0))
        stream = (length(1) + stream_node((3, 0), [b"a"], [((3, 0), [b"a", b"1"], False)])
                  + length(1) + length(3) + length(0) + length(3) + length(0) + length(2) + length(0) + length(2)
                  + length(1) + group)
        body = b"".join([
            b"\xfa" + string(b"redis-ver") + string(b"8.4.0"),
            b"\xf5" + string(b"#!lua name=lib\nredis.register_function('f', function() return 1 end)"),
            b"\xf6" + string(b"old") + string(b"LUA") + length(1) + string(b"description") + string(b"return 1"),
            b"\xfe" + length(0) + b"\xf4" + length(5) + length(10) + length(1),
            key(18, b"l:quicklist2", length(3) + length(2) + string(listpack([b"a", 1, -2, 5000, b"w" * 130]))
                + length(1) + string(b"p" * 300) + length(2) + string(listpack([b"z"]))),
            key(20, b"set:listpack", string(listpack([b"a", 1, b"b"]))),
            key(16, b"h:listpack", string(listpack([b"f", 1]))),
            key(17, b"z:listpack", string(listpack([b"m", b"2.5", b"n", 3]))),
            key(21, b"x:v3", stream),
            key(24, b"h:metadata", ms(0) + length(2) + length(0) + string(b"a") + string(b"1") + length(0) + string(b"b") + string(b"2")),
            key(22, b"h:metadata-rc", length(1) + length(0) + string(b"a") + string(b"1")),
            key(25, b"h:listpack-ex", ms(0) + string(listpack([b"a", b"1", 0, b"b", 2, 0]))),
            key(23, b"h:listpack-ex-rc", string(listpack([b"x", b"y", 0]))),
            key(20, b"set:empty", string(listpack([]))),
        ])
        source = self.write("redis8.rdb", rdb(12, body, checksum=False))
        result = self.import_file(source, self.directory / "redis8.json")
        self.assertIn("Imported 9 keys in 1 databases", result.stdout)
        self.assertIn("(RDB version 12)", result.stdout)
        self.assertIn("skipped 1 empty keys", result.stderr)
        self.assertIn("skipped 2 function libraries", result.stderr)
        values = {name: value for (_, name), (_, _, value) in self.snapshot(self.directory / "redis8.json").items()}
        self.assertEqual(values[b"l:quicklist2"], [b"a", b"1", b"-2", b"5000", b"w" * 130, b"p" * 300, b"z"])
        self.assertEqual(values[b"set:listpack"], [b"1", b"a", b"b"])
        self.assertEqual((values[b"h:listpack"], values[b"z:listpack"]), ({b"f": b"1"}, {b"m": 2.5, b"n": 3.0}))
        self.assertEqual(values[b"h:metadata"], {b"a": b"1", b"b": b"2"})
        self.assertEqual((values[b"h:metadata-rc"], values[b"h:listpack-ex-rc"]), ({b"a": b"1"}, {b"x": b"y"}))
        self.assertEqual(values[b"h:listpack-ex"], {b"a": b"1", b"b": b"2"})
        stream_value = values[b"x:v3"]
        self.assertEqual((stream_value["last_id"], stream_value["entries_added"]), ((3, 0), 2))
        self.assertEqual(stream_value["groups"][b"g"], {"last": (3, 0), "read": 2, "consumers": {b"c": 2_000},
                                                        "pending": {(3, 0): (b"c", 1_000, 2)}})

    def test_rejects_data_the_server_cannot_store_or_corrupt_files(self):
        valid = rdb(10, key(0, b"k", string(b"v")))
        cases = {
            "field-ttl": (rdb(12, key(24, b"h", ms(5_000) + length(1) + length(1) + string(b"f") + string(b"v"))),
                          "cannot import key b'h' in database 0: it is a hash with field expiration times"),
            "listpack-ttl": (rdb(12, key(25, b"h", ms(5_000) + string(listpack([b"f", b"v", 5_000])))), "field expiration times"),
            "module": (rdb(10, key(7, b"doc", length(module_id("ReJSON-RL", 3)) + length(0))),
                       "it holds a value of module type 'ReJSON-RL' (encoding version 3)"),
            "version": (b"REDIS0013" + valid[9:], "RDB version 13 is not supported; import reads versions 1 to 12"),
            "checksum": (valid[:-1] + bytes((valid[-1] ^ 1,)), "the RDB checksum does not match"),
            "truncated": (valid[:12], "unexpected end of data"),
            "duplicate": (rdb(10, key(0, b"k", string(b"v")) * 2), "duplicate key b'k' in database 0"),
            "nan": (rdb(10, key(5, b"z", length(1) + string(b"m") + struct.pack("<d", float("nan")))), "score is NaN"),
            "listpack": (rdb(10, key(16, b"h", string(b"\x09\x00\x00\x00\x00\x00\xff"))), "invalid listpack header"),
            "trailing": (valid + b"*1\r\n$4\r\nPING\r\n", "continues after its RDB data; use --format aof"),
        }
        for name, (content, message) in cases.items():
            with self.subTest(name):
                target = self.directory / f"{name}.json"
                arguments = ("--format", "rdb") if name == "trailing" else ()
                result = self.import_file(self.write(f"{name}.rdb", content), target, *arguments, check=False)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Redis import failed: "), result.stderr)
                self.assertIn(message, result.stderr)
                self.assertFalse(target.exists())
        source = self.write("valid.rdb", valid)
        existing = self.write("existing.json", b"keep")
        result = self.import_file(source, existing, check=False)
        self.assertEqual((result.returncode, existing.read_bytes()), (1, b"keep"))
        self.assertIn("Snapshot file already exists", result.stderr)
        result = self.import_file(source, self.directory / "x.json", "--format", "resp", check=False)
        self.assertIn("starts with an RDB image; use --format rdb or aof", result.stderr)
        result = self.import_file(self.directory / "missing.rdb", self.directory / "y.json", check=False)
        self.assertIn("Source not found", result.stderr)
        self.assertEqual(list(self.directory.glob(".redis-import-*")), [])

    def test_multi_part_append_only_file_replays_through_the_server(self):
        now = int(time.time() * 1000)
        origin = self.directory / "origin.json"
        store = self.fixture(origin)
        self.run_script("export", "--db-file", origin, "--out", self.directory / "aof" / "appendonly.aof.2.base.rdb")
        payload = store.execute(self.redis["Session"](1, True), [b"DUMP", b"hash"])
        self.write("aof/appendonly.aof.1.incr.aof", b"not even RESP: history files are never read")
        self.write("aof/appendonly.aof.2.incr.aof", b"".join([
            resp("SELECT", 0), b"#TS:1700000000\r\n",
            resp("SET", "late", "value", "PXAT", now + HOUR), resp("SET", "short", "5", "PXAT", now - 1),
            resp("INCR", "short"), resp("PEXPIREAT", "counter", now + 2 * HOUR), resp("DEL", "compressible"),
            resp("MULTI"), resp("RPUSH", "queue", "a", "b"), resp("LPOP", "queue"), resp("EXEC"),
            resp("XADD", "events", "1700000000200-0", "new", "entry"),
            resp("XCLAIM", "events", "workers", "carol", 0, "1700000000200-0", "TIME", now - 5000, "RETRYCOUNT", 4,
                 "FORCE", "JUSTID", "LASTID", "1700000000200-0"),
            resp("XCLAIM", "events", "workers", "alice", 0, "1700000000002-0", "TIME", now, "RETRYCOUNT", 3, "FORCE", "JUSTID"),
            resp("XGROUP", "SETID", "events", "late", "1700000000200-0", "ENTRIESREAD", 151),
            resp("XSETID", "empty", "5-5", "ENTRIESADDED", 7, "MAXDELETEDID", "5-0"),
            resp("FUNCTION", "LOAD", "#!lua name=x\nredis.register_function('f', function() return 1 end)"),
            resp("RESTORE", "restored", 0, payload), resp("SELECT", 5), resp("SET", "five", "5"), resp("SWAPDB", 5, 6),
        ]))
        self.write("aof/appendonly.aof.3.incr.aof", resp("SELECT", 6) + resp("APPEND", "five", "!"))
        self.write("aof/appendonly.aof.manifest", b"file appendonly.aof.2.base.rdb seq 2 type b\n"
                   b"file appendonly.aof.1.incr.aof seq 1 type h\nfile appendonly.aof.2.incr.aof seq 2 type i\n"
                   b"file \"appendonly.aof.3.incr.aof\" seq 3 type i\n")
        for source in (self.directory / "aof", self.directory / "aof" / "appendonly.aof.manifest"):
            with self.subTest(source.name):
                target = self.directory / f"{source.name}.json"
                result = self.import_file(source, target)
                self.assertIn("(append-only file: RDB version 10 base, 2 incr files, 21 commands)", result.stdout)
                self.assertIn("skipped 1 expired keys", result.stdout)
                self.assertIn("skipped 1 FUNCTION and SCRIPT commands", result.stderr)
                client = self.serve(target)
                self.assertEqual((client("GET", "late"), client("EXISTS", "short", "compressible")), (b"value", 0))
                self.assertEqual((client("PEXPIRETIME", "late"), client("PEXPIRETIME", "counter")), (now + HOUR, now + 2 * HOUR))
                self.assertEqual(client("LRANGE", "queue", 0, -1), [b"b"])
                self.assertEqual(client("HGETALL", "restored"), client("HGETALL", "hash"))
                pending = {item[0]: item[1:] for item in client("XPENDING", "events", "workers", "-", "+", 100)}
                self.assertEqual((pending[b"1700000000200-0"][0], pending[b"1700000000200-0"][2]), (b"carol", 4))
                self.assertNotIn(b"1700000000002-0", pending)  # XCLAIM drops pending entries that were deleted
                groups = {group[1]: group for group in client("XINFO", "GROUPS", "events")}
                self.assertEqual(groups[b"workers"][7], b"1700000000200-0")
                self.assertEqual(groups[b"late"][9], 151)
                info = client("XINFO", "STREAM", "empty")
                self.assertEqual((info[3], info[info.index(b"entries-added") + 1]), (b"5-5", 7))
                client("SELECT", 6)
                self.assertEqual(client("GET", "five"), b"5!")

    def test_truncated_and_unsupported_append_only_files(self):
        commands = resp("SET", "a", "1") + resp("SET", "b", "2")
        truncated = self.write("truncated.aof", commands + resp("SET", "c", "3")[:-4])
        result = self.import_file(truncated, self.directory / "t.json", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(f"ends with an incomplete command at byte {len(commands)}; pass --allow-truncated", result.stderr)
        result = self.import_file(truncated, self.directory / "t.json", "--allow-truncated")
        self.assertIn("Imported 2 keys", result.stdout)
        self.assertIn(f"ignored the incomplete command at byte {len(commands)}", result.stderr)
        transaction = self.write("multi.aof", commands + resp("MULTI") + resp("SET", "c", "3"))
        result = self.import_file(transaction, self.directory / "m.json", check=False)
        self.assertIn("ends with an incomplete transaction", result.stderr)
        result = self.import_file(transaction, self.directory / "m.json", "--allow-truncated")
        self.assertIn("Imported 2 keys", result.stdout)
        cases = {
            "pfadd": (resp("PFADD", "h", "a"), "unsupported command b'PFADD' at byte 0"),
            "eval": (resp("EVAL", "return 1", 0), "EVAL at byte 0 runs a script"),
            "wrongtype": (resp("SET", "a", "1") + resp("LPUSH", "a", "x"), "LPUSH at byte 27 failed: WRONGTYPE"),
            "exec": (resp("MULTI") + resp("SET", "a", "1") + resp("LPUSH", "a", "x") + resp("EXEC"), "EXEC at byte"),
            "format": (b"SET a 1\r\n", "invalid command at byte 0: expected a RESP array"),
        }
        for name, (content, message) in cases.items():
            with self.subTest(name):
                result = self.import_file(self.write(f"{name}.aof", content), self.directory / f"{name}.json", check=False)
                self.assertEqual(result.returncode, 1)
                self.assertIn(message, result.stderr)
                self.assertFalse((self.directory / f"{name}.json").exists())

    def test_stream_claim_setid_swapdb_and_dump_commands(self):
        store = self.redis["Store"](4, None, None, False)
        session = self.redis["Session"](1, True)

        def run(*args):
            with store.changed:
                try:
                    return store.execute(session, [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args])
                except self.redis["RedisError"] as error:
                    return str(error)

        for number in range(1, 6):
            run("XADD", "s", f"{number}-1", "f", number)
        run("XGROUP", "CREATE", "s", "g", "0")
        run("XREADGROUP", "GROUP", "g", "alice", "COUNT", 3, "STREAMS", "s", ">")
        self.assertEqual(run("XCLAIM", "s", "g", "bob", 0, "1-1"), [[b"1-1", [b"f", b"1"]]])
        self.assertEqual(run("XCLAIM", "s", "g", "carol", HOUR, "2-1"), [])
        self.assertEqual(run("XCLAIM", "s", "g", "carol", 0, "4-1"), [])
        self.assertEqual(run("XCLAIM", "s", "g", "carol", 0, "4-1", "FORCE", "RETRYCOUNT", 7, "JUSTID", "LASTID", "4-1"), [b"4-1"])
        self.assertEqual(run("XCLAIM", "s", "g", "x", 0, "1-1", "BOGUS"), "ERR Unrecognized XCLAIM option 'BOGUS'")
        run("XDEL", "s", "2-1")
        self.assertEqual(run("XAUTOCLAIM", "s", "g", "dave", 0, "0-0", "COUNT", 2), [b"3-1", [[b"1-1", [b"f", b"1"]]], [b"2-1"]])
        self.assertEqual(run("XAUTOCLAIM", "s", "g", "dave", 0, "(1-1", "JUSTID"), [b"0-0", [b"3-1", b"4-1"], []])
        self.assertEqual(run("XPENDING", "s", "g", "-", "+", 10, "dave")[2][3], 7)
        self.assertEqual(run("XINFO", "GROUPS", "s")[0][b"last-delivered-id"], b"4-1")
        self.assertEqual(run("XSETID", "s", "4-0"), "ERR The ID specified in XSETID is smaller than the target stream top item")
        self.assertEqual(run("XSETID", "s", "9-0", "ENTRIESADDED", 1),
                         "ERR The entries_added specified in XSETID is smaller than the target stream length")
        self.assertEqual(run("XSETID", "s", "9-0", "MAXDELETEDID", "10-0"),
                         "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id")
        self.assertEqual(run("XSETID", "s", "9-0", "ENTRIESADDED", 20), self.redis["OK"])
        self.assertEqual(run("XINFO", "STREAM", "s")[b"entries-added"], 20)
        payload = run("DUMP", "s")
        self.assertEqual(run("RESTORE", "copy", 0, payload), self.redis["OK"])
        self.assertEqual(run("RESTORE", "copy", 0, payload), "BUSYKEY Target key name already exists.")
        self.assertEqual(run("RESTORE", "bad", 0, payload[:-1] + b"\x00"), "ERR DUMP payload version or checksum are wrong")
        self.assertEqual(run("XINFO", "STREAM", "copy"), run("XINFO", "STREAM", "s"))
        run("SET", "zero", "0")
        self.assertEqual(run("SWAPDB", 0, 2), self.redis["OK"])
        self.assertEqual((run("GET", "zero"), run("SELECT", 2), run("GET", "zero")), (None, self.redis["OK"], b"0"))
        self.assertEqual((run("SWAPDB", 0, 4), run("SWAPDB", "x", 1)), ("ERR DB index is out of range", "ERR invalid first DB index"))

    @unittest.skipUnless(shutil.which("redis-server") and hasattr(socket, "AF_UNIX"), "needs redis-server")
    def test_real_redis_rdb_and_append_only_files(self):
        folder = self.directory / "r"
        folder.mkdir()
        process = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", "s", "--dir", ".", "--save", "",
                                    "--appendonly", "yes", "--appendfsync", "always", "--logfile", "log"],
                                   cwd=folder, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        deadline = time.monotonic() + 10
        while not (folder / "s").exists():
            self.assertIsNone(process.poll(), "redis-server exited")
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.02)
        connection = socket.socket(socket.AF_UNIX)
        self.addCleanup(connection.close)
        connection.connect(str(folder / "s"))
        client = Client.__new__(Client)
        client.connection, client.reader = connection, connection.makefile("rb")
        self.addCleanup(client.reader.close)
        version = client("INFO", "server").decode()
        if int(re.search(r"redis_version:(\d+)", version)[1]) < 7:
            self.skipTest("multi-part append-only files need Redis 7.0 or later")
        client("SET", "plain", "value", "PX", HOUR)
        client("SET", "number", 12345)
        client("SET", "long", "abcdefgh" * 200)
        client("RPUSH", "list", *range(200), "x" * 300)
        client("SADD", "ints", 1, 2, 3)
        client("SADD", "strings", *[f"member-{n}" for n in range(200)])
        client("HSET", "small", "a", 1, "b", "two")
        client("HSET", "big", *[part for n in range(200) for part in (f"f{n}", n)])
        client("ZADD", "zsmall", 1, "a", "inf", "b")
        client("ZADD", "zbig", *[part for n in range(200) for part in (n / 3, f"m{n}")])
        for number in range(1, 120):
            client("XADD", "events", f"{number}-1", "n", number, "kind", "tick" if number % 3 else "other")
        client("XDEL", "events", "5-1")
        client("XGROUP", "CREATE", "events", "workers", "0")
        client("XREADGROUP", "GROUP", "workers", "alice", "COUNT", 7, "STREAMS", "events", ">")
        client("XACK", "events", "workers", "2-1")
        client("XGROUP", "CREATECONSUMER", "events", "workers", "bob")
        client("SELECT", 2)
        client("SET", "two", "2")
        client("SELECT", 0)
        client("SAVE")
        client("BGREWRITEAOF")
        while not {b"aof_rewrite_in_progress:0", b"aof_rewrite_scheduled:0"} <= set(client("INFO", "persistence").split()):
            time.sleep(0.02)
        time.sleep(0.2)
        client("INCRBYFLOAT", "number", "0.5")
        client("XCLAIM", "events", "workers", "bob", 0, "3-1", "4-1")
        client("LPOP", "list", 2)
        client("MULTI")
        client("HDEL", "small", "a")
        client("EXPIRE", "small", 7200)
        client("EXEC")
        expected = walk(client, 3)
        rdb_result = self.import_file(folder / "dump.rdb", self.directory / "rdb.json")
        self.assertIn("Imported 12 keys in 2 databases", rdb_result.stdout)
        aof_result = self.import_file(folder / "appendonlydir", self.directory / "aof.json")
        self.assertIn("(append-only file: RDB version", aof_result.stdout)
        self.assertEqual(walk(self.serve(self.directory / "aof.json"), 3), expected)
        from_rdb = walk(self.serve(self.directory / "rdb.json"), 3)
        self.assertIn((b"3-1", b"alice", 1), from_rdb[0, b"events"][2][3][0][3])
        self.assertEqual({name for name in from_rdb}, {name for name in expected})


if __name__ == "__main__":
    unittest.main()
