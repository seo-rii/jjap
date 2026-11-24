"""Verify Neo4j test server exports for cypher-shell and neo4j-admin database import."""

import contextlib
import csv
import hashlib
import queue
import re
import runpy
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "neo4j_server.py"


def packstream_string(value):
    data = value.encode()
    if len(data) < 16:
        return bytes([0x80 | len(data)]) + data
    for marker, fmt, limit in ((0xD0, ">B", 0xFF), (0xD1, ">H", 0xFFFF), (0xD2, ">I", 0xFFFFFFFF)):
        if len(data) <= limit:
            return bytes([marker]) + struct.pack(fmt, len(data)) + data
    raise ValueError("statement too long")


class BoltClient:
    def __init__(self, sock):
        self.sock = sock
        self.reader = sock.makefile("rb")
        version = b"\x00\x00\x04\x04"
        sock.sendall(b"\x60\x60\xb0\x17" + version + b"\x00" * 12)
        if self.reader.read(4) != version:
            raise AssertionError("Bolt 4.4 negotiation failed")
        self.send(b"\xb1\x01\xa1\x8auser_agent\x84jjap")
        self.expect_success()

    def send(self, payload):
        for start in range(0, len(payload), 0xFFFF):
            chunk = payload[start:start + 0xFFFF]
            self.sock.sendall(struct.pack(">H", len(chunk)) + chunk)
        self.sock.sendall(b"\x00\x00")

    def receive(self):
        data = bytearray()
        while length := struct.unpack(">H", self.reader.read(2))[0]:
            data.extend(self.reader.read(length))
        return bytes(data)

    def expect_success(self):
        response = self.receive()
        if not response.startswith(b"\xb1\x70"):
            raise AssertionError(f"Expected Bolt SUCCESS, got {response[:300]!r}")

    def run(self, statement):
        self.send(b"\xb3\x10" + packstream_string(statement) + b"\xa0\xa0")
        self.expect_success()
        self.send(b"\xb1\x3f\xa1\x81n\xff")
        while (response := self.receive()).startswith(b"\xb1\x71"):
            pass
        if not response.startswith(b"\xb1\x70"):
            raise AssertionError(f"Statement failed: {statement[:200]} -> {response[:300]!r}")


@contextlib.contextmanager
def running_server(script, directory, *args):
    command = [sys.executable, "-I", "-S", "-B", "-u", str(script), "--port", "0", *args]
    with subprocess.Popen(command, cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
        lines = queue.Queue()
        reader = threading.Thread(target=lambda: [lines.put(line) for line in process.stdout] + [lines.put(None)],
                                  daemon=True)
        reader.start()
        try:
            while True:
                line = lines.get(timeout=10)
                if line is None:
                    raise AssertionError("neo4j_server.py exited during startup")
                if match := re.search(r"127\.0\.0\.1:(\d+)", line):
                    yield int(match[1])
                    break
        finally:
            process.terminate()
            process.wait(timeout=10)
            reader.join(timeout=5)


class Neo4jExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.neo4j = runpy.run_path(str(SCRIPT))
        cls.S = cls.neo4j["Structure"]

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-neo4j-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / "graph #?.sqlite3"

    def command(self, *args, check=True):
        return subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), "export", "--db-file", str(self.source), *args],
            cwd=self.directory, check=check, text=True, capture_output=True, timeout=30,
        )

    def save(self, graph, filename=None):
        store = self.neo4j["Store"](filename or self.source)
        try:
            store.commit(graph, 0, True)
        finally:
            store.close()

    def schema(self, name, kind, label, properties, unique=False, required=False):
        return {"name": name, "kind": kind, "label": label, "properties": properties,
                "unique": unique, "required": required}

    def test_cypher_script_recreates_the_graph_in_a_test_server(self):
        S, graph = self.S, self.neo4j["Graph"]()
        removed = graph.add_node({"Gone"}, {})
        alice = graph.add_node({"Person", "Admin"}, {
            "name": "O'Brien \\ \"quoted\"\nnext\ttab 한글 😀", "age": 41, "score": -2.5, "active": True,
            "minimum": -(1 << 63), "huge": 1e300, "tiny": 5e-324, "tags": ["a;b", "c|d", ""], "ranks": [3, 1, 2],
            "born": S(0x44, (-1000,)), "seen": S(0x49, (1700000000, 123000000, 3600)),
            "local": S(0x64, (-86399, 0)), "home": S(0x58, (4326, 12.5, 55.25)), "box": S(0x59, (9157, 1.0, -2.0, 3.5)),
        })
        bob = graph.add_node(set(), {"name": "bob"})
        carol = graph.add_node({"Person"}, {})
        del graph.nodes[removed.id]
        graph.add_edge(alice, alice, "SELF", {"weight": 0.5})
        graph.add_edge(alice, bob, "KNOWS `x`", {})
        graph.add_edge(alice, bob, "KNOWS `x`", {"since": 2020})
        graph.add_edge(carol, alice, "REPORTS_TO", {"note": "line\r\nbreak"})
        graph.schema["person_name"] = self.schema("person_name", "constraint", "Person", ["name"], unique=True)
        graph.schema["person_age"] = self.schema("person_age", "index", "Person", ["age", "score"])
        self.save(graph)
        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()

        output = self.directory / "cypher"
        result = self.command("--out", str(output))
        self.assertIn("Exported 3 nodes, 4 relationships, and 2 indexes/constraints", result.stdout)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)
        self.assertEqual([path.name for path in output.iterdir()], ["neo4j.cypher"])
        lines = (output / "neo4j.cypher").read_text(encoding="utf-8").splitlines()
        statements = [line for line in lines if not line.startswith("//")]

        target = self.directory / "target.sqlite3"
        with running_server(self.script, self.directory, "--db-file", str(target)) as port:
            with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
                client = BoltClient(sock)
                for statement in statements:
                    client.run(statement)
        restored = self.neo4j["read_graph_snapshot"](target)

        def shape(source):
            nodes = sorted(source.nodes.values(), key=lambda node: node.id)
            position = {node.id: index for index, node in enumerate(nodes)}
            edges = sorted(source.edges.values(), key=lambda edge: edge.id)
            return ([(node.labels, node.props) for node in nodes],
                    [(position[edge.start], position[edge.end], edge.type, edge.props) for edge in edges])

        self.assertEqual(shape(restored), shape(graph))
        self.assertEqual(restored.schema, graph.schema)

    def test_cypher_literals_cover_every_stored_type(self):
        S, warnings = self.S, set()
        literal = self.neo4j["cypher_literal"]
        cases = {
            "it's \\ \n": "'it\\'s \\\\ \\u000a'",
            float("nan"): "NaN", float("-inf"): "-Infinity", 1e16: "1.0e16", -0.0: "-0.0",
            S(0x54, (3723000000001, -19800)): "time('01:02:03.000000001-05:30')",
            S(0x74, (0,)): "localtime('00:00:00')",
            S(0x69, (1700000000, 5, "Europe/Paris")): "datetime('2023-11-14T23:13:20.000000005+01:00[Europe/Paris]')",
            S(0x49, (0, 0, 0)): "datetime('1970-01-01T00:00:00Z')",
            S(0x45, (14, 3, -2, 500000000)): "duration({months: 14, days: 3, seconds: -2, nanoseconds: 500000000})",
            S(0x59, (4979, 1.0, 2.0, 3.0)): "point({crs: 'wgs-84-3d', longitude: 1.0, latitude: 2.0, height: 3.0})",
        }
        for value, expected in cases.items():
            self.assertEqual(literal(value, "p", warnings), expected)
        self.assertEqual(literal([1, 2.5], "mixed", warnings), "[1.0, 2.5]")
        self.assertEqual(warnings, {"Property 'mixed' mixes integers and floats; it is exported as a float list"})
        for value, message in ((b"\x00", "byte array"), ([S(0x44, (1,)), S(0x58, (7203, 1.0, 2.0))], "cannot store"),
                               ([1, 2.5, (1 << 53) + 1], "exactly"), (S(0x58, (1234, 1.0, 2.0)), "SRID 1234"),
                               (S(0x58, (7203, 1.0, float("nan"))), "finite coordinates")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, message):
                literal(value, "p", set())

    def test_csv_files_follow_the_neo4j_admin_header_format(self):
        S, graph = self.S, self.neo4j["Graph"]()
        first = graph.add_node({"Person", "Admin"}, {
            "name": 'say "hi"\nthen go', "age": 41, "empty": "", "tags": ["a;b", "c"],
            "blob": b"\x00\x7f\x80\xff", "zone": S(0x69, (1700000000, 0, "Europe/Paris")),
            "span": S(0x45, (1, 2, -2, 500000000)), "spot": S(0x58, (7203, 1.5, -2.0)),
        })
        second = graph.add_node(set(), {"age": "forty"})
        graph.add_edge(first, second, "LIKES", {"since": 2020})
        graph.add_edge(second, second, "LIKES", {})
        graph.schema["admin_key"] = self.schema("admin_key", "constraint", "Admin", ["name", "age"], True, True)
        self.save(graph)

        output = self.directory / "csv"
        result = self.command("--out", str(output), "--format", "csv")
        self.assertIn("require Neo4j Enterprise Edition", result.stderr)
        self.assertEqual(sorted(path.name for path in output.iterdir()),
                         ["import.args", "nodes-1.csv", "nodes-2.csv", "relationships-1.csv", "schema.cypher"])
        self.assertEqual((output / "import.args").read_text(encoding="utf-8").splitlines(), [
            "# From this directory: neo4j-admin database import full <database> @import.args",
            "--array-delimiter=U+007C", "--bad-tolerance=0", "--multiline-fields=true",
            "--nodes=nodes-1.csv", "--nodes=nodes-2.csv", "--relationships=relationships-1.csv",
        ])

        def rows(name):
            with (output / name).open(encoding="utf-8", newline="") as stream:
                return list(csv.reader(stream))

        header, row = rows("nodes-1.csv")
        self.assertEqual(header, [":ID", ":LABEL", "age:long", "blob:byte[]", "empty:string", "name:string",
                                  "span:duration", "spot:point", "tags:string[]", "zone:datetime"])
        self.assertEqual(row, ["0", "Admin|Person", "41", "0|127|-128|-1", "", 'say "hi"\nthen go',
                               "P1M2DT-1.5S", "{crs: 'cartesian', x: 1.5, y: -2.0}", "a;b|c",
                               "2023-11-14T23:13:20+01:00[Europe/Paris]"])
        self.assertEqual(rows("nodes-2.csv"), [[":ID", ":LABEL", "age:string"], ["1", "", "forty"]])
        self.assertEqual(rows("relationships-1.csv"), [
            [":START_ID", ":END_ID", ":TYPE", "since:long"], ["0", "1", "LIKES", "2020"], ["1", "1", "LIKES", ""],
        ])
        raw = (output / "nodes-1.csv").read_text(encoding="utf-8")
        self.assertIn(',"",', raw)  # A quoted empty field is an empty string; a bare one is no property.
        self.assertTrue((output / "relationships-1.csv").read_text(encoding="utf-8").endswith('"LIKES",\n'))
        self.assertIn("CREATE CONSTRAINT `admin_key` IF NOT EXISTS FOR (n:`Admin`) REQUIRE (n.`name`, n.`age`) IS NODE KEY;",
                      (output / "schema.cypher").read_text(encoding="utf-8"))

    def test_rejects_values_without_a_lossless_representation(self):
        graph = self.neo4j["Graph"]()
        graph.add_node({"Thing"}, {"bytes": b"\x01", "none": []})
        self.save(graph)
        for fmt, message in (("cypher", "byte array, which has no Cypher literal; use --format csv"),
                             ("csv", "cannot tell apart from a missing value; use --format cypher")):
            with self.subTest(fmt=fmt):
                output = self.directory / fmt
                result = self.command("--out", str(output), "--format", fmt, check=False)
                self.assertEqual(result.returncode, 1)
                self.assertIn(message, result.stderr)
                self.assertFalse(output.exists())
        self.assertEqual(list(self.directory.glob(".neo4j-export-*")), [])

    def test_rejects_existing_destination_and_foreign_sources(self):
        self.save(self.neo4j["Graph"]())
        output = self.directory / "used"
        output.mkdir()
        (output / "keep.txt").write_text("preserve", encoding="utf-8")
        result = self.command("--out", str(output), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((output / "keep.txt").read_text(encoding="utf-8"), "preserve")

        self.source.unlink()
        with sqlite3.connect(self.source) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        result = self.command("--out", str(self.directory / "foreign"), check=False)
        self.assertIn("Expected a Neo4j test server SQLite persistence file", result.stderr)
        self.source.unlink()
        result = self.command("--out", str(self.directory / "missing"), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertFalse(self.source.exists())
        self.assertFalse((self.directory / "foreign").exists() or (self.directory / "missing").exists())

    def test_empty_graph_exports_an_importable_layout(self):
        self.save(self.neo4j["Graph"]())
        self.command("--out", str(self.directory / "csv"), "--format", "csv")
        self.assertEqual((self.directory / "csv" / "nodes-1.csv").read_text(encoding="utf-8"), ":ID,:LABEL\n")
        self.command("--out", str(self.directory / "cypher"))
        self.assertNotIn("CREATE", (self.directory / "cypher" / "neo4j.cypher").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
