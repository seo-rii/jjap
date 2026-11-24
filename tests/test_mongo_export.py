"""Verify one-file MongoDB snapshot exports and mongodump imports without external executables."""

import contextlib
import gzip
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import runpy
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.parse import quote_plus
from uuid import UUID


SCRIPT = Path(__file__).resolve().parents[1] / "mongo_server.py"


class MongoExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mongo = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-mongo-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("origin #?.sqlite3" if os.name != "nt" else "origin #.sqlite3")

    def command(self, source, output, *args, check=True):
        return subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), "export", "--db-file", str(source),
             "--out", str(output), *args],
            cwd=self.directory, check=check, text=True, capture_output=True, timeout=15,
        )

    def insert(self, connection, db, name, document):
        encoded = self.mongo["encode_bson"](document)
        connection.execute("INSERT INTO mongo_documents (db, name, data) VALUES (?, ?, ?)", (db, name, encoded))
        return encoded

    def test_dump_preserves_bson_index_options_views_and_long_names(self):
        catalog = self.mongo["Catalog"](self.source)
        regular = "labels + 한글%.folder/name"
        long_name = "긴" * 72 + "+%"
        try:
            records = catalog.create("fixtures", regular, {"validator": {"rank": {"$gte": 0}}})
            catalog.set_index(records, {"v": 2, "key": {"number": -1, "rank": 1}, "name": "rank_number", "unique": True})
            typical = self.mongo["load_shell_json"]('''{
                "_id":{"$oid":"0022446688aabbccddeeff00"},"rank":2,"number":{"$numberLong":"12"},
                "when":{"$date":{"$numberLong":"-86400000"}},"decimal":{"$numberDecimal":"13.550"},
                "payload":{"$binary":{"base64":"AP8=","subType":"00"}}
            }''')
            first = self.insert(catalog.sql, "fixtures", regular, typical)
            second = self.insert(catalog.sql, "fixtures", regular, {"_id": "second", "rank": 3, "number": 0})
            catalog.create("fixtures", "empty")
            catalog.create("fixtures", "view", {"viewOn": regular, "pipeline": [{"$match": {"rank": 2}}]})
            catalog.create("fixtures", long_name)
            catalog.create("ignored", "other")
        finally:
            catalog.sql.close()

        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()
        output = self.directory / "dump"
        self.command(self.source, output, "--db", "fixtures")
        folder = output / "fixtures"
        basename = quote_plus(regular, safe="")
        self.assertEqual((folder / f"{basename}.bson").read_bytes(), first + second)
        metadata = json.loads((folder / f"{basename}.metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["uuid"], records.uuid.hex())
        self.assertEqual(metadata["collectionName"], regular)
        self.assertEqual(list(metadata["indexes"][1]["key"]), ["number", "rank"])
        self.assertEqual(metadata["indexes"][1]["key"]["number"], {"$numberInt": "-1"})
        self.assertEqual(metadata["options"]["validator"]["rank"]["$gte"], {"$numberInt": "0"})
        self.assertEqual((folder / "empty.bson").read_bytes(), b"")
        self.assertFalse((folder / "view.bson").exists())
        view = json.loads((folder / "view.metadata.json").read_text(encoding="utf-8"))
        self.assertEqual((view["type"], view["indexes"], view["options"]["viewOn"]), ("view", [], regular))
        self.assertNotIn("uuid", view)
        truncated, = folder.glob("*%24*.metadata.json")
        self.assertEqual(len(truncated.name.removesuffix(".metadata.json")), 238)
        self.assertEqual(json.loads(truncated.read_text(encoding="utf-8"))["collectionName"], long_name)
        self.assertEqual([path.name for path in output.iterdir()], ["fixtures"])
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)

    def test_snapshot_excludes_a_concurrent_commit_across_collections(self):
        writer = self.mongo["Catalog"](self.source)
        self.addCleanup(writer.sql.close)
        writer.sql.execute("PRAGMA journal_mode=WAL")
        writer.create("fixture", "one")
        writer.create("fixture", "two")
        self.insert(writer.sql, "fixture", "one", {"_id": 1})
        old = self.insert(writer.sql, "fixture", "two", {"_id": 2})
        exporter = self.mongo["export_mongo_dump"]
        original_dump = exporter.__globals__["_dump_collection"]

        def write_concurrently(reader, collection, destination):
            if collection.name == "one":
                self.insert(writer.sql, "fixture", "two", {"_id": 3})
            return original_dump(reader, collection, destination)

        output = self.directory / "snapshot"
        with patch.dict(exporter.__globals__, {"_dump_collection": write_concurrently}):
            self.assertEqual(exporter(self.source, output), (1, 2, 2))
        self.assertEqual((output / "fixture" / "two.bson").read_bytes(), old)
        self.assertEqual(len(writer.records("fixture", "two")), 2)

    def test_legacy_collection_uuid_becomes_compatible_without_source_update(self):
        catalog = self.mongo["Catalog"](self.source)
        legacy = bytes(range(16))
        try:
            collection = catalog.create("fixture", "legacy")
            self.assertEqual(UUID(bytes=collection.uuid).version, 4)
            catalog.sql.execute("UPDATE mongo_collections SET uuid=?", (legacy,))
        finally:
            catalog.sql.close()
        target = self.directory / "dump"
        self.command(self.source, target)
        metadata = json.loads((target / "fixture" / "legacy.metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["uuid"], UUID(bytes=legacy, version=4).hex)
        with contextlib.closing(sqlite3.connect(self.source)) as connection:
            self.assertEqual(connection.execute("SELECT uuid FROM mongo_collections").fetchone(), (legacy,))

    def test_rejects_existing_destination_and_cleans_up_invalid_source(self):
        with contextlib.closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        output = self.directory / "used"
        output.mkdir()
        marker = output / "fixture.txt"
        marker.write_text("preserve", encoding="utf-8")
        result = self.command(self.source, output, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(marker.read_text(encoding="utf-8"), "preserve")

        other = self.directory / "invalid"
        result = self.command(self.source, other, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("MongoDB test server SQLite persistence file", result.stderr)
        self.assertFalse(other.exists())
        self.assertEqual(list(self.directory.glob(".mongo-export-*")), [])


# mongodump 100.20.1 output for MongoDB 8.0.38, copied verbatim.
REAL_ID_INDEX = '{"v":{"$numberInt":"2"},"key":{"_id":{"$numberInt":"1"}},"name":"_id_"}'
REAL_PLAIN_METADATA = (
    '{"indexes":[' + REAL_ID_INDEX + ',{"v":{"$numberInt":"2"},"key":{"x":{"$numberInt":"1"},"i32":{"$numberInt":"-1"}},'
    '"name":"x_i32"},{"v":{"$numberInt":"2"},"key":{"str":{"$numberInt":"1"}},"name":"str_1","unique":true,"sparse":true},'
    '{"v":{"$numberInt":"2"},"key":{"x":{"$numberInt":"1"}},"name":"x_partial","unique":true,"partialFilterExpression":'
    '{"x":{"$gt":{"$numberInt":"1"}}}},{"v":{"$numberInt":"2"},"key":{"date":{"$numberInt":"1"}},"name":"date_1",'
    '"expireAfterSeconds":{"$numberInt":"2000000000"}},{"v":{"$numberInt":"2"},"key":{"i64":{"$numberInt":"1"}},'
    '"name":"i64_1","hidden":true}],"uuid":"9e10c2ffd38b42c0a0ddf91b99940ecd","collectionName":"plain","type":"collection"}'
)
REAL_CAPPED_METADATA = (
    '{"options":{"capped":true,"size":{"$numberInt":"4096"},"max":{"$numberInt":"10"}},"indexes":[' + REAL_ID_INDEX
    + '],"uuid":"47835243a9d849f58feb49615c0d1f34","collectionName":"capped","type":"collection"}'
)
REAL_VIEW_METADATA = (
    '{"options":{"viewOn":"plain","pipeline":[{"$match":{"x":{"$gte":{"$numberInt":"2"}}}},{"$project":'
    '{"x":{"$numberInt":"1"}}}]},"indexes":[],"collectionName":"view1","type":"view"}'
)
REAL_LONG_NAME = "긴" * 72 + "+%"
REAL_LONG_STEM = "%EA%B8%B4" * 23 + "%%24lXYJ-fcKnu0WhxTBCGJk_VBg7nI"
REAL_CAPPED_CRC = -8507469742319500371  # archive EOF CRC for the ten capped documents {_id: i, v: "x" * i}, i = 5..14


def real_metadata(name, uuid):
    return ('{"indexes":[' + REAL_ID_INDEX + f'],"uuid":"{uuid}","collectionName":'
            + json.dumps(name, ensure_ascii=False) + ',"type":"collection"}')


def crc64(data, crc=0):
    crc ^= 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xC96C5795D7870F42 if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFFFFFFFFFF


def signed64(value):
    return struct.unpack("<q", struct.pack("<Q", value))[0]


class MongoImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mongo = runpy.run_path(str(SCRIPT))
        cls.encode = staticmethod(cls.mongo["encode_bson"])

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-mongo-import-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    def run_script(self, *args, check=True, stdin=None):
        result = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), *args], cwd=self.directory, input=stdin,
            capture_output=True, timeout=60,
        )
        if check and result.returncode:
            self.fail(result.stderr.decode())
        return result.returncode, result.stdout.decode(), result.stderr.decode()

    def import_(self, target, *args, check=True, stdin=None):
        return self.run_script("import", "--db-file", str(target), *args, check=check, stdin=stdin)

    @contextlib.contextmanager
    def server(self, database):
        env = {key: value for key, value in os.environ.items() if not key.endswith("_TEST_PASSWORD")}
        command = [sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0", "--db-file", str(database)]
        with subprocess.Popen(command, cwd=self.directory, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True) as process:
            lines = queue.Queue()
            reader = threading.Thread(target=lambda: [lines.put(line) for line in process.stdout] + [lines.put(None)],
                                      daemon=True)
            reader.start()
            try:
                while True:
                    line = lines.get(timeout=10)
                    self.assertIsNotNone(line, "server exited during startup")
                    if match := re.search(r"mongodb://127\.0\.0\.1:(\d+)", line):
                        with self.mongo["ShellConnection"]("127.0.0.1", int(match[1])) as client:
                            yield client, int(match[1])
                        break
            finally:
                process.terminate()
                process.wait(timeout=10)
                reader.join(timeout=5)

    def api_snapshot(self, client):
        """Byte-encoded listCollections rows, listIndexes, and find results for every namespace."""
        result = {}
        for database in client.command("admin", {"listDatabases": 1, "nameOnly": True})["databases"]:
            db = database["name"]
            for row in client.command(db, {"listCollections": 1})["cursor"]["firstBatch"]:
                indexes = [] if row["type"] == "view" else (
                    client.command(db, {"listIndexes": row["name"]})["cursor"]["firstBatch"])
                documents = client.command(db, {"find": row["name"], "batchSize": 100000})["cursor"]["firstBatch"]
                result[(db, row["name"])] = (
                    self.encode(row), [self.encode(spec) for spec in indexes], [self.encode(doc) for doc in documents]
                )
        return result

    def file_rows(self, database):
        with contextlib.closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as connection:
            collections = {
                (db, name): (bytes(options), None if b"viewOn" in options else bytes(uuid))
                for db, name, uuid, options in connection.execute("SELECT db, name, uuid, options FROM mongo_collections")
            }
            indexes = connection.execute("SELECT db, name, idx, spec FROM mongo_indexes ORDER BY db, name, rowid").fetchall()
            documents = connection.execute("SELECT db, name, data FROM mongo_documents ORDER BY db, name, record").fetchall()
        return collections, indexes, documents

    def write(self, path, data, *, compress=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        data = data.encode("utf-8") if isinstance(data, str) else data
        path.write_bytes(gzip.compress(data) if compress else data)

    def test_export_round_trip_preserves_bytes_options_indexes_views_and_uuids(self):
        source = self.directory / "source.sqlite3"
        special = "labels + 한글%.folder/name"
        load = self.mongo["load_shell_json"]
        with self.server(source) as (client, _):
            client.command("app", {"create": "capped", "capped": True, "size": 4096, "max": 3})
            client.command("app", {"create": "checked", "validator": {"$jsonSchema": {"required": ["name"]}},
                                   "validationLevel": "moderate", "validationAction": "warn"})
            typical = load('''{"_id":{"$oid":"0022446688aabbccddeeff00"},"i64":{"$numberLong":"12"},"dbl":-0.0,
                "when":{"$date":{"$numberLong":"-86400000"}},"dec":{"$numberDecimal":"13.550"},"bin":{"$binary":
                {"base64":"AP8=","subType":"04"}},"re":{"$regularExpression":{"pattern":"^a","options":"i"}},
                "ts":{"$timestamp":{"t":5,"i":6}},"min":{"$minKey":1},"nested":{"z":1,"a":[1,[2],{}]},"x":1}''')
            client.command("app", {"insert": special, "documents": [typical, {"x": 2, "_id": "second"}, {"_id": 3, "x": 3}]})
            client.command("app", {"delete": special, "deletes": [{"q": {"_id": "second"}, "limit": 1}]})
            client.command("app", {"insert": special, "documents": [{"_id": "second", "x": 2}]})
            client.command("app", {"createIndexes": special, "indexes": [
                {"key": {"x": 1, "dbl": -1}, "name": "x_dbl"},
                {"key": {"when": 1}, "name": "when_ttl", "expireAfterSeconds": 2_000_000_000},
                {"key": {"x": 1}, "name": "x_partial", "unique": True, "partialFilterExpression": {"x": {"$gt": 1}}},
                {"key": {"i64": 1}, "name": "i64_sparse", "unique": True, "sparse": True, "hidden": True},
            ]})
            client.command("app", {"insert": "capped", "documents": [{"_id": index} for index in range(5)]})
            client.command("app", {"insert": REAL_LONG_NAME, "documents": [{"_id": "long"}]})
            client.command("app", {"create": "view1", "viewOn": special, "pipeline": [{"$match": {"x": {"$gte": 2}}}]})
            client.command("other", {"create": "view2", "viewOn": "view1", "pipeline": [{"$sort": {"x": -1}}]})
            client.command("other", {"create": "empty"})
            expected = self.api_snapshot(client)
        dump = self.directory / "dump"
        self.run_script("export", "--db-file", str(source), "--out", str(dump))
        target = self.directory / "imported.sqlite3"
        code, output, errors = self.import_(target, "--dump", str(dump))
        self.assertEqual(errors, "")
        self.assertRegex(output, r"^Imported 7 documents across 7 collections/views in 2 databases into ")
        self.assertEqual(self.file_rows(target), self.file_rows(source))
        with self.server(target) as (client, _):
            self.assertEqual(self.api_snapshot(client), expected)
            self.assertEqual(len(expected[("app", "capped")][2]), 3)

    def test_imports_mongodump_directory_layout_with_gzip_names_and_skips(self):
        dump = self.directory / "dump"
        oid, integer, datetime_ = self.mongo["ObjectId"], self.mongo["Int64"], self.mongo["DateTime"]
        plain = [
            self.encode({"_id": oid(bytes.fromhex("0022446688aabbccddeeff00")), "i32": 7, "i64": integer(1 << 40),
                         "str": "héllo", "date": datetime_(-86400000)}),
            self.encode({"_id": "second", "x": 2}),
            self.encode({"_id": {"compound": 1, "k": "v"}, "x": 3}),
        ]
        capped = [self.encode({"_id": index, "v": "x" * index}) for index in range(5, 15)]
        self.write(dump / "prelude.json", '{"ServerVersion":"8.0.38","ToolVersion":"100.20.1"}')
        version = self.encode({"_id": "featureCompatibilityVersion", "version": "8.0"})
        self.write(dump / "admin" / "system.version.bson", version)
        self.write(dump / "admin" / "system.version.metadata.json",
                   real_metadata("system.version", "7c726122aadf4d698e4df60b3c39e651"))
        self.write(dump / "fixture" / "plain.bson", b"".join(plain))
        self.write(dump / "fixture" / "plain.metadata.json", REAL_PLAIN_METADATA)
        self.write(dump / "fixture" / "capped.bson.gz", b"".join(capped), compress=True)
        self.write(dump / "fixture" / "capped.metadata.json.gz", REAL_CAPPED_METADATA, compress=True)
        self.write(dump / "fixture" / "view1.metadata.json", REAL_VIEW_METADATA)
        names = {"with+space": "with space", "plus%2Bsign": "plus+sign", "slash%2Fname": "slash/name",
                 "%ED%95%9C%EA%B8%80.dots": "한글.dots", REAL_LONG_STEM: REAL_LONG_NAME}
        for number, (stem, name) in enumerate(names.items()):
            self.write(dump / "fixture" / f"{stem}.bson", self.encode({"_id": 1, "name": name}))
            self.write(dump / "fixture" / f"{stem}.metadata.json", real_metadata(name, f"{number:032x}"))
        self.write(dump / "fixture" / "renamed.metadata.json", real_metadata("original", "f" * 32))
        self.write(dump / "fixture" / "tuned.metadata.json", '{"options":{"storageEngine":{"wiredTiger":{}}},"indexes":['
                   + REAL_ID_INDEX + '],"collectionName":"tuned","type":"collection"}')
        self.write(dump / "users" / "$admin.system.users.bson", self.encode({"_id": "users.app", "user": "app"}))
        self.write(dump / "users" / "data.bson", b"")
        self.write(dump / "oplog.bson", b"")

        target = self.directory / "imported.sqlite3"
        _, output, errors = self.import_(target, "--dump", str(dump), "--gzip")
        self.assertRegex(output, r"^Imported 18 documents across 11 collections/views in 2 databases into ")
        for skipped in ("oplog.bson: oplog replay", "admin.system.version", "users.$admin.system.users", "storageEngine"):
            self.assertIn(skipped, errors)
        collections, _, documents = self.file_rows(target)
        stored = {(db, name): [] for db, name in collections}
        for db, name, data in documents:
            stored[(db, name)].append(bytes(data))
        self.assertEqual(stored[("fixture", "plain")], plain)
        self.assertEqual(stored[("fixture", "capped")], capped)
        self.assertEqual(stored[("fixture", REAL_LONG_NAME)], [self.encode({"_id": 1, "name": REAL_LONG_NAME})])
        self.assertIn(("fixture", "renamed"), collections)  # mongorestore names files by their file name
        self.assertEqual(collections[("fixture", "plain")][1], bytes.fromhex("9e10c2ffd38b42c0a0ddf91b99940ecd"))
        with self.server(target) as (client, _):
            rows = {row["name"]: row for row in client.command("fixture", {"listCollections": 1})["cursor"]["firstBatch"]}
            self.assertEqual(sorted(rows), sorted(["capped", "plain", "renamed", "tuned", "view1", *names.values()]))
            self.assertEqual(rows["capped"]["options"], {"capped": True, "size": 4096, "max": 10})
            self.assertIsInstance(rows["capped"]["options"]["size"], self.mongo["Int64"])
            self.assertEqual(rows["tuned"]["options"], {})
            self.assertEqual(bytes(rows["capped"]["info"]["uuid"]), bytes.fromhex("47835243a9d849f58feb49615c0d1f34"))
            self.assertEqual((rows["view1"]["type"], rows["view1"]["options"]["viewOn"]), ("view", "plain"))
            specs = client.command("fixture", {"listIndexes": "plain"})["cursor"]["firstBatch"]
            indexes = {spec["name"]: spec for spec in specs}
            self.assertEqual(list(indexes), ["_id_", "x_i32", "str_1", "x_partial", "date_1", "i64_1"])
            self.assertEqual(indexes["x_i32"]["key"], {"x": 1, "i32": -1})
            self.assertEqual((indexes["str_1"]["unique"], indexes["str_1"]["sparse"]), (True, True))
            self.assertEqual(indexes["x_partial"]["partialFilterExpression"], {"x": {"$gt": 1}})
            self.assertEqual((indexes["date_1"]["expireAfterSeconds"], indexes["i64_1"]["hidden"]), (2000000000, True))
            view = client.command("fixture", {"find": "view1"})["cursor"]["firstBatch"]
            self.assertEqual(view, [{"_id": "second", "x": 2}, {"_id": {"compound": 1, "k": "v"}, "x": 3}])
            reply = client.command("fixture", {"insert": "plain", "documents": [{"_id": 9, "str": "héllo"}]})
            self.assertEqual(reply["writeErrors"][0]["code"], 11000)

    def archive(self, prelude, blocks, ends):
        encode, terminator = self.encode, struct.pack("<i", -1)
        data = bytearray(struct.pack("<I", 0x8199E26D))
        data += encode({"concurrent_collections": 4, "version": "0.1", "server_version": "8.0.38",
                        "tool_version": "100.20.1"})
        for db, name, metadata, kind in prelude:
            data += encode({"db": db, "collection": name, "metadata": metadata, "size": 0, "type": kind})
        data += terminator
        for db, name, documents in blocks:
            data += encode({"db": db, "collection": name, "EOF": False, "CRC": self.mongo["Int64"](0)})
            data += b"".join(documents) + terminator
        for db, name, crc in ends:
            data += encode({"db": db, "collection": name, "EOF": True, "CRC": self.mongo["Int64"](crc)}) + terminator
        return bytes(data)

    def test_imports_interleaved_archive_from_file_and_gzip_stdin(self):
        capped = [self.encode({"_id": index, "v": "x" * index}) for index in range(5, 15)]
        self.assertEqual(signed64(crc64(b"".join(capped))), REAL_CAPPED_CRC)
        plus = [self.encode({"_id": 1, "name": "plus+sign"})]
        version = [self.encode({"_id": "featureCompatibilityVersion", "version": "8.0"})]
        prelude = [
            ("fixture", "plus+sign", real_metadata("plus+sign", "9806cd4864ac48e2be7e02e2ace29b92"), "collection"),
            ("fixture", "view1", REAL_VIEW_METADATA, "view"),
            ("fixture", "capped", REAL_CAPPED_METADATA, "collection"),
            ("fixture", "plain", REAL_PLAIN_METADATA, "collection"),
            ("admin", "system.version", real_metadata("system.version", "7c726122aadf4d698e4df60b3c39e651"), "collection"),
            ("", "oplog", "", ""),
        ]
        blocks = [("admin", "system.version", version), ("fixture", "capped", capped[:4]), ("fixture", "plus+sign", plus),
                  ("fixture", "capped", capped[4:]), ("", "oplog", [self.encode({"ts": 1})])]
        ends = [("admin", "system.version", signed64(crc64(version[0]))), ("fixture", "view1", 0), ("fixture", "plain", 0),
                ("fixture", "capped", REAL_CAPPED_CRC), ("fixture", "plus+sign", signed64(crc64(plus[0]))), ("", "oplog", 0)]
        archive = self.directory / "fixture.archive"
        archive.write_bytes(self.archive(prelude, blocks, ends))
        first = self.directory / "first.sqlite3"
        _, output, errors = self.import_(first, "--archive", str(archive))
        self.assertRegex(output, r"^Imported 11 documents across 4 collections/views in 1 databases into ")
        self.assertIn("archived oplog", errors)
        self.assertIn("admin.system.version", errors)
        collections, indexes, documents = self.file_rows(first)
        self.assertEqual([bytes(data) for db, name, data in documents if name == "capped"], capped)
        self.assertEqual(collections[("fixture", "capped")][1], bytes.fromhex("47835243a9d849f58feb49615c0d1f34"))
        self.assertEqual(len([row for row in indexes if row[1] == "plain"]), 6)
        second = self.directory / "second.sqlite3"
        self.import_(second, "--archive", "-", "--gzip", stdin=gzip.compress(archive.read_bytes()))
        self.assertEqual(self.file_rows(second), self.file_rows(first))

        broken = list(ends)
        broken[3] = ("fixture", "capped", REAL_CAPPED_CRC + 1)
        archive.write_bytes(self.archive(prelude, blocks, broken))
        code, _, errors = self.import_(self.directory / "crc.sqlite3", "--archive", str(archive), check=False)
        self.assertEqual((code, "checksum mismatch for fixture.capped" in errors), (1, True))
        archive.write_bytes(self.archive(prelude, blocks, ends)[:-60])
        code, _, errors = self.import_(self.directory / "cut.sqlite3", "--archive", str(archive), check=False)
        self.assertEqual((code, "MongoDB import failed:" in errors, "truncated" in errors), (1, True, True))
        self.assertEqual(sorted(path.name for path in self.directory.glob("*.sqlite3")), ["first.sqlite3", "second.sqlite3"])

    def test_filters_and_single_database_directory(self):
        dump = self.directory / "dump"
        for db, name in (("app", "users"), ("app", "orders"), ("app", "us*rs"), ("logs", "users")):
            self.write(dump / db / f"{quote_plus(name, safe='')}.bson", self.encode({"_id": f"{db}.{name}"}))
        target = self.directory / "filtered.sqlite3"
        _, output, _ = self.import_(target, "--dump", str(dump), "--nsInclude", "*.users", "--nsInclude", r"app.us\*rs")
        self.assertEqual(sorted(self.file_rows(target)[0]), [("app", "us*rs"), ("app", "users"), ("logs", "users")])
        target = self.directory / "one.sqlite3"
        self.import_(target, "--dump", str(dump), "--db", "logs")
        self.assertEqual(sorted(self.file_rows(target)[0]), [("logs", "users")])
        target = self.directory / "renamed.sqlite3"
        self.import_(target, "--dump", str(dump / "app"), "--db", "copy", "--nsInclude", "copy.o*")
        self.assertEqual(sorted(self.file_rows(target)[0]), [("copy", "orders")])

    def test_failures_leave_nothing_behind_and_existing_files_untouched(self):
        existing = self.directory / "existing.sqlite3"
        existing.write_text("keep", encoding="utf-8")
        dump = self.directory / "dump"
        self.write(dump / "db" / "c.bson", b"")
        code, _, errors = self.import_(existing, "--dump", str(dump), check=False)
        self.assertEqual((code, existing.read_text(encoding="utf-8")), (1, "keep"))
        self.assertIn("already exists", errors)

        def attempt(metadata, data=b"", *extra):
            shutil.rmtree(dump)
            self.write(dump / "db" / "c.bson", data)
            if metadata is not None:
                self.write(dump / "db" / "c.metadata.json", metadata)
            code, output, errors = self.import_(self.directory / "new.sqlite3", "--dump", str(dump), *extra, check=False)
            if code == 0:
                (self.directory / "new.sqlite3").unlink()
            return code, errors

        collation = ('{"options":{"collation":{"locale":"en","strength":{"$numberInt":"2"}}},"indexes":[],'
                     '"collectionName":"c"}')
        self.assertEqual(attempt(collation), (1, "MongoDB import failed: db.c: only the simple collation is supported\n"))
        timeseries = '{"options":{"timeseries":{"timeField":"t"}},"indexes":[],"collectionName":"c","type":"timeseries"}'
        self.assertIn("time series collections are unsupported", attempt(timeseries)[1])
        text = ('{"indexes":[' + REAL_ID_INDEX + ',{"v":{"$numberInt":"2"},"key":{"_fts":"text","_ftsx":{"$numberInt":"1"}},'
                '"name":"body_text","weights":{"body":{"$numberInt":"1"}}}],"collectionName":"c","type":"collection"}')
        self.assertIn("index 'body_text' is unsupported", attempt(text)[1])
        code, errors = attempt(text, b"", "--skip-unsupported-indexes")
        self.assertEqual(code, 0)
        self.assertIn("skipped db.c: index 'body_text'", errors)
        unique = ('{"indexes":[' + REAL_ID_INDEX + ',{"v":{"$numberInt":"2"},"key":{"k":{"$numberInt":"1"}},'
                  '"name":"k_1","unique":true}],"collectionName":"c","type":"collection"}')
        twice = self.encode({"_id": 1, "k": 1}) + self.encode({"_id": 2, "k": 1})
        self.assertIn("duplicate key", attempt(unique, twice)[1])
        self.assertIn("duplicate key", attempt(None, self.encode({"_id": 1}) * 2)[1])
        self.assertIn("is truncated", attempt(None, self.encode({"_id": 1})[:-1])[1])
        self.assertIn("invalid BSON", attempt(None, b"\x0c\0\0\0\x08b\0\x02\0\0\0\0")[1])
        self.assertIn("a view but the dump holds documents", attempt(REAL_VIEW_METADATA, self.encode({"_id": 1}))[1])
        self.assertEqual(sorted(path.name for path in self.directory.iterdir() if path.name != "dump"),
                         ["existing.sqlite3", "mongo_server.py"])

    @unittest.skipUnless(shutil.which("mongodump"), "mongodump is not installed")
    def test_mongodump_of_the_test_server_imports_identically(self):
        source = self.directory / "source.sqlite3"
        with self.server(source) as (client, port):
            documents = [{"_id": index, "v": [index]} for index in range(50)]
            client.command("app", {"insert": "with space", "documents": documents})
            index = {"key": {"v": -1}, "name": "v_desc", "unique": True}
            client.command("app", {"createIndexes": "with space", "indexes": [index]})
            client.command("app", {"create": "capped", "capped": True, "size": 1 << 20})
            client.command("app", {"create": "view", "viewOn": "with space", "pipeline": [{"$limit": 3}]})
            archive, folder = self.directory / "dump.archive", self.directory / "dump"
            for args in ([f"--archive={archive}", "--gzip"], [f"--out={folder}", "--gzip"]):
                subprocess.run(["mongodump", "--quiet", f"--uri=mongodb://127.0.0.1:{port}", *args], check=True, timeout=60)
            expected = self.api_snapshot(client)
        for name, source_args in (("archive", ["--archive", str(archive)]), ("directory", ["--dump", str(folder)])):
            target = self.directory / f"{name}.sqlite3"
            self.import_(target, *source_args)
            self.assertEqual(self.file_rows(target), self.file_rows(source))
            with self.server(target) as (client, _):
                self.assertEqual(self.api_snapshot(client), expected)


if __name__ == "__main__":
    unittest.main()
