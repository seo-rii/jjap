"""Verify one-file MongoDB snapshot exports without external executables."""

import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import subprocess
import sys
import tempfile
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
        with sqlite3.connect(self.source) as connection:
            self.assertEqual(connection.execute("SELECT uuid FROM mongo_collections").fetchone(), (legacy,))

    def test_rejects_existing_destination_and_cleans_up_invalid_source(self):
        with sqlite3.connect(self.source) as connection:
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


if __name__ == "__main__":
    unittest.main()
