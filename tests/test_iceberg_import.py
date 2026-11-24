"""Verify Iceberg import, relocated export, migrate --dry-run and reset without external services."""

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener


SCRIPT = Path(__file__).resolve().parents[1] / "iceberg_server.py"
FIELDS = [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "label", "required": False, "type": "string"},
]
SCHEMA = {"type": "struct", "schema-id": 0, "fields": FIELDS}
POSITION_FIELDS = [
    {"id": 2147483546, "name": "file_path", "required": True, "type": "string"},
    {"id": 2147483545, "name": "pos", "required": True, "type": "long"},
]
ENVIRONMENT = {key: value for key, value in os.environ.items() if "proxy" not in key.lower()}
HTTP = build_opener(ProxyHandler({}))
DELETE_SNAPSHOT = 32


class IcebergImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.iceberg = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-iceberg-import-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.warehouse = self.directory / "source wh ü"

    def run_script(self, *args, check=True, umask=-1):
        result = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)],
            cwd=self.directory, env=ENVIRONMENT, text=True, capture_output=True, timeout=60, umask=umask,
        )
        if check and result.returncode:
            self.fail(f"{args} exited with {result.returncode}: {result.stderr}")
        return result

    def scan(self, warehouse, identifier, db_file=None, snapshot=None):
        options = ["--db-file", db_file] if db_file else []
        extra = ["--snapshot-id", snapshot] if snapshot else []
        output = self.run_script("--warehouse", warehouse, *options, "scan", identifier, *extra).stdout
        return sorted((json.loads(line) for line in output.splitlines()), key=lambda row: row["id"])

    def serve(self, *args):
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args), "--port", "0"],
            cwd=self.directory, env=ENVIRONMENT, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        line = process.stdout.readline()
        self.assertIn("listening on http://127.0.0.1:", line)
        return line.strip().rsplit(" ", 1)[1]

    def http(self, method, url, body=None, *, token=None, form=None, expected=200):
        headers, data = {}, None
        if form is not None:
            data, headers["Content-Type"] = urlencode(form).encode(), "application/x-www-form-urlencoded"
        elif body is not None:
            data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        if token:
            headers["Authorization"] = "Bearer " + token
        try:
            response = HTTP.open(Request(url, data=data, method=method, headers=headers), timeout=10)
        except HTTPError as exc:
            response = exc
        with response:
            payload = response.read()
            self.assertEqual(response.status, expected, payload)
            if response.headers.get_content_type() != "application/json":
                return payload or None
            return json.loads(payload) if payload else None

    def build_source(self):
        """A table with appends, a copy-on-write delete, position and equality delete files and statistics."""
        module = self.iceberg
        catalog = module["IcebergCatalog"](self.warehouse)
        files = module["IcebergFiles"](catalog)
        catalog.create_namespace(("lake",), {"owner": "data team", "location": (self.warehouse / "lake").as_uri()})
        catalog.create_namespace(("lake", "raw"))
        catalog.create_table(("lake", "raw"), "events", SCHEMA)
        first = files.append(("lake", "raw"), "events", [{"id": index, "label": f"row {index}"} for index in range(1, 5)])
        metadata = catalog.load_table(("lake", "raw"), "events")["metadata"]
        previous, inventory = module["_ice_inventory"](first)
        _, manifest_schema, header, entries = inventory[0]
        location = catalog.local_path(metadata["location"])
        data_path = entries[0]["data_file"]["file_path"]
        snapshot = {"snapshot-id": DELETE_SNAPSHOT, "parent-snapshot-id": first["snapshot-id"], "sequence-number": 2,
                    "timestamp-ms": first["timestamp-ms"] + 1, "schema-id": 0,
                    "summary": {"operation": "delete", "total-files-size": "5000", "added-files-size": "700"}}
        deletes = []
        for content, path, fields, row, codec in (
            (1, location / "data" / "position.parquet", POSITION_FIELDS, {"file_path": data_path, "pos": 0}, 1),
            (2, location / "data" / "equality.parquet", FIELDS[:1], {"id": 2}, 2),
        ):
            module["parquet_write"](path, fields, [row], codec, [("delete-type", "position" if content == 1 else "eq")])
            info = {"content": content, "file_path": path.as_uri(), "file_format": "PARQUET", "partition": {},
                    "record_count": 1, "file_size_in_bytes": path.stat().st_size, "split_offsets": [4]}
            if content == 1:
                info.update(column_sizes={2147483546: 1, 2147483545: 1}, lower_bounds={2147483546: data_path.encode()},
                            upper_bounds={2147483546: data_path.encode()})
            else:
                info["equality_ids"] = [1]
            deletes.append({"status": 1, "snapshot_id": DELETE_SNAPSHOT, "sequence_number": None,
                            "file_sequence_number": None, "data_file": info})
        header = dict({key: value for key, value in header.items() if not key.startswith("avro.")}, content="deletes")
        manifest = module["_ice_write_manifest"](location / "metadata", manifest_schema, header, deletes, 0, 1,
                                                 snapshot, [])
        root = location / "metadata" / "delete-root.avro"
        module["avro_write"](root, module["iceberg_manifest_list_schema"](), [*previous, manifest], codec="snappy")
        snapshot["manifest-list"] = root.as_uri()
        statistics = location / "metadata" / "stats.puffin"
        statistics.write_bytes(b"PFA1 statistics")
        catalog.commit_table(("lake", "raw"), "events", [
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": first["snapshot-id"]},
        ], [
            {"action": "add-snapshot", "snapshot": snapshot},
            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": DELETE_SNAPSHOT, "type": "branch"},
            {"action": "set-statistics", "statistics": {
                "snapshot-id": DELETE_SNAPSHOT, "statistics-path": statistics.as_uri(), "file-size-in-bytes": 15,
                "file-footer-size-in-bytes": 4, "blob-metadata": []}},
            {"action": "set-properties", "updates": {"write.data.path": (location / "data").as_uri()}},
        ])
        files.append(("lake", "raw"), "events", [{"id": 5, "label": "five"}])
        files.delete(("lake", "raw"), "events", {"id": 5})
        files.append(("lake", "raw"), "events", [{"id": 6, "label": None}])
        catalog.create_table(("lake",), "empty", SCHEMA)
        return catalog

    def tree_digest(self, root):
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob("*") if path.is_file()}

    def decoded(self, root):
        """Every string inside the JSON, Avro and Parquet files below root, with the file it came from."""
        module = self.iceberg
        found = []

        def walk(value, path):
            if isinstance(value, dict):
                for key, item in value.items():
                    walk(key, path)
                    walk(item, path)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    walk(item, path)
            elif isinstance(value, (str, bytes)):
                found.append((path, value.decode("utf-8", "replace") if isinstance(value, bytes) else value))

        for path in sorted(root.rglob("*")):
            if path.name.endswith(".metadata.json"):
                walk(json.loads(path.read_bytes()), path)
            elif path.suffix == ".avro":
                walk(module["avro_read"](path), path)
            elif path.suffix == ".parquet":
                walk(module["parquet_read"](path), path)
        return found

    def test_relocated_export_rewrites_every_location(self):
        self.build_source()
        expected = {snapshot: self.scan(self.warehouse, "lake.raw.events", snapshot=snapshot)
                    for snapshot in (None, DELETE_SNAPSHOT)}
        before = self.tree_digest(self.warehouse)
        relocated = self.directory / "relocated"
        catalog_file = self.directory / "s3.db"
        prefix = "s3://bucket/warehouse root"
        result = self.run_script("--warehouse", self.warehouse, "export", "--relocate", relocated,
                                 "--location-prefix", prefix + "/", "--out", catalog_file, "--catalog-name", "moved",
                                 umask=0o022 if os.name != "nt" else -1)
        self.assertRegex(result.stdout, r"^Relocated 2 tables \(\d+ files\) from .* with locations under "
                                        r"s3://bucket/warehouse root and SQL catalog 'moved' to .*s3\.db\n$")
        self.assertEqual(self.tree_digest(self.warehouse), before)
        self.assertEqual(sorted(path.name for path in self.directory.iterdir()),
                         sorted([SCRIPT.name, self.warehouse.name, "relocated", "s3.db"]))

        module = self.iceberg
        source_files = {path.relative_to(self.warehouse) for path in self.warehouse.rglob("*")
                        if path.is_file() and not path.name.startswith(".iceberg-catalog")}
        copied = {path.relative_to(relocated) for path in relocated.rglob("*") if path.is_file()}
        self.assertEqual(copied, source_files)
        old = [self.warehouse.as_uri(), str(self.warehouse), "%20", "%C3"]
        locations = 0
        for path, text in self.decoded(relocated):
            self.assertFalse(any(marker in text for marker in old), (path, text))
            if "://" in text:
                locations += 1
                self.assertTrue(text.startswith(prefix + "/lake/"), (path, text))
        self.assertGreater(locations, 40)

        def local(location):
            return relocated / location.removeprefix(prefix + "/")

        for path in relocated.rglob("*.avro"):
            for record in module["avro_read"](path)[2]:
                if "manifest_path" in record:
                    self.assertEqual(record["manifest_length"], local(record["manifest_path"]).stat().st_size)
                if "data_file" in record:
                    data_file = record["data_file"]
                    self.assertEqual(data_file["file_size_in_bytes"], local(data_file["file_path"]).stat().st_size)
                    if data_file["content"] == 1:
                        position = local(data_file["file_path"])
                        details = {}
                        _, rows = module["parquet_read"](position, details)
                        self.assertEqual(details, {"codec": 1, "key_value_metadata": [(b"delete-type", b"position")]})
                        bound = rows[0]["file_path"].encode()
                        self.assertTrue(bound.startswith(prefix.encode()))
                        self.assertEqual(data_file["lower_bounds"], [{"key": 2147483546, "value": bound}])
                        self.assertEqual(data_file["upper_bounds"], [{"key": 2147483546, "value": bound}])
                        _, footer, _ = module["_pq_file"](position)
                        self.assertEqual(data_file["split_offsets"], [footer[4][0][5]])
                        self.assertEqual({item["key"]: item["value"] for item in data_file["column_sizes"]},
                                         {2147483546: footer[4][0][1][0][3][7], 2147483545: footer[4][0][1][1][3][7]})
        self.assertEqual(module["avro_read"](next(relocated.rglob("delete-root.avro")))[1]["avro.codec"], b"snappy")

        with closing(sqlite3.connect(catalog_file)) as connection:
            rows = connection.execute("SELECT table_namespace, table_name, metadata_location, "
                                      "previous_metadata_location FROM iceberg_tables ORDER BY 1, 2").fetchall()
            properties = dict(connection.execute("SELECT property_key, property_value FROM iceberg_namespace_properties "
                                                 "WHERE namespace = 'lake'").fetchall())
        self.assertEqual(properties["location"], prefix + "/lake")
        metadata = json.loads(local(rows[1][2]).read_bytes())
        self.assertEqual(rows[1][3], metadata["metadata-log"][-1]["metadata-file"])
        self.assertTrue(metadata["location"].startswith(prefix + "/lake/raw/events-"))
        self.assertEqual(metadata["properties"]["write.data.path"], metadata["location"] + "/data")
        source = json.loads(self.iceberg["_catalog_local_path"](
            self.iceberg["IcebergCatalog"](self.warehouse).load_table(("lake", "raw"), "events")["metadata-location"]
        ).read_bytes())
        delete_snapshot = {item["snapshot-id"]: item for item in metadata["snapshots"]}[DELETE_SNAPSHOT]["summary"]
        source_snapshot = {item["snapshot-id"]: item for item in source["snapshots"]}[DELETE_SNAPSHOT]["summary"]
        shrink = int(source_snapshot["total-files-size"]) - int(delete_snapshot["total-files-size"])
        self.assertGreater(shrink, 0)
        self.assertEqual(int(source_snapshot["added-files-size"]) - int(delete_snapshot["added-files-size"]), shrink)
        if os.name != "nt":
            for path in relocated.rglob("*"):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o755 if path.is_dir() else 0o644, path)
            self.assertEqual(stat.S_IMODE(relocated.stat().st_mode), 0o755)

        # Relocating to a local prefix gives a catalog that the server, PyIceberg and Iceberg Java can open.
        local_root = self.directory / "other dir ü"
        local_catalog = self.directory / "local.db"
        self.run_script("--warehouse", self.warehouse, "export", "--relocate", local_root, "--location-prefix",
                        "file://" + local_root.as_posix(), "--out", local_catalog)
        with closing(sqlite3.connect(local_catalog)) as connection:
            pointer = connection.execute("SELECT metadata_location FROM iceberg_tables WHERE table_name = 'events'"
                                         ).fetchone()[0]
        self.assertTrue(pointer.startswith("file://" + local_root.as_posix() + "/lake/raw/events-"))
        imported = self.directory / "imported.sqlite3"
        self.run_script("import", "--db-file", imported, "--warehouse", local_root, "--sql-catalog", local_catalog)
        for snapshot, rows in expected.items():
            self.assertEqual(self.scan(local_root, "lake.raw.events", imported, snapshot), rows)
        shutil.rmtree(self.warehouse)
        self.assertEqual(self.scan(local_root, "lake.raw.events", imported), expected[None])

    def test_relocated_export_refuses_existing_outputs_and_bad_prefixes(self):
        self.build_source()
        existing = self.directory / "existing"
        existing.mkdir()
        catalog_file = self.directory / "catalog.db"
        for args, message in (
            (["--relocate", existing, "--location-prefix", "s3://b/w"], "Output already exists"),
            (["--relocate", self.directory / "new", "--location-prefix", "relative/path"], "absolute URI"),
            (["--relocate", self.directory / "new"], "must be used together"),
            (["--location-prefix", "s3://b/w", "--out", catalog_file], "must be used together"),
            ([], "export needs --out"),
        ):
            with self.subTest(args=args):
                result = self.run_script("--warehouse", self.warehouse, "export", *args, check=False)
                self.assertEqual(result.returncode, 1)
                self.assertIn(message, result.stderr)
        catalog = self.iceberg["IcebergCatalog"](self.warehouse)
        broken = catalog.load_table(("lake",), "empty")["metadata-location"]
        self.iceberg["_catalog_local_path"](broken).unlink()
        result = self.run_script("--warehouse", self.warehouse, "export", "--relocate", self.directory / "new",
                                 "--location-prefix", "s3://b/w", "--out", catalog_file, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Iceberg export failed: Table lake.empty: Could not read metadata file", result.stderr)
        self.assertFalse((self.directory / "new").exists())
        self.assertFalse(catalog_file.exists())
        self.assertEqual(list(self.directory.glob(".iceberg-*")), [])

    def sql_catalog(self, path, rows, properties, typed=True):
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute("CREATE TABLE iceberg_tables (catalog_name VARCHAR(255) NOT NULL, table_namespace "
                               "VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, metadata_location "
                               "VARCHAR(1000), previous_metadata_location VARCHAR(1000)"
                               + (", iceberg_type VARCHAR(5)" if typed else "") + ")")
            connection.execute("CREATE TABLE iceberg_namespace_properties (catalog_name VARCHAR(255) NOT NULL, "
                               "namespace VARCHAR(255) NOT NULL, property_key VARCHAR(255), property_value "
                               "VARCHAR(1000))")
            columns = "?, ?, ?, ?, NULL" + (", ?" if typed else "")
            connection.executemany(f"INSERT INTO iceberg_tables VALUES ({columns})",
                                   [row if typed else row[:4] for row in rows])
            connection.executemany("INSERT INTO iceberg_namespace_properties VALUES (?, ?, ?, ?)", properties)

    def test_import_sql_catalogs_written_by_other_tools(self):
        catalog = self.build_source()
        events = catalog.load_table(("lake", "raw"), "events")["metadata-location"]
        empty = catalog.load_table(("lake",), "empty")["metadata-location"]
        # PyIceberg and Iceberg Java store raw paths in file URIs.
        raw_events = "file://" + self.iceberg["_catalog_local_path"](events).as_posix()
        self.assertIn(" ", raw_events)
        v1 = self.directory / "java.db"
        self.sql_catalog(v1, [
            ("java", "lake.raw", "events", raw_events, "TABLE"), ("java", "lake", "empty", empty, None),
            ("java", "lake", "a_view", empty, "VIEW"), ("other", "x", "y", empty, "TABLE"),
        ], [("java", "lake", "exists", "true"), ("java", "lake", "owner", "data team"),
            ("java", "deep.er", "exists", "true")])
        imported = self.directory / "java.sqlite3"
        result = self.run_script("import", "--db-file", imported, "--warehouse", self.warehouse, "--sql-catalog", v1,
                                 "--catalog-name", "java")
        self.assertIn("Imported 2 tables in 4 namespaces from SQL catalog 'java'", result.stdout)
        self.assertIn("warning: skipping view lake.a_view", result.stderr)
        server = self.iceberg["IcebergCatalog"](self.warehouse, imported)
        self.assertEqual(server.list_namespaces(), [("deep",), ("lake",)])
        self.assertEqual(server.load_namespace(("lake",))["properties"], {"owner": "data team"})
        self.assertEqual(server.load_namespace(("deep", "er"))["properties"], {})
        self.assertEqual(server.load_table(("lake", "raw"), "events")["metadata-location"], raw_events)
        self.assertEqual(self.scan(self.warehouse, "lake.raw.events", imported),
                         self.scan(self.warehouse, "lake.raw.events"))

        v0 = self.directory / "pyiceberg.db"
        self.sql_catalog(v0, [("default", "lake", "empty", empty)], [], typed=False)
        self.run_script("--warehouse", self.warehouse, "import", "--db-file", self.directory / "v0.sqlite3",
                        "--sql-catalog", v0)
        self.assertEqual(self.iceberg["IcebergCatalog"](self.warehouse, self.directory / "v0.sqlite3").list_tables(
            ("lake",)), [{"namespace": ("lake",), "name": "empty"}])

        exported = self.directory / "exported.db"
        self.run_script("--warehouse", self.warehouse, "export", "--out", exported, "--catalog-name", "lake")
        round_trip = self.directory / "round-trip.sqlite3"
        self.run_script("import", "--db-file", round_trip, "--warehouse", self.warehouse, "--sql-catalog", exported,
                        "--catalog-name", "lake")
        with closing(sqlite3.connect(self.warehouse / ".iceberg-catalog.sqlite3")) as left, \
                closing(sqlite3.connect(round_trip)) as right:
            for table in ("iceberg_namespaces", "iceberg_tables"):
                query = f"SELECT * FROM {table} ORDER BY 1, 2"
                self.assertEqual(left.execute(query).fetchall(), right.execute(query).fetchall())

        s3 = self.directory / "s3.db"
        self.sql_catalog(s3, [("default", "lake", "t", "s3://bucket/wh/t/metadata/00000-x.metadata.json", "TABLE")], [])
        outside = self.directory / "outside.db"
        self.sql_catalog(outside, [("default", "lake", "t", empty, "TABLE")], [])
        target = self.directory / "never.sqlite3"
        existing = self.directory / "existing.sqlite3"
        existing.write_bytes(b"keep")
        for args, message in (
            (["--warehouse", self.warehouse, "--sql-catalog", s3], "is not a local file location"),
            (["--warehouse", self.directory / "elsewhere", "--sql-catalog", outside], "warehouse directory does not"),
            (["--warehouse", self.warehouse / "lake" / "raw", "--sql-catalog", outside], "must stay inside the warehouse"),
            (["--warehouse", self.warehouse, "--sql-catalog", v1, "--catalog-name", "nope"],
             "Catalog 'nope' was not found"),
            (["--warehouse", self.warehouse, "--sql-catalog", self.script], "is not an Iceberg JDBC/SQL catalog"),
            (["--warehouse", self.warehouse, "--source", "http://127.0.0.1:9"], "GET http://127.0.0.1:9/v1/config"),
        ):
            with self.subTest(args=args):
                result = self.run_script("import", "--db-file", target, *args, check=False)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertTrue(result.stderr.startswith("Iceberg import failed: "), result.stderr)
                self.assertIn(message, result.stderr)
                self.assertFalse(target.exists())
        result = self.run_script("import", "--db-file", existing, "--warehouse", self.warehouse, "--sql-catalog", v0,
                                 check=False)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(existing.read_bytes(), b"keep")
        self.assertEqual(list(self.directory.glob(".iceberg-*")), [])

    def test_import_from_rest_and_polaris_catalogs(self):
        self.build_source()
        base = self.serve("--warehouse", self.warehouse, "serve")
        imported = self.directory / "rest.sqlite3"
        result = self.run_script("import", "--db-file", imported, "--warehouse", self.warehouse, "--source", base)
        self.assertIn("Imported 2 tables in 2 namespaces from " + base, result.stdout)
        with closing(sqlite3.connect(self.warehouse / ".iceberg-catalog.sqlite3")) as left, \
                closing(sqlite3.connect(imported)) as right:
            for table in ("iceberg_namespaces", "iceberg_tables"):
                query = f"SELECT * FROM {table} ORDER BY 1, 2"
                self.assertEqual(left.execute(query).fetchall(), right.execute(query).fetchall())

        polaris = self.directory / "polaris"
        state = self.iceberg["_PolarisState"](polaris, None, "root-secret", "POLARIS")
        with self.iceberg["_catalog_transaction"](state.db_file, True) as connection:
            location = (polaris / "lake").as_uri()
            state.create_entity(connection, {"catalog": {
                "name": "lake", "type": "INTERNAL", "properties": {"default-base-location": location},
                "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]},
            }}, "catalog", None)
        lake = self.iceberg["IcebergCatalog"](polaris / "lake", strict_metadata_paths=True)
        lake.create_namespace(("sales",), {"region": "eu"})
        lake.create_table(("sales",), "orders", SCHEMA)
        self.iceberg["IcebergFiles"](lake).append(("sales",), "orders", [{"id": 1, "label": "one"}])
        password = self.directory / "root-password"
        password.write_text("root-secret\n", encoding="utf-8")
        polaris_base = self.serve("--warehouse", polaris, "serve-polaris", "--password-file", password)
        credentials = self.directory / "credentials"
        credentials.write_text("root:root-secret\n", encoding="utf-8")
        from_polaris = self.directory / "polaris.sqlite3"
        result = self.run_script("import", "--db-file", from_polaris, "--warehouse", polaris / "lake", "--source",
                                 polaris_base + "/api/catalog", "--source-warehouse", "lake",
                                 "--credential-file", credentials)
        self.assertIn("Imported 1 tables in 1 namespaces", result.stdout)
        self.assertNotIn("root-secret", result.stdout + result.stderr)
        self.assertEqual(self.scan(polaris / "lake", "sales.orders", from_polaris), [{"id": 1, "label": "one"}])

    def test_migrate_dry_run_writes_nothing(self):
        self.build_source()
        target_db = self.directory / "target.sqlite3"
        base = self.serve("--warehouse", self.directory, "--db-file", target_db, "serve")
        result = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base, "--dry-run")
        self.assertEqual(self.http("GET", base + "/v1/namespaces")["namespaces"], [])
        self.assertIn("would create namespace lake\nwould create namespace lake.raw\n", result.stdout)
        self.assertIn("would register table lake.raw.events at file://", result.stdout)
        self.assertTrue(result.stdout.endswith(
            f"Would migrate to {base}: created 2 namespaces and registered 2 tables; 0 namespaces and 0 tables were "
            "already present; 0 failed (dry run, nothing was written)\n"))
        self.run_script("--warehouse", self.warehouse, "migrate", "--target", base)
        again = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base, "--dry-run")
        self.assertIn("created 0 namespaces and registered 0 tables; 2 namespaces and 2 tables", again.stdout)
        self.assertNotIn("would ", again.stdout)

        catalog = self.iceberg["IcebergCatalog"](self.warehouse)
        self.iceberg["IcebergFiles"](catalog).append(("lake", "raw"), "events", [{"id": 9, "label": "new"}])
        catalog.create_namespace(("late",))
        before = target_db.read_bytes()
        failed = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base, "--dry-run", check=False)
        self.assertEqual(failed.returncode, 1)
        self.assertIn("table lake.raw.events: conflict: the target table points at", failed.stderr)
        self.assertIn("would create namespace late\n", failed.stdout)
        self.assertIn("Iceberg migrate failed: 1 of 5 namespaces and tables failed", failed.stderr)
        self.assertEqual(target_db.read_bytes(), before)

    def test_reset_clears_serve_and_polaris_state(self):
        catalog = self.build_source()
        base = self.serve("--warehouse", self.warehouse, "serve")
        files = self.tree_digest(self.warehouse / "lake")
        for method in ("GET", "PUT", "DELETE"):
            self.http(method, base + "/_jjap/reset", expected=405)
        self.assertEqual(self.http("POST", base + "/_jjap/reset", {}), {"reset": True})
        self.assertEqual(self.http("GET", base + "/v1/namespaces")["namespaces"], [])
        self.assertEqual(catalog.list_namespaces(), [])
        self.assertEqual(self.tree_digest(self.warehouse / "lake"), files)
        self.http("POST", base + "/v1/namespaces", {"namespace": ["lake"]})
        self.http("PUT", base + "/v1/namespaces", expected=501)

        polaris = self.directory / "polaris"
        password = self.directory / "root-password"
        password.write_text("root-secret\n", encoding="utf-8")
        base = self.serve("--warehouse", polaris, "serve-polaris", "--password-file", password)
        tokens = base + "/api/catalog/v1/oauth/tokens"
        management = base + "/api/management/v1"
        root = self.http("POST", tokens, form={"grant_type": "client_credentials", "client_id": "root",
                                                "client_secret": "root-secret"})["access_token"]
        location = (polaris / "demo").as_uri()
        catalog_body = {"catalog": {"name": "demo", "type": "INTERNAL", "properties": {"default-base-location": location},
                                    "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]}}}
        self.http("POST", management + "/catalogs", catalog_body, token=root, expected=201)
        self.http("POST", base + "/api/catalog/v1/demo/namespaces", {"namespace": ["sales"]}, token=root)
        principal = self.http("POST", management + "/principals", {"principal": {"name": "user"}}, token=root,
                              expected=201)["credentials"]
        self.http("POST", management + "/principal-roles", {"principalRole": {"name": "readers"}}, token=root,
                  expected=201)
        user = self.http("POST", tokens, form={"grant_type": "client_credentials", "client_id": principal["clientId"],
                                                "client_secret": principal["clientSecret"]})["access_token"]
        self.http("POST", base + "/_jjap/reset", expected=401)
        self.http("POST", base + "/_jjap/reset", token=user, expected=403)
        self.http("GET", base + "/_jjap/reset", token=root, expected=405)
        self.assertEqual(self.http("POST", base + "/_jjap/reset", token=root), {"reset": True})
        for route, key in (("/catalogs", "catalogs"), ("/principals", "principals"), ("/principal-roles", "roles")):
            self.assertEqual(self.http("GET", management + route, token=root)[key], [])
        self.http("GET", management + "/catalogs", token=user, expected=401)
        self.http("POST", management + "/catalogs", catalog_body, token=root, expected=201)
        self.assertEqual(self.http("GET", base + "/api/catalog/v1/demo/namespaces", token=root)["namespaces"], [])

    @unittest.skipIf(os.name == "nt", "file modes are POSIX")
    def test_written_files_follow_the_umask(self):
        schema = self.directory / "schema.json"
        schema.write_text(json.dumps(FIELDS), encoding="utf-8")
        for args, rows in ((["create-table", "demo.events", "--schema", schema], None),
                           (["append", "demo.events", "-"], '{"id": 1, "label": "x"}\n')):
            subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "--warehouse", str(self.warehouse),
                            *map(str, args)], cwd=self.directory, env=ENVIRONMENT, input=rows, text=True,
                           capture_output=True, timeout=30, check=True, umask=0o022)
        written = [path for path in self.warehouse.rglob("*") if path.is_file()]
        self.assertEqual({path.suffix for path in written}, {".json", ".avro", ".parquet", ".sqlite3"})
        for path in written:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644, path)
        exported = self.directory / "catalog.db"
        self.run_script("--warehouse", self.warehouse, "export", "--out", exported, umask=0o022)
        self.assertEqual(stat.S_IMODE(exported.stat().st_mode), 0o644)
        imported = self.directory / "imported.sqlite3"
        self.run_script("import", "--db-file", imported, "--warehouse", self.warehouse, "--sql-catalog", exported,
                        umask=0o022)
        self.assertEqual(stat.S_IMODE(imported.stat().st_mode), 0o644)


if __name__ == "__main__":
    unittest.main()
