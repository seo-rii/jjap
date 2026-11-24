"""Verify SQL catalog export and REST catalog migration without external services."""

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
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener


SCRIPT = Path(__file__).resolve().parents[1] / "iceberg_server.py"
SCHEMA = {"type": "struct", "schema-id": 0, "fields": [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "label", "required": False, "type": "string"},
    {"id": 3, "name": "day", "required": False, "type": "date"},
    {"id": 4, "name": "amount", "required": False, "type": "decimal(12, 2)"},
]}
ROWS = [
    {"id": 1, "label": "ünïcode", "day": "2024-02-29", "amount": "12.50"},
    {"id": 2, "label": None, "day": None, "amount": None},
]
MORE_ROWS = [{"id": 3, "label": "before epoch", "day": "1969-12-31", "amount": "-0.01"}]
# Loopback requests must not be sent through a proxy configured for the test runner.
ENVIRONMENT = {key: value for key, value in os.environ.items() if "proxy" not in key.lower()}
HTTP = build_opener(ProxyHandler({}))


class IcebergExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.iceberg = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-iceberg-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.warehouse = self.directory / "source"
        self.source = self.directory / ("origin #?.sqlite3" if os.name != "nt" else "origin #.sqlite3")

    def run_script(self, *args, check=True):
        result = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)],
            cwd=self.directory, env=ENVIRONMENT, text=True, capture_output=True, timeout=30,
        )
        if check and result.returncode:
            self.fail(f"{args} exited with {result.returncode}: {result.stderr}")
        return result

    def scan(self, warehouse, identifier, db_file=None):
        options = ["--db-file", db_file] if db_file else []
        output = self.run_script("--warehouse", warehouse, *options, "scan", identifier).stdout
        return [json.loads(line) for line in output.splitlines()]

    def catalog(self, warehouse, db_file=None):
        return self.iceberg["IcebergCatalog"](warehouse, db_file)

    def append(self, catalog, namespace, name, rows):
        return self.iceberg["IcebergFiles"](catalog).append(namespace, name, rows)

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
            return json.loads(payload) if payload else None

    def build_source(self, db_file=None):
        catalog = self.catalog(self.warehouse, db_file)
        catalog.create_namespace(("lake",), {"owner": "data team", "comment": "ünïcode ✓"})
        catalog.create_namespace(("lake", "raw"))
        catalog.create_namespace(("empty",))
        catalog.create_table(("lake", "raw"), "events", SCHEMA)
        self.append(catalog, ("lake", "raw"), "events", ROWS)
        previous = catalog.load_table(("lake", "raw"), "events")["metadata-location"]
        self.append(catalog, ("lake", "raw"), "events", MORE_ROWS)
        catalog.create_table(("lake",), "fresh", SCHEMA)
        return catalog, previous

    def test_sql_catalog_export_matches_the_jdbc_schema_and_keeps_history(self):
        catalog, previous = self.build_source(self.source)
        events = catalog.load_table(("lake", "raw"), "events")
        fresh = catalog.load_table(("lake",), "fresh")["metadata-location"]
        self.assertEqual(events["metadata"]["metadata-log"][-1]["metadata-file"], previous)
        snapshot = hashlib.sha256(self.source.read_bytes()).hexdigest()

        output = self.directory / "exported" / "catalog.db"
        result = self.run_script("--warehouse", self.warehouse, "--db-file", self.source, "export",
                                 "--format", "sql-catalog", "--out", output, "--catalog-name", "lake_catalog")
        self.assertIn("Exported 2 tables in 3 namespaces", result.stdout)
        self.assertIn(self.warehouse.resolve().as_uri(), result.stdout)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), snapshot)
        self.assertEqual(list(output.parent.iterdir()), [output])

        with sqlite3.connect(output) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(iceberg_tables)")]
            self.assertEqual(columns, ["catalog_name", "table_namespace", "table_name", "metadata_location",
                                       "previous_metadata_location", "iceberg_type"])
            keys = [row[1] for row in connection.execute("PRAGMA table_info(iceberg_namespace_properties)") if row[5]]
            self.assertEqual(keys, ["catalog_name", "namespace", "property_key"])
            tables = connection.execute("SELECT * FROM iceberg_tables ORDER BY table_namespace, table_name").fetchall()
            properties = set(connection.execute("SELECT * FROM iceberg_namespace_properties"))
        self.assertEqual(tables, [
            ("lake_catalog", "lake", "fresh", fresh, None, "TABLE"),
            ("lake_catalog", "lake.raw", "events", events["metadata-location"], previous, "TABLE"),
        ])
        self.assertEqual(properties, {
            ("lake_catalog", "empty", "exists", "true"),
            ("lake_catalog", "lake", "exists", "true"),
            ("lake_catalog", "lake", "owner", "data team"),
            ("lake_catalog", "lake", "comment", "ünïcode ✓"),
            ("lake_catalog", "lake.raw", "exists", "true"),
        })
        # The exported pointer names the same immutable files, so schema, snapshots and rows are unchanged.
        metadata = json.loads(self.iceberg["_catalog_local_path"](tables[1][3]).read_text(encoding="utf-8"))
        self.assertEqual(metadata["schemas"][0]["fields"], SCHEMA["fields"])
        self.assertEqual(metadata["current-snapshot-id"], events["metadata"]["current-snapshot-id"])
        self.assertEqual(sorted(self.scan(self.warehouse, "lake.raw.events", self.source), key=lambda row: row["id"]),
                         ROWS + MORE_ROWS)

        default = self.directory / "default.db"
        self.run_script("--warehouse", self.warehouse, "--db-file", self.source, "export", "--out", default)
        with sqlite3.connect(default) as connection:
            self.assertEqual({row[0] for row in connection.execute("SELECT catalog_name FROM iceberg_tables")}, {"default"})

    def test_export_refuses_existing_output_and_invalid_sources(self):
        self.build_source()
        existing = self.directory / "used.db"
        existing.write_text("preserve", encoding="utf-8")
        result = self.run_script("--warehouse", self.warehouse, "export", "--out", existing, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Iceberg export failed: Output file already exists", result.stderr)
        self.assertEqual(existing.read_text(encoding="utf-8"), "preserve")

        output = self.directory / "out.db"
        missing = self.directory / "missing.sqlite3"
        result = self.run_script("--warehouse", self.warehouse, "--db-file", missing, "export", "--out", output, check=False)
        self.assertIn("not found", result.stderr)
        self.assertFalse(missing.exists())

        exported = self.directory / "exported.db"
        self.run_script("--warehouse", self.warehouse, "export", "--out", exported)
        unrelated = self.directory / "unrelated.sqlite3"
        with sqlite3.connect(unrelated) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        garbage = self.directory / "garbage.sqlite3"
        garbage.write_bytes(b"not a database" * 100)
        for source in (exported, unrelated, garbage):
            with self.subTest(source=source.name):
                result = self.run_script("--warehouse", self.warehouse, "--db-file", source, "export", "--out", output,
                                         check=False)
                self.assertEqual(result.returncode, 1)
                self.assertIn("is not an Iceberg test server SQLite catalog", result.stderr)

        catalog = self.catalog(self.warehouse)
        catalog.create_namespace(("dotted.level",))
        result = self.run_script("--warehouse", self.warehouse, "export", "--out", output, check=False)
        self.assertIn("level containing '.'", result.stderr)
        catalog.drop_namespace(("dotted.level",))
        catalog.update_namespace(("empty",), [], {"exists": "false"})
        result = self.run_script("--warehouse", self.warehouse, "export", "--out", output, check=False)
        self.assertIn("'exists', which SQL catalogs reserve", result.stderr)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.directory.glob(".iceberg-export-*")), [])

    def test_migrate_registers_tables_in_a_second_server_idempotently(self):
        catalog, _ = self.build_source()
        expected = self.scan(self.warehouse, "lake.raw.events")
        # The target must read the same absolute file URIs, so its warehouse contains the source warehouse.
        target_db = self.directory / "target.sqlite3"
        base = self.serve("--warehouse", self.directory, "--db-file", target_db, "serve")

        result = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base)
        self.assertIn("created 3 namespaces and registered 2 tables; 0 namespaces and 0 tables", result.stdout)
        self.assertEqual(self.scan(self.directory, "lake.raw.events", target_db), expected)
        self.assertEqual(self.http("GET", base + "/v1/namespaces/lake")["properties"],
                         {"owner": "data team", "comment": "ünïcode ✓"})
        self.assertEqual(self.http("GET", base + "/v1/namespaces?parent=lake")["namespaces"], [["lake", "raw"]])
        loaded = self.http("GET", base + "/v1/namespaces/lake%1Fraw/tables/events")
        self.assertEqual(loaded["metadata-location"], catalog.load_table(("lake", "raw"), "events")["metadata-location"])

        again = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base + "/")
        self.assertIn("created 0 namespaces and registered 0 tables; 3 namespaces and 2 tables", again.stdout)
        self.assertEqual(again.stderr, "")

        self.append(catalog, ("lake", "raw"), "events", [{"id": 4, "label": "new", "day": None, "amount": None}])
        broken = catalog.create_table(("lake",), "broken", SCHEMA)["metadata-location"]
        Path(self.iceberg["_catalog_local_path"](broken)).unlink()
        catalog.create_table(("empty",), "late", SCHEMA)
        failed = self.run_script("--warehouse", self.warehouse, "migrate", "--target", base, check=False)
        self.assertEqual(failed.returncode, 1)
        self.assertIn("registered 1 tables; 3 namespaces and 1 tables were already present; 2 failed", failed.stdout)
        self.assertIn("table lake.raw.events: conflict: the target table points at", failed.stderr)
        self.assertIn("table lake.broken: HTTP 400", failed.stderr)
        self.assertIn("Iceberg migrate failed: 2 of 7 namespaces and tables failed", failed.stderr)
        self.assertEqual(self.scan(self.directory, "lake.raw.events", target_db), expected)
        self.assertEqual(self.http("GET", base + "/v1/namespaces/empty/tables")["identifiers"],
                         [{"namespace": ["empty"], "name": "late"}])

        unreachable = self.run_script("--warehouse", self.warehouse, "migrate", "--target", "http://127.0.0.1:9",
                                      check=False)
        self.assertEqual(unreachable.returncode, 1)
        self.assertIn("Iceberg migrate failed: GET http://127.0.0.1:9/v1/config", unreachable.stderr)

    def test_polaris_catalog_migrates_with_oauth_and_exports_alone(self):
        state = self.iceberg["_PolarisState"](self.warehouse, None, "source-secret", "POLARIS")
        with self.iceberg["_catalog_transaction"](state.db_file, True) as connection:
            for name in ("lake", "other"):
                location = (self.warehouse / name).as_uri()
                state.create_entity(connection, {"catalog": {
                    "name": name, "type": "INTERNAL", "properties": {"default-base-location": location},
                    "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]},
                }}, "catalog", None)
        lake = self.catalog(self.warehouse / "lake")
        lake.create_namespace(("sales",), {"region": "eu"})
        lake.create_namespace(("sales", "eu"))
        created = lake.create_table(("sales", "eu"), "orders", SCHEMA)["metadata-location"]
        self.append(lake, ("sales", "eu"), "orders", ROWS)
        other = self.catalog(self.warehouse / "other")
        other.create_namespace(("private",))
        other.create_table(("private",), "secrets", SCHEMA)
        pointer = lake.load_table(("sales", "eu"), "orders")["metadata-location"]
        expected = self.scan(self.warehouse / "lake", "sales.eu.orders")

        result = self.run_script("--warehouse", self.warehouse, "export", "--out", self.directory / "x.db", check=False)
        self.assertIn("use --polaris-catalog NAME", result.stderr)
        result = self.run_script("--warehouse", self.warehouse, "export", "--polaris-catalog", "absent",
                                 "--out", self.directory / "x.db", check=False)
        self.assertIn("Polaris catalog 'absent' was not found", result.stderr)
        result = self.run_script("--warehouse", self.warehouse, "--db-file", lake.db_file, "export",
                                 "--polaris-catalog", "lake", "--out", self.directory / "x.db", check=False)
        self.assertIn("is not a Polaris test server SQLite persistence file", result.stderr)
        exported = self.directory / "lake.db"
        self.run_script("--warehouse", self.warehouse, "export", "--polaris-catalog", "lake", "--out", exported)
        with sqlite3.connect(exported) as connection:
            self.assertEqual(connection.execute("SELECT * FROM iceberg_tables").fetchall(),
                             [("lake", "sales.eu", "orders", pointer, created, "TABLE")])

        password = self.directory / "root-password"
        password.write_text("target-secret\n", encoding="utf-8")
        base = self.serve("--warehouse", self.directory, "--db-file", self.directory / "target-polaris.sqlite3",
                          "serve-polaris", "--password-file", password)
        tokens = base + "/api/catalog/v1/oauth/tokens"
        root = self.http("POST", tokens, form={"grant_type": "client_credentials", "client_id": "root",
                                                "client_secret": "target-secret"})["access_token"]
        management = base + "/api/management/v1"
        mirror = self.warehouse.resolve().as_uri()
        self.http("POST", management + "/catalogs", {"catalog": {
            "name": "mirror", "type": "INTERNAL", "properties": {"default-base-location": mirror},
            "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [mirror]},
        }}, token=root, expected=201)
        principal = self.http("POST", management + "/principals", {"principal": {"name": "migrator"}},
                              token=root, expected=201)["credentials"]
        for route, body in (
            ("/principal-roles", {"principalRole": {"name": "migrator"}}),
            ("/catalogs/mirror/catalog-roles", {"catalogRole": {"name": "migrator"}}),
        ):
            self.http("POST", management + route, body, token=root, expected=201)
        for route, body in (
            ("/principals/migrator/principal-roles", {"principalRole": {"name": "migrator"}}),
            ("/principal-roles/migrator/catalog-roles/mirror", {"catalogRole": {"name": "migrator"}}),
            ("/catalogs/mirror/catalog-roles/migrator/grants",
             {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}),
        ):
            self.http("PUT", management + route, body, token=root, expected=201)

        credentials = self.directory / "credentials"
        credentials.write_text(f"{principal['clientId']}:wrong\n", encoding="utf-8")
        target = ["--target", base + "/api/catalog", "--warehouse", "mirror"]
        result = self.run_script("--warehouse", self.warehouse, "migrate", "--polaris-catalog", "lake", *target,
                                 "--credential-file", credentials, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("OAuth2 token request failed: HTTP 401 invalid_client", result.stderr)

        credentials.write_text(f"{principal['clientId']}:{principal['clientSecret']}\n", encoding="utf-8")
        result = self.run_script("--warehouse", self.warehouse, "migrate", "--polaris-catalog", "lake", *target,
                                 "--credential-file", credentials, "--scope", "PRINCIPAL_ROLE:migrator")
        self.assertIn("created 2 namespaces and registered 1 tables", result.stdout)
        token = self.http("POST", tokens, form={
            "grant_type": "client_credentials", "client_id": principal["clientId"],
            "client_secret": principal["clientSecret"],
        })["access_token"]
        namespaces = base + "/api/catalog/v1/mirror/namespaces"
        self.assertEqual(self.http("GET", namespaces, token=token)["namespaces"], [["sales"]])
        self.assertEqual(self.http("GET", namespaces + "/sales", token=token)["properties"], {"region": "eu"})
        loaded = self.http("GET", namespaces + "/sales%1Feu/tables/orders", token=token)
        self.assertEqual(loaded["metadata-location"], pointer)
        self.http("GET", namespaces + "/private", token=token, expected=404)
        # The mirror catalog keeps its own pointers in the source warehouse root.
        self.assertEqual(self.scan(self.warehouse, "sales.eu.orders"), expected)

        token_file = self.directory / "token"
        token_file.write_text(root + "\n", encoding="utf-8")
        again = self.run_script("--warehouse", self.warehouse, "migrate", "--polaris-catalog", "lake", *target,
                                "--token-file", token_file)
        self.assertIn("2 namespaces and 1 tables were already present; 0 failed", again.stdout)


if __name__ == "__main__":
    unittest.main()
