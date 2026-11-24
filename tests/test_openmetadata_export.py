"""Export an OpenMetadata catalog as Create* request bodies and migrate it into a second server."""

import contextlib
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import quote
import uuid


SCRIPT = Path(__file__).resolve().parents[1] / "openmetadata_server.py"
ENV = {key: value for key, value in os.environ.items() if not key.lower().endswith("_proxy")}
TAG = {"tagFQN": "PII.Sensitive", "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
SERVER_KEYS = {"id", "href", "version", "updatedAt", "updatedBy"}
ORDERS = "mysql_prod.shop.public.orders"
SUMMARY = 'mysql_prod.shop.public."daily.summary"'
LEGACY = "mysql_prod.shop.public.legacy"
TOPIC = 'kafka."orders.events"'
ENTITIES = [("user", '"john.doe"'), ("user", "alice"), ("databaseService", "mysql_prod"), ("pipelineService", "airflow"),
            ("messagingService", "kafka"), ("database", "mysql_prod.shop"), ("databaseSchema", "mysql_prod.shop.public"),
            ("table", SUMMARY), ("table", ORDERS), ("pipeline", "airflow.etl"), ("topic", TOPIC)]
COLLECTIONS = {"user": "users", "databaseService": "services/databaseServices", "pipelineService": "services/pipelineServices",
               "messagingService": "services/messagingServices", "database": "databases", "databaseSchema": "databaseSchemas",
               "table": "tables", "pipeline": "pipelines", "topic": "topics"}


def request(port, method, path, body=None, headers=None):
    headers = dict(headers or {})
    if body is not None:
        body = json.dumps(body, ensure_ascii=False).encode()
        headers["Content-Type"] = "application/json-patch+json" if method == "PATCH" else "application/json"
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    with contextlib.closing(connection):
        connection.request(method, path, body, headers)
        response = connection.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if payload else None


def normalized(value, nested=False):
    """Drop instance-specific ids, and the column and task FQNs a real server derives, to compare two servers."""
    if isinstance(value, list):
        return [normalized(item, nested) for item in value]
    if isinstance(value, dict):
        return {key: normalized(item, nested or key in {"columns", "tasks"}) for key, item in value.items()
                if key not in SERVER_KEYS and not (nested and key == "fullyQualifiedName")}
    return value


class OpenMetadataExportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-openmetadata-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("catalog #?.sqlite3" if os.name != "nt" else "catalog #.sqlite3")
        self.token = self.directory / "token"
        self.token.write_text("migration-jwt\n", encoding="ascii")
        self.auth = {"Authorization": "Bearer migration-jwt"}

    def run_script(self, *args, check=True):
        return subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)], cwd=self.directory,
                              env=ENV, check=check, text=True, capture_output=True, timeout=60)

    @contextlib.contextmanager
    def server(self, db_file, *args):
        process = subprocess.Popen([sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0",
                                    "--db-file", str(db_file), *args], cwd=self.directory, env=ENV,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            lines = []
            reader = threading.Thread(target=lambda: lines.append(process.stdout.readline()), daemon=True)
            reader.start()
            reader.join(10)
            match = re.search(r"127\.0\.0\.1:(\d+)", lines[0] if lines else "")
            if match is None:
                raise AssertionError(f"server did not start: {lines}")
            yield int(match[1])
        finally:
            process.terminate()
            process.wait(timeout=10)
            process.stdout.close()

    def call(self, port, method, path, body=None, expected=(200, 201), headers=None):
        status, payload = request(port, method, "/api/v1/" + path, body, headers)
        self.assertIn(status, expected, payload)
        return payload

    def put(self, port, kind, body):
        return self.call(port, "PUT", COLLECTIONS[kind], body)

    def populate(self):
        with self.server(self.source) as port:
            alice = self.put(port, "user", {"name": "alice", "email": "alice@example.com", "displayName": "Alice Ł",
                                            "isAdmin": False, "profile": {"images": {"image": "https://example.test/a.png"}}})
            john = self.put(port, "user", {"name": "john.doe", "email": "john.doe@example.com"})
            self.put(port, "databaseService", {
                "name": "mysql_prod", "serviceType": "Mysql", "description": "Production MySQL",
                "connection": {"config": {"type": "Mysql", "hostPort": "db:3306", "username": "reader"}},
                "owners": [{"id": alice["id"], "type": "user"}]})
            self.put(port, "database", {"name": "shop", "service": "mysql_prod", "description": "Shop",
                                        "owners": [{"id": john["id"], "type": "user"}], "tags": [TAG]})
            self.put(port, "databaseSchema", {"name": "public", "database": "mysql_prod.shop"})
            columns = [
                {"name": "id", "dataType": "BIGINT", "constraint": "PRIMARY_KEY", "ordinalPosition": 1},
                {"name": "customer", "dataType": "STRUCT", "dataTypeDisplay": "struct<name:string>",
                 "fullyQualifiedName": f"{ORDERS}.customer",
                 "children": [{"name": "name", "dataType": "STRING", "fullyQualifiedName": f"{ORDERS}.customer.name",
                               "tags": [TAG]}]},
                {"name": "amount", "dataType": "DECIMAL", "precision": 12, "scale": 2},
            ]
            orders = self.put(port, "table", {"name": "orders", "databaseSchema": "mysql_prod.shop.public", "columns": columns,
                                              "tableType": "Regular", "owners": [{"id": alice["id"], "type": "user"}],
                                              "extension": {"retention": "P30D"},
                                              "followers": [{"id": john["id"], "type": "user"}]})
            summary = self.put(port, "table", {"name": "daily.summary", "databaseSchema": "mysql_prod.shop.public",
                                               "columns": [{"name": "day", "dataType": "DATE"}], "tableType": "View",
                                               "schemaDefinition": "SELECT 1"})
            legacy = self.put(port, "table", {"name": "legacy", "databaseSchema": "mysql_prod.shop.public",
                                              "columns": [{"name": "id", "dataType": "INT"}]})
            self.put(port, "pipelineService", {"name": "airflow", "serviceType": "Airflow"})
            etl = self.put(port, "pipeline", {"name": "etl", "service": "airflow", "sourceUrl": "https://airflow.test/etl",
                                              "tasks": [{"name": "extract", "fullyQualifiedName": "airflow.etl.extract",
                                                         "downstreamTasks": ["load"], "taskType": "PythonOperator"},
                                                        {"name": "load"}]})
            self.put(port, "messagingService", {"name": "kafka", "serviceType": "Kafka",
                                                "connection": {"config": {"type": "Kafka", "bootstrapServers": "kafka:9092"}}})
            topic = self.put(port, "topic", {"name": "orders.events", "service": "kafka", "partitions": 3,
                                             "cleanupPolicies": ["delete"], "retentionTime": 604800000.0,
                                             "messageSchema": {"schemaType": "JSON", "schemaText": "{}",
                                                               "schemaFields": [{"name": "id", "dataType": "INT"}]}})
            details = {"pipeline": {"id": etl["id"], "type": "pipeline"}, "sqlQuery": "INSERT INTO summary SELECT 1",
                       "columnsLineage": [{"fromColumns": [f"{ORDERS}.id"], "toColumn": f"{TOPIC}.id"}]}
            for source, target, extra in ((orders, topic, {"description": "CDC stream", "lineageDetails": details}),
                                          (orders, summary, {}), (legacy, summary, {})):
                edge = {"fromEntity": {"id": source["id"], "type": "table"},
                        "toEntity": {"id": target["id"], "type": "topic" if target is topic else "table"}, **extra}
                self.call(port, "PUT", "lineage", {"edge": edge})
            self.call(port, "PATCH", f"databases/name/{quote('mysql_prod.shop', safe='')}",
                      [{"op": "replace", "path": "/description", "value": "Shop database"}])
            self.call(port, "DELETE", f"tables/{legacy['id']}")

    def entity(self, port, kind, fqn, include="non-deleted"):
        path = f"{COLLECTIONS[kind]}/name/{quote(fqn, safe='')}?fields=*&include={include}"
        return self.call(port, "GET", path, headers=self.auth)

    def lineage(self, port, kind, fqn):
        graph = self.call(port, "GET", f"lineage/{kind}/name/{quote(fqn, safe='')}?upstreamDepth=3&downstreamDepth=3",
                          headers=self.auth)
        names = {node["id"]: node["fullyQualifiedName"] for node in graph["nodes"] + [graph["entity"]]}

        def edges(key):
            result = []
            for edge in graph[key]:
                edge = {**edge, "fromEntity": names[edge["fromEntity"]], "toEntity": names[edge["toEntity"]]}
                if "pipeline" in edge.get("lineageDetails", {}):
                    pipeline = self.call(port, "GET", f"pipelines/{edge['lineageDetails']['pipeline']['id']}", headers=self.auth)
                    edge["lineageDetails"] = {**edge["lineageDetails"], "pipeline": pipeline["fullyQualifiedName"]}
                result.append(edge)
            return sorted(result, key=json.dumps)

        return sorted(names.values()), edges("upstreamEdges"), edges("downstreamEdges")

    def catalog(self, port):
        state = {f"{kind} {fqn}": normalized(self.entity(port, kind, fqn)) for kind, fqn in ENTITIES}
        state["lineage"] = [self.lineage(port, "table", ORDERS), self.lineage(port, "table", SUMMARY),
                            self.lineage(port, "topic", TOPIC)]
        return state

    def test_export_writes_create_requests_in_dependency_order(self):
        self.populate()
        fingerprint = hashlib.sha256(self.source.read_bytes()).hexdigest()
        output = self.directory / "export"
        result = self.run_script("export", "--db-file", self.source, "--out", output)
        self.assertIn("Exported 11 entities and 2 lineage edges", result.stdout)
        self.assertIn("dropped [followers] from 1 table entities", result.stderr)
        self.assertIn("skipped 1 soft-deleted entities", result.stderr)
        self.assertIn("skipped 1 lineage edges", result.stderr)
        self.assertIn("earlier entity versions are not migratable", result.stderr)
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["skipped"], {"deletedEntities": 1, "orphans": 0, "lineageEdges": 1, "versionHistory": 2})
        requests = manifest["requests"]
        self.assertEqual([(entry.get("entityType"), entry.get("fullyQualifiedName")) for entry in requests],
                         ENTITIES + [(None, None)] * 2)
        self.assertEqual({entry["method"] for entry in requests}, {"PUT"})
        self.assertEqual(requests[5]["path"], "v1/databases")
        self.assertEqual(requests[-1]["path"], "v1/lineage")
        self.assertEqual(sorted(path.name for path in (output / "requests").iterdir()),
                         sorted(Path(entry["body"]).name for entry in requests))
        bodies = [json.loads((output / entry["body"]).read_text(encoding="utf-8")) for entry in requests]

        for body in bodies[:-2]:
            self.assertFalse({"id", "href", "version", "updatedAt", "updatedBy", "fullyQualifiedName", "deleted"} & set(body))
        self.assertEqual(bodies[2]["owners"], [{"type": "user", "fullyQualifiedName": "alice"}])
        self.assertEqual(bodies[2]["serviceType"], "Mysql")
        self.assertEqual(bodies[5], {"name": "shop", "service": "mysql_prod", "description": "Shop database",
                                     "owners": [{"type": "user", "fullyQualifiedName": '"john.doe"'}], "tags": [TAG]})
        self.assertEqual(bodies[6], {"name": "public", "database": "mysql_prod.shop"})
        orders = bodies[8]
        self.assertEqual(orders["databaseSchema"], "mysql_prod.shop.public")
        self.assertNotIn("followers", orders)
        self.assertEqual(orders["columns"][1]["children"], [{"name": "name", "dataType": "STRING", "tags": [TAG]}])
        self.assertNotIn("fullyQualifiedName", orders["columns"][1])
        self.assertEqual(orders["columns"][2], {"name": "amount", "dataType": "DECIMAL", "precision": 12, "scale": 2})
        self.assertEqual(bodies[7]["name"], "daily.summary")
        self.assertEqual(bodies[9]["tasks"][0], {"name": "extract", "downstreamTasks": ["load"], "taskType": "PythonOperator"})
        self.assertEqual(bodies[10]["service"], "kafka")
        self.assertIs(type(bodies[10]["retentionTime"]), float)
        self.assertEqual(bodies[-2], {"edge": {
            "fromEntity": {"type": "table", "fullyQualifiedName": ORDERS},
            "toEntity": {"type": "topic", "fullyQualifiedName": TOPIC}, "description": "CDC stream",
            "lineageDetails": {"pipeline": {"type": "pipeline", "fullyQualifiedName": "airflow.etl"},
                               "sqlQuery": "INSERT INTO summary SELECT 1",
                               "columnsLineage": [{"fromColumns": [f"{ORDERS}.id"], "toColumn": f"{TOPIC}.id"}]}}})
        self.assertEqual(bodies[-1], {"edge": {"fromEntity": {"type": "table", "fullyQualifiedName": ORDERS},
                                               "toEntity": {"type": "table", "fullyQualifiedName": SUMMARY}}})
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

        everything = self.directory / "everything"
        self.run_script("export", "--db-file", self.source, "--out", everything, "--include-deleted")
        manifest = json.loads((everything / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["skipped"]["deletedEntities"], 0)
        self.assertEqual(sum(entry["path"] == "v1/lineage" for entry in manifest["requests"]), 3)
        self.assertEqual(manifest["requests"][-1], {"method": "DELETE", "path": "v1/tables/{id}",
                                                    "query": "hardDelete=false&recursive=true",
                                                    "entityType": "table", "fullyQualifiedName": LEGACY})

    def test_migrate_recreates_entities_fqns_and_lineage(self):
        self.populate()
        with self.server(self.source) as port:
            original = self.catalog(port)
            deleted = normalized(self.entity(port, "table", LEGACY, "deleted"))
        original[f"table {ORDERS}"].pop("followers")
        self.assertEqual(len(original["lineage"][0][2]), 2)
        fingerprint = hashlib.sha256(self.source.read_bytes()).hexdigest()

        with self.server(self.directory / "target.sqlite3", "--password-file", str(self.token)) as port:
            url = f"http://127.0.0.1:{port}/api"
            denied = self.run_script("migrate", "--db-file", self.source, "--target", url, check=False)
            self.assertEqual(denied.returncode, 1)
            self.assertIn("HTTP 401", denied.stderr)
            result = self.run_script("migrate", "--db-file", self.source, "--target", url, "--token-file", self.token)
            self.assertIn("Migrated 11 entities and 2 lineage edges", result.stdout)
            self.assertEqual(self.catalog(port), original)
            versions = {key: value.get("version") for key, value in
                        ((f"{kind} {fqn}", self.entity(port, kind, fqn)) for kind, fqn in ENTITIES)}
            self.run_script("migrate", "--db-file", self.source, "--target", url + "/", "--token-file", self.token)
            self.assertEqual({f"{kind} {fqn}": self.entity(port, kind, fqn).get("version") for kind, fqn in ENTITIES}, versions)
            self.assertEqual(self.call(port, "GET", f"tables/name/{quote(LEGACY, safe='')}?include=all", expected=(404,),
                                       headers=self.auth)["code"], 404)

        target = self.directory / "with-deleted.sqlite3"
        with self.server(target) as port:
            result = self.run_script("migrate", "--db-file", self.source, "--target", f"http://127.0.0.1:{port}/api",
                                     "--include-deleted")
            self.assertIn("Migrated 12 entities and 3 lineage edges (soft-deleted 1 again)", result.stdout)
            self.assertEqual(normalized(self.entity(port, "table", LEGACY, "deleted")), deleted)
            self.assertEqual(self.catalog(port), original)
        with contextlib.closing(sqlite3.connect(target)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM metadata_lineage").fetchone(), (3,))
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

    def test_migrate_reports_failures_and_skips_dependents(self):
        self.populate()
        with self.server(self.directory / "conflict.sqlite3") as port:
            self.put(port, "databaseService", {"name": "mysql_prod", "serviceType": "Mysql"})
            shop = self.put(port, "database", {"name": "shop", "service": "mysql_prod"})
            self.call(port, "DELETE", f"databases/{shop['id']}")
            result = self.run_script("migrate", "--db-file", self.source, "--target", f"http://127.0.0.1:{port}/api", check=False)
            self.assertEqual(result.returncode, 1)
            self.assertIn("database mysql_prod.shop: PUT v1/databases: HTTP 409", result.stderr)
            self.assertIn("databaseSchema mysql_prod.shop.public: skipped because database mysql_prod.shop failed", result.stderr)
            self.assertIn(f"table {ORDERS}: skipped because databaseSchema mysql_prod.shop.public failed", result.stderr)
            self.assertIn(f"lineage {ORDERS} -> {TOPIC}: skipped because table {ORDERS} failed", result.stderr)
            self.assertIn("OpenMetadata migrate failed: 6 failures; migrated 7 entities and 0 lineage edges", result.stderr)
            self.entity(port, "topic", TOPIC)

        dangling = self.directory / "dangling.sqlite3"
        with self.server(dangling) as port:
            self.put(port, "messagingService", {"name": "kafka", "serviceType": "Kafka",
                                                "owners": [{"id": str(uuid.uuid4()), "type": "team"}]})
        output = self.directory / "dangling"
        result = self.run_script("export", "--db-file", dangling, "--out", output)
        self.assertIn("messagingService kafka: owner", result.stderr)
        self.assertIn("is not an exported entity; dropped", result.stderr)
        body = json.loads((output / "requests" / "00001-messagingService.json").read_text(encoding="utf-8"))
        self.assertEqual(body, {"name": "kafka", "serviceType": "Kafka", "owners": []})

        used = self.directory / "used"
        used.mkdir()
        result = self.run_script("export", "--db-file", self.source, "--out", used, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(list(used.iterdir()), [])
        other = self.directory / "other.sqlite3"
        with contextlib.closing(sqlite3.connect(other)) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        for source in (other, self.directory / "missing.sqlite3"):
            result = self.run_script("export", "--db-file", source, "--out", self.directory / "invalid", check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("OpenMetadata export failed:"), result.stderr)
            self.assertFalse((self.directory / "invalid").exists())
        self.assertIn("OpenMetadata test server SQLite persistence file",
                      self.run_script("export", "--db-file", other, "--out", self.directory / "invalid", check=False).stderr)
        self.assertEqual(list(self.directory.glob(".openmetadata-export-*")), [])
        self.assertFalse((self.directory / "missing.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
