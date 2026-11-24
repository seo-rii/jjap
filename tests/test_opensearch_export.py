"""Export OpenSearch indexes as REST request bodies and migrate them into a second server."""

import base64
import contextlib
import gzip
import hashlib
import http.client
import http.server
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


SCRIPT = Path(__file__).resolve().parents[1] / "opensearch_server.py"
ENV = {key: value for key, value in os.environ.items() if not key.lower().endswith("_proxy")}
PRODUCTS = {
    "settings": {"number_of_replicas": 0, "refresh_interval": "-1", "index": {
        "max_result_window": 20000,
        "analysis": {"analyzer": {"folded": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}}},
    }},
    "mappings": {"properties": {
        "name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
        "sku": {"type": "keyword"}, "price": {"type": "double"}, "stock": {"type": "integer"},
        "released": {"type": "date"}, "tags": {"type": "keyword"},
    }},
    "aliases": {"cheap": {"filter": {"range": {"price": {"lt": 10}}}, "routing": "1"}, "catalog": {}},
}
DOCUMENTS = {
    "1": {"name": "Blue Mug", "sku": "MUG-1", "price": 7.5, "stock": 3, "tags": ["kitchen", "blue"], "released": "2025-12-01"},
    "sku/π": {"name": "Ünïcode Plate ☕", "sku": "PLATE", "price": 12.25, "stock": 0, "tags": ["kitchen"],
              "specs": {"diameter_mm": 270, "dishwasher": True, "ratio": 0.1, "huge": 9223372036854775807, "note": None}},
    "gone": {"name": "Removed", "price": 1.0},
    "3": {"name": "Red Mug", "sku": "MUG-2", "price": 8, "stock": 10, "tags": ["kitchen", "red"]},
}


def request(port, method, path, body=None, headers=None):
    if body is not None and not isinstance(body, bytes):
        body = json.dumps(body, ensure_ascii=False).encode()
        headers = {"Content-Type": "application/json", **(headers or {})}
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    with contextlib.closing(connection):
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if payload else None


def bulk_body(*operations):
    return "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in operations).encode()


class OpenSearchExportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-opensearch-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("search #?.sqlite3" if os.name != "nt" else "search #.sqlite3")

    def run_script(self, *args, check=True):
        return subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)], cwd=self.directory,
                              env=ENV, check=check, text=True, capture_output=True, timeout=60)

    @contextlib.contextmanager
    def server(self, db_file):
        process = subprocess.Popen([sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0",
                                    "--db-file", str(db_file)], cwd=self.directory, env=ENV,
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

    def call(self, port, method, path, body=None, expected=200):
        status, payload = request(port, method, path, body,
                                  {"Content-Type": "application/x-ndjson"} if isinstance(body, bytes) else None)
        self.assertEqual(status, expected, payload)
        return payload

    def populate(self):
        with self.server(self.source) as port:
            self.call(port, "PUT", "/products", PRODUCTS)
            self.call(port, "PUT", "/logs-2025.12", {"aliases": {"logs": {"is_write_index": False}}})
            self.call(port, "PUT", "/logs-2026.01", {"aliases": {"logs": {"is_write_index": True, "search_routing": "a,b"}}})
            self.call(port, "PUT", "/vide-%C3%A9", {"settings": {"number_of_replicas": 0}})
            operations = []
            for doc_id, source in DOCUMENTS.items():
                operations += [{"index": {"_index": "products", "_id": doc_id}}, source]
            result = self.call(port, "POST", "/_bulk", bulk_body(*operations))
            self.assertFalse(result["errors"], result)
            self.call(port, "POST", "/products/_refresh")
            self.call(port, "POST", "/products/_update/1", {"doc": {"stock": 2}})
            self.call(port, "POST", "/products/_update/1", {"doc": {"tags": ["kitchen", "blue", "sale"]}})
            self.call(port, "DELETE", "/products/_doc/gone")
            self.call(port, "PUT", "/products/_doc/late", {"name": "Late Bowl", "price": 3.0, "added": {"dynamic": "yes"}},
                      expected=201)
            self.call(port, "POST", "/logs/_doc", {"message": "written through the write alias", "level": 3}, expected=201)
            self.call(port, "POST", "/logs-2025.12/_doc/old?refresh=true", {"message": "december"}, expected=201)
            hidden = self.call(port, "GET", "/products/_count")
            self.assertEqual(hidden["count"], 4)
            self.assertEqual(self.call(port, "GET", "/products/_doc/1")["_version"], 3)

    def state(self, port, names):
        def without_ids(settings):
            for value in settings.values():
                value["settings"]["index"].pop("uuid")
                value["settings"]["index"].pop("provided_name")
            return settings

        queries = [
            {"query": {"match": {"name": "mug"}}, "sort": [{"price": "desc"}]},
            {"query": {"term": {"tags": "kitchen"}}, "aggs": {"tags": {"terms": {"field": "tags"}}, "price": {"stats": {"field": "price"}}}},
            {"query": {"range": {"specs.huge": {"gte": 1}}}},
            {"query": {"match_all": {}}, "sort": ["_id"], "size": 50, "version": False},
        ]
        result = {
            "aliases": self.call(port, "GET", "/_alias"),
            "mappings": self.call(port, "GET", "/" + ",".join(quote(name, safe="") for name in names) + "/_mapping"),
            "settings": without_ids(self.call(port, "GET", "/" + ",".join(quote(name, safe="") for name in names) + "/_settings")),
            "searches": [],
        }
        for target, body in [("products", query) for query in queries] + [("cheap", queries[3]), ("logs", queries[3])]:
            response = self.call(port, "POST", f"/{target}/_search", body)
            response.pop("took")
            result["searches"].append(response)
        return result

    def documents(self, port, index, ids):
        response = self.call(port, "POST", f"/{quote(index, safe='')}/_mget", {"ids": ids})
        return {item["_id"]: item.get("_source") for item in response["docs"]}

    def test_export_writes_portable_bodies_and_live_bulk_documents(self):
        self.populate()
        fingerprint = hashlib.sha256(self.source.read_bytes()).hexdigest()
        output = self.directory / "export"
        result = self.run_script("export", "--db-file", self.source, "--out", output)
        self.assertIn("Exported 6 documents across 4 indexes", result.stdout)
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual([entry["index"] for entry in manifest["indexes"]], ["logs-2025.12", "logs-2026.01", "products", "vide-é"])
        self.assertEqual(sorted(path.name for path in output.iterdir()),
                         sorted(["manifest.json"] + [entry[key] for entry in manifest["indexes"] for key in ("create", "bulk")]))
        entry = manifest["indexes"][2]
        self.assertEqual((entry["documents"], entry["aliases"]), (4, ["catalog", "cheap"]))
        self.assertEqual(manifest["indexes"][3]["create"], "0004-vide-%C3%A9.index.json")

        body = json.loads((output / entry["create"]).read_text(encoding="utf-8"))
        self.assertEqual(set(body), {"settings", "mappings", "aliases"})
        self.assertEqual(body["settings"]["index"], {
            "number_of_replicas": "0", "refresh_interval": "-1", "number_of_shards": "1", "max_result_window": 20000,
            "analysis": PRODUCTS["settings"]["index"]["analysis"]})
        self.assertEqual(body["aliases"], PRODUCTS["aliases"])
        properties = body["mappings"]["properties"]
        self.assertEqual(properties["name"], PRODUCTS["mappings"]["properties"]["name"])
        self.assertEqual(properties["added"], {"properties": {"dynamic": {"type": "text", "fields": {
            "keyword": {"type": "keyword", "ignore_above": 256}}}}})
        logs = json.loads((output / manifest["indexes"][1]["create"]).read_text(encoding="utf-8"))
        self.assertEqual(logs["aliases"], {"logs": {"is_write_index": True, "search_routing": "a,b"}})

        lines = (output / entry["bulk"]).read_bytes().split(b"\n")
        self.assertEqual(lines[-1], b"")
        pairs = [(json.loads(lines[index]), json.loads(lines[index + 1])) for index in range(0, len(lines) - 1, 2)]
        self.assertEqual([action["index"]["_id"] for action, _ in pairs], ["sku/π", "3", "1", "late"])
        self.assertEqual([set(action["index"]) for action, _ in pairs], [{"_index", "_id"}] * 4)
        sources = dict((action["index"]["_id"], source) for action, source in pairs)
        self.assertEqual(sources["sku/π"], DOCUMENTS["sku/π"])
        self.assertIs(type(sources["3"]["price"]), int)
        self.assertEqual(sources["1"]["tags"], ["kitchen", "blue", "sale"])
        self.assertEqual(sources["1"]["stock"], 2)
        self.assertEqual((output / manifest["indexes"][3]["bulk"]).read_bytes(), b"")
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

        versions = self.directory / "versions"
        self.run_script("export", "--db-file", self.source, "--out", versions, "--preserve-versions")
        action = json.loads((versions / entry["bulk"]).read_text(encoding="utf-8").splitlines()[4])
        self.assertEqual(action, {"index": {"_index": "products", "_id": "1", "version": 3, "version_type": "external"}})

    def test_migrate_reproduces_documents_mappings_aliases_and_searches(self):
        self.populate()
        names = ["logs-2025.12", "logs-2026.01", "products", "vide-é"]
        with self.server(self.source) as port:
            live = {name: self.documents(port, name, ["1", "sku/π", "3", "late", "gone", "old"]) for name in names}
            self.call(port, "POST", "/_refresh")
            original = self.state(port, names)
        self.assertEqual(len(original["searches"][3]["hits"]["hits"]), 4)
        fingerprint = hashlib.sha256(self.source.read_bytes()).hexdigest()

        with self.server(self.directory / "target.sqlite3") as port:
            url = f"http://127.0.0.1:{port}"
            result = self.run_script("migrate", "--db-file", self.source, "--target", url, "--chunk-docs", "2", "--gzip")
            self.assertIn("Migrated 6 documents into 4 indexes with 4 aliases", result.stdout)
            self.assertEqual({name: self.documents(port, name, ["1", "sku/π", "3", "late", "gone", "old"]) for name in names}, live)
            self.assertEqual(self.state(port, names), original)
            written = self.call(port, "POST", "/logs/_doc", {"message": "after migration"}, expected=201)
            self.assertEqual(written["_index"], "logs-2026.01")

            refused = self.run_script("migrate", "--db-file", self.source, "--target", url, check=False)
            self.assertEqual(refused.returncode, 1)
            self.assertIn("already has logs-2025.12, logs-2026.01, products, vide-é; use --skip-existing", refused.stderr)
            skipped = self.run_script("migrate", "--db-file", self.source, "--target", url, "--skip-existing")
            self.assertIn("Migrated 0 documents into 0 indexes", skipped.stdout)
            self.assertIn("[products] already exists on the target; skipped", skipped.stderr)
            self.assertEqual(self.call(port, "GET", "/products/_count")["count"], 4)

        with self.server(self.directory / "partial.sqlite3") as port:
            url = f"http://127.0.0.1:{port}"
            self.run_script("migrate", "--db-file", self.source, "--target", url, "--index", "vide-é", "logs-2025.12")
            self.assertEqual(sorted(self.call(port, "GET", "/_alias")), ["logs-2025.12", "vide-é"])
            missing = self.run_script("migrate", "--db-file", self.source, "--target", url, "--index", "nope", check=False)
            self.assertIn("Index not found in the snapshot: nope", missing.stderr)

        with self.server(self.directory / "versions.sqlite3") as port:
            rejected = self.run_script("migrate", "--db-file", self.source, "--target", f"http://127.0.0.1:{port}",
                                       "--preserve-versions", check=False)
            self.assertEqual(rejected.returncode, 1)
            self.assertIn("document [products][sku/π]: HTTP 400 illegal_argument_exception: external and explicit versions", rejected.stderr)
            self.assertIn("OpenSearch migrate failed: 6 failures; migrated 0 documents into 4 indexes", rejected.stderr)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

    def test_rejects_existing_destination_invalid_source_and_bad_credentials(self):
        self.populate()
        output = self.directory / "used"
        output.mkdir()
        (output / "keep.txt").write_text("preserve", encoding="utf-8")
        result = self.run_script("export", "--db-file", self.source, "--out", output, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((output / "keep.txt").read_text(encoding="utf-8"), "preserve")

        other = self.directory / "other.sqlite3"
        with contextlib.closing(sqlite3.connect(other)) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        target = self.directory / "invalid"
        for source in (other, self.directory / "missing.sqlite3"):
            result = self.run_script("export", "--db-file", source, "--out", target, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("OpenSearch export failed:"), result.stderr)
            self.assertFalse(target.exists())
            self.assertEqual(list(self.directory.glob(".opensearch-export-*")), [])
        self.assertIn("OpenSearch test server SQLite persistence file",
                      self.run_script("export", "--db-file", other, "--out", target, check=False).stderr)
        self.assertFalse((self.directory / "missing.sqlite3").exists())

        result = self.run_script("migrate", "--db-file", self.source, "--target", "http://127.0.0.1:9", "--username", "admin",
                                 check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--username and --password-file must be used together", result.stderr)
        result = self.run_script("migrate", "--db-file", self.source, "--target", "http://127.0.0.1:9", "--retries", "0",
                                 check=False)
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("OpenSearch migrate failed: HEAD http://127.0.0.1:9/"), result.stderr)

    def test_basic_auth_header_and_gzip_bodies_reach_the_cluster(self):
        self.populate()
        password = self.directory / "password"
        password.write_text("s3cret: with spaces\n", encoding="utf-8")
        seen = []

        class Cluster(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, payload=None):
                body = json.dumps(payload).encode() if payload is not None else b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_HEAD(self):
                seen.append(("HEAD", self.path, self.headers["Authorization"]))
                self.reply(404)

            def do_PUT(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                seen.append(("PUT", self.path, sorted(body)))
                self.reply(200, {"acknowledged": True})

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/_bulk":
                    lines = gzip.decompress(body).splitlines()
                    seen.append(("BULK", self.headers["Content-Encoding"], len(lines) // 2))
                    items = [{"index": {"status": 201, "_id": json.loads(line)["index"]["_id"]}} for line in lines[::2]]
                    items[-1]["index"] = {"status": 400, "_index": "products", "_id": "late",
                                          "error": {"type": "mapper_parsing_exception", "reason": "bad field"}}
                    self.reply(200, {"errors": True, "items": items})
                else:
                    seen.append(("POST", self.path))
                    self.reply(200, {"acknowledged": True})

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Cluster)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        result = self.run_script("migrate", "--db-file", self.source, "--target", f"http://127.0.0.1:{server.server_port}",
                                 "--username", "admin", "--password-file", password, "--index", "products", "--gzip",
                                 check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("document [products][late]: HTTP 400 mapper_parsing_exception: bad field", result.stderr)
        self.assertIn("1 failures; migrated 3 documents into 1 indexes with 2 aliases", result.stderr)
        self.assertEqual(seen, [
            ("HEAD", "/products", "Basic " + base64.b64encode(b"admin:s3cret: with spaces").decode()),
            ("PUT", "/products", ["mappings", "settings"]), ("BULK", "gzip", 4),
            ("POST", "/_aliases"), ("POST", "/products/_refresh"),
        ])

    def uuids(self, port):
        return {name: value["settings"]["index"]["uuid"] for name, value in self.call(port, "GET", "/_all/_settings").items()}

    def versions(self, port, index, ids):
        response = self.call(port, "POST", f"/{quote(index, safe='')}/_mget", {"ids": ids})
        return {item["_id"]: item.get("_version") for item in response["docs"]}

    def test_import_round_trips_export_output_and_reads_a_live_cluster(self):
        self.populate()
        names = ["logs-2025.12", "logs-2026.01", "products", "vide-é"]
        ids = ["1", "sku/π", "3", "late", "gone", "old"]
        with self.server(self.source) as port:
            self.call(port, "POST", "/_refresh")
            original = self.state(port, names)
            live = {name: self.documents(port, name, ids) for name in names}
            versions = self.versions(port, "products", ids)
            uuids = self.uuids(port)
            source_url = f"http://127.0.0.1:{port}"
            result = self.run_script("import", "--db-file", self.directory / "live.db", "--source", source_url, "--page-size", "1")
            self.assertIn("Imported 6 documents into 4 indexes with 4 aliases to", result.stdout)
            self.assertEqual(self.call(port, "GET", "/_search/point_in_time/_all"), {"pits": []})
            subset = self.run_script("import", "--db-file", self.directory / "subset.db", "--source", source_url,
                                     "--index", "logs-*", "vide-é")
            self.assertIn("Imported 2 documents into 3 indexes with 2 aliases", subset.stdout)
            missing = self.run_script("import", "--db-file", self.directory / "missing.db", "--source", source_url,
                                      "--index", "nope", check=False)
            self.assertIn("OpenSearch import failed: Index not found on the source: nope", missing.stderr)
        self.assertEqual(versions["1"], 3)

        output, plain = self.directory / "export", self.directory / "plain-export"
        self.run_script("export", "--db-file", self.source, "--out", output, "--preserve-versions")
        self.run_script("export", "--db-file", self.source, "--out", plain)
        result = self.run_script("import", "--db-file", self.directory / "files.db", "--in", output)
        self.assertIn("Imported 6 documents into 4 indexes with 4 aliases to", result.stdout)
        self.run_script("import", "--db-file", self.directory / "plain.db", "--in", plain, "--index", "products")
        for db_file in ("live.db", "files.db"):
            with self.subTest(db_file=db_file), self.server(self.directory / db_file) as port:
                self.assertEqual(self.state(port, names), original)
                self.assertEqual({name: self.documents(port, name, ids) for name in names}, live)
                self.assertEqual(self.versions(port, "products", ids), versions)
                if db_file == "live.db":
                    self.assertEqual(self.uuids(port), uuids)
                self.assertEqual(self.call(port, "POST", "/logs/_doc", {"message": "new"}, expected=201)["_index"], "logs-2026.01")
                updated = self.call(port, "POST", "/products/_update/1", {"doc": {"stock": 1}})
                self.assertEqual(updated["_version"], 4)
        with self.server(self.directory / "subset.db") as port:
            self.assertEqual(sorted(self.call(port, "GET", "/_alias")), ["logs-2025.12", "logs-2026.01", "vide-é"])
        with self.server(self.directory / "plain.db") as port:
            self.assertEqual(set(self.versions(port, "products", ["1", "3", "late", "sku/π"]).values()), {1})
            self.assertEqual(self.call(port, "GET", "/products/_count")["count"], 4)

        existing = self.directory / "files.db"
        fingerprint = hashlib.sha256(existing.read_bytes()).hexdigest()
        refused = self.run_script("import", "--db-file", existing, "--in", output, check=False)
        self.assertEqual(refused.returncode, 1)
        self.assertIn("already exists", refused.stderr)
        self.assertEqual(hashlib.sha256(existing.read_bytes()).hexdigest(), fingerprint)
        self.assertFalse((self.directory / "missing.db").exists())
        self.assertEqual(list(self.directory.glob(".opensearch-import-*")), [])

    def test_import_reads_bulk_and_elasticdump_files(self):
        bulk = self.directory / "load.ndjson.gz"
        bulk.write_bytes(gzip.compress(bulk_body(
            {"index": {"_id": "1"}}, {"title": "First", "views": 3},
            {"create": {"_index": "notes", "_id": "2", "routing": "user-7"}}, {"title": "Second", "views": 5},
            {"update": {"_id": "1", "retry_on_conflict": 3}}, {"doc": {"views": 4}},
            {"index": {"_index": "notes", "_id": "3", "version": 42, "version_type": "external"}}, {"title": "Third"},
            {"delete": {"_id": "2"}},
            {"index": {"_index": "other"}}, {"auto": True},
        )))
        body = self.directory / "notes.json"
        body.write_text(json.dumps({"settings": {"number_of_replicas": 0}, "mappings": {"properties": {"views": {"type": "integer"}}},
                                    "aliases": {"all-notes": {}}}), encoding="utf-8")
        result = self.run_script("import", "--db-file", self.directory / "bulk.db", "--bulk", bulk, "--index-body", body,
                                 "--default-index", "notes")
        self.assertIn("Imported 3 documents into 2 indexes with 1 aliases to", result.stdout)
        self.assertIn("[notes]: 1 documents carried a custom routing value", result.stderr)
        with self.server(self.directory / "bulk.db") as port:
            notes = self.call(port, "GET", "/notes")["notes"]
            self.assertEqual(notes["settings"]["index"]["number_of_replicas"], "0")
            self.assertEqual(notes["mappings"]["properties"]["views"], {"type": "integer"})
            self.assertEqual(self.documents(port, "all-notes", ["1", "2", "3"]), {"1": {"title": "First", "views": 4}, "2": None,
                                                                                    "3": {"title": "Third"}})
            self.assertEqual(self.versions(port, "notes", ["1", "3"]), {"1": 2, "3": 42})
            self.assertEqual(self.call(port, "GET", "/other/_count")["count"], 1)

        dump = self.directory / "dump"
        dump.mkdir()
        hits = [{"_index": "events", "_type": "_doc", "_id": str(number), "_score": 1,
                 "_source": {"n": number, "at": f"2026-01-0{number}T00:00:00Z"}} for number in (1, 2, 3)]
        (dump / "data.json").write_text("".join(json.dumps(hit) + "\n" for hit in hits), encoding="utf-8")
        mappings = {"properties": {"at": {"type": "date"}, "n": {"type": "long"}}}
        (dump / "mapping.json").write_text(json.dumps({"events": {"mappings": mappings}}), encoding="utf-8")
        (dump / "settings.json").write_text(json.dumps({"events": {"settings": {"index": {
            "number_of_shards": "3", "number_of_replicas": "1", "uuid": "Zm9vYmFyYmF6", "creation_date": "1767225600000",
            "provided_name": "events", "version": {"created": "136407927"}, "refresh_interval": "5s"}}}}), encoding="utf-8")
        result = self.run_script("import", "--db-file", self.directory / "dump.db", "--bulk", dump / "data.json",
                                 "--index-body", dump / "mapping.json", "--index-body", dump / "settings.json")
        self.assertIn("Imported 3 documents into 1 indexes", result.stdout)
        self.assertIn("[events] has 3 primary shards; imported as one shard", result.stderr)
        with self.server(self.directory / "dump.db") as port:
            settings = self.call(port, "GET", "/events/_settings")["events"]["settings"]["index"]
            self.assertEqual({key: settings[key] for key in ("number_of_shards", "number_of_replicas", "refresh_interval", "uuid")},
                             {"number_of_shards": "1", "number_of_replicas": "1", "refresh_interval": "5s", "uuid": "Zm9vYmFyYmF6"})
            self.assertNotIn("creation_date", settings)
            found = self.call(port, "POST", "/events/_search", {"query": {"range": {"at": {"gte": "2026-01-02"}}}, "sort": ["n"]})
            self.assertEqual([hit["_id"] for hit in found["hits"]["hits"]], ["2", "3"])

        for lines, message in (
            ([{"create": {"_index": "x", "_id": "1"}}, {"a": 1}, {"create": {"_index": "x", "_id": "1"}}, {"a": 2}],
             "line 3: [x][1]: HTTP 409 version_conflict_engine_exception"),
            ([{"index": {"_index": "x", "_id": "1", "pipeline": "geo"}}, {"a": 1}],
             "line 1: [x][1]: HTTP 400 illegal_argument_exception: ingest pipelines"),
            ([{"index": {"_index": "x", "_id": "1", "version": 5, "version_type": "external"}}, {"a": 1},
              {"index": {"_index": "x", "_id": "1", "version": 5, "version_type": "external"}}, {"a": 2}], "line 3: [x][1]: HTTP 409"),
            ([{"index": {"_id": "1"}}, {"a": 1}], "line 1: [None][1]: HTTP 400 illegal_argument_exception: the action names no index"),
            ([{"update": {"_index": "x", "_id": "1", "version": 2, "version_type": "external"}}, {"doc": {"a": 1}}],
             "version type [external] is not supported by the update API"),
            ([{"index": {"_index": "x"}}], "line 1: bulk action [index] requires a source line"),
        ):
            bad = self.directory / "bad.ndjson"
            bad.write_bytes(bulk_body(*lines))
            result = self.run_script("import", "--db-file", self.directory / "bad.db", "--bulk", bad, check=False)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertTrue(result.stderr.startswith("OpenSearch import failed:"), result.stderr)
            self.assertIn(message, result.stderr)
        bad.write_bytes(b'{"index": {"_index": "x"}}\n{"a": \n')
        self.assertIn("bad.ndjson line 2: Invalid JSON content",
                      self.run_script("import", "--db-file", self.directory / "bad.db", "--bulk", bad, check=False).stderr)
        two = self.directory / "two.ndjson"
        two.write_bytes(bulk_body({"index": {"_index": "a"}}, {}, {"index": {"_index": "b"}}, {}))
        self.assertIn("names no index, but the bulk files create both [a] and [b]",
                      self.run_script("import", "--db-file", self.directory / "bad.db", "--bulk", two, "--index-body", body,
                                      check=False).stderr)
        self.assertFalse((self.directory / "bad.db").exists())
        self.assertEqual(list(self.directory.glob(".opensearch-import-*")), [])

    def test_import_falls_back_to_scroll_and_skips_hidden_indexes(self):
        first = [{"_index": "multi", "_id": "a", "_version": 7, "_source": {"v": 1}, "sort": [0]},
                 {"_index": "multi", "_id": "b", "_version": 1, "_routing": "r", "_source": {"v": 2}, "sort": [0]}]
        seen, scrolls = [], {"pages": [first, [{"_index": "multi", "_id": "c", "_version": 2, "_source": {"v": 3}, "sort": [1]}], []]}
        details = {"aliases": {"current": {"is_write_index": True}}, "mappings": {"properties": {"v": {"type": "long"}}},
                   "settings": {"index": {"number_of_shards": "3", "number_of_replicas": "1", "uuid": "bXVsdGk",
                                          "creation_date": "1767225600000", "provided_name": "multi", "version": {"created": "135217827"},
                                          "replication": {"type": "DOCUMENT"}}}}

        class Cluster(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, payload):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def handle_any(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                seen.append((self.command, self.path, body, self.headers["Authorization"]))
                if self.path == "/":
                    return self.reply(200, {"version": {"distribution": "opensearch", "number": "1.3.20"}})
                if self.path.startswith("/_cat/indices"):
                    return self.reply(200, [{"index": "multi", "status": "open", "pri": "3"},
                                            {"index": ".kibana_1", "status": "open", "pri": "1"},
                                            {"index": "hidden-log", "status": "open", "pri": "1"}])
                if self.path == "/multi":
                    return self.reply(200, {"multi": details})
                if self.path == "/hidden-log":
                    return self.reply(200, {"hidden-log": {"settings": {"index": {"hidden": "true"}}}})
                if self.path in ("/multi/_search?scroll=5m", "/_search/scroll") and self.command == "POST":
                    total = {"hits": {"total": {"value": 3, "relation": "eq"}}} if self.path != "/_search/scroll" else {}
                    hits = scrolls["pages"].pop(0)
                    return self.reply(200, {"_scroll_id": f"scroll-{len(scrolls['pages'])}", "_shards": {"total": 3, "successful": 3,
                                            "skipped": 0, "failed": 0}, "hits": {**total.get("hits", {}), "hits": hits}})
                if self.path == "/_search/scroll" and self.command == "DELETE":
                    return self.reply(200, {"succeeded": True, "num_freed": 1})
                self.reply(400, {"error": f"no handler found for uri [{self.path}] and method [{self.command}]", "status": 400})

            do_GET = do_POST = do_DELETE = do_HEAD = handle_any

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Cluster)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        password = self.directory / "password"
        password.write_text("reader-pw\n", encoding="utf-8")
        result = self.run_script("import", "--db-file", self.directory / "scrolled.db", "--source",
                                 f"http://127.0.0.1:{server.server_port}", "--username", "reader", "--password-file", password,
                                 "--page-size", "2")
        self.assertIn("Imported 3 documents into 1 indexes with 1 aliases", result.stdout)
        for warning in ("[multi] has 3 primary shards; imported as one shard", "[multi]: 1 documents carried a custom routing value",
                        "skipped 2 hidden or system indexes"):
            self.assertIn(warning, result.stderr)
        self.assertEqual({entry[3] for entry in seen}, {"Basic " + base64.b64encode(b"reader:reader-pw").decode()})
        self.assertEqual([entry[:3] for entry in seen if "scroll" in entry[1]], [
            ("POST", "/multi/_search?scroll=5m", {"size": 2, "sort": ["_doc"], "version": True, "track_total_hits": True,
                                                  "query": {"match_all": {}}}),
            ("POST", "/_search/scroll", {"scroll": "5m", "scroll_id": "scroll-2"}),
            ("POST", "/_search/scroll", {"scroll": "5m", "scroll_id": "scroll-1"}),
            ("DELETE", "/_search/scroll", {"scroll_id": ["scroll-0"]})])
        with self.server(self.directory / "scrolled.db") as port:
            index = self.call(port, "GET", "/multi")["multi"]
            self.assertEqual(index["aliases"], details["aliases"])
            self.assertEqual(index["settings"]["index"]["replication"], {"type": "DOCUMENT"})
            self.assertEqual(index["settings"]["index"]["uuid"], "bXVsdGk")
            self.assertEqual(self.versions(port, "current", ["a", "b", "c"]), {"a": 7, "b": 1, "c": 2})

        scrolls["pages"] = [[{"_index": "multi", "_id": "a", "_source": {"v": 1}}], []]
        short = self.run_script("import", "--db-file", self.directory / "short.db", "--source",
                                f"http://127.0.0.1:{server.server_port}", check=False)
        self.assertIn("OpenSearch import failed: [multi]: read 1 of 3 documents", short.stderr)
        self.assertFalse((self.directory / "short.db").exists())

    def test_reset_and_points_in_time(self):
        target = self.directory / "reset.sqlite3"
        with self.server(target) as port:
            self.call(port, "PUT", "/products", PRODUCTS)
            for doc_id in ("1", "3"):
                self.call(port, "PUT", f"/products/_doc/{doc_id}", DOCUMENTS[doc_id], expected=201)
            self.call(port, "POST", "/products/_refresh")
            pit = self.call(port, "POST", "/catalog/_search/point_in_time?keep_alive=1m")
            self.assertEqual(set(pit), {"pit_id", "_shards", "creation_time"})
            self.call(port, "DELETE", "/products/_doc/3")
            self.call(port, "POST", "/products/_refresh")
            first = self.call(port, "POST", "/_search", {"pit": {"id": pit["pit_id"], "keep_alive": "2m"}, "size": 1,
                                                          "sort": [{"_doc": "asc"}]})
            self.assertEqual((first["pit_id"], [hit["_id"] for hit in first["hits"]["hits"]]), (pit["pit_id"], ["1"]))
            second = self.call(port, "POST", "/_search", {"pit": {"id": pit["pit_id"]}, "size": 1, "sort": [{"_doc": "asc"}],
                                                           "search_after": first["hits"]["hits"][0]["sort"]})
            self.assertEqual([hit["_id"] for hit in second["hits"]["hits"]], ["3"])
            self.assertEqual(self.call(port, "GET", "/products/_count")["count"], 1)
            self.assertEqual(self.call(port, "GET", "/_search/point_in_time/_all")["pits"][0]["keep_alive"], 120000)
            self.call(port, "POST", "/products/_search", {"pit": {"id": pit["pit_id"]}}, expected=400)
            self.call(port, "POST", "/products/_search/point_in_time", expected=400)

            self.call(port, "GET", "/_jjap/reset", expected=405)
            self.assertEqual(self.call(port, "POST", "/_jjap/reset"), {"reset": True})
            self.assertEqual(self.call(port, "GET", "/_cat/indices?format=json"), [])
            self.assertEqual(self.call(port, "GET", "/_alias"), {})
            self.call(port, "POST", "/_search", {"pit": {"id": pit["pit_id"]}}, expected=404)
            self.call(port, "GET", "/catalog/_search", expected=404)
            other = self.call(port, "POST", "/notes/_search/point_in_time?keep_alive=1m", expected=404)
            self.assertEqual(other["error"]["type"], "index_not_found_exception")
        with self.server(target) as port:
            self.assertEqual(self.call(port, "GET", "/_cat/indices?format=json"), [])
            self.call(port, "PUT", "/products", PRODUCTS)
            pit = self.call(port, "POST", "/products/_search/point_in_time?keep_alive=1m")
            self.assertEqual(self.call(port, "DELETE", "/_search/point_in_time", {"pit_id": [pit["pit_id"], "unknown"]}),
                             {"pits": [{"successful": True, "pit_id": pit["pit_id"]}, {"successful": False, "pit_id": "unknown"}]})
            self.call(port, "POST", "/products/_search/point_in_time?keep_alive=1m")
            self.assertEqual(len(self.call(port, "DELETE", "/_search/point_in_time/_all")["pits"]), 1)

    def test_migrate_dry_run_reports_the_plan_and_conflicts(self):
        self.populate()
        with self.server(self.directory / "target.sqlite3") as port:
            url = f"http://127.0.0.1:{port}"
            planned = self.run_script("migrate", "--db-file", self.source, "--target", url, "--dry-run")
            self.assertIn("would create [products] with 4 documents and 2 aliases", planned.stdout)
            self.assertIn(f"Dry run: would migrate 6 documents into 4 indexes with 4 aliases at {url}", planned.stdout)
            self.assertEqual(self.call(port, "GET", "/_cat/indices?format=json"), [])
            self.call(port, "PUT", "/catalog", {})
            self.call(port, "PUT", "/logs-old", {"aliases": {"logs": {"is_write_index": True}}})
            conflicts = self.run_script("migrate", "--db-file", self.source, "--target", url, "--dry-run", check=False)
            real = self.run_script("migrate", "--db-file", self.source, "--target", url, check=False)
            for result in (conflicts, real):
                self.assertEqual(result.returncode, 1)
                self.assertIn("aliases of [logs-2026.01]: ", result.stderr)
                self.assertIn("aliases of [products]: ", result.stderr)
            self.assertIn("alias [logs] already has the write index [logs-old] on the target", conflicts.stderr)
            self.assertIn("[catalog] is an index on the target", conflicts.stderr)
            self.assertIn("OpenSearch migrate failed: dry run found 2 failures; would migrate 6 documents into 4 indexes "
                          "with 1 aliases", conflicts.stderr)
            self.assertIn("OpenSearch migrate failed: 2 failures; migrated 6 documents into 4 indexes with 1 aliases", real.stderr)

            refused = self.run_script("migrate", "--db-file", self.source, "--target", url, "--dry-run", check=False)
            self.assertEqual(refused.returncode, 1)
            self.assertIn("OpenSearch migrate failed: the target already has logs-2025.12, logs-2026.01, products, vide-é; "
                          "use --skip-existing", refused.stderr)
            skipped = self.run_script("migrate", "--db-file", self.source, "--target", url, "--dry-run", "--skip-existing")
            self.assertIn("[products] already exists on the target; skipped", skipped.stderr)
            self.assertIn("Dry run: would migrate 0 documents into 0 indexes with 0 aliases", skipped.stdout)
            self.assertEqual(self.call(port, "GET", "/products/_count")["count"], 4)

if __name__ == "__main__":
    unittest.main()
