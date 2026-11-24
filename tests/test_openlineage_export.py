"""Export stored OpenLineage events as NDJSON and replay them into a second collector."""

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
from urllib.parse import quote, urlencode
import uuid


SCRIPT = Path(__file__).resolve().parents[1] / "openlineage_server.py"
PRODUCER = "https://example.test/export-test"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
ENV = {key: value for key, value in os.environ.items() if not key.lower().endswith("_proxy")}


def event(kind, time="2026-01-01T01:00:00Z", **values):
    return {"eventTime": time, "producer": PRODUCER, "schemaURL": f"{SCHEMA}#/$defs/{kind}", **values}


def request(port, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    with contextlib.closing(connection):
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        return response.status, response.read()


class OpenLineageExportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-openlineage-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / ("lineage #?.sqlite3" if os.name != "nt" else "lineage #.sqlite3")

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

    def post(self, port, data, path="/api/v1/lineage", headers=None, compressed=False):
        body = json.dumps(data, ensure_ascii=False).encode()
        headers = {"Content-Type": "application/json", **(headers or {})}
        if compressed:
            body, headers["Content-Encoding"] = gzip.compress(body), "gzip"
        status, payload = request(port, "POST", path, body, headers)
        self.assertIn(status, (200, 204), payload)

    def get(self, port, path, headers=None):
        status, body = request(port, "GET", path, headers=headers)
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def stored_payloads(self):
        with contextlib.closing(sqlite3.connect(f"{self.source.as_uri()}?mode=ro", uri=True)) as connection:
            return [row[0] for row in connection.execute("SELECT payload FROM lineage_events ORDER BY seq")]

    def populate(self):
        first, second = str(uuid.uuid4()), str(uuid.uuid4())
        job = {"namespace": "spark://クラスタ", "name": "nightly.load", "facets": {"jobType": {
            "_producer": PRODUCER, "_schemaURL": SCHEMA, "processingType": "BATCH", "integration": "SPARK", "jobType": "JOB"}}}
        source = {"namespace": "postgres://db:5432", "name": "public.orders", "facets": {"schema": {
            "_producer": PRODUCER, "_schemaURL": SCHEMA, "fields": [{"name": "id", "type": "BIGINT"}, {"name": "note", "type": "TEXT"}]}}}
        target = {"namespace": "s3://bucket", "name": "warehouse/orders", "outputFacets": {"outputStatistics": {
            "_producer": PRODUCER, "_schemaURL": SCHEMA, "rowCount": 12345678901234, "size": 1.5e9}}}
        run_facets = {"nominalTime": {"_producer": PRODUCER, "_schemaURL": SCHEMA, "nominalStartTime": "2026-01-01T00:00:00Z"}}
        complete = event("RunEvent", "2026-01-01T01:00:05.123456789Z", eventType="COMPLETE",
                         run={"runId": first, "facets": run_facets}, job=job, inputs=[source], outputs=[target])
        start = event("RunEvent", "2025-12-31T20:00:00-05:00", eventType="START", run={"runId": first}, job=job, inputs=[source])
        failing = [event("RunEvent", "2026-01-02T00:00:00Z", eventType="START", run={"runId": second}, job=job),
                   event("RunEvent", "2026-01-02T00:00:01Z", eventType="FAIL", run={"runId": second}, job=job)]
        dataset = event("DatasetEvent", "2026-01-03T00:00:00Z", dataset={**source, "facets": {"documentation": {
            "_producer": PRODUCER, "_schemaURL": SCHEMA, "description": "Orders — überprüft"}}})
        planned = event("JobEvent", "2026-01-03T00:00:01Z", job={"namespace": "airflow", "name": "report"}, inputs=[target])
        with self.server(self.source) as port:
            self.post(port, complete, compressed=True)
            self.post(port, start)
            self.post(port, complete)
            self.post(port, failing, "/api/v1/lineage/batch")
            self.post(port, dataset)
            self.post(port, planned)
        return first, second

    def snapshot(self, port, run_ids, headers=None):
        def read(path):
            return self.get(port, path, headers)

        namespaces = read("/api/v1/namespaces")
        state = {"namespaces": namespaces, "runs": {}, "events": {}, "jobs": {}, "datasets": {}, "lineage": {}}
        for namespace in namespaces["namespaces"]:
            name = quote(namespace["name"], safe="")
            for kind in ("jobs", "datasets"):
                for entity in read(f"/api/v1/namespaces/{name}/{kind}")[kind]:
                    path = f"/api/v1/namespaces/{name}/{kind}/{quote(entity['name'], safe='')}"
                    state[kind][namespace["name"], entity["name"]] = read(path)
                    if kind == "jobs":
                        state["jobs"][namespace["name"], entity["name"], "runs"] = read(path + "/runs")
                    node = f"{kind[:-1]}:{namespace['name']}:{entity['name']}"
                    state["lineage"][node] = read("/api/v1/lineage?" + urlencode({"nodeId": node, "depth": 5}))
        for run_id in run_ids:
            state["runs"][run_id] = read(f"/api/v1/runs/{run_id}")
            state["events"][run_id] = read(f"/api/v1/runs/{run_id}/events")
        return state

    def test_export_is_byte_faithful_and_migrate_rebuilds_runs_and_lineage(self):
        run_ids = self.populate()
        stored = self.stored_payloads()
        self.assertEqual(len(stored), 6)
        fingerprint = hashlib.sha256(self.source.read_bytes()).hexdigest()

        plain, packed = self.directory / "events.ndjson", self.directory / "out" / "events.ndjson.gz"
        result = self.run_script("export", "--db-file", self.source, "--out", plain)
        self.assertIn("Exported 6 OpenLineage events", result.stdout)
        self.run_script("export", "--db-file", self.source, "--out", packed)
        expected = "".join(payload + "\n" for payload in stored).encode()
        self.assertEqual(plain.read_bytes(), expected)
        self.assertEqual(gzip.decompress(packed.read_bytes()), expected)
        events = [json.loads(line) for line in plain.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([item.get("eventType") for item in events], ["COMPLETE", "START", "START", "FAIL", None, None])
        self.assertEqual(events[0]["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"], 12345678901234)
        self.assertEqual(events[0]["eventTime"], "2026-01-01T01:00:05.123456789Z")
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

        with self.server(self.source) as port:
            original = self.snapshot(port, run_ids)
        self.assertEqual(original["runs"][run_ids[0]]["state"], "COMPLETED")
        self.assertEqual(original["runs"][run_ids[1]]["state"], "FAILED")

        token = self.directory / "token"
        token.write_text("replay-token\n", encoding="ascii")
        auth = {"Authorization": "Bearer replay-token"}
        for target_db, options in ((self.directory / "single.db", ()), (self.directory / "batch.db", ("--batch-size", "4")),
                                   (self.directory / "endpoint.db", ("--endpoint", "api/v1/lineage/batch"))):
            with self.subTest(options=options), self.server(target_db, "--password-file", str(token)) as port:
                url = f"http://127.0.0.1:{port}"
                denied = self.run_script("migrate", "--db-file", self.source, "--target", url, *options, check=False)
                self.assertEqual(denied.returncode, 1)
                self.assertIn("HTTP 401", denied.stderr)
                result = self.run_script("migrate", "--db-file", self.source, "--target", url, "--token-file", token, *options)
                self.assertIn("Migrated 6 OpenLineage events", result.stdout)
                self.assertEqual(self.snapshot(port, run_ids, auth), original)
                self.run_script("migrate", "--db-file", self.source, "--target", url + "/", "--token-file", token, *options)
            with contextlib.closing(sqlite3.connect(target_db)) as connection:
                replayed = [row[0] for row in connection.execute("SELECT payload FROM lineage_events ORDER BY seq")]
            self.assertEqual(replayed, stored)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), fingerprint)

    def test_migrate_retries_server_errors_and_fails_on_rejections(self):
        self.populate()
        received, failures = [], {"remaining": 2}

        class Collector(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if failures["remaining"]:
                    failures["remaining"] -= 1
                    self.send_response(503)
                elif json.loads(body).get("eventType") == "FAIL":
                    self.send_response(422)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"message":"rejected for the test"}')
                    return
                else:
                    received.append((self.path, self.headers["Content-Type"], body.decode()))
                    self.send_response(201)
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Collector)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/marquez"
        result = self.run_script("migrate", "--db-file", self.source, "--target", url, "--endpoint", "/api/v1/lineage/",
                                 check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("seq 4 rejected: HTTP 422", result.stderr)
        self.assertIn("1 of 6 events were rejected", result.stderr)
        stored = self.stored_payloads()
        self.assertEqual(received, [("/marquez/api/v1/lineage", "application/json", payload)
                                    for index, payload in enumerate(stored) if index != 3])

        server.shutdown()
        server.server_close()
        result = self.run_script("migrate", "--db-file", self.source, "--target", url, "--retries", "1", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("failed after 0 events", result.stderr)

    def test_rejects_existing_destination_and_invalid_source(self):
        self.populate()
        output = self.directory / "used.ndjson"
        output.write_text("preserve", encoding="utf-8")
        result = self.run_script("export", "--db-file", self.source, "--out", output, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(output.read_text(encoding="utf-8"), "preserve")

        other = self.directory / "other.sqlite3"
        with contextlib.closing(sqlite3.connect(other)) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        for source in (other, self.directory / "missing.sqlite3"):
            target = self.directory / "invalid.ndjson.gz"
            result = self.run_script("export", "--db-file", source, "--out", target, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("OpenLineage export failed:"), result.stderr)
            self.assertFalse(target.exists())
            self.assertEqual(list(self.directory.glob(".openlineage-export-*")), [])
        self.assertIn("OpenLineage test server SQLite persistence file",
                      self.run_script("export", "--db-file", other, "--out", target, check=False).stderr)
        self.assertFalse((self.directory / "missing.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
