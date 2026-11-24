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
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
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

    def stored(self, db_file):
        with contextlib.closing(sqlite3.connect(f"{db_file.as_uri()}?mode=ro", uri=True)) as connection:
            return [row[0] for row in connection.execute("SELECT payload FROM lineage_events ORDER BY seq")]

    def test_import_round_trips_export_files_and_reads_marquez_events(self):
        run_ids = self.populate()
        stored = self.stored_payloads()
        with self.server(self.source) as port:
            original = self.snapshot(port, run_ids)
        plain, packed = self.directory / "events.ndjson", self.directory / "events.ndjson.gz"
        self.run_script("export", "--db-file", self.source, "--out", plain)
        self.run_script("export", "--db-file", self.source, "--out", packed)
        for name, source in (("plain.db", plain), ("packed.db", packed)):
            result = self.run_script("import", "--db-file", self.directory / name, "--in", source)
            self.assertIn("Imported 6 OpenLineage events to", result.stdout)
            self.assertEqual(self.stored(self.directory / name), stored)
        with self.server(self.directory / "packed.db") as port:
            self.assertEqual(self.snapshot(port, run_ids), original)

        events = [json.loads(payload) for payload in stored]
        array = self.directory / "array.json"
        array.write_text(json.dumps(events[3:] + events[:3], indent=2, ensure_ascii=False), encoding="utf-8")
        spaced = self.directory / "spaced.ndjson"
        spaced.write_text("\n\n".join(json.dumps(item) for item in events[:4]) + "\n", encoding="utf-8")
        result = self.run_script("import", "--db-file", self.directory / "mixed.db", "--in", array, "--in", spaced)
        self.assertIn("Imported 6 OpenLineage events (4 duplicate events dropped)", result.stdout)
        self.assertEqual(self.stored(self.directory / "mixed.db"), stored[3:] + stored[:3])
        with self.server(self.directory / "mixed.db") as port:
            self.assertEqual(self.snapshot(port, run_ids), original)

        token = self.directory / "token"
        token.write_text("source-token\n", encoding="ascii")
        with self.server(self.source, "--password-file", str(token)) as port:
            url = f"http://127.0.0.1:{port}"
            denied = self.run_script("import", "--db-file", self.directory / "denied.db", "--source", url, check=False)
            self.assertEqual(denied.returncode, 1)
            self.assertIn("OpenLineage import failed: GET", denied.stderr)
            self.assertIn("HTTP 401", denied.stderr)
            result = self.run_script("import", "--db-file", self.directory / "live.db", "--source", url,
                                     "--token-file", token, "--page-size", "1")
            self.assertIn("Imported 4 OpenLineage events to", result.stdout)
            self.assertIn("the source reports 6 events in the time range but returned 4", result.stderr)
            windowed = self.run_script("import", "--db-file", self.directory / "window.db", "--source", url, "--token-file",
                                       token, "--after", "2026-01-02T00:00:00Z", "--before", "2026-01-02T00:00:01Z")
            self.assertIn("Imported 1 OpenLineage events", windowed.stdout)
        run_events = [payload for payload in stored if '"runId"' in payload]
        self.assertEqual(sorted(self.stored(self.directory / "live.db")), sorted(run_events))
        with self.server(self.directory / "live.db") as port:
            live = self.snapshot(port, run_ids)
        self.assertEqual(live["runs"], original["runs"])
        self.assertEqual(live["events"], original["events"])
        self.assertEqual(list(self.directory.glob(".openlineage-import-*")), [])
        self.assertFalse((self.directory / "denied.db").exists())

    def test_events_api_pages_like_marquez(self):
        job = {"namespace": "ties", "name": "job"}
        run_ids = [str(uuid.uuid4()) for _ in range(7)]
        events = [event("RunEvent", "2026-03-01T00:00:00Z" if index < 5 else f"2026-03-0{index - 3}T00:00:00Z",
                        eventType="START", run={"runId": run_id}, job=job) for index, run_id in enumerate(run_ids)]
        events.append(event("JobEvent", "2026-03-01T00:00:00Z", job={"namespace": "ties", "name": "static"}))
        with self.server(self.source) as port:
            self.post(port, events[::-1], "/api/v1/lineage/batch")
            listing = self.get(port, "/api/v1/events/lineage?sortDirection=asc&limit=3&offset=4")
            self.assertEqual(listing["totalCount"], 8)
            self.assertEqual([item["run"]["runId"] for item in listing["events"]], [run_ids[0], run_ids[5], run_ids[6]])
            newest = self.get(port, "/api/v1/events/lineage?limit=1")["events"][0]
            self.assertEqual(newest["run"]["runId"], run_ids[6])
            window = self.get(port, "/api/v1/events/lineage?" + urlencode(
                {"after": "2026-03-02T00:00:00+00:00", "before": "2026-03-03T00:00:00Z", "sortDirection": "ASC"}))
            self.assertEqual(([item["run"]["runId"] for item in window["events"]], window["totalCount"]), ([run_ids[5]], 1))
            self.assertEqual(request(port, "GET", "/api/v1/events/lineage?sortDirection=up")[0], 400)
            self.assertEqual(request(port, "GET", "/api/v1/events/lineage?after=yesterday")[0], 400)
            result = self.run_script("import", "--db-file", self.directory / "ties.db", "--source",
                                     f"http://127.0.0.1:{port}", "--page-size", "2")
        self.assertIn("Imported 7 OpenLineage events", result.stdout)
        self.assertEqual(sorted(json.loads(payload)["run"]["runId"] for payload in self.stored(self.directory / "ties.db")),
                         sorted(run_ids))

    def test_import_pages_marquez_that_caps_limit_and_rejects_unsorted_listings(self):
        job = {"namespace": "fake", "name": "job"}
        stored = [event("RunEvent", f"2026-04-01T00:00:0{index // 3}Z", eventType="START", run={"runId": str(uuid.uuid4())}, job=job)
                  for index in range(7)]
        seen, behavior = [], {"descending": False}

        class Marquez(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                query = dict(parse_qsl(urlsplit(self.path).query))
                seen.append((query["after"], query["offset"]))
                rows = [item for item in stored if query["after"] <= item["eventTime"].replace("Z", ".000000Z") < query["before"]]
                rows = rows[::-1] if behavior["descending"] else rows
                start = int(query["offset"])
                body = json.dumps({"events": rows[start:start + min(int(query["limit"]), 2)], "totalCount": len(rows)}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Marquez)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}"
        result = self.run_script("import", "--db-file", self.directory / "capped.db", "--source", url, "--page-size", "100")
        self.assertIn("Imported 7 OpenLineage events to", result.stdout)
        self.assertEqual(result.stderr, "")
        self.assertEqual(sorted(self.stored(self.directory / "capped.db")),
                         sorted(json.dumps(item, sort_keys=True, separators=(",", ":")) for item in stored))
        self.assertEqual(seen[:3], [("1900-01-01T00:00:00.000000Z", "0"), ("2026-04-01T00:00:00.000000Z", "0"),
                                    ("2026-04-01T00:00:00.000000Z", "2")])
        behavior["descending"] = True
        result = self.run_script("import", "--db-file", self.directory / "unsorted.db", "--source", url, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("OpenLineage import failed: the source ignored sortDirection=asc", result.stderr)
        self.assertFalse((self.directory / "unsorted.db").exists())

    def test_import_reports_invalid_events_and_refuses_existing_files(self):
        start = event("RunEvent", eventType="START", run={"runId": str(uuid.uuid4())}, job={"namespace": "n", "name": "j"})
        lines = [json.dumps(start),
                 json.dumps({"eventTime": "2026-01-01T00:00:00Z", "schemaURL": SCHEMA}), "{not json"]
        for content, message in (("\n".join(lines[:2]), "line 2: producer is required"),
                                 (lines[0] + "\n\n" + lines[2], "line 3: invalid JSON"),
                                 ('"just text"', "line 1: Expected an OpenLineage event object")):
            source = self.directory / "bad.ndjson.gz"
            source.write_bytes(gzip.compress(content.encode()))
            result = self.run_script("import", "--db-file", self.directory / "bad.db", "--in", source, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("OpenLineage import failed:"), result.stderr)
            self.assertIn(message, result.stderr)
            self.assertFalse((self.directory / "bad.db").exists())
        conflict = [json.loads(lines[0]), {**json.loads(lines[0]), "job": {"namespace": "n", "name": "other"}}]
        (self.directory / "conflict.json").write_text(json.dumps(conflict), encoding="utf-8")
        result = self.run_script("import", "--db-file", self.directory / "bad.db", "--in", self.directory / "conflict.json",
                                 check=False)
        self.assertIn("item 1: runId already belongs to a different job", result.stderr)
        existing = self.directory / "existing.db"
        existing.write_bytes(b"keep")
        result = self.run_script("import", "--db-file", existing, "--in", self.directory / "conflict.json", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual(existing.read_bytes(), b"keep")
        self.assertEqual(list(self.directory.glob(".openlineage-import-*")), [])

    def test_reset_empties_the_collector_and_dry_run_posts_nothing(self):
        run_ids = self.populate()
        token = self.directory / "token"
        token.write_text("reset-token\n", encoding="ascii")
        auth = {"Authorization": "Bearer reset-token"}
        target = self.directory / "target.db"
        with self.server(target, "--password-file", str(token)) as port:
            url = f"http://127.0.0.1:{port}"
            refused = self.run_script("migrate", "--db-file", self.source, "--target", url, "--dry-run", check=False)
            self.assertEqual(refused.returncode, 1)
            self.assertIn("dry run: the target refused the credentials: HTTP 401", refused.stderr)
            planned = self.run_script("migrate", "--db-file", self.source, "--target", url, "--token-file", token,
                                      "--batch-size", "4", "--dry-run")
            self.assertIn("would post 6 events in 2 requests", planned.stdout)
            self.assertIn(f"Dry run: would migrate 6 OpenLineage events to {url}/api/v1/lineage/batch", planned.stdout)
            self.assertEqual(self.get(port, "/api/v1/namespaces", auth)["totalCount"], 0)
            self.run_script("migrate", "--db-file", self.source, "--target", url, "--token-file", token)
            again = self.run_script("migrate", "--db-file", self.source, "--target", url, "--token-file", token, "--dry-run")
            self.assertIn("the target already holds 6 events in the same time range", again.stderr)

            self.assertEqual(request(port, "POST", "/_jjap/reset")[0], 401)
            self.assertEqual(request(port, "GET", "/_jjap/reset", headers=auth)[0], 405)
            self.assertEqual(request(port, "DELETE", "/_jjap/reset", headers=auth)[0], 405)
            status, body = request(port, "POST", "/_jjap/reset", headers=auth)
            self.assertEqual((status, json.loads(body)), (200, {"reset": True}))
            self.assertEqual(self.get(port, "/api/v1/namespaces", auth), {"namespaces": [], "totalCount": 0})
            self.assertEqual(request(port, "GET", f"/api/v1/runs/{run_ids[0]}", headers=auth)[0], 404)
        with self.server(target, "--password-file", str(token)) as port:
            self.assertEqual(self.get(port, "/api/v1/events/lineage", auth), {"events": [], "totalCount": 0})
            self.post(port, event("JobEvent", job={"namespace": "after", "name": "reset"}), headers=auth)
            self.assertEqual(self.get(port, "/api/v1/namespaces", auth)["totalCount"], 1)
        with contextlib.closing(sqlite3.connect(target)) as connection:
            self.assertEqual(connection.execute("SELECT seq FROM lineage_events").fetchall(), [(1,)])


if __name__ == "__main__":
    unittest.main()
