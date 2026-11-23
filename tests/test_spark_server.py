import contextlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from iceberg_server import IcebergCatalog, IcebergFiles
from spark_server import LOCAL_MAIN_CLASS, SparkHTTPServer, SparkService, load_jobs


BASE = "/v1/submissions"
SCHEMA = {"value": "long"}


def register(directory, jobs):
    path = directory / "jobs.json"
    path.write_text(json.dumps({"jobs": jobs}), encoding="utf-8")
    return load_jobs(path)


def local_job(path, sql="SELECT 1 AS value", **output_options):
    return {"sql": sql, "output": {"format": "jsonl", "path": path, "schema": SCHEMA, **output_options}}


def request(root, path, method="GET", body=None, token=None, status=200):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    payload = None if body is None else json.dumps(body).encode()
    try:
        response = urlopen(Request(root + path, method=method, data=payload, headers=headers), timeout=2)
    except HTTPError as exc:
        response = exc
    with response:
        data = json.load(response)
        if response.status != status:
            raise AssertionError((response.status, status, data))
        return data


def submission(job, **overrides):
    return {"action": "CreateSubmissionRequest", "appResource": job, "mainClass": LOCAL_MAIN_CLASS,
            "appArgs": [], "environmentVariables": {}, "sparkProperties": {"spark.app.name": job},
            "clientSparkVersion": "4.1.1", **overrides}


def submit(root, job, token=None):
    result = request(root, BASE + "/create", "POST", submission(job), token)
    if not result["success"] or result["action"] != "CreateSubmissionResponse":
        raise AssertionError(result)
    return result["submissionId"]


def status(root, identifier, token=None):
    return request(root, BASE + "/status/" + identifier, token=token)


def wait_state(root, identifier, states, *, token=None, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = status(root, identifier, token)
        if current.get("driverState") in states:
            return current
        time.sleep(0.025)
    raise AssertionError(f"Expected {states!r}, received {current!r}")


@contextlib.contextmanager
def serve(jobs, database=None, token=None):
    service = SparkService(jobs, database)
    try:
        with SparkHTTPServer(("127.0.0.1", 0), service, token=token) as server:
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.025})
            service.start()
            thread.start()
            try:
                yield f"http://127.0.0.1:{server.server_port}"
            finally:
                server.shutdown()
                thread.join()
    finally:
        service.close()


class SparkTests(unittest.TestCase):
    def test_official_rest_shapes_and_typed_join(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            (directory / "rows.csv").write_text("amount;label\n4;one\n3;one\n;two\n8;two\n", encoding="utf-8")
            (directory / "flags.jsonl").write_text('{"label":"one","kept":true}\n{"label":"two","kept":false}\n',
                                                  encoding="utf-8")
            query = """
                SELECT data.label, SUM(amount) AS total, kept AS enabled
                FROM data JOIN flags ON data.label=flags.label
                GROUP BY data.label, kept ORDER BY data.label
            """
            jobs = register(directory, {"rollup": {
                "sql": query,
                "inputs": {"data": {"format": "csv", "path": "rows.csv", "delimiter": ";",
                                    "schema": {"label": "string", "amount": "long"}},
                           "flags": {"format": "jsonl", "path": "flags.jsonl",
                                     "schema": {"label": "string", "kept": "boolean"}}},
                "output": {"format": "jsonl", "path": "result.jsonl",
                           "schema": {"label": "string", "total": "long", "enabled": "boolean"}},
            }})
            token = "spark-test-token"
            with serve(jobs, directory / "spark.sqlite3", token) as root:
                unauthenticated = request(root, BASE + "/status/unregistered", status=401)
                self.assertEqual("ErrorResponse", unauthenticated["action"])
                negotiation = request(root, "/v2/submissions/status/driver-1", token=token, status=468)
                self.assertEqual("v1", negotiation["highestProtocolVersion"])
                for body in (submission("rollup", sql="SELECT 99"), submission("rollup", appArgs=["outside"]),
                             submission("rollup", environmentVariables={"ENV": "outside"}),
                             submission("rollup", mainClass="other.Main")):
                    self.assertEqual("ErrorResponse", request(root, BASE + "/create", "POST", body, token, status=400)["action"])
                unregistered = request(root, BASE + "/create", "POST", submission("missing"), token)
                self.assertFalse(unregistered["success"])
                self.assertFalse(status(root, "unregistered", token)["success"])
                identifier = submit(root, "rollup", token)
                self.assertRegex(identifier, r"^driver-\d{14}-\d{4,}$")
                done = wait_state(root, identifier, {"FINISHED", "FAILED"}, token=token)
                self.assertEqual("FINISHED", done["driverState"], done)
                self.assertEqual("SubmissionStatusResponse", done["action"])
                self.assertEqual("4.1.1", done["serverSparkVersion"])
                kill = request(root, BASE + "/kill/" + identifier, "POST", token=token)
                self.assertEqual(("KillSubmissionResponse", False), (kill["action"], kill["success"]))
            rows = [json.loads(line) for line in (directory / "result.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([{"label": "one", "total": 7, "enabled": True},
                              {"label": "two", "total": 8, "enabled": False}], rows)

    def test_cancel_queued_and_running_without_publication(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            query = """WITH RECURSIVE series(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM series WHERE n<1000000000)
                       SELECT SUM(n) AS value FROM series"""
            jobs = register(directory, {"slow": local_job("slow.jsonl", query), "next": local_job("next.jsonl")})
            with serve(jobs) as root:
                slow = submit(root, "slow")
                wait_state(root, slow, {"RUNNING"})
                queued = submit(root, "next")
                self.assertEqual("SUBMITTED", status(root, queued)["driverState"])
                for identifier in (queued, slow):
                    reply = request(root, BASE + "/kill/" + identifier, "POST")
                    self.assertTrue(reply["success"], reply)
                    wait_state(root, identifier, {"KILLED"})
            self.assertFalse(list(directory.glob("*.jsonl")))
            self.assertFalse(list(directory.glob(".jjap-spark-*")))

    def test_kill_refused_once_publication_starts(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            jobs = register(directory, {"publish": local_job("published.jsonl")})
            publishing = threading.Event()
            proceed = threading.Event()
            link = os.link

            def pause_link(source, target):
                publishing.set()
                if not proceed.wait(3):
                    raise OSError("Publication gate timed out")
                link(source, target)

            with patch("spark_server.os.link", side_effect=pause_link), serve(jobs) as root:
                identifier = submit(root, "publish")
                try:
                    self.assertTrue(publishing.wait(3))
                    result = request(root, BASE + "/kill/" + identifier, "POST")
                    self.assertFalse(result["success"])
                finally:
                    proceed.set()
                wait_state(root, identifier, {"FINISHED"})
            self.assertEqual({"value": 1}, json.loads((directory / "published.jsonl").read_text(encoding="utf-8")))

    def test_restart_recovers_queue_and_refuses_changed_job(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            database = directory / "spark.sqlite3"
            config = {name: local_job(name + ".jsonl") for name in ("interrupted", "queued", "changed")}
            jobs = register(directory, config)
            service = SparkService(jobs, database)
            try:
                with self.assertRaisesRegex(ValueError, "scheduler lock"):
                    SparkService(jobs, database)
                interrupted = service.submit("interrupted")["submissionId"]
                queued = service.submit("queued")["submissionId"]
                changed = service.submit("changed")["submissionId"]
                self.assertEqual(interrupted, service._claim().identifier)
            finally:
                service.close()
            jobs = register(directory, {**config, "changed": local_job("other.jsonl")})
            with serve(jobs, database) as root:
                self.assertEqual("ERROR", status(root, interrupted)["driverState"])
                wait_state(root, queued, {"FINISHED"})
                recovered = wait_state(root, changed, {"ERROR"})
                self.assertIn("Registered job changed", recovered["message"])
            self.assertTrue((directory / "queued.jsonl").exists())
            self.assertFalse((directory / "interrupted.jsonl").exists())
            self.assertFalse((directory / "other.jsonl").exists())

    def test_existing_output_and_read_only_query(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            destination = directory / "output.jsonl"
            destination.write_text("existing bytes", encoding="utf-8")
            jobs = register(directory, {"protected": local_job("output.jsonl"),
                                        "overwrite": local_job("output.jsonl", "SELECT 2 AS value", overwrite=True),
                                        "mutating": local_job("mutating.jsonl", "CREATE TABLE forbidden(value INTEGER)"),
                                        "csv": {"sql": "SELECT 1 AS flag, 2.5 AS amount, NULL AS blank",
                                                "output": {"format": "csv", "path": "result.csv", "delimiter": ";",
                                                           "schema": {"amount": "double", "flag": "boolean",
                                                                      "blank": "int"}}}})
            with serve(jobs) as root:
                wait_state(root, submit(root, "protected"), {"FAILED"})
                self.assertEqual("existing bytes", destination.read_text(encoding="utf-8"))
                result = wait_state(root, submit(root, "mutating"), {"FAILED"})
                self.assertIn("authorized", result["message"])
                wait_state(root, submit(root, "overwrite"), {"FINISHED"})
                wait_state(root, submit(root, "csv"), {"FINISHED"})
            self.assertFalse((directory / "mutating.jsonl").exists())
            self.assertEqual({"value": 2}, json.loads(destination.read_text(encoding="utf-8")))
            self.assertEqual("amount;flag;blank\n2.5;true;\n", (directory / "result.csv").read_text(encoding="utf-8"))

    def test_query_timeout_releases_worker(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            query = """WITH RECURSIVE series(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM series WHERE n<1000000000)
                       SELECT SUM(n) AS value FROM series"""
            jobs = register(directory, {"slow": {**local_job("slow.jsonl", query), "timeout_seconds": 0.05},
                                        "next": local_job("next.jsonl")})
            with serve(jobs) as root:
                slow = submit(root, "slow")
                following = submit(root, "next")
                result = wait_state(root, slow, {"FAILED"})
                self.assertIn("timed out", result["message"])
                wait_state(root, following, {"FINISHED"})
            self.assertFalse((directory / "slow.jsonl").exists())

    def test_iceberg_snapshot_scan_and_append(self):
        with tempfile.TemporaryDirectory() as path:
            directory = Path(path)
            catalog = IcebergCatalog(directory / "warehouse")
            catalog.create_namespace(("demo",))
            schema = {"type": "struct", "schema-id": 0,
                      "fields": [{"id": 1, "name": "value", "required": False, "type": "long"}]}
            for name in ("source", "destination"):
                catalog.create_table(("demo",), name, schema)
            files = IcebergFiles(catalog)
            snapshot = files.append(("demo",), "source", [{"value": 2}, {"value": 4}])
            files.append(("demo",), "source", [{"value": 100}])
            relation = {"format": "iceberg", "warehouse": "warehouse", "schema": SCHEMA}
            jobs = register(directory, {"rollup": {
                "inputs": {"source": {**relation, "table": "demo.source", "snapshot_id": snapshot["snapshot-id"]}},
                "sql": "SELECT SUM(value) AS value FROM source",
                "output": {**relation, "table": "demo.destination"},
            }})
            with serve(jobs) as root:
                result = wait_state(root, submit(root, "rollup"), {"FINISHED", "ERROR", "FAILED"})
                self.assertEqual("FINISHED", result["driverState"], result)
            self.assertEqual([{"value": 6}], list(files.scan(("demo",), "destination")))


if __name__ == "__main__":
    unittest.main()
