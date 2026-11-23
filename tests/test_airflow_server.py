import contextlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from airflow_server import AirflowHTTPServer, AirflowService, normalize_dag, read_dags


def command(code):
    return [sys.executable, "-c", code]


def demo(tasks, **settings):
    spec = normalize_dag({"dag_id": "demo", "tasks": tasks, **settings}, "test-dags.json")
    return {"demo": spec}


@contextlib.contextmanager
def run_http(server):
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


@contextlib.contextmanager
def run_airflow(dags, db_file=None, *, token=None, **options):
    service = AirflowService(dags, db_file, **options)
    try:
        service.start()
        with run_http(AirflowHTTPServer(("127.0.0.1", 0), service, token=token)) as root:
            yield root
    finally:
        service.close()


def call(root, path, method="GET", body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    with urlopen(Request(root + path, method=method, data=data, headers=headers), timeout=2) as response:
        return json.load(response)


def eventually(fn, predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if predicate(value):
            return value
        time.sleep(0.02)
    raise AssertionError(f"Condition did not become true: {value!r}")


class AirflowTests(unittest.TestCase):
    dag_url = "/api/v2/dags/demo"
    runs_url = dag_url + "/dagRuns"

    def test_pause_trigger_then_retry_and_run_dependencies(self):
        dags = demo([
            {"task_id": "finish", "upstream": ["extract"], "command": command("print('finished')")},
            {"task_id": "extract", "retries": 1, "retry_delay_seconds": 0.05,
             "command": command("import os; n = int(os.environ['JJAP_AIRFLOW_TRY_NUMBER']); print(n); raise SystemExit(n == 1)")},
        ], is_paused=True)
        with run_airflow(dags, token="fixture-token") as root:
            with self.assertRaises(HTTPError) as unauthenticated:
                call(root, self.dag_url)
            self.assertEqual(401, unauthenticated.exception.code)
            auth = call(root, "/auth/token", "POST", {"username": "airflow", "password": "fixture-token"})

            def request(path, method="GET", body=None):
                return call(root, path, method, body, auth["access_token"])

            tasks = request(self.dag_url + "/tasks")
            self.assertEqual(["finish"], tasks["tasks"][1]["downstream_task_ids"])
            with self.assertRaises(HTTPError) as override:
                request(self.runs_url, "POST", {"command": command("print('remote code')")})
            self.assertEqual(422, override.exception.code)
            run = request(self.runs_url, "POST", {"dag_run_id": "demo-run", "conf": {"limit": 3}})
            run_url = self.runs_url + "/demo-run"
            self.assertEqual("queued", run["state"])
            self.assertEqual({"limit": 3}, request(run_url)["conf"])
            with self.assertRaises(HTTPError) as duplicate:
                request(self.runs_url, "POST", {"dag_run_id": "demo-run"})
            self.assertEqual(409, duplicate.exception.code)
            request(self.dag_url, "PATCH", {"is_paused": False})
            finished = eventually(lambda: request(run_url), lambda run: run["state"] == "success")
            self.assertIsNotNone(finished["end_date"])
            instances = {item["task_id"]: item for item in request(run_url + "/taskInstances")["task_instances"]}
            self.assertEqual(2, instances["extract"]["try_number"])
            self.assertGreaterEqual(instances["finish"]["start_date"], instances["extract"]["end_date"])
            for attempt in (1, 2):
                log = request(run_url + f"/taskInstances/extract/logs/{attempt}")
                self.assertEqual(str(attempt), log["content"][0])
            runs = request(self.runs_url + "?limit=1&state=success")
            self.assertEqual("demo-run", runs["dag_runs"][0]["dag_run_id"])

    def test_reopens_database_and_marks_interrupted_attempt_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "airflow.sqlite3"
            dags = demo([
                {"task_id": "first", "command": command("print(1)"), "retries": 2},
                {"task_id": "blocked", "command": command("print(2)"), "upstream": ["first"]},
            ])
            service = AirflowService(dags, database)
            try:
                with self.assertRaisesRegex(ValueError, "scheduler lock"):
                    AirflowService(dags, database)
                service.trigger("demo", {"dag_run_id": "interrupted"})
                self.assertEqual(("demo", "interrupted", "first"), service._tick()[0])
            finally:
                service.close()
            with run_airflow(dags, database) as root:
                run_url = self.runs_url + "/interrupted"
                eventually(lambda: call(root, run_url), lambda run: run["state"] == "failed")
                entries = call(root, run_url + "/taskInstances")["task_instances"]
                self.assertEqual([("failed", 1), ("upstream_failed", 0)],
                                 [(task["state"], task["try_number"]) for task in entries])
                log = call(root, run_url + "/taskInstances/first/logs/1")["content"]
                self.assertIn("Scheduler stopped before the task outcome was recorded.", log)

    def test_interval_run_and_openlineage_events(self):
        seen = []

        class LineageReceiver(BaseHTTPRequestHandler):
            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                status = 503 if not seen else 200
                seen.append((status, self.path, self.headers.get("Authorization"), data))
                self.send_response(status)
                self.end_headers()

        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "dag.json"
            config.write_text(json.dumps({
                "dag_id": "demo", "schedule_seconds": 1, "tasks": [{
                    "task_id": "load", "command": command("print('loaded')"),
                    "inputs": [{"namespace": "warehouse", "name": "staging"}],
                    "outputs": [{"namespace": "warehouse", "name": "curated"}],
                }],
            }), encoding="utf-8")
            with run_http(HTTPServer(("127.0.0.1", 0), LineageReceiver)) as lineage_root:
                with run_airflow(read_dags([config]), lineage_url=lineage_root + "/api/v1/lineage",
                                 lineage_token="test-lineage-token") as root:
                    runs = eventually(lambda: call(root, self.runs_url + "?state=success"),
                                      lambda value: value["total_entries"] > 0)
                    self.assertEqual("scheduled", runs["dag_runs"][0]["run_type"])
                    events = eventually(lambda: list(seen), lambda value: len(value) >= 3)
            self.assertEqual([503, 200, 200], [event[0] for event in events[:3]])
            self.assertEqual({("/api/v1/lineage", "Bearer test-lineage-token")}, {event[1:3] for event in events})
            self.assertEqual(events[0][3], events[1][3])
            start, complete = (event[3] for event in events[1:3])
            self.assertEqual(("START", "COMPLETE"), (start["eventType"], complete["eventType"]))
            uuid.UUID(start["run"]["runId"])
            self.assertEqual(start["run"], complete["run"])
            self.assertEqual({"namespace": "airflow", "name": "demo.load"}, complete["job"])
            self.assertEqual([{"namespace": "warehouse", "name": "curated"}], complete["outputs"])

    def test_invalid_dag_dependencies(self):
        for upstream, detail in (("bogus", "unknown upstream"), ("a", "cycle")):
            with self.assertRaisesRegex(ValueError, detail):
                demo([{"task_id": "a", "command": command("pass"), "upstream": [upstream]}])


if __name__ == "__main__":
    unittest.main()
