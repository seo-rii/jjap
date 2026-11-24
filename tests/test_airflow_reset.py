"""Verify POST /_jjap/reset on the Airflow test server, which runs as a separate process."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


SCRIPT = Path(__file__).resolve().parents[1] / "airflow_server.py"
TOKEN = "reset-token"


class AirflowResetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-airflow-reset-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        (self.directory / "token").write_text(TOKEN + "\n", encoding="ascii")
        pid_file = self.directory / "task.pid"
        dags = [
            {"dag_id": "slow", "tasks": [{"task_id": "sleep", "command": [
                sys.executable, "-c", f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(60)"]}]},
            {"dag_id": "quick", "tasks": [{"task_id": "echo", "command": [sys.executable, "-c", "print('done')"]}]},
            {"dag_id": "hourly", "schedule_seconds": 3600, "is_paused": True,
             "tasks": [{"task_id": "noop", "command": [sys.executable, "-c", "pass"]}]},
        ]
        (self.directory / "dags.json").write_text(json.dumps(dags), encoding="utf-8")
        self.pid_file = pid_file

    def start(self):
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-B", str(self.script), "--port", "0", "--dag-file", "dags.json",
             "--db-file", "airflow.sqlite3", "--password-file", "token"],
            cwd=self.directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        line = process.stdout.readline()
        self.assertIn("listening on http://", line, process.stderr.read() if not line else line)
        self.root = line.split("listening on ", 1)[1].strip().removesuffix("/api/v2")
        return process

    def call(self, path, method="GET", body=None, token=TOKEN):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        data = json.dumps(body).encode() if body is not None else None
        try:
            with urlopen(Request(self.root + path, data=data, method=method, headers=headers), timeout=10) as response:
                return response.status, json.load(response), response.headers
        except HTTPError as error:
            with error:
                return error.code, json.load(error) if error.headers.get("Content-Type") == "application/json" else None, error.headers

    def wait(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while not predicate():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.02)

    def test_reset_kills_the_running_attempt_and_restores_startup_dags(self):
        process = self.start()
        self.assertEqual(self.call("/api/v2/dags/slow/dagRuns", "POST", {"dag_run_id": "before"})[0], 200)
        self.wait(self.pid_file.exists)
        self.wait(lambda: self.pid_file.read_text())
        pid = int(self.pid_file.read_text())
        self.assertEqual(self.call("/api/v2/dags/hourly", "PATCH", {"is_paused": False})[1]["is_paused"], False)
        schedule = self.call("/api/v2/dags/hourly")[1]["next_dagrun_run_after"]

        self.assertEqual(self.call("/_jjap/reset", "POST", token=None)[0], 401)
        self.assertEqual(self.call("/_jjap/reset", "POST", token="wrong")[0], 401)
        for method in ("GET", "PUT", "DELETE", "PATCH"):
            status, _, headers = self.call("/_jjap/reset", method, {} if method == "PATCH" else None)
            self.assertEqual((method, status, headers.get("Allow")), (method, 405, "POST"))
        self.assertEqual(self.call("/api/v2/dags/slow", "PUT")[0], 501)
        time.sleep(0.01)
        status, body, _ = self.call("/_jjap/reset", "POST")
        self.assertEqual((status, body), (200, {"reset": True}))
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

        self.assertEqual(self.call("/api/v2/dags/slow/dagRuns")[1]["total_entries"], 0)
        hourly = self.call("/api/v2/dags/hourly")[1]
        self.assertTrue(hourly["is_paused"])
        self.assertGreater(hourly["next_dagrun_run_after"], schedule)
        self.assertEqual(sorted(dag["dag_id"] for dag in self.call("/api/v2/dags")[1]["dags"]), ["hourly", "quick", "slow"])
        time.sleep(0.3)
        self.assertEqual(self.call("/api/v2/dags/slow/dagRuns")[1]["total_entries"], 0)

        self.call("/api/v2/dags/quick/dagRuns", "POST", {"dag_run_id": "after"})
        self.wait(lambda: self.call("/api/v2/dags/quick/dagRuns/after")[1]["state"] == "success")
        process.terminate()
        process.wait(10)
        self.start()
        self.assertEqual(self.call("/api/v2/dags/slow/dagRuns")[1]["total_entries"], 0)
        self.assertEqual(self.call("/api/v2/dags/quick/dagRuns/after")[1]["state"], "success")
        self.assertEqual(self.call("/_jjap/reset", "POST")[1], {"reset": True})
        self.assertEqual(self.call("/api/v2/dags/quick/dagRuns")[1]["total_entries"], 0)


if __name__ == "__main__":
    unittest.main()
