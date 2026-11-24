"""Verify POST /_jjap/reset on the Spark Standalone test server, which runs as a separate process."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


SCRIPT = Path(__file__).resolve().parents[1] / "spark_server.py"
TOKEN = "spark-reset-token"
SLOW_SQL = "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1000000000000) SELECT count(*) AS value FROM n"


class SparkResetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-spark-reset-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        (self.directory / "token").write_text(TOKEN, encoding="ascii")
        output = {"format": "jsonl", "schema": {"value": "long"}, "overwrite": True}
        jobs = {"slow": {"sql": SLOW_SQL, "output": {**output, "path": "slow.jsonl"}},
                "quick": {"sql": "SELECT 7 AS value", "output": {**output, "path": "quick.jsonl"}}}
        (self.directory / "jobs.json").write_text(json.dumps({"jobs": jobs}), encoding="utf-8")

    def start(self):
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-B", str(self.script), "--port", "0", "--config", "jobs.json",
             "--db-file", "spark.sqlite3", "--password-file", "token"],
            cwd=self.directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        line = process.stdout.readline()
        self.assertIn("listening on http://", line, process.stderr.read() if not line else line)
        self.root = line.split("listening on ", 1)[1].strip()
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

    def submit(self, job):
        body = {"action": "CreateSubmissionRequest", "appResource": job, "mainClass": "jjap.LocalSqlJob",
                "clientSparkVersion": "4.1.1", "appArgs": [], "environmentVariables": {}, "sparkProperties": {}}
        return self.call("/v1/submissions/create", "POST", body)[1]["submissionId"]

    def state(self, identifier):
        return self.call(f"/v1/submissions/status/{identifier}")[1].get("driverState")

    def wait(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while not predicate():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.02)

    def test_reset_cancels_the_running_job_and_forgets_submissions(self):
        process = self.start()
        slow = self.submit("slow")
        self.wait(lambda: self.state(slow) == "RUNNING")
        queued = self.submit("quick")
        self.assertEqual(self.state(queued), "SUBMITTED")

        self.assertEqual(self.call("/_jjap/reset", "POST", token=None)[0], 401)
        for method in ("GET", "PUT", "DELETE", "PATCH"):
            status, _, headers = self.call("/_jjap/reset", method)
            self.assertEqual((method, status, headers.get("Allow")), (method, 405, "POST"))
        self.assertEqual(self.call("/v1/submissions/create", "PUT")[0], 501)
        started = time.monotonic()
        status, body, _ = self.call("/_jjap/reset", "POST")
        self.assertEqual((status, body), (200, {"reset": True}))
        self.assertLess(time.monotonic() - started, 5)
        for identifier in (slow, queued):
            self.assertEqual(self.call(f"/v1/submissions/status/{identifier}")[1]["success"], False)
        self.assertFalse((self.directory / "slow.jsonl").exists())

        quick = self.submit("quick")
        self.wait(lambda: self.state(quick) == "FINISHED")
        self.assertEqual((self.directory / "quick.jsonl").read_text(encoding="utf-8"), '{"value": 7}\n')
        process.terminate()
        process.wait(10)
        self.start()
        self.assertEqual(self.state(quick), "FINISHED")
        self.assertEqual(self.call(f"/v1/submissions/status/{slow}")[1]["success"], False)
        self.assertEqual(self.call("/_jjap/reset", "POST")[1], {"reset": True})
        self.assertEqual(self.call(f"/v1/submissions/status/{quick}")[1]["success"], False)


if __name__ == "__main__":
    unittest.main()
