"""Verify PySpark script exports; the end-to-end test runs only where spark-submit is installed."""

import ast
import csv
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "spark_server.py"


def constants(path):
    """Read the literal assignments at the top of an exported script without importing PySpark."""
    values = {}
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try:
                values[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                pass
    return values


class FakeWriter:
    """Mimics DataFrameWriter by writing one part file into the requested directory."""

    def __init__(self, content):
        self.content, self.settings = content, {}

    def mode(self, value):
        self.settings["mode"] = value
        return self

    def options(self, **values):
        self.settings.update(values)
        return self

    def option(self, key, value):
        self.settings[key] = value
        return self

    def save_to(self, location, suffix):
        directory = Path(location.removeprefix("file://"))
        directory.mkdir()
        (directory / f"part-00000-x.c000.{suffix}").write_text(self.content, encoding="utf-8")
        (directory / "_SUCCESS").write_text("", encoding="utf-8")

    def csv(self, location):
        self.save_to(location, "csv")

    def json(self, location):
        self.save_to(location, "json")


class SparkExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-spark-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    def command(self, config, output, *args, check=True):
        return subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(self.script), "export", "--config", str(config), "--out", str(output),
             *args], cwd=self.directory, check=check, text=True, capture_output=True, timeout=15)

    def write_config(self, jobs, name="jobs.json"):
        path = self.directory / name
        path.write_text(json.dumps({"jobs": jobs}), encoding="utf-8")
        return path

    def load_script(self, path):
        sql = types.SimpleNamespace(SparkSession=None, functions=None, types=None)
        with patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": sql}):
            return runpy.run_path(str(path))

    def test_scripts_embed_the_resolved_job_and_submit_lines(self):
        relation = {"format": "csv", "path": "in [1]/da'ta{x}.csv", "delimiter": ";", "header": False,
                    "schema": {"label": "string", "amount": "long", "ok": "boolean", "ratio": "double"}}
        config = self.write_config({
            "csv.job-1": {"inputs": {"events": relation}, "sql": "SELECT label FROM events WHERE label <> '\"'''",
                          "timeout_seconds": 2.5,
                          "output": {"format": "csv", "path": "out/x.csv", "overwrite": True, "schema": {"label": "string"}}},
            "lines": {"sql": "SELECT 1 AS value", "output": {"format": "jsonl", "path": "v.jsonl", "schema": {"value": "int"}}},
        })
        output = self.directory / "export"
        result = self.command(config, output)
        self.assertEqual(result.stdout.strip(), f"Exported 2 PySpark jobs and submit.sh to {output}")
        self.assertEqual(sorted(path.name for path in output.iterdir()), ["csv.job-1.py", "lines.py", "submit.sh"])

        jobs = self.spark["load_jobs"](config)
        values = constants(output / "csv.job-1.py")
        self.assertEqual(values["JOB_NAME"], "csv.job-1")
        self.assertEqual(values["JOB"], jobs["csv.job-1"])
        self.assertEqual(values["JOB"]["inputs"]["events"]["path"], str(self.directory / "in [1]" / "da'ta{x}.csv"))
        self.assertEqual(values["SPARK_CONFIG"], {})
        self.assertEqual(constants(output / "lines.py")["JOB"], jobs["lines"])

        submit = output / "submit.sh"
        self.assertTrue(os.access(submit, os.X_OK))
        lines = submit.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines[-2:], [
            '"${SPARK_SUBMIT:-spark-submit}" --name csv.job-1 "$@" csv.job-1.py',
            '"${SPARK_SUBMIT:-spark-submit}" --name lines "$@" lines.py',
        ])
        if shutil.which("sh"):
            subprocess.run(["sh", "-n", str(submit)], check=True, timeout=10)

        module = self.load_script(output / "csv.job-1.py")
        source = jobs["csv.job-1"]["inputs"]["events"]["path"]
        Path(source).parent.mkdir()
        Path(source).write_text("x;1;true;1.5\n", encoding="utf-8")
        self.assertEqual(module["input_uri"](source), "file://" + source.replace("[", "\\[").replace("]", "\\]")
                         .replace("{", "\\{").replace("}", "\\}"))
        with self.assertRaisesRegex(ValueError, "not a regular file"):
            module["input_uri"](str(self.directory))

    def test_file_publication_is_atomic_and_honors_overwrite(self):
        config = self.write_config({"job": {"sql": "SELECT 1 AS value",
                                            "output": {"format": "csv", "path": "result.csv", "schema": {"value": "int"}}}})
        output = self.directory / "export"
        self.command(config, output)
        module = self.load_script(output / "job.py")
        relation = module["JOB"]["output"]
        timer = types.SimpleNamespace(cancel=lambda: None, expired=threading.Event())
        frame = types.SimpleNamespace(coalesce=lambda count: types.SimpleNamespace(write=FakeWriter("value\n1\n")))

        module["write_file"](frame, relation, timer)
        target = Path(relation["path"])
        self.assertEqual(target.read_text(encoding="utf-8"), "value\n1\n")
        frame = types.SimpleNamespace(coalesce=lambda count: types.SimpleNamespace(write=FakeWriter("value\n2\n")))
        with self.assertRaisesRegex(ValueError, "already exists"):
            module["write_file"](frame, relation, timer)
        self.assertEqual(target.read_text(encoding="utf-8"), "value\n1\n")
        module["write_file"](frame, {**relation, "overwrite": True}, timer)
        self.assertEqual(target.read_text(encoding="utf-8"), "value\n2\n")
        timer.expired.set()
        with self.assertRaisesRegex(ValueError, "timed out"):
            module["write_file"](frame, {**relation, "overwrite": True}, timer)
        self.assertEqual(target.read_text(encoding="utf-8"), "value\n2\n")
        self.assertEqual(sorted(path.name for path in self.directory.glob(".jjap-spark-*")), [])

    def test_iceberg_jobs_use_one_rest_catalog(self):
        warehouse = {"format": "iceberg", "warehouse": "lake", "table": "db.events", "schema": {"value": "long"}}
        config = self.write_config({
            "ice": {"inputs": {"events": {**warehouse, "snapshot_id": 7}}, "sql": "SELECT value FROM events",
                    "output": {**warehouse, "table": "db.copy"}},
            "plain": {"sql": "SELECT 1 AS value", "output": {"format": "jsonl", "path": "v.jsonl", "schema": {"value": "int"}}},
        })
        output = self.directory / "export"
        self.command(config, output, "--iceberg-uri", "http://127.0.0.1:18181")
        values = constants(output / "ice.py")
        self.assertEqual(values["SPARK_CONFIG"], {
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "rest",
            "spark.sql.catalog.iceberg.uri": "http://127.0.0.1:18181",
        })
        self.assertEqual(values["JOB"]["inputs"]["events"]["snapshot_id"], 7)
        self.assertIn(f"--packages {self.spark['ICEBERG_PACKAGE']} \"$@\" ice.py",
                      (output / "submit.sh").read_text(encoding="utf-8"))
        module = self.load_script(output / "ice.py")
        self.assertEqual(module["iceberg_table"]("db-x.events"), "`iceberg`.`db-x`.`events`")

        mixed = self.write_config({
            "a": {"sql": "SELECT value FROM events", "inputs": {"events": warehouse},
                  "output": {"format": "jsonl", "path": "a.jsonl", "schema": {"value": "long"}}},
            "b": {"sql": "SELECT value FROM events", "inputs": {"events": {**warehouse, "warehouse": "other"}},
                  "output": {"format": "jsonl", "path": "b.jsonl", "schema": {"value": "long"}}},
        }, "mixed.json")
        target = self.directory / "mixed"
        result = self.command(mixed, target, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Spark export failed: Jobs a, b use 2 local Iceberg catalogs", result.stderr)
        result = self.command(config, target, "--iceberg-uri", "ftp://host", check=False)
        self.assertIn("--iceberg-uri must be an HTTP(S) URL", result.stderr)
        self.assertFalse(target.exists())

    def test_rejects_existing_destination_and_invalid_config(self):
        config = self.write_config({"job": {"sql": "SELECT 1 AS value",
                                            "output": {"format": "csv", "path": "r.csv", "schema": {"value": "int"}}}})
        used = self.directory / "used"
        used.mkdir()
        (used / "keep.txt").write_text("preserve", encoding="utf-8")
        result = self.command(config, used, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((used / "keep.txt").read_text(encoding="utf-8"), "preserve")

        invalid = self.write_config({"job": {"sql": "SELECT 1", "output": {"format": "parquet"}}}, "invalid.json")
        target = self.directory / "out"
        result = self.command(invalid, target, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Spark export failed: Relation format must be csv, jsonl, or iceberg", result.stderr)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.directory.glob(".spark-export-*")), [])
        self.assertEqual(json.loads(invalid.read_text(encoding="utf-8"))["jobs"]["job"]["sql"], "SELECT 1")

    @unittest.skipUnless(shutil.which("spark-submit"), "spark-submit is not installed")
    def test_spark_submit_matches_local_job_outputs(self):
        (self.directory / "events.csv").write_text(
            'amount,label,flag\n2,books,1\n3,"books, rare",TRUE\n,"multi\nline ""q""",0\n-7,caf\u00e9,false\n', encoding="utf-8")
        (self.directory / "scores.jsonl").write_text(
            '{"user": "ann", "score": 10, "weight": 1.5}\n\n{"user": "bob", "score": null, "weight": 2}\n', encoding="utf-8")
        events = {"format": "csv", "path": "events.csv", "schema": {"label": "string", "amount": "long", "flag": "boolean"}}
        scores = {"format": "jsonl", "path": "scores.jsonl", "schema": {"user": "string", "score": "long", "weight": "double"}}
        config = self.write_config({
            "csv": {"inputs": {"events": events}, "sql": "SELECT label, amount, flag FROM events ORDER BY amount",
                    "output": {"format": "csv", "path": "out/events.csv", "schema": events["schema"]}},
            "jsonl": {"inputs": {"scores": scores},
                      "sql": "SELECT user, sum(score) AS total, avg(weight) AS mean FROM scores GROUP BY user ORDER BY user",
                      "output": {"format": "jsonl", "path": "out/scores.jsonl",
                                 "schema": {"user": "string", "total": "long", "mean": "double"}}},
        })
        (self.directory / "out").mkdir()
        jobs = self.spark["load_jobs"](config)
        service = self.spark["SparkService"](jobs)
        try:
            for name in jobs:
                service._execute(self.spark["JobRun"](name, jobs[name], threading.Event()))
        finally:
            service.close()
        (self.directory / "out").rename(self.directory / "local")
        (self.directory / "out").mkdir()
        output = self.directory / "export"
        self.command(config, output)
        subprocess.run([str(output / "submit.sh"), "--master", "local[1]", "--conf", "spark.ui.enabled=false"],
                       check=True, capture_output=True, timeout=600)
        read_csv = lambda path: list(csv.reader(path.open(newline="", encoding="utf-8")))
        read_lines = lambda path: [json.loads(line) for line in path.open(encoding="utf-8")]
        self.assertEqual(read_csv(self.directory / "out/events.csv"), read_csv(self.directory / "local/events.csv"))
        self.assertEqual(read_lines(self.directory / "out/scores.jsonl"), read_lines(self.directory / "local/scores.jsonl"))


if __name__ == "__main__":
    unittest.main()
