"""Verify Airflow 3 DAG file exports without Airflow installed."""

import ast
import datetime
import hashlib
import json
from pathlib import Path
import runpy
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "airflow_server.py"
ESCAPED_BRACE = "{{ '{' }}"


class Recorder:
    """Stand-ins for the Airflow classes a DAG file uses; they record their arguments."""

    def __init__(self):
        self.dags, self.tasks, self.upstream = [], {}, {}
        recorder = self

        class DAG:
            def __init__(self, **kwargs):
                recorder.dags.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        class BashOperator:
            def __init__(self, **kwargs):
                recorder.tasks[kwargs["task_id"]] = kwargs
                self.task_id = kwargs["task_id"]

            def set_upstream(self, tasks):
                recorder.upstream[self.task_id] = [task.task_id for task in tasks]

        class DeltaDataIntervalTimetable:
            def __init__(self, delta):
                self.delta = delta

        class Dataset:
            def __init__(self, namespace, name):
                self.pair = (namespace, name)

        self.modules = {
            "airflow": types.ModuleType("airflow"),
            "airflow.sdk": types.SimpleNamespace(DAG=DAG, DeltaDataIntervalTimetable=DeltaDataIntervalTimetable),
            "airflow.providers.standard.operators.bash": types.SimpleNamespace(BashOperator=BashOperator),
            "openlineage.client.event_v2": types.SimpleNamespace(Dataset=Dataset),
        }

    def load(self, path):
        with patch.dict(sys.modules, self.modules):
            runpy.run_path(str(path))
        return self


def render(template):
    """Apply the only Jinja construct the exporter emits, after checking nothing else would render."""
    assert "{" not in template.replace(ESCAPED_BRACE, ""), template
    return template.replace(ESCAPED_BRACE, "{")


class AirflowExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.airflow = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-airflow-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    def command(self, *args, check=True):
        return subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "export", *args],
                              cwd=self.directory, check=check, text=True, capture_output=True, timeout=15)

    def write_dags(self, dags, name="dags.json"):
        path = self.directory / name
        path.write_text(json.dumps(dags), encoding="utf-8")
        return path

    def test_dag_file_export_keeps_commands_literal(self):
        tricky = ["{{ ds }}", "{% raw %}", "{#", "it's \"quoted\"\n\\n", '"""', "$HOME `id`", "\ud55c\uae00 \u2615", "x.sh"]
        printer = [sys.executable, "-c", "import json, sys; print(json.dumps(sys.argv[1:]))", *tricky]
        source = self.write_dags([
            {"dag_id": "etl.v2-x", "description": "'''\n{{ desc }}", "schedule_seconds": 90, "tags": ["a", "{{ b }}"],
             "tasks": [
                 {"task_id": "extract", "command": printer, "outputs": [{"namespace": "file", "name": "/raw '\""}]},
                 {"task_id": "load.sh", "command": ["FOO=bar", "if", "-x"], "upstream": ["extract", "extract"],
                  "retries": 3, "retry_delay_seconds": 0.25, "timeout_seconds": 12.5,
                  "inputs": [{"namespace": "file", "name": "/raw '\""}]},
             ]},
            {"dag_id": "manual", "is_paused": True, "tasks": [{"task_id": "only", "command": ["true"]}]},
        ])
        output = self.directory / "dags"
        result = self.command("--dag-file", str(source), "--out", str(output), "--cwd", str(self.directory / "w.sh"))
        self.assertEqual(result.stdout.strip(), f"Exported 2 DAGs with 3 tasks to {output}")
        self.assertEqual(sorted(path.name for path in output.iterdir()), ["etl.v2-x.py", "manual.py"])

        text = (output / "etl.v2-x.py").read_text(encoding="utf-8")
        ast.parse(text)
        recorded = Recorder().load(output / "etl.v2-x.py")
        dag, = recorded.dags
        self.assertEqual(dag["dag_id"], "etl.v2-x")
        self.assertEqual(dag["description"], "'''\n{{ desc }}")
        self.assertEqual(dag["schedule"].delta, datetime.timedelta(seconds=90))
        self.assertEqual((dag["catchup"], dag["is_paused_upon_creation"]), (False, False))
        self.assertEqual((dag["max_active_runs"], dag["max_active_tasks"], dag["tags"]), (1, 1, ["a", "{{ b }}"]))
        self.assertEqual(dag["start_date"].tzinfo, datetime.timezone.utc)
        self.assertLess(abs(dag["start_date"].timestamp() - datetime.datetime.now().timestamp()), 60)

        extract, load = recorded.tasks["extract"], recorded.tasks["load.sh"]
        self.assertEqual((extract["retries"], extract["retry_delay"], extract["execution_timeout"]),
                         (0, datetime.timedelta(seconds=1), datetime.timedelta(seconds=300)))
        self.assertEqual((load["retries"], load["retry_delay"], load["execution_timeout"]),
                         (3, datetime.timedelta(seconds=0.25), datetime.timedelta(seconds=12.5)))
        self.assertEqual(recorded.upstream, {"load.sh": ["extract"]})
        self.assertEqual([item.pair for item in extract["outlets"]], [("file", "/raw '\"")])
        self.assertEqual([item.pair for item in load["inlets"]], [("file", "/raw '\"")])
        self.assertEqual((extract["priority_weight"], load["priority_weight"], load["weight_rule"]), (2, 1, "absolute"))
        self.assertIsNone(load["skip_on_exit_code"])
        self.assertTrue(load["append_env"])
        self.assertEqual(load["cwd"], str(self.directory / "w.sh") + "/")
        for task in (extract, load):
            self.assertFalse(any(value.endswith((".sh", ".bash")) for value in task["env"].values()))
            self.assertFalse(task["bash_command"].endswith((".sh", ".bash")))
        self.assertEqual(render(load["bash_command"]), "exec -- FOO=bar if -x")

        # bash runs the rendered command with the original argument vector.
        printed = subprocess.run(["bash", "-c", render(extract["bash_command"])], check=True,
                                 capture_output=True, text=True, timeout=10).stdout
        self.assertEqual(json.loads(printed), tricky)
        self.assertEqual(render(extract["bash_command"]), "exec -- " + shlex.join(printer) + " ")

        manual = Recorder().load(output / "manual.py")
        self.assertEqual(manual.dags[0]["schedule"], None)
        self.assertNotIn("start_date", manual.dags[0])
        self.assertTrue(manual.dags[0]["is_paused_upon_creation"])
        self.assertNotIn("import DeltaDataIntervalTimetable", (output / "manual.py").read_text(encoding="utf-8"))

    def test_database_export_uses_live_pause_state_and_schedule_phase(self):
        source = self.directory / "state #?.sqlite3"
        normalize = self.airflow["normalize_dag"]
        spec = {"tasks": [{"task_id": "work", "command": ["true"], "retries": 1}]}
        first = {name: normalize({"dag_id": name, **spec, **extra}, "dags.json")
                 for name, extra in (("hourly", {"schedule_seconds": 3600}), ("manual", {}), ("gone", {}))}
        service = self.airflow["AirflowService"](first, source)
        try:
            service.pause("manual", True)
            service.trigger("hourly", {"dag_run_id": "manual__fixture"})
        finally:
            service.close()
        service = self.airflow["AirflowService"]({name: first[name] for name in ("hourly", "manual")}, source)
        service.close()
        with sqlite3.connect(source) as connection:
            next_run = connection.execute("SELECT next_run FROM dags WHERE dag_id='hourly'").fetchone()[0]
        connection.close()
        snapshot = hashlib.sha256(source.read_bytes()).hexdigest()

        output = self.directory / "from-db"
        result = self.command("--db-file", str(source), "--out", str(output))
        self.assertEqual(sorted(path.name for path in output.iterdir()), ["hourly.py", "manual.py"])
        self.assertIn("skipped DAGs that are no longer registered: gone", result.stderr)
        self.assertIn("run history is not portable to Airflow; 1 DAG runs", result.stderr)
        hourly = Recorder().load(output / "hourly.py").dags[0]
        self.assertAlmostEqual(hourly["start_date"].timestamp(), next_run - 3600, places=5)
        self.assertFalse(hourly["is_paused_upon_creation"])
        self.assertTrue(Recorder().load(output / "manual.py").dags[0]["is_paused_upon_creation"])
        self.assertEqual(Recorder().load(output / "hourly.py").tasks["work"]["retries"], 1)
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), snapshot)

    def test_rejects_existing_destination_and_cleans_up_invalid_sources(self):
        source = self.write_dags({"dag_id": "demo", "tasks": [{"task_id": "t", "command": ["true"]}]})
        used = self.directory / "used"
        used.mkdir()
        (used / "keep.txt").write_text("preserve", encoding="utf-8")
        result = self.command("--dag-file", str(source), "--out", str(used), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((used / "keep.txt").read_text(encoding="utf-8"), "preserve")

        other = self.directory / "other.sqlite3"
        with sqlite3.connect(other) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        connection.close()
        missing = self.directory / "missing.sqlite3"
        long_tag = self.write_dags({"dag_id": "demo", "tags": ["x" * 101], "tasks": [{"task_id": "t", "command": ["true"]}]},
                                   "tag.json")
        long_command = self.write_dags({"dag_id": "demo", "tasks": [{"task_id": "t", "command": ["echo", "x" * 140_000]}]},
                                       "long.json")
        cases = [
            (["--db-file", str(other)], "not an Airflow test server SQLite persistence file"),
            (["--db-file", str(missing)], "database not found"),
            (["--dag-file", str(long_tag)], "tag longer than Airflow's 100 characters"),
            (["--dag-file", str(long_command)], "Task demo.t cannot be exported"),
        ]
        for arguments, message in cases:
            target = self.directory / "out"
            result = self.command(*arguments, "--out", str(target), check=False)
            self.assertEqual(result.returncode, 1, arguments)
            self.assertTrue(result.stderr.startswith("Airflow export failed: "), result.stderr)
            self.assertIn(message, result.stderr)
            self.assertFalse(target.exists())
        self.assertFalse(missing.exists())
        self.assertEqual(list(self.directory.glob(".airflow-export-*")), [])

    def test_server_invocation_still_parses_arguments(self):
        result = subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "--port", "70000"],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--port must be between 0 and 65535", result.stderr)


if __name__ == "__main__":
    unittest.main()
