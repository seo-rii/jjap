#!/usr/bin/env python3
"""Airflow 3 REST API subset and a local DAG scheduler."""

import argparse
import datetime as dt
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen
import uuid


MAX_BODY = 1024 * 1024
MAX_LOG = 1024 * 1024
ID_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,250}")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9_.:+-]{1,250}")
LINEAGE_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
LINEAGE_PRODUCER = "https://github.com/seo-rii/jjap/blob/main/airflow_server.py"


class APIError(ValueError):
    def __init__(self, status, detail):
        super().__init__(detail)
        self.status = status


def timestamp(value):
    return None if value is None else dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat()


def datetime_value(value, field):
    if value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        if parsed.utcoffset() is not None:
            result = parsed.timestamp()
            timestamp(result)
            return result
    except (AttributeError, ValueError, OverflowError, OSError):
        pass
    raise APIError(422, f"{field} must be an ISO 8601 datetime with a timezone")


def read_dags(paths):
    dags = {}
    for path in paths:
        path = Path(path).resolve()
        data = json.loads(path.read_text(encoding="utf-8"))
        for entry in data if isinstance(data, list) else [data]:
            dag = normalize_dag(entry, str(path))
            if dag["dag_id"] in dags:
                raise ValueError(f"Duplicate DAG {dag['dag_id']}")
            dags[dag["dag_id"]] = dag
    return dags


def check_number(value, field, minimum, maximum, *, integer=False):
    allowed_types = (int,) if integer else (int, float)
    if type(value) not in allowed_types or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return value


def string_list(value, field):
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} must be an array of nonempty strings")
    return value


def valid_id(value, pattern=ID_PATTERN):
    return isinstance(value, str) and pattern.fullmatch(value) is not None and value not in {".", ".."}


def normalize_dag(value, path):
    if not isinstance(value, dict) or not valid_id(dag_id := value.get("dag_id")):
        raise ValueError("A DAG must have a valid dag_id")
    if not isinstance(value.get("tasks"), list) or not value["tasks"]:
        raise ValueError(f"DAG {dag_id} requires a nonempty tasks array")
    schedule = value.get("schedule_seconds")
    if schedule is not None:
        check_number(schedule, "schedule_seconds", 1, 31_536_000, integer=True)
    if not isinstance(paused := value.get("is_paused", False), bool):
        raise ValueError("is_paused must be boolean")
    if (description := value.get("description")) is not None and not isinstance(description, str):
        raise ValueError("description must be a string")
    tasks = {}
    for entry in value["tasks"]:
        if not isinstance(entry, dict) or not valid_id(task_id := entry.get("task_id")):
            raise ValueError(f"DAG {dag_id} contains an invalid task_id")
        if task_id in tasks:
            raise ValueError(f"DAG {dag_id} has duplicate task {task_id}")
        command = entry.get("command")
        if not isinstance(command, list) or not command or not command[0] or any(
            not isinstance(arg, str) or "\0" in arg for arg in command
        ):
            raise ValueError(f"Task {task_id} requires an argument array with a command name")
        tasks[task_id] = {
            "command": command,
            "upstream": string_list(entry.get("upstream", []), "upstream"),
            "retries": check_number(entry.get("retries", 0), "retries", 0, 100, integer=True),
            "retry_delay_seconds": check_number(entry.get("retry_delay_seconds", 1), "retry_delay_seconds", 0, 86_400),
            "timeout_seconds": check_number(entry.get("timeout_seconds", 300), "timeout_seconds", 0.01, 86_400),
        }
        for field in ("inputs", "outputs"):
            datasets = entry.get(field, [])
            if not isinstance(datasets, list) or any(
                not isinstance(dataset, dict) or any(
                    not isinstance(dataset.get(key), str) or not dataset[key] for key in ("namespace", "name")
                ) for dataset in datasets
            ):
                raise ValueError(f"{task_id}.{field} must contain dataset namespace/name objects")
            tasks[task_id][field] = [{key: dataset[key] for key in ("namespace", "name")} for dataset in datasets]
    for task_id, task in tasks.items():
        if set(task["upstream"]) - tasks.keys():
            raise ValueError(f"Task {task_id} has an unknown upstream task")
    visited = set()
    remaining = set(tasks)
    while remaining:
        ready = {task_id for task_id in remaining if set(tasks[task_id]["upstream"]) <= visited}
        if not ready:
            raise ValueError(f"DAG {dag_id} contains a cycle")
        remaining -= ready
        visited |= ready
    return {
        "dag_id": dag_id, "description": description, "fileloc": path, "tasks": tasks,
        "schedule_seconds": schedule, "is_paused": paused,
        "tags": string_list(value.get("tags", []), "tags"),
    }


class AirflowService:
    def __init__(self, dags, db_file=None, *, lineage_url=None, lineage_namespace="airflow", lineage_token=None):
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.lineage_url = lineage_url
        self.lineage_namespace = lineage_namespace
        self.lineage_token = lineage_token
        self.scheduler = threading.Thread(target=self._schedule, name="airflow-scheduler", daemon=True)
        self.publisher = threading.Thread(target=self._publish, name="airflow-lineage", daemon=True)
        self.lease = None
        database = ":memory:"
        if db_file is not None:
            database = str(Path(db_file).resolve())
            # A dedicated SQLite lock releases when the scheduler exits or crashes.
            self.lease = sqlite3.connect(database + ".lock", timeout=0)
            try:
                self.lease.execute("BEGIN EXCLUSIVE")
            except sqlite3.OperationalError as exc:
                self.lease.close()
                raise ValueError(f"Cannot acquire scheduler lock for {database}: {exc}") from exc
        self.db = None
        try:
            self.db = sqlite3.connect(database, check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self._initialize(dags)
        except BaseException:
            self.close()
            raise

    def _initialize(self, dags):
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS dags (
                dag_id TEXT PRIMARY KEY, spec TEXT NOT NULL, paused INTEGER NOT NULL,
                active INTEGER NOT NULL, next_run REAL
            );
            CREATE TABLE IF NOT EXISTS runs (
                dag_id TEXT NOT NULL REFERENCES dags, run_id TEXT NOT NULL, spec TEXT NOT NULL,
                state TEXT NOT NULL, run_type TEXT NOT NULL, queued REAL NOT NULL, run_after REAL NOT NULL,
                logical_date REAL, data_begin REAL, data_end REAL, started REAL, ended REAL,
                conf TEXT NOT NULL, note TEXT, PRIMARY KEY (dag_id, run_id)
            );
            CREATE TABLE IF NOT EXISTS tasks (
                dag_id TEXT NOT NULL, run_id TEXT NOT NULL, task_id TEXT NOT NULL, id TEXT NOT NULL,
                state TEXT, ordinal INTEGER NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                ready REAL NOT NULL, started REAL, ended REAL,
                PRIMARY KEY (dag_id, run_id, task_id), FOREIGN KEY (dag_id, run_id) REFERENCES runs
            );
            CREATE TABLE IF NOT EXISTS attempts (
                dag_id TEXT NOT NULL, run_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                lineage_id TEXT NOT NULL, log BLOB NOT NULL DEFAULT X'', truncated INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (dag_id, run_id, task_id, attempt),
                FOREIGN KEY (dag_id, run_id, task_id) REFERENCES tasks
            );
            CREATE TABLE IF NOT EXISTS lineage_outbox (
                id INTEGER PRIMARY KEY, event TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, ready REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS active_runs ON runs (state, run_after);
        """)
        now = time.time()
        with self.db:
            self.db.execute("UPDATE dags SET active=0")
            for dag_id, dag in dags.items():
                previous = self.db.execute("SELECT spec, next_run FROM dags WHERE dag_id=?", (dag_id,)).fetchone()
                schedule = dag["schedule_seconds"]
                next_run = now + schedule if schedule is not None else None
                if previous and json.loads(previous["spec"])["schedule_seconds"] == schedule:
                    next_run = previous["next_run"]
                self.db.execute(
                    """INSERT INTO dags VALUES (?, ?, ?, 1, ?)
                       ON CONFLICT(dag_id) DO UPDATE SET spec=excluded.spec, active=1, next_run=excluded.next_run""",
                    (dag_id, json.dumps(dag), dag["is_paused"], next_run),
                )
            interrupted = self.db.execute("""
                SELECT tasks.*, runs.spec, attempts.lineage_id FROM tasks JOIN runs USING (dag_id, run_id)
                JOIN attempts USING (dag_id, run_id, task_id, attempt) WHERE tasks.state='running'
            """).fetchall()
            for task in interrupted:
                key = (task["dag_id"], task["run_id"], task["task_id"])
                self._append_log((*key, task["attempt"]), b"\nScheduler stopped before the task outcome was recorded.\n")
                self.db.execute("UPDATE tasks SET state='failed', ended=? WHERE dag_id=? AND run_id=? AND task_id=?",
                                (now, *key))
                self._event(task["lineage_id"], key, json.loads(task["spec"])["tasks"][key[2]], "FAIL", now)
                self._finalize_run(key[0], key[1], now)

    def start(self):
        self.scheduler.start()
        if self.lineage_url:
            self.publisher.start()

    def close(self):
        self.stop.set()
        self.wake.set()
        for worker in (self.scheduler, self.publisher):
            if worker.ident is not None:
                worker.join()
        if self.db is not None:
            self.db.close()
        if self.lease is not None:
            self.lease.close()

    def _insert_run(self, dag, run_id, run_type, now, run_after, *, logical=None, begin=None, end=None,
                    conf=None, note=None):
        spec = json.loads(dag["spec"])
        dag_id = dag["dag_id"]
        self.db.execute("""
            INSERT INTO runs (dag_id, run_id, spec, state, run_type, queued, run_after,
                              logical_date, data_begin, data_end, conf, note)
            VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (dag_id, run_id, dag["spec"], run_type, now, run_after, logical, begin, end,
              json.dumps(conf or {}, allow_nan=False), note))
        self.db.executemany("""
            INSERT INTO tasks (dag_id, run_id, task_id, id, ordinal, ready) VALUES (?, ?, ?, ?, ?, ?)
        """, [(dag_id, run_id, task_id, str(uuid.uuid4()), index, run_after)
              for index, task_id in enumerate(spec["tasks"])])

    def trigger(self, dag_id, body):
        if set(body) - {"dag_run_id", "run_after", "logical_date", "data_interval_start", "data_interval_end", "conf", "note"}:
            raise APIError(422, "Unknown trigger field")
        conf = body.get("conf")
        note = body.get("note")
        if conf is not None and not isinstance(conf, dict):
            raise APIError(422, "conf must be an object")
        if note is not None and not isinstance(note, str):
            raise APIError(422, "note must be a string")
        now = time.time()
        run_id = body.get("dag_run_id")
        if run_id is None:
            run_id = f"manual__{timestamp(now)}"
        if not valid_id(run_id, RUN_ID_PATTERN) or run_id.startswith("scheduled__"):
            raise APIError(422, "Invalid dag_run_id")
        run_after = datetime_value(body.get("run_after"), "run_after")
        logical = datetime_value(body.get("logical_date"), "logical_date")
        begin = datetime_value(body.get("data_interval_start"), "data_interval_start")
        end = datetime_value(body.get("data_interval_end"), "data_interval_end")
        if (begin is None) != (end is None) or (begin is not None and begin > end):
            raise APIError(422, "Provide both data interval dates in chronological order")
        try:
            with self.lock, self.db:
                dag = self.dag(dag_id)
                if not dag["active"]:
                    raise APIError(404, f"DAG {dag_id} is no longer registered")
                self._insert_run(dag, run_id, "manual", now, now if run_after is None else run_after,
                                 logical=logical, begin=begin, end=end, conf=conf, note=note)
                run = self.run(dag_id, run_id)
        except sqlite3.IntegrityError as exc:
            raise APIError(409, f"DAG run {run_id} already exists") from exc
        self.wake.set()
        return run

    def _tick(self):
        now = time.time()
        with self.lock, self.db:
            for dag in self.db.execute("""
                SELECT * FROM dags WHERE active=1 AND paused=0 AND next_run<=?
                  AND NOT EXISTS (SELECT 1 FROM runs WHERE runs.dag_id=dags.dag_id AND state IN ('queued', 'running'))
            """, (now,)).fetchall():
                interval = json.loads(dag["spec"])["schedule_seconds"]
                scheduled = dag["next_run"]
                due = scheduled + math.floor((now - scheduled) / interval) * interval
                self._insert_run(dag, f"scheduled__{timestamp(due)}", "scheduled", now, due,
                                 logical=due - interval, begin=due - interval, end=due)
                self.db.execute("UPDATE dags SET next_run=? WHERE dag_id=?", (due + interval, dag["dag_id"]))
            runs = self.db.execute("""
                SELECT runs.* FROM runs JOIN dags USING (dag_id) WHERE dags.active=1 AND dags.paused=0
                  AND (runs.state='running' OR (runs.state='queued' AND run_after<=? AND NOT EXISTS (
                       SELECT 1 FROM runs AS active WHERE active.dag_id=runs.dag_id AND active.state='running'
                  ))) ORDER BY queued
            """, (now,)).fetchall()
            for run in runs:
                work = self._claim(run, now)
                if work:
                    return work
        return None

    def _claim(self, run, now):
        dag_id, run_id = run["dag_id"], run["run_id"]
        tasks = self.db.execute("SELECT * FROM tasks WHERE dag_id=? AND run_id=? ORDER BY ordinal",
                                (dag_id, run_id)).fetchall()
        states = {task["task_id"]: task["state"] for task in tasks}
        specs = json.loads(run["spec"])["tasks"]
        failed = {"failed", "upstream_failed"}
        for task in tasks:
            task_id = task["task_id"]
            if task["state"] not in {None, "up_for_retry"} or task["ready"] > now:
                continue
            key = (dag_id, run_id, task_id)
            spec = specs[task_id]
            upstream = {states[parent] for parent in spec["upstream"]}
            if upstream & failed:
                self.db.execute("UPDATE tasks SET state='upstream_failed', ended=? WHERE dag_id=? AND run_id=? AND task_id=?",
                                (now, *key))
                states[task_id] = "upstream_failed"
                continue
            if upstream - {"success"}:
                continue
            attempt = task["attempt"] + 1
            lineage_id = str(uuid.uuid4())
            self.db.execute("""UPDATE tasks SET state='running', attempt=?, started=?, ended=NULL
                               WHERE dag_id=? AND run_id=? AND task_id=?""", (attempt, now, *key))
            self.db.execute("UPDATE runs SET state='running', started=COALESCE(started, ?) WHERE dag_id=? AND run_id=?",
                            (now, dag_id, run_id))
            self.db.execute("INSERT INTO attempts (dag_id, run_id, task_id, attempt, lineage_id) VALUES (?, ?, ?, ?, ?)",
                            (*key, attempt, lineage_id))
            self._event(lineage_id, key, spec, "START", now)
            return key, attempt, spec, lineage_id
        self._finalize_run(dag_id, run_id, now)
        return None

    def _finalize_run(self, dag_id, run_id, now):
        states = {row[0] for row in self.db.execute("SELECT state FROM tasks WHERE dag_id=? AND run_id=?", (dag_id, run_id))}
        if states <= {"success", "failed", "upstream_failed"}:
            state = "success" if states == {"success"} else "failed"
            self.db.execute("UPDATE runs SET state=?, ended=? WHERE dag_id=? AND run_id=?", (state, now, dag_id, run_id))

    def _append_log(self, key, chunk):
        self.db.execute("""
            UPDATE attempts SET log=CAST(log || substr(?, 1, ? - length(log)) AS BLOB),
                truncated=CASE WHEN length(log) + length(?) > ? THEN 1 ELSE truncated END
                WHERE dag_id=? AND run_id=? AND task_id=? AND attempt=? AND truncated=0
        """, (chunk, MAX_LOG, chunk, MAX_LOG, *key))

    def _copy_log(self, key, pipe, done):
        with pipe:
            while chunk := pipe.read1(16384):
                with self.lock:
                    if done.is_set():
                        return
                    with self.db:
                        self._append_log(key, chunk)

    @staticmethod
    def _kill_process(process):
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass

    def _execute(self, key, attempt, spec):
        env = dict(os.environ, JJAP_AIRFLOW_DAG_ID=key[0], JJAP_AIRFLOW_RUN_ID=key[1],
                   JJAP_AIRFLOW_TASK_ID=key[2], JJAP_AIRFLOW_TRY_NUMBER=str(attempt))
        try:
            process = subprocess.Popen(spec["command"], env=env, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       start_new_session=(os.name == "posix"))
        except (OSError, ValueError) as exc:
            return False, f"Could not start task: {exc}\n"
        done = threading.Event()
        reader = threading.Thread(target=self._copy_log, args=((*key, attempt), process.stdout, done), daemon=True)
        reader.start()
        deadline = time.monotonic() + spec["timeout_seconds"]
        error = ""
        while process.poll() is None:
            if self.stop.wait(min(0.05, max(0, deadline - time.monotonic()))):
                error = "Scheduler stopped while task was running.\n"
            elif time.monotonic() >= deadline:
                error = "Task exceeded its execution timeout.\n"
            if error:
                self._kill_process(process)
                break
        code = process.wait()
        reader.join(timeout=1)
        if reader.is_alive():
            self._kill_process(process)
            reader.join(timeout=1)
        with self.lock:
            done.set()
        return code == 0 and not error, error or (f"Task exited with status {code}.\n" if code else "")

    def _schedule(self):
        while not self.stop.is_set():
            work = self._tick()
            if work is None:
                self.wake.wait(0.1)
                self.wake.clear()
                continue
            key, attempt, spec, lineage_id = work
            success, detail = self._execute(key, attempt, spec)
            now = time.time()
            retry = not success and attempt <= spec["retries"] and not self.stop.is_set()
            state = "success" if success else ("up_for_retry" if retry else "failed")
            with self.lock, self.db:
                if detail:
                    self._append_log((*key, attempt), ("\n" + detail).encode("utf-8"))
                self.db.execute("UPDATE tasks SET state=?, ended=?, ready=? WHERE dag_id=? AND run_id=? AND task_id=?",
                                (state, now, now + spec["retry_delay_seconds"] if retry else now, *key))
                self._event(lineage_id, key, spec, "COMPLETE" if success else "FAIL", now)
                self._finalize_run(key[0], key[1], now)

    def _event(self, lineage_id, key, spec, event_type, now):
        if self.lineage_url:
            event = {
                "eventType": event_type, "eventTime": timestamp(now), "producer": LINEAGE_PRODUCER,
                "schemaURL": LINEAGE_SCHEMA, "run": {"runId": lineage_id},
                "job": {"namespace": self.lineage_namespace, "name": f"{key[0]}.{key[2]}"},
                "inputs": spec["inputs"], "outputs": spec["outputs"],
            }
            self.db.execute("INSERT INTO lineage_outbox (event, ready) VALUES (?, ?)", (json.dumps(event), now))

    def _publish(self):
        headers = {"Content-Type": "application/json"}
        if self.lineage_token:
            headers["Authorization"] = "Bearer " + self.lineage_token
        while not self.stop.wait(0.1):
            with self.lock:
                row = self.db.execute("SELECT * FROM lineage_outbox ORDER BY id LIMIT 1").fetchone()
            if row is None or row["ready"] > time.time():
                continue
            try:
                with urlopen(Request(self.lineage_url, data=row["event"].encode(), headers=headers), timeout=2):
                    pass
            except OSError as exc:
                with self.lock, self.db:
                    self.db.execute("UPDATE lineage_outbox SET ready=?, tries=tries+1 WHERE id=?",
                                    (time.time() + min(30, 2 ** min(5, row["tries"])), row["id"]))
                if row["tries"] == 0:
                    print(f"OpenLineage delivery deferred: {exc}", file=sys.stderr)
            else:
                with self.lock, self.db:
                    self.db.execute("DELETE FROM lineage_outbox WHERE id=?", (row["id"],))

    def dag(self, dag_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM dags WHERE dag_id=?", (dag_id,)).fetchone()
        if row is None:
            raise APIError(404, f"DAG {dag_id} was not found")
        return row

    def dags(self):
        with self.lock:
            return self.db.execute("SELECT * FROM dags ORDER BY dag_id").fetchall()

    def pause(self, dag_id, paused):
        with self.lock, self.db:
            self.dag(dag_id)
            self.db.execute("UPDATE dags SET paused=? WHERE dag_id=?", (paused, dag_id))
        self.wake.set()
        return self.dag(dag_id)

    def run(self, dag_id, run_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM runs WHERE dag_id=? AND run_id=?", (dag_id, run_id)).fetchone()
        if row is None:
            raise APIError(404, f"DAG run {run_id} was not found")
        return row

    def runs(self, dag_id):
        with self.lock:
            self.dag(dag_id)
            return self.db.execute("SELECT * FROM runs WHERE dag_id=? ORDER BY run_after DESC, run_id", (dag_id,)).fetchall()

    def tasks(self, dag_id, run_id):
        with self.lock:
            run = self.run(dag_id, run_id)
            rows = self.db.execute("SELECT * FROM tasks WHERE dag_id=? AND run_id=? ORDER BY ordinal",
                                   (dag_id, run_id)).fetchall()
        return run, rows

    def log(self, key, attempt):
        with self.lock:
            row = self.db.execute("SELECT log, truncated FROM attempts WHERE dag_id=? AND run_id=? AND task_id=? AND attempt=?",
                                  (*key, attempt)).fetchone()
        if row is None:
            raise APIError(404, "Task attempt was not found")
        lines = row["log"].decode("utf-8", errors="replace").splitlines()
        if row["truncated"]:
            lines.append("[Task output truncated]")
        return {"content": lines, "continuation_token": None}


def dag_response(row, details=False):
    spec = json.loads(row["spec"])
    schedule = spec["schedule_seconds"]
    next_run = row["next_run"]
    interval_start = None if next_run is None else next_run - schedule
    response = {
        "dag_id": row["dag_id"], "dag_display_name": row["dag_id"], "description": spec["description"],
        "is_paused": bool(row["paused"]), "is_stale": not row["active"], "fileloc": spec["fileloc"],
        "file_token": "", "bundle_name": "jjap-dags", "has_import_errors": False,
        "has_task_concurrency_limits": False, "max_active_runs": 1, "max_active_tasks": 1,
        "max_consecutive_failed_dag_runs": 0, "owners": ["airflow"],
        "tags": [{"name": tag} for tag in spec["tags"]],
        "timetable_summary": None if schedule is None else f"Every {schedule} seconds",
        "next_dagrun_run_after": timestamp(next_run), "next_dagrun_logical_date": timestamp(interval_start),
        "next_dagrun_data_interval_start": timestamp(interval_start), "next_dagrun_data_interval_end": timestamp(next_run),
    }
    if details:
        response.update(catchup=False, concurrency=1, template_search_path=[], params={},
                        render_template_as_native_obj=False, timezone="UTC")
    return response


def run_response(row):
    return {
        "dag_id": row["dag_id"], "dag_display_name": row["dag_id"], "dag_run_id": row["run_id"],
        "state": row["state"], "run_type": row["run_type"], "dag_versions": [],
        "triggered_by": "rest_api" if row["run_type"] == "manual" else "timetable",
        "logical_date": timestamp(row["logical_date"]), "run_after": timestamp(row["run_after"]),
        "queued_at": timestamp(row["queued"]), "start_date": timestamp(row["started"]),
        "end_date": timestamp(row["ended"]), "data_interval_start": timestamp(row["data_begin"]),
        "data_interval_end": timestamp(row["data_end"]), "conf": json.loads(row["conf"]), "note": row["note"],
        "duration": row["ended"] - row["started"] if row["ended"] is not None and row["started"] is not None else None,
    }


def task_instance_response(row, run, spec):
    return {
        "id": row["id"], "dag_id": row["dag_id"], "dag_display_name": row["dag_id"],
        "dag_run_id": row["run_id"], "task_id": row["task_id"], "task_display_name": row["task_id"],
        "map_index": -1, "state": row["state"], "logical_date": timestamp(run["logical_date"]),
        "try_number": row["attempt"], "max_tries": spec["retries"], "run_after": timestamp(row["ready"]),
        "start_date": timestamp(row["started"]), "end_date": timestamp(row["ended"]),
        "duration": row["ended"] - row["started"] if row["ended"] is not None and row["started"] is not None else None,
        "executor_config": "{}", "pool": "default_pool", "pool_slots": 1,
        "operator": "LocalCommand", "operator_name": "LocalCommand",
    }


def task_response(task_id, tasks):
    return {
        "task_id": task_id, "task_display_name": task_id, "depends_on_past": False, "extra_links": [],
        "downstream_task_ids": [child for child, task in tasks.items() if task_id in task["upstream"]],
        "operator_name": "LocalCommand", "is_mapped": False, "trigger_rule": "all_success",
        "retries": tasks[task_id]["retries"], "template_fields": [],
        "retry_exponential_backoff": False, "wait_for_downstream": False,
    }


def query_int(query, name, default=0):
    try:
        result = int(query.get(name, [default])[0])
    except ValueError:
        raise APIError(422, f"{name} must be an integer") from None
    return result


def collection(name, rows, query):
    limit = query_int(query, "limit", 50)
    offset = query_int(query, "offset")
    if limit < 0 or offset < 0 or limit > 100:
        raise APIError(422, "offset must be nonnegative and limit must be between zero and 100")
    return {name: rows[offset:offset + limit] if limit else rows[offset:], "total_entries": len(rows)}


class AirflowHTTPServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, address, service, *, token=None):
        super().__init__(address, AirflowHandler)
        self.service = service
        self.token = token


class AirflowHandler(BaseHTTPRequestHandler):
    timeout = 5

    def _body(self):
        if self.headers.get("Transfer-Encoding"):
            raise APIError(400, "Chunked request bodies are unsupported")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise APIError(400, "Invalid Content-Length") from None
        if length < 0 or length > MAX_BODY:
            raise APIError(413, "Request body is too large")
        try:
            value = json.loads(self.rfile.read(length)) if length else {}
            if not isinstance(value, dict):
                raise ValueError
            json.dumps(value, allow_nan=False)
        except (ValueError, RecursionError):
            raise APIError(422, "Expected a JSON object") from None
        return value

    def _authenticate(self, value):
        token = self.server.token
        return token is None or (isinstance(value, str) and hmac.compare_digest(value.encode(), token.encode()))

    def _dispatch(self):
        try:
            parsed = urlsplit(self.path)
            path = [unquote(part) for part in parsed.path.strip("/").split("/")]
            query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=128)
            body = self._route(path, query)
            self._send(200, body)
        except APIError as exc:
            detail = str(exc)
            if exc.status == 422:
                detail = [{"loc": ["request"], "msg": detail, "type": "value_error"}]
            self._send(exc.status, {"detail": detail})
        except ValueError as exc:
            self._send(400, {"detail": str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send(self, status, data):
        encoded = json.dumps(data, ensure_ascii=True, allow_nan=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            if status == 401:
                self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_PATCH(self):
        self._dispatch()

    def _route(self, path, query):
        service = self.server.service
        method = self.command
        if path == ["auth", "token"] and method == "POST":
            body = self._body()
            if self.server.token is None or body.get("username") != "airflow" or not self._authenticate(body.get("password")):
                raise APIError(401, "Invalid username or password")
            return {"access_token": self.server.token}
        scheme, _, token = self.headers.get("Authorization", "").partition(" ")
        if not self._authenticate(token if scheme.lower() == "bearer" else None):
            raise APIError(401, "Invalid bearer token")
        if path[:2] != ["api", "v2"]:
            raise APIError(404, "Endpoint was not found")
        path = path[2:]
        if path == ["version"] and method == "GET":
            return {"version": "3.1.0", "git_version": "jjap"}
        if path == ["monitor", "health"] and method == "GET":
            return {"metadatabase": {"status": "healthy"},
                    "scheduler": {"status": "healthy" if service.scheduler.is_alive() else "unhealthy"}}
        if path == ["dags"] and method == "GET":
            dags = service.dags()
            if query.get("exclude_stale", ["true"])[0].lower() != "false":
                dags = [dag for dag in dags if dag["active"]]
            if "paused" in query:
                paused = query["paused"][0].lower()
                if paused not in {"true", "false"}:
                    raise APIError(422, "paused must be a boolean")
                dags = [dag for dag in dags if bool(dag["paused"]) == (paused == "true")]
            return collection("dags", [dag_response(row) for row in dags], query)
        if len(path) < 2 or path[0] != "dags":
            raise APIError(404, "Endpoint was not found")
        dag_id, rest = path[1], path[2:]
        if method == "GET" and rest in ([], ["details"]):
            return dag_response(service.dag(dag_id), details=bool(rest))
        if method == "PATCH" and not rest:
            body = self._body()
            if set(body) != {"is_paused"} or not isinstance(body["is_paused"], bool):
                raise APIError(422, "Expected an is_paused boolean")
            return dag_response(service.pause(dag_id, body["is_paused"]))
        if method == "GET" and 1 <= len(rest) <= 2 and rest[0] == "tasks":
            tasks = json.loads(service.dag(dag_id)["spec"])["tasks"]
            if len(rest) == 1:
                return collection("tasks", [task_response(task_id, tasks) for task_id in tasks], query)
            if rest[1] in tasks:
                return task_response(rest[1], tasks)
            raise APIError(404, f"Task {rest[1]} was not found")
        if rest == ["dagRuns"]:
            if method == "POST":
                return run_response(service.trigger(dag_id, self._body()))
            if method == "GET":
                return collection("dag_runs", [run_response(row) for row in service.runs(dag_id)
                                               if ("state" not in query or row["state"] in query["state"])
                                               and ("run_type" not in query or row["run_type"] in query["run_type"])], query)
        if method == "GET" and len(rest) >= 2 and rest[0] == "dagRuns":
            return self._get_run(dag_id, rest[1], rest[2:], query)
        raise APIError(404, "Endpoint was not found")

    def _get_run(self, dag_id, run_id, rest, query):
        service = self.server.service
        if not rest:
            return run_response(service.run(dag_id, run_id))
        if rest[0] != "taskInstances":
            raise APIError(404, "Endpoint was not found")
        if len(rest) == 4 and rest[2] == "logs":
            if query_int(query, "map_index", -1) != -1 or not re.fullmatch(r"[0-9]{1,9}", rest[3]):
                raise APIError(404, "Task attempt was not found")
            return service.log((dag_id, run_id, rest[1]), int(rest[3]))
        run, rows = service.tasks(dag_id, run_id)
        specs = json.loads(run["spec"])["tasks"]
        if len(rest) == 1:
            return collection("task_instances", [task_instance_response(row, run, specs[row["task_id"]]) for row in rows
                                                  if "state" not in query or row["state"] in query["state"]], query)
        if len(rest) == 2 or (len(rest) == 3 and rest[2] == "-1"):
            for row in rows:
                if row["task_id"] == rest[1]:
                    return task_instance_response(row, run, specs[row["task_id"]])
        raise APIError(404, "Task instance was not found")


def read_token(filename):
    if filename is None:
        return None
    token = Path(filename).read_text(encoding="ascii").strip()
    if not token or any(not 33 <= ord(char) <= 126 for char in token):
        raise ValueError("Token file must contain one nonempty ASCII token")
    return token


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--dag-file", action="append", default=[], help="JSON DAG or DAG array, repeatable")
    parser.add_argument("--db-file", help="SQLite database path (default: in memory)")
    parser.add_argument("--password-file", help="File containing the API bearer token and airflow login password")
    parser.add_argument("--openlineage-url", help="Complete OpenLineage HTTP event endpoint")
    parser.add_argument("--openlineage-namespace", default="airflow", help="Namespace for OpenLineage jobs")
    parser.add_argument("--openlineage-token-file", help="File containing the OpenLineage bearer token")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    try:
        if args.openlineage_url:
            lineage = urlsplit(args.openlineage_url)
            if lineage.scheme not in {"http", "https"} or not lineage.hostname or lineage.username is not None:
                raise ValueError("--openlineage-url must be an HTTP(S) URL without user information")
        if not args.openlineage_namespace:
            raise ValueError("--openlineage-namespace must not be empty")
        token = read_token(args.password_file)
        service = AirflowService(read_dags(args.dag_file), args.db_file, lineage_url=args.openlineage_url,
                                 lineage_namespace=args.openlineage_namespace,
                                 lineage_token=read_token(args.openlineage_token_file))
        try:
            with AirflowHTTPServer((args.host, args.port), service, token=token) as httpd:
                service.start()
                print(f"Airflow REST API listening on http://{args.host}:{httpd.server_port}/api/v2", flush=True)
                httpd.serve_forever()
        finally:
            service.close()
    except (OSError, sqlite3.Error, ValueError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    except KeyboardInterrupt:
        return 130
    return 0


def terminate(_signal, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    raise SystemExit(main())
