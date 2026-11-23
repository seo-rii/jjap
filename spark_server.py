#!/usr/bin/env python3
"""Spark Standalone submission REST subset for registered local SQL jobs."""

import argparse
import csv
import datetime as dt
import hashlib
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
import tempfile
import threading
import time
from urllib.parse import unquote, urlsplit


SPARK_VERSION = "4.1.1"
LOCAL_MAIN_CLASS = "jjap.LocalSqlJob"
MAX_REQUEST = 1024 * 1024
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
JOB_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
SQL_TYPES = {"string": "TEXT", "int": "INTEGER", "long": "INTEGER", "double": "REAL", "boolean": "INTEGER"}
QUERY_OPERATIONS = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}


class RESTError(ValueError):
    def __init__(self, message, status=400, **fields):
        super().__init__(message)
        self.status = status
        self.fields = fields


class JobCanceled(Exception):
    pass


def check_object(value, description):
    if not isinstance(value, dict):
        raise ValueError(f"{description} must be an object")
    return value


def local_path(value, directory, field):
    if not isinstance(value, str) or not value or "\0" in value or "://" in value:
        raise ValueError(f"{field} must be a local filesystem path")
    return str((directory / value).resolve())


def normalize_relation(value, directory, *, output=False):
    options = check_object(value, "Relation")
    kind = options.get("format")
    if kind not in ("csv", "jsonl", "iceberg"):
        raise ValueError("Relation format must be csv, jsonl, or iceberg")
    schema = check_object(options.get("schema"), "Relation schema")
    if not schema or any(not IDENTIFIER.fullmatch(name) or not isinstance(typ, str) or typ not in SQL_TYPES
                         for name, typ in schema.items()):
        raise ValueError("Schema must map column names to string, int, long, double, or boolean")
    if len({column.lower() for column in schema}) != len(schema):
        raise ValueError("Schema column names must be unique ignoring case")
    relation = {"format": kind, "schema": list(schema.items())}
    allowed = {"format", "schema"}
    if kind == "iceberg":
        allowed.update({"warehouse", "table", "db_file"})
        if not output:
            allowed.add("snapshot_id")
        identifier = options.get("table")
        if not isinstance(identifier, str) or not re.fullmatch(r"[\w-]+(?:\.[\w-]+)+", identifier, re.ASCII):
            raise ValueError("Iceberg table must be a qualified namespace.table identifier")
        relation["table"] = identifier
        relation["warehouse"] = local_path(options.get("warehouse"), directory, "warehouse")
        if "db_file" in options:
            relation["db_file"] = local_path(options["db_file"], directory, "db_file")
        if "snapshot_id" in options:
            snapshot = options["snapshot_id"]
            if type(snapshot) is not int or not 0 < snapshot < 2**63:
                raise ValueError("snapshot_id must be a positive signed 64-bit integer")
            relation["snapshot_id"] = snapshot
    else:
        allowed.add("path")
        relation["path"] = local_path(options.get("path"), directory, "path")
        if kind == "csv":
            allowed.update({"delimiter", "header"})
            delimiter = options.get("delimiter", ",")
            header = options.get("header", True)
            if not isinstance(delimiter, str) or len(delimiter) != 1 or delimiter in {'\r', '\n', '"', '\0'}:
                raise ValueError("CSV delimiter must be one character other than a quote, newline, or NUL")
            if type(header) is not bool:
                raise ValueError("CSV header must be a boolean")
            relation.update(delimiter=delimiter, header=header)
        if output:
            allowed.add("overwrite")
            overwrite = options.get("overwrite", False)
            if type(overwrite) is not bool:
                raise ValueError("overwrite must be a boolean")
            relation["overwrite"] = overwrite
    unknown = set(options) - allowed
    if unknown:
        raise ValueError(f"Unsupported relation fields: {', '.join(sorted(unknown))}")
    return relation


def load_jobs(path):
    path = Path(path).resolve()
    config = check_object(json.loads(path.read_text(encoding="utf-8")), "Configuration")
    entries = check_object(config.get("jobs"), "jobs")
    if not entries or set(config) != {"jobs"}:
        raise ValueError("Configuration must contain a nonempty jobs object")
    jobs = {}
    for name, entry in entries.items():
        if not JOB_ID.fullmatch(name):
            raise ValueError(f"Invalid job name {name!r}")
        job = check_object(entry, f"Job {name}")
        if set(job) - {"inputs", "sql", "output", "timeout_seconds"}:
            raise ValueError(f"Job {name} contains unsupported fields")
        inputs = check_object(job.get("inputs", {}), "inputs")
        if any(not IDENTIFIER.fullmatch(table) for table in inputs) or len({table.lower() for table in inputs}) != len(inputs):
            raise ValueError(f"Job {name} has invalid or duplicate input table names")
        sql = job.get("sql")
        if not isinstance(sql, str) or not sql.strip() or len(sql) > 65_536 or "\0" in sql:
            raise ValueError(f"Job {name} requires a SQL query of at most 65536 characters")
        timeout = job.get("timeout_seconds", 60)
        if type(timeout) not in (int, float) or not 0.05 <= timeout <= 3600:
            raise ValueError(f"Job {name} timeout_seconds must be between 0.05 and 3600")
        normalized = {"inputs": {table: normalize_relation(value, path.parent) for table, value in inputs.items()},
                      "sql": sql, "output": normalize_relation(job.get("output"), path.parent, output=True),
                      "timeout_seconds": timeout}
        jobs[name] = normalized
    return jobs


def job_digest(job):
    return hashlib.sha256(json.dumps(job, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def scalar(value, kind, source):
    if value is None or (source == "csv" and value == ""):
        return None
    if kind == "string" and isinstance(value, str):
        return value
    if kind in ("int", "long"):
        if source == "csv" and re.fullmatch(r"[+-]?[0-9]+", value):
            value = int(value)
        bits = 32 if kind == "int" else 64
        if type(value) is int and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            return value
    if kind == "double":
        if source == "csv" or type(value) in (int, float):
            number = float(value)
            if math.isfinite(number):
                return number
    if kind == "boolean":
        if source == "csv" and value.strip().lower() in ("0", "1", "false", "true"):
            return value.strip().lower() in ("1", "true")
        if type(value) is bool or (source == "sql" and type(value) is int and value in (0, 1)):
            return bool(value)
    raise ValueError(f"Invalid {kind} value")


def csv_rows(stream, relation):
    schema = relation["schema"]
    names = [name for name, _ in schema]
    reader = csv.reader(stream, delimiter=relation["delimiter"], strict=True)
    order = list(range(len(schema)))
    if relation["header"]:
        header = next(reader, [])
        if len(header) != len(schema) or set(header) != set(names):
            raise ValueError("CSV header does not match schema")
        order = [header.index(name) for name in names]
    for row in reader:
        if len(row) != len(schema):
            raise ValueError(f"CSV record at line {reader.line_num} does not match schema width")
        yield [scalar(row[index], kind, "csv") for index, (_, kind) in zip(order, schema)]


def json_rows(stream, relation):
    schema = relation["schema"]
    names = {name for name, _ in schema}
    for number, line in enumerate(stream, 1):
        if not line.strip():
            continue
        try:
            row = check_object(json.loads(line), "JSON Lines record")
            if row.keys() - names:
                raise ValueError("Unexpected columns")
            yield [scalar(row.get(name), kind, "jsonl") for name, kind in schema]
        except (ValueError, OverflowError, RecursionError) as exc:
            raise ValueError(f"Invalid JSON Lines record at line {number}: {exc}") from exc


class JobRun:
    def __init__(self, submission_id, job, stop):
        self.identifier = submission_id
        self.job = job
        self.stop = stop
        self.canceled = threading.Event()
        self.publishing = False
        self.deadline = time.monotonic() + job["timeout_seconds"]

    def interrupted(self):
        return self.canceled.is_set() or (self.stop.is_set() and not self.publishing)

    def check(self):
        if self.interrupted():
            raise JobCanceled
        if time.monotonic() >= self.deadline:
            raise ValueError("SQL job timed out")

    def progress(self):
        return int(self.interrupted() or time.monotonic() >= self.deadline)


class SparkService:
    def __init__(self, jobs, db_file=None, *, iceberg_script=None):
        self.jobs = jobs
        self.digests = {name: job_digest(job) for name, job in jobs.items()}
        self.iceberg_script = Path(iceberg_script or Path(__file__).with_name("iceberg_server.py")).resolve()
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.active = None
        self.worker = threading.Thread(target=self._work, name="spark-sql-worker")
        self.db = None
        self.lease = None
        database = ":memory:" if db_file is None else str(Path(db_file).resolve())
        try:
            if db_file is not None:
                self.lease = sqlite3.connect(database + ".lock", timeout=0)
                try:
                    self.lease.execute("BEGIN EXCLUSIVE")
                except sqlite3.OperationalError as exc:
                    raise ValueError(f"Cannot acquire Spark scheduler lock for {database}: {exc}") from exc
            self.db = sqlite3.connect(database, check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA journal_mode=WAL")
            with self.db:
                self.db.execute("""
                    CREATE TABLE IF NOT EXISTS submissions (
                        ordinal INTEGER PRIMARY KEY, submission_id TEXT UNIQUE,
                        job TEXT NOT NULL, digest TEXT NOT NULL, state TEXT NOT NULL,
                        worker_id TEXT, message TEXT
                    )
                """)
                self.db.execute("CREATE INDEX IF NOT EXISTS queued_submissions ON submissions (state, ordinal)")
                self.db.execute("""
                    UPDATE submissions SET state='ERROR', message='Service exited before the job outcome was recorded.'
                    WHERE state='RUNNING'
                """)
        except BaseException:
            self.close()
            raise

    def start(self):
        self.worker.start()

    def close(self):
        self.stop.set()
        self.wake.set()
        if self.worker.ident is not None:
            self.worker.join()
        if self.db is not None:
            self.db.close()
        if self.lease is not None:
            self.lease.close()

    def submit(self, name):
        if name not in self.jobs:
            return {"success": False, "message": "No registered SQL job matches appResource."}
        with self.lock, self.db:
            cursor = self.db.execute("INSERT INTO submissions (job, digest, state) VALUES (?, ?, 'SUBMITTED')",
                                     (name, self.digests[name]))
            sequence = cursor.lastrowid
            identifier = f"driver-{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}-{sequence:04d}"
            self.db.execute("UPDATE submissions SET submission_id=? WHERE ordinal=?", (identifier, sequence))
        self.wake.set()
        return {"success": True, "submissionId": identifier, "message": f"Driver successfully submitted as {identifier}"}

    def status(self, identifier):
        with self.lock:
            row = self.db.execute("SELECT state, worker_id, message FROM submissions WHERE submission_id=?",
                                  (identifier,)).fetchone()
        response = {"success": row is not None, "submissionId": identifier}
        if row is not None:
            response["driverState"] = row["state"]
            if row["worker_id"] is not None:
                response["workerId"] = row["worker_id"]
            if row["message"] is not None:
                response["message"] = row["message"]
        return response

    def kill(self, identifier):
        success = False
        with self.lock, self.db:
            row = self.db.execute("SELECT state FROM submissions WHERE submission_id=?", (identifier,)).fetchone()
            if row is not None and row["state"] == "SUBMITTED":
                self._finish(identifier, "KILLED", "Canceled before execution.")
                success = True
            elif self.active is not None and self.active.identifier == identifier and not self.active.publishing:
                self.active.canceled.set()
                success = True
        self.wake.set()
        message = (f"Kill request for {identifier} submitted" if success else
                   f"Driver {identifier} is publishing output, has finished, or does not exist")
        return {"success": success, "submissionId": identifier, "message": message}

    def _finish(self, identifier, state, message):
        self.db.execute("UPDATE submissions SET state=?, message=? WHERE submission_id=?",
                        (state, message, identifier))

    def _claim(self):
        with self.lock, self.db:
            if self.stop.is_set():
                return None
            while row := self.db.execute("""
                SELECT * FROM submissions WHERE state='SUBMITTED' ORDER BY ordinal LIMIT 1
            """).fetchone():
                identifier = row["submission_id"]
                name = row["job"]
                if self.digests.get(name) != row["digest"]:
                    self._finish(identifier, "ERROR", "Registered job changed or was removed before execution.")
                    continue
                self.db.execute("UPDATE submissions SET state='RUNNING', worker_id='worker-local' WHERE submission_id=?",
                                (identifier,))
                self.active = JobRun(identifier, self.jobs[name], self.stop)
                return self.active
        return None

    def _work(self):
        while not self.stop.is_set():
            run = self._claim()
            if run is None:
                self.wake.wait(0.1)
                self.wake.clear()
                continue
            try:
                count = self._execute(run)
                state, message = "FINISHED", f"Local SQL job finished with {count} output rows."
            except JobCanceled:
                state, message = "KILLED", "Local SQL job was canceled."
            except (OSError, ValueError, sqlite3.Error, csv.Error, OverflowError, RecursionError) as exc:
                if run.publishing and run.job["output"]["format"] == "iceberg":
                    state, message = "ERROR", f"Iceberg publication did not report completion: {exc}"
                else:
                    state, message = "FAILED", str(exc)
            with self.lock, self.db:
                if run.interrupted() and not run.publishing:
                    state, message = "KILLED", "Local SQL job was canceled."
                self._finish(run.identifier, state, message[:4096])
                self.active = None

    def _companion(self, run, relation, arguments, *, stdout):
        command = [sys.executable, "-I", "-B", str(self.iceberg_script), "--warehouse", relation["warehouse"]]
        if "db_file" in relation:
            command.extend(("--db-file", relation["db_file"]))
        with tempfile.TemporaryFile() as error:
            process = subprocess.Popen([*command, *arguments], stdin=subprocess.DEVNULL, stdout=stdout, stderr=error)
            try:
                while process.poll() is None:
                    run.check()
                    run.canceled.wait(0.03)
                if process.returncode:
                    error.seek(0)
                    detail = error.read(4096).decode("utf-8", "replace").strip()
                    raise ValueError(detail or f"Iceberg command exited with status {process.returncode}")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def _input_rows(self, run, relation):
        kind = relation["format"]
        if kind == "iceberg":
            arguments = ["scan", relation["table"], "--columns", ",".join(name for name, _ in relation["schema"])]
            if "snapshot_id" in relation:
                arguments.extend(("--snapshot-id", str(relation["snapshot_id"])))
            with tempfile.TemporaryFile(mode="w+t", encoding="utf-8", newline="") as stream:
                self._companion(run, relation, arguments, stdout=stream)
                stream.seek(0)
                yield from json_rows(stream, relation)
        else:
            path = Path(relation["path"])
            if not path.is_file():
                raise ValueError(f"Input relation is not a regular file: {path}")
            with path.open(encoding="utf-8-sig", newline="") as stream:
                yield from (csv_rows(stream, relation) if kind == "csv" else json_rows(stream, relation))

    def _load_inputs(self, connection, run):
        for name, relation in run.job["inputs"].items():
            schema = relation["schema"]
            columns = ", ".join(f"{quoted(column)} {SQL_TYPES[typ]}" for column, typ in schema)
            connection.execute(f"CREATE TABLE {quoted(name)} ({columns})")
            insertion = f"INSERT INTO {quoted(name)} VALUES ({', '.join('?' for _ in schema)})"
            rows = self._input_rows(run, relation)
            try:
                for row in rows:
                    run.check()
                    connection.execute(insertion, row)
            finally:
                rows.close()
        connection.commit()

    def _write_rows(self, run, cursor, stream):
        relation = run.job["output"]
        schema = relation["schema"]
        names = [name for name, _ in schema]
        result_columns = [column[0] for column in cursor.description or ()]
        if len(result_columns) != len(names) or set(result_columns) != set(names):
            raise ValueError("SQL projection must match the registered output schema")
        indices = [result_columns.index(name) for name in names]
        writer = None
        if relation["format"] == "csv":
            writer = csv.writer(stream, delimiter=relation["delimiter"])
            if relation["header"]:
                writer.writerow(names)
        count = 0
        for row in cursor:
            run.check()
            values = [scalar(row[index], typ, "sql") for index, (_, typ) in zip(indices, schema)]
            if writer is None:
                stream.write(json.dumps(dict(zip(names, values)), ensure_ascii=True, allow_nan=False) + "\n")
            else:
                writer.writerow([str(value).lower() if type(value) is bool else value for value in values])
            count += 1
        return count

    def _execute(self, run):
        job = run.job
        output = job["output"]
        target = Path(output["path"]) if "path" in output else None
        temporary = None
        with tempfile.TemporaryDirectory() as work:
            connection = sqlite3.connect(Path(work) / "query.sqlite3")
            try:
                connection.set_progress_handler(run.progress, 1000)
                self._load_inputs(connection, run)
                connection.execute("PRAGMA query_only=ON")
                connection.set_authorizer(
                    lambda action, *_: sqlite3.SQLITE_OK if action in QUERY_OPERATIONS else sqlite3.SQLITE_DENY
                )
                cursor = connection.execute(job["sql"])
                with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", delete=False,
                                                 dir=target.parent if target is not None else work,
                                                 prefix=".jjap-spark-", suffix=".tmp") as stream:
                    temporary = Path(stream.name)
                    count = self._write_rows(run, cursor, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                with self.lock:
                    run.check()
                    run.publishing = True
                if target is None:
                    if count:
                        self._companion(run, output, ["append", output["table"], str(temporary)], stdout=subprocess.DEVNULL)
                elif output["overwrite"]:
                    os.replace(temporary, target)
                else:
                    os.link(temporary, target)
                return count
            except sqlite3.Error:
                run.check()
                raise
            finally:
                connection.close()
                if temporary is not None:
                    temporary.unlink(missing_ok=True)


def submission_resource(body):
    expected = {"action", "appResource", "mainClass", "clientSparkVersion", "sparkProperties",
                "environmentVariables", "appArgs"}
    if not isinstance(body, dict) or set(body) - expected or body.get("action") != "CreateSubmissionRequest":
        raise RESTError("Expected a Spark CreateSubmissionRequest with only the standard submission fields")
    name = body.get("appResource")
    if not isinstance(name, str) or not JOB_ID.fullmatch(name):
        raise RESTError("appResource must name a registered local SQL job")
    if body.get("mainClass") != LOCAL_MAIN_CLASS or not isinstance(body.get("clientSparkVersion"), str):
        raise RESTError(f"Submissions require mainClass={LOCAL_MAIN_CLASS} and a clientSparkVersion string")
    properties = body.get("sparkProperties", {})
    if not isinstance(properties, dict) or any(not isinstance(value, str) for value in properties.values()):
        raise RESTError("sparkProperties must map strings to strings")
    if body.get("environmentVariables", {}) != {} or body.get("appArgs", []) != []:
        raise RESTError("Registered SQL jobs receive their settings from the local configuration")
    return name


class SparkHTTPServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, address, service, *, token=None):
        super().__init__(address, SparkHandler)
        self.service = service
        self.token = token


class SparkHandler(BaseHTTPRequestHandler):
    timeout = 5

    def _read_json(self):
        length = self.headers.get("Content-Length", "")
        if self.headers.get("Transfer-Encoding") or not length.isascii() or not length.isdecimal() or len(length) > 10:
            raise RESTError("Expected a valid Content-Length for a JSON submission")
        length = int(length)
        if length > MAX_REQUEST:
            raise RESTError("Submission is too large", 413)
        payload = self.rfile.read(length)
        if len(payload) != length:
            raise RESTError("Incomplete submission body")
        try:
            return json.loads(payload)
        except (ValueError, RecursionError) as exc:
            raise RESTError("Malformed JSON submission") from exc

    def _route(self):
        token = self.server.token
        if token is not None:
            scheme, _, supplied = self.headers.get("Authorization", "").partition(" ")
            if scheme.lower() != "bearer" or not hmac.compare_digest(supplied.encode(), token.encode()):
                raise RESTError("Invalid bearer token", 401)
        path = [unquote(segment) for segment in urlsplit(self.path).path.strip("/").split("/")]
        if path[0] and path[0] != "v1":
            raise RESTError("Unknown Spark REST protocol version", 468, highestProtocolVersion="v1")
        service = self.server.service
        if path == ["v1", "submissions", "create"] and self.command == "POST":
            return {"action": "CreateSubmissionResponse", **service.submit(submission_resource(self._read_json()))}
        if len(path) == 4 and path[:2] == ["v1", "submissions"] and JOB_ID.fullmatch(path[3]):
            if path[2] == "status" and self.command == "GET":
                return {"action": "SubmissionStatusResponse", **service.status(path[3])}
            if path[2] == "kill" and self.command == "POST":
                return {"action": "KillSubmissionResponse", **service.kill(path[3])}
        raise RESTError("Use /v1/submissions/create, /status/<id>, or /kill/<id>")

    def _dispatch(self):
        try:
            status, data = 200, self._route()
        except RESTError as exc:
            status, data = exc.status, {"action": "ErrorResponse", "message": str(exc), **exc.fields}
        except ValueError:
            status, data = 400, {"action": "ErrorResponse", "message": "Invalid Spark submission request"}
        except sqlite3.Error:
            status, data = 500, {"action": "ErrorResponse", "message": "Submission state storage failed"}
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        payload = json.dumps({"serverSparkVersion": SPARK_VERSION, **data}).encode()
        try:
            self.send_response(status)
            if status == 401:
                self.send_header("WWW-Authenticate", "Bearer")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    do_GET = do_POST = _dispatch


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path, help="JSON file containing locally registered SQL jobs")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6066)
    parser.add_argument("--db-file", type=Path, help="SQLite submission state (default: in memory)")
    parser.add_argument("--password-file", type=Path, help="File containing an optional API bearer token")
    parser.add_argument("--iceberg-cli", type=Path, help="Iceberg command path (default: sibling iceberg_server.py)")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    try:
        token = args.password_file.read_text(encoding="ascii").strip() if args.password_file else None
        if token is not None and (not token or any(not 33 <= ord(char) <= 126 for char in token)):
            raise ValueError("Token file must contain one nonempty ASCII token")
        service = SparkService(load_jobs(args.config), args.db_file, iceberg_script=args.iceberg_cli)
        try:
            with SparkHTTPServer((args.host, args.port), service, token=token) as server:
                service.start()
                print(f"Spark Standalone REST listening on http://{args.host}:{server.server_port}", flush=True)
                server.serve_forever()
        finally:
            service.close()
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    except KeyboardInterrupt:
        return 130
    return 0


def terminate(_signal, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, terminate)
    raise SystemExit(main())
