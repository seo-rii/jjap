#!/usr/bin/env python3
"""A single-file OpenLineage collector with basic Marquez-style queries."""

import argparse
import gzip
import hashlib
import hmac
import io
import json
import re
import sqlite3
import threading
import uuid
import zlib
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit


EVENT_LIMIT = 4 * 1024 * 1024
STATES = {
    "START": "RUNNING", "RUNNING": "RUNNING", "COMPLETE": "COMPLETED",
    "ABORT": "ABORTED", "FAIL": "FAILED",
}
TERMINAL_EVENTS = {"COMPLETE", "ABORT", "FAIL"}
EVENT_TIME = re.compile(r"(\d{4}-\d\d-\d\d[Tt]\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|z|[+-]\d\d:\d\d)?")


class RequestError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def timestamp(value):
    match = EVENT_TIME.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError("eventTime must be an ISO 8601 timestamp")
    wall_time, fraction, offset = match.groups()
    # Normalize fractional precision for Python 3.10 and treat naive client timestamps as UTC.
    micros = (fraction or "")[:6].ljust(6, "0")
    offset = "+00:00" if offset in (None, "Z", "z") else offset
    parsed = datetime.fromisoformat(f"{wall_time}.{micros}{offset}")
    return parsed.astimezone(timezone.utc)


def identity(entity):
    if not isinstance(entity, dict) or any(
        not isinstance(entity.get(key), str) or not entity[key] for key in ("namespace", "name")
    ):
        raise ValueError("A job or dataset requires a namespace and name")
    namespace, name = entity["namespace"], entity["name"]
    namespace.encode("utf-8")
    name.encode("utf-8")
    return namespace, name


def reference(key):
    return {"namespace": key[0], "name": key[1]}


def validate_facets(entity, *keys):
    for key in keys:
        facets = entity.get(key)
        if facets is not None and (
            not isinstance(facets, dict) or any(not isinstance(facet, dict) for facet in facets.values())
        ):
            raise ValueError(f"{key} must map facet names to objects")


def event_row(event):
    if not isinstance(event, dict):
        raise ValueError("Expected an OpenLineage event object")
    for key in ("producer", "schemaURL"):
        if not isinstance(event.get(key), str) or not event[key]:
            raise ValueError(f"{key} is required")
    time = timestamp(event.get("eventTime")).isoformat(timespec="microseconds")
    run_id = namespace = job_name = None
    if "job" in event:
        if "dataset" in event:
            raise ValueError("Expected a run, job, or dataset event")
        namespace, job_name = identity(event["job"])
        validate_facets(event["job"], "facets")
        if "run" in event:
            run = event["run"]
            if not isinstance(run, dict) or not isinstance(run.get("runId"), str):
                raise ValueError("run.runId must be a UUID")
            run_id = str(uuid.UUID(run["runId"]))
            validate_facets(run, "facets")
        event_type = event.get("eventType")
        if event_type is not None and (
            not run_id or not isinstance(event_type, str) or
            (event_type not in STATES and event_type != "OTHER")
        ):
            raise ValueError("Unrecognized run eventType")
        for direction in ("inputs", "outputs"):
            datasets = event.get(direction, [])
            if datasets is None:
                continue
            if not isinstance(datasets, list):
                raise ValueError(f"{direction} must be an array of datasets")
            for dataset in datasets:
                identity(dataset)
                validate_facets(dataset, "facets", "inputFacets", "outputFacets")
    elif "dataset" in event and "run" not in event and "eventType" not in event:
        identity(event["dataset"])
        validate_facets(event["dataset"], "facets")
    else:
        raise ValueError("Expected a run, job, or dataset event")
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).digest(), time, run_id, namespace, job_name, payload


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path or ":memory:", check_same_thread=False)
        self.lock = threading.Lock()
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                seq INTEGER PRIMARY KEY,
                digest BLOB NOT NULL UNIQUE,
                occurred TEXT NOT NULL,
                run_id TEXT,
                job_namespace TEXT,
                job_name TEXT,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS lineage_events_time ON lineage_events (occurred, seq);
            CREATE INDEX IF NOT EXISTS lineage_events_run ON lineage_events (run_id, occurred, seq);
        """)

    def add(self, events):
        try:
            rows = [event_row(event) for event in events]
        except (ValueError, OverflowError, RecursionError) as error:
            raise RequestError(400, str(error)) from None
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            for row in rows:
                run_id = row[2]
                if run_id:
                    existing = self.connection.execute(
                        "SELECT job_namespace, job_name FROM lineage_events WHERE run_id = ? LIMIT 1",
                        (run_id,),
                    ).fetchone()
                    if existing is not None and existing != row[3:5]:
                        raise RequestError(409, "runId already belongs to a different job")
                self.connection.execute("""
                    INSERT INTO lineage_events (digest, occurred, run_id, job_namespace, job_name, payload)
                    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(digest) DO NOTHING
                """, row)

    def events(self, run_id=None):
        with self.lock:
            if run_id is None:
                rows = self.connection.execute(
                    "SELECT occurred, payload FROM lineage_events ORDER BY occurred, seq",
                ).fetchall()
            else:
                rows = self.connection.execute("""
                    SELECT occurred, payload FROM lineage_events WHERE run_id = ? ORDER BY occurred, seq
                """, (run_id,)).fetchall()
        return [(occurred, json.loads(payload)) for occurred, payload in rows]


def touch(entity, occurred, facets=None):
    entity.setdefault("createdAt", occurred)
    entity["updatedAt"] = occurred
    entity.setdefault("facets", {}).update(facets or {})


class Projection:
    """Rebuild a read snapshot in event-time order, including late events."""

    def __init__(self, events):
        self.namespaces = {}
        self.jobs = {}
        self.datasets = {}
        self.runs = {}
        self.job_runs = defaultdict(list)
        for occurred, event in events:
            if "dataset" in event:
                self.dataset(event["dataset"], occurred)
                continue
            job_key = identity(event["job"])
            self.namespace(job_key[0], occurred)
            job = self.jobs.setdefault(job_key, {"id": reference(job_key), **reference(job_key),
                                                "inputs": [], "outputs": []})
            touch(job, occurred, event["job"].get("facets"))
            job["type"] = job["facets"].get("jobType", {}).get("processingType", "BATCH")
            run = self.apply_run(job_key, event, occurred) if "run" in event else None
            for direction in ("inputs", "outputs"):
                for observation in event.get(direction) or []:
                    key = self.dataset(observation, occurred)
                    dataset_ref = reference(key)
                    if dataset_ref not in job[direction]:
                        job[direction].append(dataset_ref)
                    if run is not None:
                        facet_key = "inputFacets" if direction == "inputs" else "outputFacets"
                        data = run[direction].setdefault(key, {**dataset_ref, facet_key: {}})
                        data[facet_key].update(observation.get(facet_key) or {})
        for run in self.runs.values():
            run["inputs"] = list(run["inputs"].values())
            run["outputs"] = list(run["outputs"].values())
            start, end = run["startedAt"], run["endedAt"]
            run["durationMs"] = (timestamp(end) - timestamp(start)) // timedelta(milliseconds=1) if start and end else None
        for key, job in self.jobs.items():
            runs = self.runs_for(key)
            job["latestRun"] = runs[0] if runs else None

    def namespace(self, name, occurred):
        entry = self.namespaces.setdefault(name, {"name": name, "ownerName": "anonymous", "description": None})
        entry.setdefault("createdAt", occurred)
        entry["updatedAt"] = occurred

    def dataset(self, observation, occurred):
        key = identity(observation)
        self.namespace(key[0], occurred)
        entry = self.datasets.setdefault(key, {
            "id": reference(key), **reference(key), "type": "DB_TABLE", "physicalName": key[1],
            "sourceName": key[0], "tags": [], "fields": [], "description": None,
        })
        touch(entry, occurred, observation.get("facets"))
        fields = entry["facets"].get("schema", {}).get("fields", [])
        if isinstance(fields, list):
            entry["fields"] = [{"tags": [], **field} for field in fields if isinstance(field, dict)]
        return key

    def apply_run(self, job_key, event, occurred):
        run_data = event["run"]
        run_id = str(uuid.UUID(run_data["runId"]))
        if run_id not in self.runs:
            self.job_runs[job_key].append(run_id)
            self.runs[run_id] = {
                "id": run_id, "job": reference(job_key), "state": "NEW", "startedAt": None,
                "endedAt": None, "inputs": {}, "outputs": {}, "args": {},
            }
        run = self.runs[run_id]
        touch(run, occurred, run_data.get("facets"))
        nominal_time = run["facets"].get("nominalTime", {})
        run["nominalStartTime"] = nominal_time.get("nominalStartTime")
        run["nominalEndTime"] = nominal_time.get("nominalEndTime")
        event_type = event.get("eventType")
        if event_type == "START" and run["startedAt"] is None and run["endedAt"] is None:
            run["startedAt"] = occurred
        if event_type in STATES and run["endedAt"] is None:
            run["state"] = STATES[event_type]
            if event_type in TERMINAL_EVENTS:
                run["endedAt"] = occurred
        return run

    def runs_for(self, job_key):
        return sorted((self.runs[run_id] for run_id in self.job_runs.get(job_key, ())),
                      key=lambda run: run["createdAt"], reverse=True)

    def graph(self, node_id, depth):
        def node(kind, key):
            return f"{kind}:{key[0]}:{key[1]}"

        nodes = {node("job", key): ("JOB", job) for key, job in self.jobs.items()}
        nodes.update({node("dataset", key): ("DATASET", dataset) for key, dataset in self.datasets.items()})
        if node_id not in nodes:
            raise RequestError(404, "Lineage node not found")
        incoming = defaultdict(set)
        outgoing = defaultdict(set)
        for key, job in self.jobs.items():
            job_id = node("job", key)
            for dataset in job["inputs"]:
                dataset_id = node("dataset", identity(dataset))
                incoming[job_id].add(dataset_id)
                outgoing[dataset_id].add(job_id)
            for dataset in job["outputs"]:
                dataset_id = node("dataset", identity(dataset))
                incoming[dataset_id].add(job_id)
                outgoing[job_id].add(dataset_id)
        visited = {node_id}
        queue = deque([(node_id, 0)])
        while queue:
            current, distance = queue.popleft()
            if distance < depth:
                for neighbor in (incoming[current] | outgoing[current]) - visited:
                    visited.add(neighbor)
                    queue.append((neighbor, distance + 1))
        graph = []
        for current in sorted(visited):
            kind, data = nodes[current]
            graph.append({
                "id": current, "type": kind, "data": data,
                "inEdges": [{"origin": origin, "destination": current}
                            for origin in sorted(incoming[current] & visited)],
                "outEdges": [{"origin": current, "destination": target}
                             for target in sorted(outgoing[current] & visited)],
            })
        return {"graph": graph}


def integer(params, key, default, maximum):
    try:
        value = int(params.get(key, [default])[-1])
    except ValueError:
        raise RequestError(400, f"Invalid {key}") from None
    if not 0 <= value <= maximum:
        raise RequestError(400, f"{key} must be between 0 and {maximum}")
    return value


def page(key, values, params):
    offset = integer(params, "offset", 0, 2**31 - 1)
    limit = integer(params, "limit", 100, 10_000)
    return {key: values[offset:offset + limit], "totalCount": len(values)}


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(15)
        super().setup()

    def respond(self, status, payload=None):
        body = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        if status == 401:
            self.send_header("WWW-Authenticate", "Bearer")
        if body:
            self.send_header("Content-Type", "application/json")
        if status != 204:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def read_json(self):
        if self.headers.get("Transfer-Encoding"):
            raise RequestError(400, "Expected a Content-Length header")
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            raise RequestError(411, "Content-Length required") from None
        if length < 0 or length > EVENT_LIMIT:
            raise RequestError(413, "Event body exceeds the size limit")
        if self.headers.get_content_type() != "application/json":
            raise RequestError(415, "Expected application/json")
        encoding = self.headers.get("Content-Encoding", "identity").lower()
        if encoding not in {"identity", "gzip"}:
            raise RequestError(415, "Unsupported Content-Encoding")
        try:
            body = self.rfile.read(length)
            if len(body) != length:
                raise RequestError(400, "Incomplete request body")
            if encoding == "gzip":
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                    body = stream.read(EVENT_LIMIT + 1)
                if len(body) > EVENT_LIMIT:
                    raise RequestError(413, "Decoded event body exceeds the size limit")
            return json.loads(body)
        except TimeoutError:
            raise RequestError(408, "Request body timed out") from None
        except (ValueError, OSError, EOFError, RecursionError, zlib.error):
            raise RequestError(400, "Invalid JSON or gzip request body") from None

    def dispatch(self):
        url = urlsplit(self.path)
        path = url.path.rstrip("/")
        if self.command == "GET" and path == "/health":
            return 200, {"status": "ok"}
        authorization = self.headers.get("Authorization", "")
        if self.server.token and not hmac.compare_digest(authorization.encode(), self.server.token):
            raise RequestError(401, "A bearer token is required")
        if self.command == "POST":
            if path == "/api/v1/lineage":
                self.server.store.add([self.read_json()])
                return 200, None
            if path == "/api/v1/lineage/batch":
                events = self.read_json()
                if not isinstance(events, list):
                    raise RequestError(400, "Expected an array of events")
                self.server.store.add(events)
                return 204, None
        if self.command == "GET" and path.startswith("/api/v1/"):
            segments = [unquote(part) for part in path[8:].split("/")]
            params = parse_qs(url.query)
            return 200, self.query(segments, params)
        raise RequestError(404, "Endpoint not found")

    def query(self, segments, params):
        store = self.server.store
        match segments:
            case ["runs", run_id] | ["runs", run_id, "events"]:
                try:
                    run_id = str(uuid.UUID(run_id))
                except ValueError:
                    raise RequestError(400, "Run ID must be a UUID") from None
                events = store.events(run_id)
                if not events:
                    raise RequestError(404, "Run not found")
                if len(segments) == 3:
                    return {"events": [event for _, event in events]}
                return Projection(events).runs[run_id]
        result = Projection(store.events())
        match segments:
            case ["lineage"]:
                node_id = params.get("nodeId", [""])[-1]
                if not node_id:
                    raise RequestError(400, "nodeId is required")
                return result.graph(node_id, integer(params, "depth", 20, 100))
            case ["namespaces"]:
                return page("namespaces", [result.namespaces[key] for key in sorted(result.namespaces)], params)
            case ["namespaces", namespace, *rest] if namespace in result.namespaces:
                return self.namespace_query(result, namespace, rest, params)
            case _:
                raise RequestError(404, "Endpoint or namespace not found")

    def namespace_query(self, result, namespace, segments, params):
        if not segments:
            return result.namespaces[namespace]
        collection = {"jobs": result.jobs, "datasets": result.datasets}.get(segments[0])
        if collection is not None:
            if len(segments) == 1:
                return page(segments[0], [collection[key] for key in sorted(collection) if key[0] == namespace], params)
            key = (namespace, segments[1])
            if key in collection:
                if len(segments) == 2:
                    return collection[key]
                if segments[0] == "jobs" and segments[2:] == ["runs"]:
                    return page("runs", result.runs_for(key), params)
        raise RequestError(404, "Entity not found")

    def handle_http(self):
        try:
            status, payload = self.dispatch()
        except RequestError as error:
            status, payload = error.status, {"code": error.status, "message": str(error)}
        except ValueError:
            status, payload = 400, {"code": 400, "message": "Invalid request URL"}
        except sqlite3.Error:
            status, payload = 500, {"code": 500, "message": "Lineage storage operation failed"}
        try:
            self.respond(status, payload)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    do_GET = do_POST = handle_http


class Server(ThreadingHTTPServer):
    daemon_threads = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--db-file", type=Path, help="SQLite file for lineage events")
    parser.add_argument("--password-file", type=Path, help="File containing the bearer token")
    args = parser.parse_args()
    token = None
    if args.password_file:
        try:
            token = args.password_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as error:
            parser.error(str(error))
        if not token or not token.isascii() or any(char.isspace() for char in token):
            parser.error("The password file must contain one nonempty ASCII token")
        token = f"Bearer {token}".encode()
    store = Store(args.db_file)
    with closing(store.connection), Server((args.host, args.port), Handler) as server:
        server.store = store
        server.token = token
        print(f"OpenLineage server listening on {args.host}:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
