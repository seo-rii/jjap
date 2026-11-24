#!/usr/bin/env python3
"""A single-file OpenLineage collector with basic Marquez-style queries."""

import argparse
import gzip
import hashlib
import hmac
import http.client
import io
import json
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlsplit


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

    def reset(self):
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute("DELETE FROM lineage_events")

    def page(self, after, before, ascending, limit, offset):
        """Marquez's GET /api/v1/events/lineage: run events only, but totalCount covers every event in the range."""
        order = "ASC" if ascending else "DESC"
        with self.lock:
            rows = self.connection.execute(f"""
                SELECT payload FROM lineage_events WHERE occurred >= ? AND occurred < ? AND run_id IS NOT NULL
                ORDER BY occurred {order}, seq {order} LIMIT ? OFFSET ?
            """, (after, before, limit, offset)).fetchall()
            total, = self.connection.execute(
                "SELECT COUNT(*) FROM lineage_events WHERE occurred >= ? AND occurred < ?", (after, before),
            ).fetchone()
        return {"events": [json.loads(payload) for payload, in rows], "totalCount": total}

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


def moment(params, key, default):
    try:
        return timestamp(params.get(key, [default])[-1]).isoformat(timespec="microseconds")
    except ValueError:
        raise RequestError(400, f"{key} must be an ISO 8601 date-time") from None


def page(key, values, params):
    offset = integer(params, "offset", 0, 2**31 - 1)
    limit = integer(params, "limit", 100, 10_000)
    return {key: values[offset:offset + limit], "totalCount": len(values)}


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(15)
        super().setup()

    def respond(self, status, payload=None):
        body = b"" if payload is None or self.command == "HEAD" else json.dumps(payload, separators=(",", ":")).encode()
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
        if path == "/_jjap/reset":
            if self.command != "POST":
                raise RequestError(405, "Method not allowed")
            self.server.store.reset()
            return 200, {"reset": True}
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
            case ["events", "lineage"]:
                direction = params.get("sortDirection", ["desc"])[-1].lower()
                if direction not in {"asc", "desc"}:
                    raise RequestError(400, "sortDirection must be asc or desc")
                return store.page(moment(params, "after", "1970-01-01T00:00:00+00:00"),
                                  moment(params, "before", "2030-01-01T00:00:00+00:00"), direction == "asc",
                                  integer(params, "limit", 100, 2**31 - 1), integer(params, "offset", 0, 2**31 - 1))
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

    do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = handle_http


class Server(ThreadingHTTPServer):
    daemon_threads = False


# NDJSON export and replay into another OpenLineage collector such as Marquez


def read_token(path):
    token = path.expanduser().read_text(encoding="utf-8").strip()
    if not token or not token.isascii() or any(char.isspace() for char in token):
        raise ValueError(f"{path} must contain one nonempty ASCII token")
    return token


def snapshot_events(filename):
    """Yield (seq, payload) from one read transaction, in the order the collector stored them."""
    source = Path(filename).expanduser().resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"Expected a SQLite persistence file: {source}")
    connection = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, isolation_level=None)
    with closing(connection):
        connection.execute("BEGIN")
        columns = {row[1] for row in connection.execute("PRAGMA table_info(lineage_events)")}
        if not {"seq", "digest", "occurred", "run_id", "payload"} <= columns:
            raise ValueError("Expected an OpenLineage test server SQLite persistence file")
        for seq, payload in connection.execute("SELECT seq, payload FROM lineage_events ORDER BY seq"):
            if not isinstance(payload, str) or not isinstance(json.loads(payload), dict):
                raise ValueError(f"Stored event {seq} is not a JSON object")
            yield seq, payload


def export_events(filename, destination):
    count = 0
    with destination.open("xb") as raw:
        stream = gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) if destination.name.endswith(".gz") else raw
        for _, payload in snapshot_events(filename):
            stream.write(payload.encode("utf-8") + b"\n")
            count += 1
        if stream is not raw:
            stream.close()
    return count


def export_main(argv):
    parser = argparse.ArgumentParser(prog="openlineage_server.py export",
                                     description="Export stored OpenLineage events as NDJSON (gzip when OUT ends with .gz).")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--out", type=Path, required=True, help="new .ndjson or .ndjson.gz file")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output file already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".openlineage-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / destination.name
            count = export_events(args.db_file, staged)
            if os.name == "nt":
                staged.rename(destination)
            else:
                os.link(staged, destination)
    except (OSError, ValueError, sqlite3.Error) as error:
        parser.exit(1, f"OpenLineage export failed: {error}\n")
    print(f"Exported {count} OpenLineage events to {destination}")
    return 0


def post_events(url, body, headers, retries, timeout):
    """POST once, retrying 429/5xx responses and connection failures with a bounded backoff."""
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                response.read()
                return None
        except urllib.error.HTTPError as error:
            detail = error.read(500).decode("utf-8", "replace").strip()
            problem = f"HTTP {error.code}{': ' + detail if detail else ''}"
            if error.code != 429 and error.code < 500:
                return problem
        except (urllib.error.URLError, OSError, http.client.HTTPException) as error:
            problem = ConnectionError(f"{url}: {getattr(error, 'reason', error)}")
        if attempt < retries:
            time.sleep(min(0.25 * 2 ** attempt, 4))
    return problem


def migrate_main(argv):
    parser = argparse.ArgumentParser(prog="openlineage_server.py migrate",
                                     description="Replay stored OpenLineage events, in stored order, into a running collector.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--target", required=True, help="collector base URL, for example http://localhost:5000")
    parser.add_argument("--token-file", type=Path, help="file containing a bearer token for the target")
    parser.add_argument("--endpoint", default="api/v1/lineage", help="event endpoint below the target (default: %(default)s)")
    parser.add_argument("--batch-size", type=int, default=0,
                        help="POST arrays of up to N events to ENDPOINT/batch instead of one event per request "
                             "(default: 100 when ENDPOINT ends with /batch, else one per request)")
    parser.add_argument("--retries", type=int, default=3, help="retries per request on 429, 5xx, or connection errors")
    parser.add_argument("--timeout", type=float, default=30, help="seconds per HTTP request")
    parser.add_argument("--dry-run", action="store_true", help="read the events and check the target without posting anything")
    args = parser.parse_args(argv)
    target = args.target.rstrip("/")
    if urlsplit(target).scheme not in {"http", "https"} or args.batch_size < 0 or args.retries < 0 or args.timeout <= 0:
        parser.error("--target must be an http(s) URL, --batch-size and --retries not negative, --timeout positive")
    endpoint = args.endpoint.strip("/")
    batch_size = args.batch_size or (100 if endpoint.endswith("/batch") else 0)
    if batch_size and not endpoint.endswith("/batch"):
        endpoint += "/batch"
    url = f"{target}/{endpoint}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    sent = failed = 0
    try:
        if args.token_file:
            headers["Authorization"] = f"Bearer {read_token(args.token_file)}"
        if args.dry_run:
            return dry_run(parser, args, target, url, headers, batch_size)
        pending = []

        def flush():
            nonlocal sent, failed
            batch = [payload for _, payload in pending]
            body = ("[" + ",".join(batch) + "]" if batch_size else batch[0]).encode("utf-8")
            problem = post_events(url, body, headers, args.retries, args.timeout)
            if isinstance(problem, ConnectionError):
                raise problem
            if problem:
                first, last = pending[0][0], pending[-1][0]
                label = f"event seq {first}" if first == last else f"events seq {first}-{last}"
                print(f"OpenLineage migrate: {label} rejected: {problem}", file=sys.stderr)
                failed += len(pending)
            else:
                sent += len(pending)
            pending.clear()

        for item in snapshot_events(args.db_file):
            pending.append(item)
            if len(pending) >= max(batch_size, 1):
                flush()
        if pending:
            flush()
    except (OSError, ValueError, sqlite3.Error) as error:
        parser.exit(1, f"OpenLineage migrate failed after {sent} events: {error}\n")
    if failed:
        parser.exit(1, f"OpenLineage migrate failed: {failed} of {sent + failed} events were rejected by {url}\n")
    print(f"Migrated {sent} OpenLineage events to {url}")
    return 0


def fetch_json(url, headers, retries, timeout):
    """GET a JSON document, retrying 429/5xx responses and connection failures; return (status, payload)."""
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, headers={key: value for key, value in headers.items() if key != "Content-Type"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read(500)
        except (urllib.error.URLError, OSError, http.client.HTTPException) as error:
            if attempt == retries:
                raise ConnectionError(f"{url}: {getattr(error, 'reason', error)}") from None
            time.sleep(min(0.25 * 2 ** attempt, 4))
            continue
        if (status == 429 or status >= 500) and attempt < retries:
            time.sleep(min(0.25 * 2 ** attempt, 4))
            continue
        try:
            return status, json.loads(raw) if raw else None
        except ValueError:
            return status, raw.decode("utf-8", "replace").strip()


def utc_text(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def dry_run(parser, args, target, url, headers, batch_size):
    """Read the snapshot and check that the target answers and accepts the credentials; post nothing."""
    count, times = 0, []
    for _, payload in snapshot_events(args.db_file):
        count += 1
        times.append(timestamp(json.loads(payload)["eventTime"]))
    requests = -(-count // batch_size) if batch_size else count
    status, payload = fetch_json(f"{target}/api/v1/namespaces?limit=1", headers, args.retries, args.timeout)
    if status in {401, 403}:
        parser.exit(1, f"OpenLineage migrate failed: dry run: the target refused the credentials: HTTP {status}\n")
    if status == 200 and times:
        latest = max(times)
        latest = latest + timedelta(microseconds=1) if latest.year < 9999 else latest
        query = urlencode({"after": utc_text(min(times)), "before": utc_text(latest), "limit": 0})
        status, payload = fetch_json(f"{target}/api/v1/events/lineage?{query}", headers, args.retries, args.timeout)
        if status == 200 and isinstance(payload, dict) and payload.get("totalCount"):
            print(f"warning: the target already holds {payload['totalCount']} events in the same time range; "
                  "Marquez does not deduplicate, so they would be stored again", file=sys.stderr)
    elif status != 200:
        print(f"warning: GET {target}/api/v1/namespaces returned HTTP {status}; the target's read API was not checked",
              file=sys.stderr)
    print(f"would post {count} events in {requests} requests to {url}")
    print(f"Dry run: would migrate {count} OpenLineage events to {url}")
    return 0


# Import NDJSON or JSON-array event files, or the raw events of a running Marquez server

IMPORT_AFTER, IMPORT_BEFORE = "1900-01-01T00:00:00Z", "9999-12-31T23:59:59.999999Z"


def file_events(path):
    """Yield (label, event) from NDJSON, one event per line, or a JSON array file; gzip is detected by content."""
    with path.expanduser().open("rb") as raw:
        stream = gzip.GzipFile(fileobj=raw) if raw.peek(2)[:2] == b"\x1f\x8b" else raw
        with io.TextIOWrapper(stream, encoding="utf-8-sig") as text:
            for number, line in enumerate(text, 1):
                if not line.strip():
                    continue
                if line.lstrip().startswith("["):
                    events = json.loads(line + text.read())
                    if not isinstance(events, list):
                        raise ValueError(f"{path}: expected a JSON array of events")
                    for position, event in enumerate(events):
                        yield f"{path} item {position}", event
                    return
                try:
                    event = json.loads(line)
                except ValueError as error:
                    raise ValueError(f"{path} line {number}: invalid JSON ({error})") from None
                yield f"{path} line {number}", event


def marquez_events(source, headers, after, before, page_size, retries, timeout, stats):
    """Page through GET /api/v1/events/lineage in ascending event time.

    Its after bound is inclusive, so each page restarts at the last event time seen and skips the events of that
    time it already returned; offset only advances while a whole page shares one event time. Paging ends on an empty
    page rather than a short one, so a server that caps limit loses nothing."""
    cursor, offset, boundary, stale = after, 0, set(), 0
    while True:
        query = urlencode({"sortDirection": "asc", "after": utc_text(cursor), "before": utc_text(before),
                           "limit": page_size, "offset": offset})
        status, payload = fetch_json(f"{source}/api/v1/events/lineage?{query}", headers, retries, timeout)
        if status != 200 or not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            raise ValueError(f"GET {source}/api/v1/events/lineage returned HTTP {status}: {str(payload)[:300]}")
        if stats.get("total") is None and isinstance(payload.get("totalCount"), int):
            stats["total"] = payload["totalCount"]
        seen, read = [], stats.get("read", 0)
        for event in payload["events"]:
            label = f"source event {stats.get('read', 0) + 1}"
            try:
                moment = timestamp(event.get("eventTime") if isinstance(event, dict) else None)
            except ValueError as error:
                raise ValueError(f"{label}: {error}") from None
            digest = hashlib.sha256(json.dumps(event, sort_keys=True, separators=(",", ":")).encode()).digest()
            seen.append((moment, digest))
            if moment == cursor and digest in boundary:
                continue
            stats["read"] = stats.get("read", 0) + 1
            yield label, event
        times = [moment for moment, _ in seen]
        if times != sorted(times) or times and times[0] < cursor:
            raise ValueError("the source ignored sortDirection=asc; it needs a Marquez release with sorted event listings")
        if not seen:
            return
        stale = stale + 1 if stats.get("read", 0) == read else 0
        if stale > 1:
            raise ValueError("the source returned the same events again; it ignores the offset parameter")
        if times[-1] == cursor:
            offset += len(seen)
            boundary.update(digest for _, digest in seen)
        else:
            cursor, offset = times[-1], 0
            boundary = {digest for moment, digest in seen if moment == cursor}


def load_events(store, events):
    batch = []

    def flush():
        try:
            store.add([event for _, event in batch])
        except RequestError:
            for label, event in batch:
                try:
                    store.add([event])
                except RequestError as error:
                    raise ValueError(f"{label}: {error}") from None
        batch.clear()

    read = 0
    for label, event in events:
        try:
            event_row(event)
        except (ValueError, OverflowError, RecursionError) as error:
            raise ValueError(f"{label}: {error}") from None
        read += 1
        batch.append((label, event))
        if len(batch) >= 500:
            flush()
    if batch:
        flush()
    stored, = store.connection.execute("SELECT COUNT(*) FROM lineage_events").fetchone()
    return read, stored


def publish_file(staged, destination):
    """Publish a finished file; a hard link, unlike rename, refuses a destination that appeared meanwhile."""
    try:
        os.link(staged, destination)
    except FileExistsError:
        raise FileExistsError(f"Database file already exists: {destination}") from None
    except OSError:
        if os.path.lexists(destination):
            raise FileExistsError(f"Database file already exists: {destination}") from None
        os.rename(staged, destination)


def import_main(argv):
    parser = argparse.ArgumentParser(prog="openlineage_server.py import",
                                     description="Build a new SQLite persistence file from OpenLineage event files "
                                                 "or from the raw events of a running Marquez server.")
    parser.add_argument("--db-file", type=Path, required=True, help="new SQLite persistence file")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--in", dest="inputs", type=Path, action="append", metavar="FILE",
                        help="NDJSON (one event per line) or JSON-array file, optionally gzip-compressed; repeatable")
    source.add_argument("--source", help="Marquez API base URL, for example http://localhost:5000")
    parser.add_argument("--token-file", type=Path, help="file containing a bearer token for --source")
    parser.add_argument("--after", default=IMPORT_AFTER, help="earliest event time to read from --source (inclusive)")
    parser.add_argument("--before", default=IMPORT_BEFORE, help="event time to stop reading from --source (exclusive)")
    parser.add_argument("--page-size", type=int, default=500, help="events per request to --source")
    parser.add_argument("--retries", type=int, default=3, help="retries per request on 429, 5xx, or connection errors")
    parser.add_argument("--timeout", type=float, default=30, help="seconds per HTTP request")
    args = parser.parse_args(argv)
    source_url = (args.source or "").rstrip("/")
    if args.source is not None and urlsplit(source_url).scheme not in {"http", "https"}:
        parser.error("--source must be an http(s) URL")
    if args.token_file and args.source is None:
        parser.error("--token-file needs --source")
    if args.page_size < 1 or args.retries < 0 or args.timeout <= 0:
        parser.error("--page-size and --timeout must be positive and --retries not negative")
    destination = args.db_file.expanduser().absolute()
    stats = {}
    try:
        after, before = timestamp(args.after), timestamp(args.before)
        if os.path.lexists(destination) or os.path.lexists(f"{destination}-wal"):
            raise FileExistsError(f"Database file already exists: {destination}")
        if args.source is not None:
            headers = {"Accept": "application/json"}
            if args.token_file:
                headers["Authorization"] = f"Bearer {read_token(args.token_file)}"
            events = marquez_events(source_url, headers, after, before, args.page_size, args.retries, args.timeout, stats)
        else:
            events = (item for path in args.inputs for item in file_events(path))
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".openlineage-import-", dir=destination.parent) as temporary:
            staged = Path(temporary) / destination.name
            store = Store(staged)
            try:
                read, stored = load_events(store, events)
                store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                store.connection.close()
            publish_file(staged, destination)
    except (OSError, ValueError, EOFError, zlib.error, RecursionError, sqlite3.Error) as error:
        parser.exit(1, f"OpenLineage import failed: {error}\n")
    if stats.get("total") is not None and stats.get("read", 0) < stats["total"]:
        print(f"warning: the source reports {stats['total']} events in the time range but returned {stats.get('read', 0)}; "
              "Marquez's events API lists run events only, so job and dataset events were not imported", file=sys.stderr)
    duplicates = f" ({read - stored} duplicate events dropped)" if read > stored else ""
    print(f"Imported {stored} OpenLineage events{duplicates} to {destination}")
    return 0


def serve_main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--db-file", type=Path, help="SQLite file for lineage events")
    parser.add_argument("--password-file", type=Path, help="File containing the bearer token")
    args = parser.parse_args(argv)
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
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["export"]:
        return export_main(argv[1:])
    if argv[:1] == ["migrate"]:
        return migrate_main(argv[1:])
    if argv[:1] == ["import"]:
        return import_main(argv[1:])
    return serve_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
