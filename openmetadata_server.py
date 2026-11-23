#!/usr/bin/env python3
"""A single-file OpenMetadata catalog and lineage API for integration tests."""

import argparse
import base64
import binascii
import copy
import fnmatch
import hmac
import json
import re
import sqlite3
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import closing, contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit


API_VERSION = "2.0.9"
BODY_LIMIT = 4 * 1024 * 1024
API_ROOT = "/api/v1/"


class RequestError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class EntityType:
    name: str
    collection: str
    parent_field: str = ""
    parent_kind: str = ""
    required: tuple = ()


TYPES = {entity.name: entity for entity in (
    EntityType("databaseService", "services/databaseServices", required=("serviceType",)),
    EntityType("pipelineService", "services/pipelineServices", required=("serviceType",)),
    EntityType("messagingService", "services/messagingServices", required=("serviceType",)),
    EntityType("database", "databases", "service", "databaseService"),
    EntityType("databaseSchema", "databaseSchemas", "database", "database"),
    EntityType("table", "tables", "databaseSchema", "databaseSchema", ("columns",)),
    EntityType("pipeline", "pipelines", "service", "pipelineService"),
    EntityType("topic", "topics", "service", "messagingService", ("partitions",)),
    EntityType("user", "users", required=("email",)),
)}
COLLECTIONS = {entity.collection: entity for entity in TYPES.values()}
MANAGED_FIELDS = {
    "id", "name", "fullyQualifiedName", "href", "updatedAt", "updatedBy", "version", "deleted",
    "service", "serviceType", "database", "databaseSchema",
}
OPTIONAL_FIELDS = {"owners", "tags", "followers", "extension", "domains", "usageSummary"}


def dump(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False)


def object_body(value):
    if not isinstance(value, dict):
        raise RequestError(400, "Expected a JSON object")
    return value


def now_ms():
    return time.time_ns() // 1_000_000


def name_field(value):
    if not isinstance(value, str) or not value.strip() or '"' in value or len(value) > 256:
        raise RequestError(400, "name must contain 1 to 256 characters and no double quotes")
    value.encode("utf-8")
    if any(ord(char) < 32 for char in value):
        raise RequestError(400, "name cannot contain control characters")
    return value


def escape_name(value):
    return f'"{value}"' if "." in value else value


def cursor(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def read_cursor(value):
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True).decode()
    except (binascii.Error, ValueError):
        raise RequestError(400, "Invalid pagination cursor") from None


def integer(params, key, default, minimum, maximum):
    try:
        value = int(params.get(key, default))
    except ValueError:
        raise RequestError(400, f"Invalid {key}") from None
    if not minimum <= value <= maximum:
        raise RequestError(400, f"{key} must be between {minimum} and {maximum}")
    return value


def flag(params, name):
    value = params.get(name, "false").lower()
    if value not in {"true", "false"}:
        raise RequestError(400, f"{name} must be true or false")
    return value == "true"


def project_fields(entity, requested):
    if requested is None or "*" in requested.split(","):
        return entity
    omitted = OPTIONAL_FIELDS - set(requested.split(","))
    return {key: value for key, value in entity.items() if key not in omitted}


def pointer_get(document, pointer, parent=False):
    if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
        raise ValueError("Invalid JSON Pointer")
    if re.search(r"~(?![01])", pointer):
        raise ValueError("Invalid JSON Pointer escape")
    keys = [value.replace("~1", "/").replace("~0", "~") for value in pointer.split("/")[1:]]
    if parent:
        if not keys:
            raise ValueError("Patching the entire entity is unsupported")
        key = keys.pop()
    for key_part in keys:
        if isinstance(document, list):
            document = document[list_index(key_part, len(document))]
        elif isinstance(document, dict):
            document = document[key_part]
        else:
            raise ValueError("The JSON Pointer has no target")
    return (document, key) if parent else document


def list_index(index, length, insert=False):
    if insert and index == "-":
        return length
    if not re.fullmatch(r"0|[1-9][0-9]*", index):
        raise ValueError("Invalid array index")
    index = int(index)
    if index >= length + int(insert):
        raise ValueError("Array index out of range")
    return index


def patch_change(document, path, operation, value=None):
    target, key = pointer_get(document, path, parent=True)
    if isinstance(target, list):
        key = list_index(key, len(target), insert=operation == "add")
        if operation == "add":
            target.insert(key, value)
            return
    elif not isinstance(target, dict):
        raise ValueError("The JSON Pointer parent is not a container")
    if operation == "remove":
        del target[key]
    else:
        if operation == "replace" and isinstance(target, dict) and key not in target:
            raise ValueError("Cannot replace a nonexistent field")
        target[key] = value


def json_patch(entity, operations):
    if not isinstance(operations, list):
        raise RequestError(400, "Expected a JSON Patch array")
    result = copy.deepcopy(entity)
    try:
        for operation in operations:
            if not isinstance(operation, dict):
                raise ValueError("Expected a JSON Patch operation")
            name, path = operation["op"], operation["path"]
            if name == "test":
                if pointer_get(result, path) != operation["value"]:
                    raise RequestError(409, "JSON Patch test failed")
            elif name in ("add", "replace"):
                patch_change(result, path, name, operation["value"])
            elif name == "remove":
                patch_change(result, path, name)
            elif name in ("copy", "move"):
                source = operation["from"]
                value = copy.deepcopy(pointer_get(result, source))
                if name == "move":
                    patch_change(result, source, "remove")
                patch_change(result, path, "add", value)
            else:
                raise ValueError("Unsupported JSON Patch operation")
    except (KeyError, ValueError, RecursionError) as error:
        raise RequestError(400, f"Invalid JSON Patch: {error}") from None
    return result


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path or ":memory:", check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.connection.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS metadata_entities (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                fqn TEXT NOT NULL,
                parent_id TEXT REFERENCES metadata_entities(id) ON DELETE CASCADE,
                version INTEGER NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                content TEXT NOT NULL,
                UNIQUE (kind, fqn)
            );
            CREATE INDEX IF NOT EXISTS metadata_entity_parent ON metadata_entities(parent_id);
            CREATE TABLE IF NOT EXISTS metadata_versions (
                entity_id TEXT REFERENCES metadata_entities(id) ON DELETE CASCADE,
                version INTEGER NOT NULL,
                content TEXT NOT NULL,
                PRIMARY KEY (entity_id, version)
            );
            CREATE TABLE IF NOT EXISTS metadata_lineage (
                source TEXT REFERENCES metadata_entities(id) ON DELETE CASCADE,
                target TEXT REFERENCES metadata_entities(id) ON DELETE CASCADE,
                details TEXT,
                PRIMARY KEY (source, target)
            );
            CREATE INDEX IF NOT EXISTS metadata_lineage_target ON metadata_lineage(target);
        """)

    @contextmanager
    def write(self):
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            yield

    def entity(self, kind, key, by_name=False, include="non-deleted", optional=False):
        field = "fqn" if by_name else "id"
        key.encode("utf-8")
        row = self.connection.execute(
            f"SELECT * FROM metadata_entities WHERE kind = ? AND {field} = ?", (kind, key),
        ).fetchone()
        if row is not None and self.included(row, include):
            return row
        if optional:
            return None
        raise RequestError(404, f"{kind} not found")

    def by_id(self, entity_id):
        return self.connection.execute("SELECT * FROM metadata_entities WHERE id = ?", (entity_id,)).fetchone()

    @staticmethod
    def included(row, selection):
        if selection not in {"all", "deleted", "non-deleted"}:
            raise RequestError(400, "Invalid include option")
        return selection == "all" or (selection == "deleted") == bool(row["deleted"])

    def resolve(self, kind, value):
        if isinstance(value, dict) and isinstance(value.get("id"), str):
            if value.get("type", kind) != kind:
                raise RequestError(400, f"Expected a {kind} reference")
            return self.entity(kind, value["id"])
        if not isinstance(value, str) or not value:
            raise RequestError(400, f"Expected the fully qualified name of a {kind}")
        return self.entity(kind, value, by_name=True)

    def reference(self, row, origin):
        content = json.loads(row["content"])
        return {
            "id": row["id"], "type": row["kind"], "name": content["name"],
            "fullyQualifiedName": row["fqn"], "deleted": bool(row["deleted"]),
            "href": self.href(row, origin),
        }

    @staticmethod
    def href(row, origin):
        return f"{origin}{API_ROOT}{TYPES[row['kind']].collection}/{row['id']}"

    def view(self, row, origin, content=None):
        result = json.loads(row["content"]) if content is None else content.copy()
        result["href"] = self.href(row, origin)
        current = row
        while current["parent_id"] is not None:
            parent = self.by_id(current["parent_id"])
            result[TYPES[current["kind"]].parent_field] = self.reference(parent, origin)
            if "serviceType" in (parent_content := json.loads(parent["content"])):
                result.setdefault("serviceType", parent_content["serviceType"])
            current = parent
        if "owners" in result and isinstance(result["owners"], list):
            owners = []
            for reference in result["owners"]:
                owner_id = reference.get("id") if isinstance(reference, dict) else None
                owner = self.by_id(owner_id) if isinstance(owner_id, str) else None
                owners.append(self.reference(owner, origin) if owner is not None else reference)
            result["owners"] = owners
        return result

    def rows(self, kind):
        return self.connection.execute(
            "SELECT * FROM metadata_entities WHERE kind = ? ORDER BY fqn", (kind,),
        ).fetchall()

    def filtered_rows(self, kind, params, origin):
        include = params.get("include", "non-deleted")
        if include not in {"all", "deleted", "non-deleted"}:
            raise RequestError(400, "Invalid include option")
        filters = {key: params[key] for key in ("database", "databaseSchema", "service") if key in params}
        output = []
        for row in self.rows(kind):
            if self.included(row, include):
                value = self.view(row, origin)
                if all(value.get(key, {}).get("fullyQualifiedName") == fqn for key, fqn in filters.items()):
                    output.append((row["fqn"], project_fields(value, params.get("fields"))))
        return output

    @staticmethod
    def validate_content(kind, content):
        for required in TYPES[kind].required:
            if required not in content or content[required] is None:
                raise RequestError(400, f"{required} is required")
        for key in ("columns", "owners", "tags", "tasks"):
            field = content.get(key)
            if field is not None and (not isinstance(field, list) or any(not isinstance(item, dict) for item in field)):
                raise RequestError(400, f"{key} must be an array of objects")
        for column in content.get("columns") or []:
            if not isinstance(column.get("name"), str) or not isinstance(column.get("dataType"), str):
                raise RequestError(400, "Columns require name and dataType")
        for key in ("serviceType", "email"):
            if key in TYPES[kind].required and (not isinstance(content[key], str) or not content[key].strip()):
                raise RequestError(400, f"{key} must be a nonempty string")
        if kind == "topic" and (type(content["partitions"]) is not int or content["partitions"] < 1):
            raise RequestError(400, "partitions must be a positive integer")

    def save(self, row, content):
        current = json.loads(row["content"])
        if content == current:
            return row
        version = row["version"] + 1
        content = {**content, "version": version / 10, "updatedAt": now_ms(), "updatedBy": "admin"}
        serialized = dump(content)
        self.connection.execute("""
            UPDATE metadata_entities SET version = ?, deleted = ?, content = ? WHERE id = ?
        """, (version, int(content["deleted"]), serialized, row["id"]))
        self.connection.execute("INSERT INTO metadata_versions VALUES (?, ?, ?)", (row["id"], version, serialized))
        return self.by_id(row["id"])

    def put(self, entity_type, body, create_only):
        body = object_body(body)
        name = name_field(body.get("name"))
        parent = self.resolve(entity_type.parent_kind, body.get(entity_type.parent_field)) if entity_type.parent_kind else None
        fqn = escape_name(name) if parent is None else f"{parent['fqn']}.{escape_name(name)}"
        row = self.entity(entity_type.name, fqn, by_name=True, include="all", optional=True)
        if row is not None and (create_only or row["deleted"]):
            raise RequestError(409, "An entity with this name already exists")
        blocked = MANAGED_FIELDS if parent is not None else MANAGED_FIELDS - {"serviceType"}
        fields = {key: value for key, value in body.items() if key not in blocked}
        if row is not None:
            content = {**json.loads(row["content"]), **fields}
            self.validate_content(entity_type.name, content)
            return 200, self.save(row, content)
        entity_id = str(uuid.uuid4())
        content = {**fields, "id": entity_id, "name": name, "fullyQualifiedName": fqn, "version": 0.1,
                   "updatedAt": now_ms(), "updatedBy": "admin", "deleted": False}
        self.validate_content(entity_type.name, content)
        serialized = dump(content)
        parent_id = parent["id"] if parent is not None else None
        self.connection.execute("""
            INSERT INTO metadata_entities (id, kind, fqn, parent_id, version, content) VALUES (?, ?, ?, ?, 1, ?)
        """, (entity_id, entity_type.name, fqn, parent_id, serialized))
        self.connection.execute("INSERT INTO metadata_versions VALUES (?, 1, ?)", (entity_id, serialized))
        return 201, self.by_id(entity_id)

    def apply_patch(self, row, body, origin):
        original = self.view(row, origin)
        updated = json_patch(original, body)
        if any(original.get(key) != updated.get(key) for key in MANAGED_FIELDS):
            raise RequestError(400, "JSON Patch cannot modify entity identifiers, parents, or server fields")
        content = json.loads(row["content"])
        values = {key: val for key, val in updated.items() if key not in MANAGED_FIELDS}
        for key, val in content.items():
            if key in MANAGED_FIELDS or original.get(key) == updated.get(key) and key in updated:
                values[key] = val
        self.validate_content(row["kind"], values)
        return self.save(row, values)

    def delete(self, row, hard, recursive):
        descendants = self.connection.execute("""
            WITH RECURSIVE descendants(id, depth) AS (
                SELECT id, 1 FROM metadata_entities WHERE parent_id = ?
                UNION ALL
                SELECT e.id, d.depth + 1 FROM metadata_entities e JOIN descendants d ON e.parent_id = d.id
            )
            SELECT e.* FROM metadata_entities e JOIN descendants d ON e.id = d.id ORDER BY d.depth DESC
        """, (row["id"],)).fetchall()
        if not hard:
            descendants = [child for child in descendants if not child["deleted"]]
        if descendants and not recursive:
            raise RequestError(409, "Entity has children. Use recursive=true to delete them")
        if hard:
            self.connection.execute("DELETE FROM metadata_entities WHERE id = ?", (row["id"],))
            return None
        for child in [*descendants, row]:
            self.save(child, {**json.loads(child["content"]), "deleted": True})
        return self.by_id(row["id"])

    def restore(self, row):
        parent = self.by_id(row["parent_id"]) if row["parent_id"] else None
        if parent is not None and parent["deleted"]:
            raise RequestError(409, "Restore the parent entity first")
        return self.save(row, {**json.loads(row["content"]), "deleted": False})

    def history(self, row, version, origin):
        snapshots = self.connection.execute("""
            SELECT content FROM metadata_versions WHERE entity_id = ? ORDER BY version DESC
        """, (row["id"],)).fetchall()
        values = [self.view(row, origin, json.loads(value["content"])) for value in snapshots]
        if version is None:
            return {"entityType": row["kind"], "versions": values}
        for value in values:
            if str(value["version"]) == version:
                return value
        raise RequestError(404, "Entity version not found")

    def related(self, entity_reference):
        ref = object_body(entity_reference)
        kind, entity_id = ref.get("type"), ref.get("id")
        if not isinstance(kind, str) or kind not in TYPES or not isinstance(entity_id, str):
            raise RequestError(400, "An entity reference requires a supported type and an ID")
        return self.entity(kind, entity_id)

    def add_lineage(self, body, origin):
        edge = object_body(object_body(body).get("edge"))
        source = self.related(edge.get("fromEntity"))
        target = self.related(edge.get("toEntity"))
        annotations = {}
        description = edge.get("description")
        if description is not None:
            if not isinstance(description, str):
                raise RequestError(400, "description must be a string")
            annotations["description"] = description
        details = edge.get("lineageDetails")
        if details is not None:
            details = object_body(details)
            pipeline = details.get("pipeline")
            if pipeline is not None:
                if self.related(pipeline)["kind"] != "pipeline":
                    raise RequestError(400, "Pipeline lineage requires a pipeline entity")
            annotations["lineageDetails"] = details
        self.connection.execute("""
            INSERT INTO metadata_lineage VALUES (?, ?, ?)
            ON CONFLICT(source, target) DO UPDATE SET details=excluded.details
        """, (source["id"], target["id"], dump(annotations) if annotations else None))
        return self.lineage(source, 1, 1, origin)

    def delete_lineage(self, source, target):
        if self.connection.execute(
            "DELETE FROM metadata_lineage WHERE source = ? AND target = ?", (source["id"], target["id"]),
        ).rowcount == 0:
            raise RequestError(404, "Lineage edge not found")

    def lineage(self, row, upstream, downstream, origin):
        links = self.connection.execute("""
            SELECT l.* FROM metadata_lineage l
            JOIN metadata_entities s ON s.id = l.source AND s.deleted = 0
            JOIN metadata_entities t ON t.id = l.target AND t.deleted = 0
            ORDER BY l.source, l.target
        """).fetchall()
        parents, children = defaultdict(list), defaultdict(list)
        for link in links:
            parents[link["target"]].append(link)
            children[link["source"]].append(link)
        root = row["id"]
        nodes = set()

        def walk(adjacency, other, depth):
            queue = deque([(root, 0)])
            visited = {root}
            edges = []
            while queue:
                node, level = queue.popleft()
                if level >= depth:
                    continue
                for edge in adjacency[node]:
                    neighbor = edge[other]
                    value = {"fromEntity": edge["source"], "toEntity": edge["target"]}
                    if edge["details"] is not None:
                        value.update(json.loads(edge["details"]))
                    edges.append(value)
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, level + 1))
            nodes.update(visited - {root})
            return edges

        upstream_edges = walk(parents, "source", upstream)
        downstream_edges = walk(children, "target", downstream)
        return {"entity": self.reference(row, origin), "nodes": [self.reference(self.by_id(node), origin)
                                                                 for node in sorted(nodes)],
                "upstreamEdges": upstream_edges, "downstreamEdges": downstream_edges}


def paginate(rows, params):
    limit = integer(params, "limit", 10, 0, 1_000_000)
    total = len(rows)
    after, before = params.get("after"), params.get("before")
    if after is not None and before is not None:
        raise RequestError(400, "Use either before or after")
    start, end = 0, min(limit, total)
    if after is not None:
        lower = read_cursor(after)
        start = sum(fqn <= lower for fqn, _ in rows)
        end = min(start + limit, total)
    if before is not None:
        upper = read_cursor(before)
        end = sum(fqn < upper for fqn, _ in rows)
        start = max(end - limit, 0)
    paging = {"total": total}
    if end > start and start > 0:
        paging["before"] = cursor(rows[start][0])
    if end > start and end < total:
        paging["after"] = cursor(rows[end - 1][0])
    return {"data": [value for _, value in rows[start:end]], "paging": paging}


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(15)
        super().setup()

    @property
    def origin(self):
        return f"http://{self.headers.get('Host') or self.server.server_name + ':' + str(self.server.server_port)}"

    def read_json(self, patch=False):
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            raise RequestError(411, "Content-Length required") from None
        if self.headers.get("Transfer-Encoding"):
            raise RequestError(400, "Expected a Content-Length header")
        if length < 0 or length > BODY_LIMIT:
            raise RequestError(413, "Request exceeds the size limit")
        media_types = {"application/json", "application/json-patch+json"} if patch else {"application/json"}
        if self.headers.get_content_type() not in media_types:
            raise RequestError(415, "Expected a JSON request body")
        try:
            body = self.rfile.read(length)
            if len(body) != length:
                raise RequestError(400, "Incomplete request body")
            value = json.loads(body)
            dump(value)
            return value
        except TimeoutError:
            raise RequestError(408, "Request body timed out") from None
        except (ValueError, OSError, RecursionError):
            raise RequestError(400, "Invalid JSON request body") from None

    def respond(self, status, payload):
        body = b"" if payload is None else dump(payload).encode()
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

    def dispatch(self):
        url = urlsplit(self.path)
        path = url.path.rstrip("/")
        if self.command == "GET" and path in {"/health", "/api/v1/system/health"}:
            return 200, {"status": "healthy"}
        if self.command == "GET" and path == "/api/v1/system/version":
            return 200, {"version": self.server.version}
        if not path.startswith(API_ROOT):
            raise RequestError(404, "Endpoint not found")
        authorization = self.headers.get("Authorization", "")
        if self.server.token and not hmac.compare_digest(authorization.encode(), self.server.token):
            raise RequestError(401, "A bearer token is required")
        segments = [unquote(part) for part in path[len(API_ROOT):].split("/")]
        params = {key: values[-1] for key, values in parse_qs(url.query).items()}
        if segments[0] == "lineage":
            return self.lineage(segments[1:], params)
        if self.command == "GET" and len(segments) == 2 and segments[0] == "search":
            if segments[1] in {"query", "fieldQuery"}:
                return 200, self.search(params, exact=segments[1] == "fieldQuery")
        prefix = 2 if segments[0] == "services" else 1
        entity_type = COLLECTIONS.get("/".join(segments[:prefix]))
        if entity_type is None:
            raise RequestError(404, "Endpoint not found")
        return self.entities(entity_type, segments[prefix:], params)

    def entities(self, entity_type, segments, params):
        store = self.server.store
        kind = entity_type.name
        method = self.command
        origin = self.origin
        if not segments:
            if method == "GET":
                with store.lock:
                    return 200, paginate(store.filtered_rows(kind, params, origin), params)
            if method in ("POST", "PUT"):
                body = self.read_json()
                with store.write():
                    status, row = store.put(entity_type, body, method == "POST")
                    return status, store.view(row, origin)
        if segments == ["restore"] and method == "PUT":
            body = object_body(self.read_json())
            entity_id = body.get("id")
            if not isinstance(entity_id, str):
                raise RequestError(400, "id is required")
            with store.write():
                row = store.entity(kind, entity_id, include="all")
                return 200, store.view(store.restore(row), origin)
        if not segments:
            raise RequestError(405, "Method not allowed for collection")
        by_name = len(segments) >= 2 and segments[0] == "name"
        key = segments[1] if by_name else segments[0]
        suffix = segments[2:] if by_name else segments[1:]
        if method == "GET":
            with store.lock:
                if suffix and suffix[0] == "versions" and len(suffix) <= 2:
                    row = store.entity(kind, key, by_name, include="all")
                    return 200, store.history(row, suffix[1] if len(suffix) == 2 else None, origin)
                if not suffix:
                    row = store.entity(kind, key, by_name, include=params.get("include", "non-deleted"))
                    return 200, project_fields(store.view(row, origin), params.get("fields"))
        if method == "PATCH" and not suffix:
            body = self.read_json(patch=True)
            with store.write():
                row = store.entity(kind, key, by_name)
                return 200, store.view(store.apply_patch(row, body, origin), origin)
        if method == "DELETE" and not suffix:
            hard = flag(params, "hardDelete")
            with store.write():
                row = store.entity(kind, key, by_name, include="all" if hard else "non-deleted")
                deleted = store.delete(row, hard, flag(params, "recursive"))
                return (204, None) if hard else (200, store.view(deleted, origin))
        raise RequestError(404, "Endpoint not found")

    def lineage(self, segments, params):
        store = self.server.store
        origin = self.origin
        if self.command == "PUT" and not segments:
            body = self.read_json()
            with store.write():
                return 200, store.add_lineage(body, origin)
        if self.command == "GET":
            match segments:
                case [kind, key] | [kind, "name", key] if kind in TYPES:
                    upstream = integer(params, "upstreamDepth", 1, 0, 3)
                    downstream = integer(params, "downstreamDepth", 1, 0, 3)
                    with store.lock:
                        row = store.entity(kind, key, by_name=len(segments) == 3)
                        return 200, store.lineage(row, upstream, downstream, origin)
        if self.command == "DELETE" and len(segments) == 4:
            source_type, source_id, target_type, target_id = segments
            with store.write():
                source = store.related({"type": source_type, "id": source_id})
                target = store.related({"type": target_type, "id": target_id})
                store.delete_lineage(source, target)
                return 200, {"status": "success"}
        raise RequestError(404, "Endpoint not found")

    def search(self, params, exact):
        index = params.get("index", "all")
        kinds = [kind for kind in TYPES if index in {"all", kind, f"{kind}_search_index"}]
        if not kinds:
            raise RequestError(400, "Unknown search index")
        start, size = integer(params, "from", 0, 0, 1_000_000), integer(params, "size", 10, 0, 10_000)
        fields = ["name", "displayName", "fullyQualifiedName", "description", "email"]
        if exact:
            field = params.get("fieldName", "").removesuffix(".keyword")
            if field not in fields or "fieldValue" not in params:
                raise RequestError(400, "Expected a supported fieldName and fieldValue")
            fields = [field]
            query = params["fieldValue"].casefold()
        else:
            query = params.get("q", "*").strip().casefold()
            selection, delimiter, pattern = query.partition(":")
            selection = selection.removesuffix(".keyword")
            if delimiter and selection in {key.casefold() for key in fields}:
                fields = [key for key in fields if key.casefold() == selection]
                query = pattern.strip('"')
            if "*" not in query and "?" not in query:
                query = f"*{query}*"
        store, origin = self.server.store, self.origin
        hits = []
        with store.lock:
            for kind in kinds:
                found = store.filtered_rows(kind, {}, origin)
                for _, document in found:
                    candidates = [str(document.get(key) or "").casefold() for key in fields]
                    if any(fnmatch.fnmatchcase(value, query) for value in candidates):
                        hits.append({"_id": document["id"], "_index": f"{kind}_search_index", "_score": 1.0,
                                     "_source": {**document, "entityType": kind}})
        return {"took": 0, "timed_out": False,
                "hits": {"total": {"value": len(hits), "relation": "eq"}, "hits": hits[start:start + size]}}

    def handle_http(self):
        try:
            status, payload = self.dispatch()
        except RequestError as error:
            status, payload = error.status, {"code": error.status, "message": str(error)}
        except (ValueError, RecursionError):
            status, payload = 400, {"code": 400, "message": "Invalid request value"}
        except sqlite3.Error:
            status, payload = 500, {"code": 500, "message": "Catalog storage operation failed"}
        try:
            self.respond(status, payload)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = handle_http


class Server(ThreadingHTTPServer):
    daemon_threads = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8585)
    parser.add_argument("--db-file", type=Path, help="SQLite file for catalog entities and lineage")
    parser.add_argument("--password-file", type=Path, help="File containing the bearer token")
    parser.add_argument("--version", default=API_VERSION, help="API version returned to clients")
    args = parser.parse_args()
    token = None
    if args.password_file:
        try:
            password = args.password_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as error:
            parser.error(str(error))
        if not password or not password.isascii() or any(char.isspace() for char in password):
            parser.error("The password file must contain one nonempty ASCII token")
        token = f"Bearer {password}".encode()
    store = Store(args.db_file)
    with closing(store.connection), Server((args.host, args.port), Handler) as server:
        server.store = store
        server.token = token
        server.version = args.version
        print(f"OpenMetadata server listening on {args.host}:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
