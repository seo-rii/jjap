#!/usr/bin/env python3
"""A single-file OpenMetadata catalog and lineage API for integration tests."""

import argparse
import base64
import binascii
import copy
import fnmatch
import hmac
import http.client
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
from collections import defaultdict, deque
from contextlib import closing, contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit


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

    def put(self, entity_type, body, create_only, entity_id=None):
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
        entity_id = entity_id or str(uuid.uuid4())
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

    def reset(self):
        self.connection.execute("DELETE FROM metadata_lineage")
        self.connection.execute("DELETE FROM metadata_versions")
        self.connection.execute("DELETE FROM metadata_entities")

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
        body = b"" if payload is None or self.command == "HEAD" else dump(payload).encode()
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
        if not path.startswith(API_ROOT) and path != "/_jjap/reset":
            raise RequestError(404, "Endpoint not found")
        authorization = self.headers.get("Authorization", "")
        if self.server.token and not hmac.compare_digest(authorization.encode(), self.server.token):
            raise RequestError(401, "A bearer token is required")
        if path == "/_jjap/reset":
            if self.command != "POST":
                raise RequestError(405, "Method not allowed")
            with self.server.store.write():
                self.server.store.reset()
            return 200, {"reset": True}
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

    do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = handle_http


class Server(ThreadingHTTPServer):
    daemon_threads = False


# Export as OpenMetadata Create* request bodies and replay them into a running server

EXPORT_ORDER = ("user", "databaseService", "pipelineService", "messagingService",
                "database", "databaseSchema", "table", "pipeline", "topic")
SERVICE_FIELDS = {"name", "displayName", "description", "tags", "style", "serviceType", "connection", "owners",
                  "dataProducts", "domains", "ingestionRunner"}
ASSET_FIELDS = {"name", "displayName", "description", "tags", "owners", "dataProducts", "domains", "extension",
                "sourceUrl", "lifeCycle", "sourceHash"}
# Properties of the OpenMetadata 2.0 Create*Request schemas, which reject anything else.
CREATE_FIELDS = {
    "databaseService": SERVICE_FIELDS,
    "pipelineService": SERVICE_FIELDS | {"scheduleInterval", "lifeCycle"},
    "messagingService": SERVICE_FIELDS,
    "database": ASSET_FIELDS | {"service", "default", "retentionPeriod"},
    "databaseSchema": ASSET_FIELDS | {"database", "retentionPeriod"},
    "table": ASSET_FIELDS | {"databaseSchema", "tableType", "columns", "dataModel", "locationPath", "tableConstraints",
                             "aliases", "tablePartition", "tableProfilerConfig", "schemaDefinition", "retentionPeriod",
                             "fileFormat", "tableSharding"},
    "pipeline": ASSET_FIELDS | {"service", "concurrency", "pipelineLocation", "startDate", "tasks", "state", "scheduleInterval"},
    "topic": ASSET_FIELDS | {"service", "messageSchema", "partitions", "cleanupPolicies", "replicationFactor", "retentionTime",
                             "maximumMessageSize", "minimumInSyncReplicas", "retentionSize", "topicConfig"},
    # Passwords and authentication mechanisms are secrets that OpenMetadata never returns; they are not exported.
    "user": {"name", "description", "displayName", "externalId", "scimUserName", "email", "timezone", "isBot", "botName",
             "isAdmin", "profile", "domains"},
}
SERVER_FIELDS = {"id", "href", "version", "updatedAt", "updatedBy", "fullyQualifiedName", "changeDescription",
                 "incrementalChangeDescription", "deleted", "usageSummary", "serviceType", "service", "database",
                 "databaseSchema"}
REFERENCE_COLLECTIONS = {**{kind: entity.collection for kind, entity in TYPES.items()}, "team": "teams"}


def warn(message):
    print(f"warning: {message}", file=sys.stderr)


def fqn_list(values):
    if not isinstance(values, list):
        return values
    return [value["fullyQualifiedName"] if isinstance(value, dict) and isinstance(value.get("fullyQualifiedName"), str)
            else value for value in values]


def without_derived_fqn(items, nested=None):
    if not isinstance(items, list):
        return items
    result = []
    for item in items:
        if isinstance(item, dict):
            item = {key: value for key, value in item.items() if key != "fullyQualifiedName"}
            if nested in item:
                item[nested] = without_derived_fqn(item[nested], nested)
        result.append(item)
    return result


def catalog_requests(filename, include_deleted=False):
    """Read one snapshot and return the ordered REST requests that recreate it, plus skipped-item counts."""
    source = Path(filename).expanduser().resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"Expected a SQLite persistence file: {source}")
    connection = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, isolation_level=None)
    connection.row_factory = sqlite3.Row
    with closing(connection):
        connection.execute("BEGIN")
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        if not {"metadata_entities", "metadata_versions", "metadata_lineage"} <= tables:
            raise ValueError("Expected an OpenMetadata test server SQLite persistence file")
        rows = {row["id"]: row for row in connection.execute("SELECT * FROM metadata_entities")}
        versions, = connection.execute("SELECT COUNT(*) FROM metadata_versions").fetchone()
        links = connection.execute("SELECT source, target, details FROM metadata_lineage ORDER BY rowid").fetchall()
    skipped = {"deletedEntities": 0, "orphans": 0, "lineageEdges": 0, "versionHistory": max(versions - len(rows), 0)}
    ordered = sorted((row for row in rows.values() if row["kind"] in EXPORT_ORDER),
                     key=lambda row: (EXPORT_ORDER.index(row["kind"]), row["fqn"]))
    for row in rows.values():
        if row["kind"] not in EXPORT_ORDER:
            warn(f"{row['kind']} {row['fqn']} has no supported Create request; skipped")
    exported, dropped, requests, deletions = {}, defaultdict(int), [], []
    for row in ordered:
        kind, fqn = row["kind"], row["fqn"]
        parent = rows.get(row["parent_id"]) if row["parent_id"] else None
        if row["deleted"] and not include_deleted:
            skipped["deletedEntities"] += 1
            continue
        if row["parent_id"] and row["parent_id"] not in exported:
            warn(f"{kind} {fqn} was skipped because its parent is not exported")
            skipped["orphans"] += 1
            continue
        content = json.loads(row["content"])
        entity_type = TYPES[kind]
        allowed = CREATE_FIELDS[kind]
        body = {}
        for key, value in content.items():
            if key == entity_type.parent_field or key not in allowed:
                if key not in SERVER_FIELDS:
                    dropped[kind, key] += 1
                continue
            if key == "owners" and isinstance(value, list):
                value = owner_references(rows, exported, value, f"{kind} {fqn}")
            elif key in {"domains", "dataProducts"}:
                value = fqn_list(value)
            elif key == "columns":
                value = without_derived_fqn(value, "children")
            elif key == "tasks":
                value = without_derived_fqn(value)
            body[key] = value
        if parent is not None:
            body[entity_type.parent_field] = parent["fqn"]
        exported[row["id"]] = row
        requests.append({"method": "PUT", "path": f"v1/{entity_type.collection}", "entityType": kind,
                         "fullyQualifiedName": fqn, "body": body})
        if row["deleted"]:
            deletions.append({"method": "DELETE", "path": f"v1/{entity_type.collection}/{{id}}",
                              "query": "hardDelete=false&recursive=true", "entityType": kind, "fullyQualifiedName": fqn})
    for (kind, key), count in sorted(dropped.items()):
        warn(f"dropped [{key}] from {count} {kind} entities: not accepted by the OpenMetadata Create request")
    for link in links:
        ends = [exported.get(link["source"]), exported.get(link["target"])]
        if None in ends:
            skipped["lineageEdges"] += 1
            continue
        edge = {"fromEntity": {"type": ends[0]["kind"], "fullyQualifiedName": ends[0]["fqn"]},
                "toEntity": {"type": ends[1]["kind"], "fullyQualifiedName": ends[1]["fqn"]}}
        annotations = json.loads(link["details"]) if link["details"] else {}
        if "description" in annotations:
            edge["description"] = annotations["description"]
        if "lineageDetails" in annotations:
            details = dict(annotations["lineageDetails"])
            pipeline = details.get("pipeline")
            if isinstance(pipeline, dict) and isinstance(pipeline.get("id"), str):
                if (row := exported.get(pipeline["id"])) is None:
                    warn(f"lineage {ends[0]['fqn']} -> {ends[1]['fqn']}: pipeline {pipeline['id']} is not exported; reference dropped")
                    del details["pipeline"]
                else:
                    details["pipeline"] = {"type": row["kind"], "fullyQualifiedName": row["fqn"]}
            edge["lineageDetails"] = details
        requests.append({"method": "PUT", "path": "v1/lineage", "body": {"edge": edge}})
    if skipped["deletedEntities"]:
        warn(f"skipped {skipped['deletedEntities']} soft-deleted entities (use --include-deleted to keep them)")
    if skipped["lineageEdges"]:
        warn(f"skipped {skipped['lineageEdges']} lineage edges that touch entities which are not exported")
    if skipped["versionHistory"]:
        warn(f"{skipped['versionHistory']} earlier entity versions are not migratable; the target starts a new version history")
    return requests + deletions[::-1], skipped


def owner_references(rows, exported, owners, label):
    result = []
    for owner in owners:
        owner_id = owner.get("id") if isinstance(owner, dict) else None
        if isinstance(owner_id, str) and owner_id in exported:
            result.append({"type": rows[owner_id]["kind"], "fullyQualifiedName": rows[owner_id]["fqn"]})
        elif owner_id is None and isinstance(owner, dict) and all(isinstance(owner.get(key), str)
                                                                  for key in ("type", "fullyQualifiedName")):
            result.append({"type": owner["type"], "fullyQualifiedName": owner["fullyQualifiedName"]})
        else:
            warn(f"{label}: owner {json.dumps(owner)[:200]} is not an exported entity; dropped")
    return result


def export_catalog(filename, destination, include_deleted):
    requests, skipped = catalog_requests(filename, include_deleted)
    destination.mkdir(mode=0o700)
    (destination / "requests").mkdir()
    manifest = {
        "format": "jjap-openmetadata-export", "formatVersion": 1, "apiVersion": API_VERSION,
        "includeDeleted": include_deleted,
        "references": ("Owners, lineage endpoints and lineageDetails.pipeline name entities by type and fullyQualifiedName. "
                       "Set each id from the target (GET /v1/<collection>/name/<fqn>) before sending, and replace {id} "
                       "in DELETE paths the same way."),
        "requests": [], "skipped": skipped,
    }
    for position, request in enumerate(requests, 1):
        entry = {key: value for key, value in request.items() if key != "body"}
        if "body" in request:
            kind = request.get("entityType", "lineage")
            entry["body"] = f"requests/{position:05d}-{kind}.json"
            with (destination / entry["body"]).open("x", encoding="utf-8") as file:
                file.write(json.dumps(request["body"], indent=2, ensure_ascii=False, allow_nan=False) + "\n")
        manifest["requests"].append(entry)
    with (destination / "manifest.json").open("x", encoding="utf-8") as file:
        file.write(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    entities = sum(request["method"] == "PUT" and "entityType" in request for request in requests)
    return entities, sum(request["path"] == "v1/lineage" for request in requests)


def export_main(argv):
    parser = argparse.ArgumentParser(prog="openmetadata_server.py export",
                                     description="Write OpenMetadata Create* and AddLineage request bodies in dependency order.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--include-deleted", action="store_true", help="also export soft-deleted entities and their lineage")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".openmetadata-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "export"
            entities, edges = export_catalog(args.db_file, staged, args.include_deleted)
            if os.name == "nt":
                staged.rename(destination)
            else:
                destination.mkdir(mode=0o700)
                try:
                    staged.replace(destination)
                except BaseException:
                    destination.rmdir()
                    raise
    except (OSError, ValueError, KeyError, sqlite3.Error) as error:
        parser.exit(1, f"OpenMetadata export failed: {error}\n")
    print(f"Exported {entities} entities and {edges} lineage edges to {destination}")
    return 0


class Catalog:
    def __init__(self, url, token, timeout, retries):
        self.url, self.timeout, self.retries = url, timeout, retries
        self.headers = {"Accept": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        self.ids = {}

    def call(self, method, path, body=None, entity=True):
        data = None if body is None else dump(body).encode()
        headers = {**self.headers, **({"Content-Type": "application/json"} if data is not None else {})}
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(f"{self.url}/{path}", data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    status, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                status, raw = error.code, error.read()
            except (urllib.error.URLError, OSError, http.client.HTTPException) as error:
                if attempt == self.retries:
                    raise ConnectionError(f"{method} {self.url}/{path}: {getattr(error, 'reason', error)}") from None
                time.sleep(min(0.25 * 2 ** attempt, 4))
                continue
            if status in {502, 503, 504} and attempt < self.retries:
                time.sleep(min(0.25 * 2 ** attempt, 4))
                continue
            try:
                payload = json.loads(raw) if raw else {}
            except ValueError:
                payload = {"message": raw[:300].decode("utf-8", "replace")}
            if 200 <= status < 300:
                if (not entity or method == "DELETE" or path == "v1/lineage"
                        or isinstance(payload, dict) and isinstance(payload.get("id"), str)):
                    return payload
                raise RequestError(status, f"{method} {path}: HTTP {status} response carries no entity id")
            message = payload.get("message", payload) if isinstance(payload, dict) else payload
            raise RequestError(status, f"{method} {path}: HTTP {status} {str(message)[:300]}")

    def resolve(self, reference):
        kind, fqn = reference.get("type"), reference.get("fullyQualifiedName")
        if (kind, fqn) not in self.ids:
            if kind not in REFERENCE_COLLECTIONS or not isinstance(fqn, str):
                raise RequestError(400, f"cannot resolve a {kind} reference")
            entity = self.call("GET", f"v1/{REFERENCE_COLLECTIONS[kind]}/name/{quote(fqn, safe='')}")
            self.ids[kind, fqn] = entity["id"]
        return {"id": self.ids[kind, fqn], "type": kind}

    def find(self, kind, fqn):
        """Return the target's entity with this name, soft-deleted or not, or None."""
        try:
            return self.call("GET", f"v1/{REFERENCE_COLLECTIONS[kind]}/name/{quote(fqn, safe='')}?include=all")
        except RequestError as error:
            if error.status == 404:
                return None
            raise


def migrate_main(argv):
    parser = argparse.ArgumentParser(prog="openmetadata_server.py migrate",
                                     description="Create or update the stored catalog in a running OpenMetadata server.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--target", required=True, help="API base URL, for example http://localhost:8585/api")
    parser.add_argument("--token-file", type=Path, help="file containing a bot or personal access JWT")
    parser.add_argument("--include-deleted", action="store_true",
                        help="also migrate soft-deleted entities, deleting them again after lineage is added")
    parser.add_argument("--retries", type=int, default=3, help="retries on 502-504 or connection errors")
    parser.add_argument("--timeout", type=float, default=60, help="seconds per HTTP request")
    parser.add_argument("--dry-run", action="store_true",
                        help="look up every entity and reference on the target and report the plan without writing")
    args = parser.parse_args(argv)
    target = args.target.rstrip("/")
    if urlsplit(target).scheme not in {"http", "https"} or args.retries < 0 or args.timeout <= 0:
        parser.error("--target must be an http(s) URL, --retries not negative and --timeout positive")
    failures, failed, counts, aborted = [], set(), defaultdict(int), None
    dry_run = args.dry_run
    try:
        token = None
        if args.token_file:
            token = args.token_file.expanduser().read_text(encoding="utf-8").strip()
            if not token or not token.isascii() or any(char.isspace() for char in token):
                raise ValueError(f"{args.token_file} must contain one nonempty ASCII token")
        catalog = Catalog(target, token, args.timeout, args.retries)
        requests, _ = catalog_requests(args.db_file, args.include_deleted)
        for request in requests:
            kind, fqn = request.get("entityType"), request.get("fullyQualifiedName")
            label = f"{kind} {fqn}" if kind else "lineage {} -> {}".format(
                *(request["body"]["edge"][end]["fullyQualifiedName"] for end in ("fromEntity", "toEntity")))
            try:
                if request["method"] == "DELETE":
                    if (kind, fqn) in failed:
                        continue
                    if dry_run:
                        print(f"would soft-delete {label} again")
                        counts["deleted"] += 1
                        continue
                    reference = catalog.resolve({"type": kind, "fullyQualifiedName": fqn})
                    catalog.call("DELETE", request["path"].replace("{id}", reference["id"]) + "?" + request["query"])
                    counts["deleted"] += 1
                    continue
                body = copy.deepcopy(request["body"])
                if kind is None:
                    edge = body["edge"]
                    if blocked := {(end["type"], end["fullyQualifiedName"]) for end in (edge["fromEntity"], edge["toEntity"])} & failed:
                        raise RequestError(424, f"skipped because {' and '.join(' '.join(item) for item in sorted(blocked))} failed")
                    edge["fromEntity"], edge["toEntity"] = catalog.resolve(edge["fromEntity"]), catalog.resolve(edge["toEntity"])
                    if isinstance(edge.get("lineageDetails", {}).get("pipeline"), dict):
                        edge["lineageDetails"]["pipeline"] = catalog.resolve(edge["lineageDetails"]["pipeline"])
                    if dry_run:
                        print(f"would add {label}")
                    else:
                        catalog.call("PUT", request["path"], body)
                    counts["lineage"] += 1
                    continue
                parent = TYPES[kind].parent_kind, body.get(TYPES[kind].parent_field)
                if parent in failed:
                    raise RequestError(424, f"skipped because {' '.join(parent)} failed")
                owners = []
                for owner in body.get("owners") or []:
                    try:
                        owners.append(catalog.resolve(owner))
                    except RequestError as error:
                        failures.append(f"{label}: owner {owner['type']} {owner['fullyQualifiedName']} not found ({error})")
                if "owners" in body:
                    body["owners"] = owners
                if dry_run:
                    existing = catalog.find(kind, fqn)
                    if existing is None:
                        print(f"would create {label}")
                    elif existing.get("deleted"):
                        print(f"would restore and update {label}")
                        warn(f"{label} is soft-deleted on the target; OpenMetadata restores it on PUT, "
                             "while the jjap test server answers HTTP 409")
                    else:
                        print(f"would update {label}")
                    catalog.ids[kind, fqn] = existing["id"] if existing else f"<new {kind}>"
                else:
                    entity = catalog.call("PUT", request["path"], body)
                    catalog.ids[kind, fqn] = entity["id"]
                counts["entities"] += 1
            except RequestError as error:
                failures.append(f"{label}: {error}")
                if kind:
                    failed.add((kind, fqn))
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        aborted = error
    for failure in failures:
        print(f"OpenMetadata migrate: {failure}", file=sys.stderr)
    if aborted is not None:
        parser.exit(1, f"OpenMetadata migrate failed: {aborted}\n")
    summary = f"{counts['entities']} entities and {counts['lineage']} lineage edges"
    if counts["deleted"]:
        summary += f" (soft-deleted {counts['deleted']} again)"
    summary += f" at {target}"
    if dry_run:
        if failures:
            parser.exit(1, f"OpenMetadata migrate failed: dry run found {len(failures)} failures; would migrate {summary}\n")
        print(f"Dry run: would migrate {summary}")
        return 0
    if failures:
        parser.exit(1, f"OpenMetadata migrate failed: {len(failures)} failures; migrated {summary}\n")
    print(f"Migrated {summary}")
    return 0


# Import export output or a running OpenMetadata server into a new persistence file

IMPORT_ORIGIN = "http://127.0.0.1:8585"


def local_reference(store, reference):
    """Turn a type and FQN reference into the id reference this catalog stores; None when the entity is absent."""
    reference = object_body(reference)
    if isinstance(reference.get("id"), str):
        return reference
    kind, fqn = reference.get("type"), reference.get("fullyQualifiedName")
    if kind not in TYPES or not isinstance(fqn, str):
        return None
    row = store.entity(kind, fqn, by_name=True, optional=True)
    return {"id": row["id"], "type": kind} if row is not None else None


def import_export(store, directory):
    """Replay export output offline in manifest order, as migrate would send it to this server."""
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("format") != "jjap-openmetadata-export":
        raise ValueError(f"{directory / 'manifest.json'} is not openmetadata_server.py export output")
    counts = defaultdict(int)
    for entry in manifest["requests"]:
        kind, fqn = entry.get("entityType"), entry.get("fullyQualifiedName")
        label = f"{kind} {fqn}" if kind else entry.get("body", "lineage")
        try:
            if entry["method"] == "DELETE":
                params = {key: values[-1] for key, values in parse_qs(entry.get("query", "")).items()}
                store.delete(store.entity(kind, fqn, by_name=True), flag(params, "hardDelete"), flag(params, "recursive"))
                counts["deleted"] += 1
                continue
            body = json.loads((directory / entry["body"]).read_text(encoding="utf-8"))
            if entry["path"] == "v1/lineage":
                edge = object_body(object_body(body).get("edge"))
                label = "lineage {} -> {}".format(*(edge[end].get("fullyQualifiedName") for end in ("fromEntity", "toEntity")))
                details = edge.get("lineageDetails")
                ends = [(edge, "fromEntity"), (edge, "toEntity")]
                if isinstance(details, dict) and isinstance(details.get("pipeline"), dict):
                    ends.append((details, "pipeline"))
                for holder, key in ends:
                    holder[key] = local_reference(store, holder[key])
                    if holder[key] is None:
                        raise RequestError(404, f"{key} is not in the catalog")
                store.add_lineage(body, IMPORT_ORIGIN)
                counts["lineage"] += 1
                continue
            entity_type = COLLECTIONS.get(entry["path"].removeprefix("v1/"))
            if entity_type is None or entity_type.name != kind:
                raise RequestError(400, f"unsupported request path {entry['path']}")
            if isinstance(body.get("owners"), list):
                owners = []
                for owner in body["owners"]:
                    resolved = local_reference(store, owner)
                    if resolved is None:
                        warn(f"{label}: owner {json.dumps(owner)[:200]} is not in the catalog; kept unresolved")
                    owners.append(resolved or owner)
                body["owners"] = owners
            store.put(entity_type, body, create_only=False)
            counts["entities"] += 1
        except (RequestError, KeyError, TypeError, ValueError) as error:
            raise ValueError(f"{label}: {error}") from None
    return counts


def version_tenths(value, label):
    if type(value) not in {int, float} or not 0 < value < 2 ** 31:
        raise ValueError(f"{label}: invalid version {value!r}")
    return round(value * 10)


def stored_form(entity_type, entity):
    """The content this server stores for an entity read back through the API."""
    derived = {"href"} | ({"service", "database", "databaseSchema", "serviceType"} if entity_type.parent_kind else set())
    return {key: value for key, value in entity.items() if key not in derived}


def adopt(store, entity_type, entity, history):
    """Create a source entity through put (validation, parent and FQN) keeping its id, version and history."""
    label = f"{entity_type.name} {entity.get('fullyQualifiedName')}"
    entity_id = entity.get("id")
    if not isinstance(entity_id, str) or not entity_id:
        raise ValueError(f"{label}: the source entity has no id")
    body = dict(entity)
    parent = entity.get(entity_type.parent_field) if entity_type.parent_kind else None
    if isinstance(parent, dict) and isinstance(parent.get("id"), str):
        body[entity_type.parent_field] = {"id": parent["id"], "type": entity_type.parent_kind}
    try:
        _, row = store.put(entity_type, body, create_only=True, entity_id=entity_id)
    except RequestError as error:
        raise ValueError(f"{label}: {error}") from None
    if row["fqn"] != entity.get("fullyQualifiedName"):
        warn(f"{label}: this server derives the fully qualified name {row['fqn']}")
    content = json.loads(row["content"])
    content.update({key: entity[key] for key in ("version", "updatedAt", "updatedBy", "deleted") if key in entity})
    current = version_tenths(content["version"], label)
    versions = {current: content}
    for item in history:
        item = json.loads(item) if isinstance(item, str) else item
        if isinstance(item, dict) and item.get("version") is not None:
            versions.setdefault(version_tenths(item["version"], label), stored_form(entity_type, item))
    store.connection.execute("DELETE FROM metadata_versions WHERE entity_id = ?", (entity_id,))
    store.connection.executemany("INSERT INTO metadata_versions VALUES (?, ?, ?)",
                                 [(entity_id, number, dump(value)) for number, value in sorted(versions.items())])
    store.connection.execute("UPDATE metadata_entities SET version = ?, content = ? WHERE id = ?",
                             (current, dump(content), entity_id))
    return bool(content.get("deleted"))


def import_source(store, catalog, include_deleted, page_size):
    """Read every supported entity type through the list APIs, then each entity's version history and lineage."""
    kinds, deleted, counts = {}, [], defaultdict(int)
    warn("only users, database, pipeline and messaging services, databases, schemas, tables, pipelines and topics "
         "are imported; other entity types are not")
    for kind in EXPORT_ORDER:
        entity_type, after, cursors = TYPES[kind], None, set()
        while True:
            query = {"limit": page_size, "fields": "*", "include": "all" if include_deleted else "non-deleted"}
            if after:
                query["after"] = after
            page = catalog.call("GET", f"v1/{entity_type.collection}?{urlencode(query)}", entity=False)
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise ValueError(f"GET v1/{entity_type.collection} did not return a list of entities")
            for entity in page["data"]:
                entity = object_body(entity)
                history = catalog.call("GET", f"v1/{entity_type.collection}/{quote(str(entity.get('id')), safe='')}/versions",
                                       entity=False)
                versions = history.get("versions") if isinstance(history, dict) else None
                if adopt(store, entity_type, entity, versions if isinstance(versions, list) else []):
                    deleted.append(entity["id"])
                kinds[entity["id"]] = kind
                counts["entities"] += 1
            after = (page.get("paging") or {}).get("after")
            if not after:
                break
            if after in cursors:
                raise ValueError(f"GET v1/{entity_type.collection} repeated the paging cursor {after}")
            cursors.add(after)
    skipped, pipelines = 0, 0
    for entity_id, kind in kinds.items():
        if entity_id in deleted:
            continue
        graph = catalog.call("GET", f"v1/lineage/{kind}/{quote(entity_id, safe='')}?upstreamDepth=0&downstreamDepth=1",
                             entity=False)
        for edge in graph.get("downstreamEdges") or []:
            source, target = edge.get("fromEntity"), edge.get("toEntity")
            if source != entity_id:
                continue
            if target not in kinds or target in deleted:
                skipped += 1
                continue
            body = {"fromEntity": {"id": source, "type": kind}, "toEntity": {"id": target, "type": kinds[target]}}
            body.update({key: edge[key] for key in ("description", "lineageDetails") if edge.get(key) is not None})
            details = body.get("lineageDetails")
            if isinstance(details, dict) and isinstance(details.get("pipeline"), dict):
                if kinds.get(details["pipeline"].get("id")) != "pipeline" or details["pipeline"]["id"] in deleted:
                    body["lineageDetails"] = {key: value for key, value in details.items() if key != "pipeline"}
                    pipelines += 1
            try:
                store.add_lineage({"edge": body}, IMPORT_ORIGIN)
            except RequestError as error:
                raise ValueError(f"lineage {source} -> {target}: {error}") from None
            counts["lineage"] += 1
    if skipped:
        warn(f"skipped {skipped} lineage edges to entities that were not imported")
    if pipelines:
        warn(f"dropped lineageDetails.pipeline from {pipelines} edges whose pipeline was not imported")
    if deleted:
        store.connection.executemany("UPDATE metadata_entities SET deleted = 1 WHERE id = ?", [(item,) for item in deleted])
        warn(f"the lineage API does not return the edges of the {len(deleted)} soft-deleted entities; they were not imported")
    counts["soft-deleted"] = len(deleted)
    return counts


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
    parser = argparse.ArgumentParser(prog="openmetadata_server.py import",
                                     description="Build a new SQLite persistence file from export output or from a "
                                                 "running OpenMetadata server.")
    parser.add_argument("--db-file", type=Path, required=True, help="new SQLite persistence file")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--in", dest="input", type=Path, metavar="DIR", help="directory written by export")
    source.add_argument("--source", help="API base URL of a running server, for example http://localhost:8585/api")
    parser.add_argument("--token-file", type=Path, help="file containing a bot or personal access JWT for --source")
    parser.add_argument("--include-deleted", action="store_true", help="also import soft-deleted entities from --source")
    parser.add_argument("--page-size", type=int, default=100, help="entities per list request to --source")
    parser.add_argument("--retries", type=int, default=3, help="retries on 502-504 or connection errors")
    parser.add_argument("--timeout", type=float, default=60, help="seconds per HTTP request")
    args = parser.parse_args(argv)
    source_url = (args.source or "").rstrip("/")
    if args.source is not None and urlsplit(source_url).scheme not in {"http", "https"}:
        parser.error("--source must be an http(s) URL")
    if (args.token_file or args.include_deleted) and args.source is None:
        parser.error("--token-file and --include-deleted need --source")
    if not 1 <= args.page_size <= 1_000_000 or args.retries < 0 or args.timeout <= 0:
        parser.error("--page-size must be between 1 and 1000000, --retries not negative and --timeout positive")
    destination = args.db_file.expanduser().absolute()
    try:
        if os.path.lexists(destination) or os.path.lexists(f"{destination}-wal"):
            raise FileExistsError(f"Database file already exists: {destination}")
        catalog = None
        if args.source is not None:
            token = None
            if args.token_file:
                token = args.token_file.expanduser().read_text(encoding="utf-8").strip()
                if not token or not token.isascii() or any(char.isspace() for char in token):
                    raise ValueError(f"{args.token_file} must contain one nonempty ASCII token")
            catalog = Catalog(source_url, token, args.timeout, args.retries)
            catalog.call("GET", "v1/system/version", entity=False)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".openmetadata-import-", dir=destination.parent) as temporary:
            staged = Path(temporary) / destination.name
            store = Store(staged)
            try:
                with store.write():
                    if catalog is None:
                        counts = import_export(store, args.input.expanduser())
                    else:
                        counts = import_source(store, catalog, args.include_deleted, args.page_size)
                store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                store.connection.close()
            publish_file(staged, destination)
    except (OSError, ValueError, KeyError, TypeError, RequestError, sqlite3.Error) as error:
        parser.exit(1, f"OpenMetadata import failed: {error}\n")
    deleted = counts["deleted"] or counts["soft-deleted"]
    note = f" ({deleted} soft-deleted)" if deleted else ""
    print(f"Imported {counts['entities']} entities{note} and {counts['lineage']} lineage edges to {destination}")
    return 0


def serve_main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8585)
    parser.add_argument("--db-file", type=Path, help="SQLite file for catalog entities and lineage")
    parser.add_argument("--password-file", type=Path, help="File containing the bearer token")
    parser.add_argument("--version", default=API_VERSION, help="API version returned to clients")
    args = parser.parse_args(argv)
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
