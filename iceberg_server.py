#!/usr/bin/env python3
"""Standalone Apache Iceberg v2 REST catalog and local file tools (Python 3.10+).

Start: python3 iceberg_server.py --warehouse ./warehouse serve
Use create-table, append, scan, delete and snapshots for local file operations.
Use serve-polaris for a Polaris-compatible catalog with FILE storage.

Protocol references:
https://iceberg.apache.org/spec/
https://iceberg.apache.org/rest-catalog-spec/
https://polaris.apache.org/guides/rest-api/
https://avro.apache.org/docs/current/specification/
https://parquet.apache.org/docs/file-format/
"""

import argparse
import base64
import binascii
import bz2
import copy
import datetime
import decimal
import gzip
import hashlib
import hmac
import importlib
import io
import itertools
import json
import lzma
import math
import os
import re
import secrets
import sqlite3
import struct
import sys
import tempfile
import threading
import time
import uuid
import zlib
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import url2pathname


# Iceberg v2 table metadata and REST catalog.


class IcebergCatalogError(ValueError):
    """An Iceberg catalog error with an HTTP status and REST error type."""

    def __init__(self, message, status=400, error_type="BadRequestException"):
        super().__init__(message)
        self.status = status
        self.error_type = error_type


def _catalog_component(value):
    if (
        not isinstance(value, str)
        or not 0 < len(value) <= 255
        or value in {".", ".."}
        or any(char in "/\\" or ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise IcebergCatalogError(f"Invalid identifier component: {value!r}")
    return value


def _catalog_namespace(value):
    if not isinstance(value, (tuple, list)) or not value or len(value) > 32:
        raise IcebergCatalogError("Namespace must be a nonempty array of identifier components")
    return tuple(_catalog_component(part) for part in value)


def _catalog_object(value, description):
    if not isinstance(value, dict):
        raise IcebergCatalogError(f"{description} must be an object")
    return value


def _catalog_array(value, description):
    if not isinstance(value, (list, tuple)):
        raise IcebergCatalogError(f"{description} must be an array")
    return value


def _catalog_id(value, description, minimum=0):
    if type(value) is not int or not minimum <= value <= 2**63 - 1:
        raise IcebergCatalogError(f"Invalid {description}: {value!r}")
    return value


def _catalog_properties(value):
    value = _catalog_object(value, "Properties")
    if any(not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()):
        raise IcebergCatalogError("Properties must contain string values")
    return dict(value)


def _catalog_local_path(location):
    if isinstance(location, Path):
        return location.resolve()
    if not isinstance(location, str) or not location or "\x00" in location:
        raise IcebergCatalogError("Invalid filesystem location")
    # Parse only a URI, so that literal spaces, # and ? in filesystem paths are preserved.
    if not location.startswith("file:"):
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", location):
            raise IcebergCatalogError("Only local file locations are supported")
        return Path(location).resolve()
    try:
        parsed = urlsplit(location)
        if parsed.netloc not in {"", "localhost"} or parsed.query or parsed.fragment:
            raise ValueError
        path = Path(url2pathname(parsed.path))
        if not path.is_absolute() or "\x00" in str(path):
            raise ValueError
        return path.resolve()
    except (ValueError, OSError, RuntimeError) as exc:
        raise IcebergCatalogError("Invalid local file URI") from exc


def _catalog_schema(schema):
    schema = copy.deepcopy(_catalog_object(schema, "Schema"))
    if schema.get("type") != "struct":
        raise IcebergCatalogError("Table schema must have type struct")
    schema["schema-id"] = _catalog_id(schema.get("schema-id", 0), "schema ID")
    schema.setdefault("identifier-field-ids", [])
    field_ids = set()

    def add_id(value):
        identifier = _catalog_id(value, "field ID", 1)
        if identifier in field_ids:
            raise IcebergCatalogError(f"Duplicate schema field ID {identifier}")
        field_ids.add(identifier)

    def visit_type(value, depth):
        if depth > 100:
            raise IcebergCatalogError("Schema nesting exceeds 100 levels")
        if isinstance(value, str) and value:
            return
        value = _catalog_object(value, "Field type")
        kind = value.get("type")
        if kind == "struct":
            names = set()
            for field in _catalog_array(value.get("fields"), "Struct fields"):
                field = _catalog_object(field, "Schema field")
                add_id(field.get("id"))
                name = field.get("name")
                if not isinstance(name, str) or not name or name in names or type(field.get("required")) is not bool:
                    raise IcebergCatalogError("Invalid or duplicate schema field")
                names.add(name)
                visit_type(field.get("type"), depth + 1)
        elif kind == "list":
            add_id(value.get("element-id"))
            if type(value.get("element-required")) is not bool:
                raise IcebergCatalogError("List element-required must be boolean")
            visit_type(value.get("element"), depth + 1)
        elif kind == "map":
            add_id(value.get("key-id"))
            add_id(value.get("value-id"))
            if type(value.get("value-required")) is not bool:
                raise IcebergCatalogError("Map value-required must be boolean")
            visit_type(value.get("key"), depth + 1)
            visit_type(value.get("value"), depth + 1)
        else:
            raise IcebergCatalogError(f"Unsupported schema type {kind!r}")

    visit_type(schema, 0)
    for identifier in _catalog_array(schema["identifier-field-ids"], "Identifier field IDs"):
        if _catalog_id(identifier, "identifier field ID", 1) not in field_ids:
            raise IcebergCatalogError(f"Unknown identifier field ID {identifier}")
    return schema, max(field_ids, default=0)


def _catalog_layout(value, key, baseline):
    value = copy.deepcopy(_catalog_object(value, "Table layout"))
    value[key] = _catalog_id(value.get(key, baseline), key)
    field_ids = set()
    fields = _catalog_array(value.get("fields"), "Layout fields")
    for index, field in enumerate(fields, 1000):
        field = _catalog_object(field, "Layout field")
        _catalog_id(field.get("source-id"), "source field ID", 1)
        if not isinstance(field.get("transform"), str) or not field["transform"]:
            raise IcebergCatalogError("Layout field transform must be a nonempty string")
        if key == "spec-id":
            field["field-id"] = _catalog_id(field.get("field-id", index), "partition field ID", 1000)
            if field["field-id"] in field_ids or not isinstance(field.get("name"), str) or not field["name"]:
                raise IcebergCatalogError("Invalid or duplicate partition field")
            field_ids.add(field["field-id"])
        elif field.get("direction") not in ("asc", "desc") or field.get("null-order") not in (
            "nulls-first", "nulls-last",
        ):
            raise IcebergCatalogError("Invalid sort field direction or null order")
    if key == "order-id" and ((not fields) != (value[key] == 0)):
        raise IcebergCatalogError("Sort order ID 0 is reserved for unsorted tables")
    return value, max(field_ids, default=999)


def _catalog_now_ms():
    return time.time_ns() // 1_000_000


def _catalog_metadata(location):
    return {
        "format-version": 2,
        "table-uuid": str(uuid.uuid4()),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": _catalog_now_ms(),
        "last-column-id": 0,
        "schemas": [],
        "current-schema-id": 0,
        "partition-specs": [],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "sort-orders": [],
        "default-sort-order-id": 0,
        "properties": {"write.parquet.compression-codec": "snappy"},
        "current-snapshot-id": None,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {},
    }


def _catalog_response(metadata, location):
    return {"metadata": metadata, "metadata-location": location}


def _catalog_add_entry(entries, key, value):
    for entry in entries:
        if entry[key] == value[key]:
            if entry != value:
                raise IcebergCatalogError(f"Conflicting {key} {value[key]}")
            return
    entries.append(value)


def _catalog_snapshot(metadata, snapshot_id):
    for snapshot in metadata["snapshots"]:
        if snapshot["snapshot-id"] == snapshot_id:
            return snapshot
    raise IcebergCatalogError(f"Unknown snapshot ID {snapshot_id}")


def _catalog_check_requirements(metadata, requirements):
    conditions = {
        "assert-table-uuid": ("uuid", "table-uuid"),
        "assert-last-assigned-field-id": ("last-assigned-field-id", "last-column-id"),
        "assert-current-schema-id": ("current-schema-id", "current-schema-id"),
        "assert-last-assigned-partition-id": ("last-assigned-partition-id", "last-partition-id"),
        "assert-default-spec-id": ("default-spec-id", "default-spec-id"),
        "assert-default-sort-order-id": ("default-sort-order-id", "default-sort-order-id"),
    }
    failures = []
    for requirement in requirements:
        kind = requirement.get("type")
        if kind == "assert-create":
            if metadata is not None:
                failures.append("table already exists")
        elif kind == "assert-ref-snapshot-id":
            ref = _catalog_component(requirement.get("ref"))
            if "snapshot-id" not in requirement:
                raise IcebergCatalogError("Ref requirement must supply snapshot-id")
            expected = requirement["snapshot-id"]
            if expected is not None:
                _catalog_id(expected, "snapshot ID", 1)
            existing = (metadata or {}).get("refs", {}).get(ref, {}).get("snapshot-id")
            if metadata is None or existing != expected:
                failures.append(f"ref {ref!r} expected snapshot {expected!r}, found {existing!r}")
        elif isinstance(kind, str) and kind in conditions:
            request_key, metadata_key = conditions[kind]
            if request_key not in requirement:
                raise IcebergCatalogError(f"Requirement {kind} must supply {request_key}")
            if metadata is None or metadata.get(metadata_key) != requirement[request_key]:
                failures.append(f"{kind} failed")
        else:
            raise IcebergCatalogError(f"Unsupported table requirement {kind!r}")
    if failures:
        raise IcebergCatalogError("; ".join(failures), 409, "CommitFailedException")


@contextmanager
def _catalog_transaction(db_file, write=False):
    connection = sqlite3.connect(db_file, timeout=15, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


class IcebergCatalog:
    """Persistent local Iceberg v2 catalog using immutable JSON and a SQLite pointer."""

    def __init__(self, warehouse, db_file=None):
        self.warehouse = _catalog_local_path(warehouse)
        self.warehouse.mkdir(parents=True, exist_ok=True)
        self.db_file = Path(db_file).resolve() if db_file is not None else self.warehouse / ".iceberg-catalog.sqlite3"
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction(write=True) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS iceberg_namespaces "
                "(namespace TEXT PRIMARY KEY, properties TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS iceberg_tables "
                "(namespace TEXT NOT NULL REFERENCES iceberg_namespaces(namespace), name TEXT NOT NULL, "
                " metadata_location TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(namespace, name))"
            )

    def _transaction(self, write=False):
        return _catalog_transaction(self.db_file, write)

    def local_path(self, location):
        path = _catalog_local_path(location)
        if not path.is_relative_to(self.warehouse):
            raise IcebergCatalogError("File locations must stay inside the warehouse")
        return path

    @staticmethod
    def _namespace_key(namespace):
        return json.dumps(namespace, ensure_ascii=False, separators=(",", ":"))

    def _namespace_row(self, connection, namespace):
        row = connection.execute(
            "SELECT properties FROM iceberg_namespaces WHERE namespace = ?", (self._namespace_key(namespace),),
        ).fetchone()
        if row is None:
            raise IcebergCatalogError(f"Namespace {'.'.join(namespace)!r} was not found", 404, "NoSuchNamespaceException")
        return json.loads(row[0])

    def _table_row(self, connection, namespace, name):
        return connection.execute(
            "SELECT metadata_location, version FROM iceberg_tables WHERE namespace = ? AND name = ?",
            (self._namespace_key(namespace), name),
        ).fetchone()

    def _required_table_row(self, connection, namespace, name):
        row = self._table_row(connection, namespace, name)
        if row is None:
            raise IcebergCatalogError(f"Table {'.'.join((*namespace, name))!r} was not found", 404, "NoSuchTableException")
        return row

    def _new_table_location(self, namespace, name, properties):
        # Distinct creations of the same identifier should not share old files.
        parent = self.local_path(properties["location"]) if "location" in properties else self.warehouse.joinpath(*namespace)
        return self.local_path(parent / f"{name}-{uuid.uuid4().hex}").as_uri()

    def list_namespaces(self, parent=()):
        parent = _catalog_namespace(parent) if parent else ()
        with self._transaction() as connection:
            if parent:
                self._namespace_row(connection, parent)
            rows = connection.execute("SELECT namespace FROM iceberg_namespaces")
            namespaces = (tuple(json.loads(row[0])) for row in rows)
            return sorted(namespace for namespace in namespaces if namespace[:-1] == parent)

    def create_namespace(self, namespace, properties=None):
        namespace = _catalog_namespace(namespace)
        properties = _catalog_properties({} if properties is None else properties)
        if "location" in properties:
            properties["location"] = self.local_path(properties["location"]).as_uri()
        with self._transaction(write=True) as connection:
            if len(namespace) > 1:
                self._namespace_row(connection, namespace[:-1])
            try:
                connection.execute(
                    "INSERT INTO iceberg_namespaces VALUES (?, ?)", (self._namespace_key(namespace), json.dumps(properties)),
                )
            except sqlite3.IntegrityError as exc:
                raise IcebergCatalogError(f"Namespace {'.'.join(namespace)!r} already exists", 409, "AlreadyExistsException") from exc
        return {"namespace": namespace, "properties": properties}

    def load_namespace(self, namespace):
        namespace = _catalog_namespace(namespace)
        with self._transaction() as connection:
            return {"namespace": namespace, "properties": self._namespace_row(connection, namespace)}

    def update_namespace(self, namespace, removals, updates):
        namespace = _catalog_namespace(namespace)
        updates = _catalog_properties(updates)
        if "location" in updates:
            updates["location"] = self.local_path(updates["location"]).as_uri()
        removals = tuple(dict.fromkeys(_catalog_component(key) for key in _catalog_array(removals, "Removals")))
        if set(removals).intersection(updates):
            raise IcebergCatalogError("A property cannot be both removed and updated")
        with self._transaction(write=True) as connection:
            properties = self._namespace_row(connection, namespace)
            removed = [name for name in removals if name in properties]
            missing = [name for name in removals if name not in properties]
            for name in removed:
                del properties[name]
            properties.update(updates)
            connection.execute(
                "UPDATE iceberg_namespaces SET properties = ? WHERE namespace = ?",
                (json.dumps(properties), self._namespace_key(namespace)),
            )
        return {"removed": removed, "updated": sorted(updates), "missing": missing}

    def drop_namespace(self, namespace):
        namespace = _catalog_namespace(namespace)
        key = self._namespace_key(namespace)
        with self._transaction(write=True) as connection:
            self._namespace_row(connection, namespace)
            has_children = any(
                tuple(json.loads(row[0]))[:-1] == namespace
                for row in connection.execute("SELECT namespace FROM iceberg_namespaces")
            )
            has_tables = connection.execute("SELECT 1 FROM iceberg_tables WHERE namespace = ?", (key,)).fetchone()
            if has_children or has_tables:
                raise IcebergCatalogError(f"Namespace {'.'.join(namespace)!r} is not empty", 409, "NamespaceNotEmptyException")
            connection.execute("DELETE FROM iceberg_namespaces WHERE namespace = ?", (key,))

    def list_tables(self, namespace):
        namespace = _catalog_namespace(namespace)
        with self._transaction() as connection:
            self._namespace_row(connection, namespace)
            return [
                {"namespace": namespace, "name": row[0]}
                for row in connection.execute(
                    "SELECT name FROM iceberg_tables WHERE namespace = ? ORDER BY name", (self._namespace_key(namespace),),
                )
            ]

    def _read_metadata(self, location):
        path = self.local_path(location)
        try:
            opener = gzip.open if path.name.endswith((".gz", ".gz.metadata.json")) else open
            with opener(path, "rb") as source:
                raw = source.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise IcebergCatalogError("Table metadata exceeds the 32 MiB limit")
            metadata = _catalog_object(json.loads(raw), "Table metadata")
        except (ValueError, OSError, EOFError) as exc:
            if isinstance(exc, IcebergCatalogError):
                raise
            raise IcebergCatalogError(f"Could not read metadata file {path.name!r}") from exc
        self._validate_metadata(metadata)
        return metadata

    def _validate_metadata(self, metadata):
        if metadata.get("format-version") != 2:
            raise IcebergCatalogError("Only Iceberg format version 2 is supported")
        self.local_path(metadata.get("location"))
        try:
            uuid.UUID(metadata.get("table-uuid", ""))
        except (AttributeError, ValueError, TypeError) as exc:
            raise IcebergCatalogError("Invalid table UUID") from exc
        arrays = ("schemas", "partition-specs", "sort-orders", "snapshots", "snapshot-log", "metadata-log")
        for key in arrays:
            for item in _catalog_array(metadata.get(key, []), key):
                _catalog_object(item, key)
        selectors = (
            ("schemas", "schema-id", "current-schema-id"),
            ("partition-specs", "spec-id", "default-spec-id"),
            ("sort-orders", "order-id", "default-sort-order-id"),
        )
        for array, key, selector in selectors:
            selected = _catalog_id(metadata.get(selector), selector)
            if selected not in {entry.get(key) for entry in metadata.get(array, ())}:
                raise IcebergCatalogError(f"Missing entry for {selector} {selected}")
        _catalog_id(metadata.get("last-column-id"), "last column ID")
        _catalog_id(metadata.get("last-sequence-number"), "last sequence number")
        _catalog_id(metadata.get("last-updated-ms"), "last updated time")
        refs = _catalog_object(metadata.get("refs", {}), "Snapshot refs")
        main_snapshot = _catalog_object(refs.get("main", {}), "Main snapshot ref").get("snapshot-id")
        current = metadata.get("current-snapshot-id")
        if (None if current == -1 else current) != main_snapshot:
            raise IcebergCatalogError("Current snapshot must match the main snapshot reference")
        for ref in refs.values():
            _catalog_snapshot(metadata, _catalog_object(ref, "Snapshot reference").get("snapshot-id"))

    def load_table(self, namespace, name):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        with self._transaction() as connection:
            location, _ = self._required_table_row(connection, namespace, name)
            return _catalog_response(self._read_metadata(location), location)

    def table_exists(self, namespace, name):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        with self._transaction() as connection:
            return self._table_row(connection, namespace, name) is not None

    def _write_metadata(self, metadata, version):
        directory = self.local_path(self.local_path(metadata["location"]) / "metadata")
        parents_to_sync = []
        ancestor = directory
        while not ancestor.exists():
            ancestor = ancestor.parent
            parents_to_sync.append(ancestor)
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / f"{version:05d}-{uuid.uuid4()}.metadata.json"
        payload = json.dumps(metadata, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(payload) > 32 * 1024 * 1024:
            raise IcebergCatalogError("Table metadata exceeds the 32 MiB limit")
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".iceberg-", delete=False) as output:
            temporary = Path(output.name)
            try:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary, destination)
            if os.name != "nt":
                for parent in (directory, *parents_to_sync):
                    directory_fd = os.open(parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
        return destination.as_uri()

    def _insert_table(self, connection, namespace, name, location, version):
        connection.execute(
            "INSERT INTO iceberg_tables VALUES (?, ?, ?, ?)",
            (self._namespace_key(namespace), name, location, version),
        )

    def create_table(
        self, namespace, name, schema, *, partition_spec=None, sort_order=None,
        properties=None, location=None, stage_create=False,
    ):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        schema, last_field = _catalog_schema(schema)
        spec, last_partition = _catalog_layout(
            {"fields": []} if partition_spec is None else partition_spec, "spec-id", 0,
        )
        sort_order = {"fields": []} if sort_order is None else sort_order
        sort_order, _ = _catalog_layout(sort_order, "order-id", int(bool(_catalog_object(sort_order, "Sort order").get("fields"))))
        properties = _catalog_properties({} if properties is None else properties)
        if properties.pop("format-version", "2") != "2":
            raise IcebergCatalogError("Only Iceberg format version 2 is supported")
        if type(stage_create) is not bool:
            raise IcebergCatalogError("stage-create must be a boolean")
        with self._transaction(write=True) as connection:
            namespace_properties = self._namespace_row(connection, namespace)
            if self._table_row(connection, namespace, name) is not None:
                raise IcebergCatalogError(f"Table {'.'.join((*namespace, name))!r} already exists", 409, "AlreadyExistsException")
            location = self._new_table_location(namespace, name, namespace_properties) if location is None else self.local_path(location).as_uri()
            metadata = _catalog_metadata(location)
            metadata.update({
                "last-column-id": last_field, "schemas": [schema], "current-schema-id": schema["schema-id"],
                "partition-specs": [spec], "default-spec-id": spec["spec-id"], "last-partition-id": last_partition,
                "sort-orders": [sort_order], "default-sort-order-id": sort_order["order-id"],
            })
            metadata["properties"].update(properties)
            if stage_create:
                return _catalog_response(metadata, None)
            pointer = self._write_metadata(metadata, 0)
            self._insert_table(connection, namespace, name, pointer, 0)
            return _catalog_response(metadata, pointer)

    def register_table(self, namespace, name, location, overwrite=False):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        if type(overwrite) is not bool:
            raise IcebergCatalogError("overwrite must be a boolean")
        path = self.local_path(location)
        version_prefix = re.match(r"^([0-9]{1,16})-", path.name)
        version = int(version_prefix[1]) if version_prefix else 0
        pointer = path.as_uri()
        metadata = self._read_metadata(pointer)
        with self._transaction(write=True) as connection:
            self._namespace_row(connection, namespace)
            row = self._table_row(connection, namespace, name)
            if row is not None:
                if not overwrite:
                    raise IcebergCatalogError("Table already exists", 409, "AlreadyExistsException")
                connection.execute(
                    "UPDATE iceberg_tables SET metadata_location = ?, version = ? WHERE namespace = ? AND name = ?",
                    (pointer, version, self._namespace_key(namespace), name),
                )
            else:
                self._insert_table(connection, namespace, name, pointer, version)
        return _catalog_response(metadata, pointer)

    def rename_table(self, source_namespace, source_name, target_namespace, target_name):
        source_namespace, source_name = _catalog_namespace(source_namespace), _catalog_component(source_name)
        target_namespace, target_name = _catalog_namespace(target_namespace), _catalog_component(target_name)
        with self._transaction(write=True) as connection:
            self._required_table_row(connection, source_namespace, source_name)
            self._namespace_row(connection, target_namespace)
            if (source_namespace, source_name) == (target_namespace, target_name):
                return
            if self._table_row(connection, target_namespace, target_name) is not None:
                raise IcebergCatalogError("Destination table already exists", 409, "AlreadyExistsException")
            connection.execute(
                "UPDATE iceberg_tables SET namespace = ?, name = ? WHERE namespace = ? AND name = ?",
                (self._namespace_key(target_namespace), target_name, self._namespace_key(source_namespace), source_name),
            )

    def drop_table(self, namespace, name, *, return_metadata=False):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        with self._transaction(write=True) as connection:
            location, _ = self._required_table_row(connection, namespace, name)
            result = _catalog_response(self._read_metadata(location), location) if return_metadata else None
            connection.execute(
                "DELETE FROM iceberg_tables WHERE namespace = ? AND name = ?", (self._namespace_key(namespace), name),
            )
            return result

    def _apply_updates(self, metadata, updates, creating):
        last_added = {}
        layouts = {
            "add-spec": ("spec", "partition-specs", "spec-id", "last-partition-id"),
            "add-sort-order": ("sort-order", "sort-orders", "order-id", None),
        }
        selectors = {
            "set-current-schema": ("schemas", "schema-id", "schema-id", "current-schema-id"),
            "set-default-spec": ("partition-specs", "spec-id", "spec-id", "default-spec-id"),
            "set-default-sort-order": ("sort-orders", "order-id", "sort-order-id", "default-sort-order-id"),
        }
        for update in updates:
            action = update.get("action")
            if not isinstance(action, str):
                raise IcebergCatalogError("A table update requires an action")
            if action == "assign-uuid":
                try:
                    value = str(uuid.UUID(update.get("uuid", "")))
                except (ValueError, TypeError, AttributeError) as exc:
                    raise IcebergCatalogError("Invalid table UUID") from exc
                if not creating and value != metadata["table-uuid"]:
                    raise IcebergCatalogError("Table UUID is immutable")
                metadata["table-uuid"] = value
            elif action == "upgrade-format-version":
                if update.get("format-version") != 2:
                    raise IcebergCatalogError("Only Iceberg format version 2 is supported")
            elif action == "set-location":
                metadata["location"] = self.local_path(update.get("location")).as_uri()
            elif action == "set-properties":
                properties = _catalog_properties(update.get("updates"))
                if properties.pop("format-version", "2") != "2":
                    raise IcebergCatalogError("Only Iceberg format version 2 is supported")
                metadata["properties"].update(properties)
            elif action == "remove-properties":
                for name in _catalog_array(update.get("removals"), "Property removals"):
                    if not isinstance(name, str):
                        raise IcebergCatalogError("Property names must be strings")
                    metadata["properties"].pop(name, None)
            elif action == "add-schema":
                schema, last_field = _catalog_schema(update.get("schema"))
                _catalog_add_entry(metadata["schemas"], "schema-id", schema)
                explicit_last_id = update.get("last-column-id")
                if explicit_last_id is not None:
                    last_field = max(last_field, _catalog_id(explicit_last_id, "last column ID"))
                metadata["last-column-id"] = max(metadata["last-column-id"], last_field)
                last_added["schemas"] = schema["schema-id"]
            elif action in layouts:
                input_key, array, id_key, maximum_key = layouts[action]
                value = _catalog_object(update.get(input_key), input_key)
                default_id = max((entry[id_key] for entry in metadata[array]), default=-1) + 1
                if action == "add-sort-order":
                    default_id = max(1, default_id) if value.get("fields") else 0
                value, maximum = _catalog_layout(value, id_key, default_id)
                _catalog_add_entry(metadata[array], id_key, value)
                last_added[array] = value[id_key]
                if maximum_key is not None:
                    metadata[maximum_key] = max(metadata[maximum_key], maximum)
            elif action in selectors:
                array, id_key, request_key, destination = selectors[action]
                selected = _catalog_id(update.get(request_key), request_key, -1)
                if selected == -1:
                    selected = last_added.get(array)
                if selected not in {entry[id_key] for entry in metadata[array]}:
                    raise IcebergCatalogError(f"No {array} entry with ID {selected}")
                metadata[destination] = selected
            elif action == "add-snapshot":
                self._add_snapshot(metadata, update.get("snapshot"))
            elif action == "set-snapshot-ref":
                self._set_snapshot_ref(metadata, update)
            elif action == "remove-snapshot-ref":
                ref_name = _catalog_component(update.get("ref-name"))
                if ref_name == "main":
                    raise IcebergCatalogError("The main branch cannot be removed")
                metadata["refs"].pop(ref_name, None)
            elif action == "remove-snapshots":
                self._remove_snapshots(metadata, update)
            elif action in {"set-statistics", "remove-statistics", "set-partition-statistics", "remove-partition-statistics"}:
                self._apply_statistics(metadata, update)
            else:
                raise IcebergCatalogError(f"Unsupported table update {action!r}")

    def _add_snapshot(self, metadata, value):
        snapshot = copy.deepcopy(_catalog_object(value, "Snapshot"))
        snapshot_id = _catalog_id(snapshot.get("snapshot-id"), "snapshot ID", 1)
        if any(item["snapshot-id"] == snapshot_id for item in metadata["snapshots"]):
            raise IcebergCatalogError(f"Snapshot {snapshot_id} already exists", 409, "CommitFailedException")
        sequence = _catalog_id(snapshot.get("sequence-number"), "snapshot sequence number", 1)
        if sequence != metadata["last-sequence-number"] + 1:
            raise IcebergCatalogError("Snapshot sequence does not follow the current table sequence", 409, "CommitFailedException")
        snapshot["timestamp-ms"] = _catalog_id(snapshot.get("timestamp-ms"), "snapshot timestamp")
        if snapshot.get("parent-snapshot-id") is not None:
            _catalog_snapshot(metadata, _catalog_id(snapshot["parent-snapshot-id"], "parent snapshot ID", 1))
        if snapshot.get("schema-id") is not None:
            schema_id = _catalog_id(snapshot["schema-id"], "snapshot schema ID")
            if schema_id not in {schema["schema-id"] for schema in metadata["schemas"]}:
                raise IcebergCatalogError("Snapshot refers to an unknown schema")
        self.local_path(snapshot.get("manifest-list"))
        summary = _catalog_properties(snapshot.get("summary"))
        if summary.get("operation") not in {"append", "replace", "overwrite", "delete"}:
            raise IcebergCatalogError("Invalid snapshot operation")
        metadata["last-sequence-number"] = sequence
        metadata["snapshots"].append(snapshot)

    def _set_snapshot_ref(self, metadata, update):
        name = _catalog_component(update.get("ref-name"))
        snapshot_id = _catalog_id(update.get("snapshot-id"), "snapshot ID", 1)
        _catalog_snapshot(metadata, snapshot_id)
        ref_type = update.get("type")
        if ref_type not in ("branch", "tag") or (name == "main" and ref_type != "branch"):
            raise IcebergCatalogError("Invalid snapshot reference type")
        ref = {"snapshot-id": snapshot_id, "type": ref_type}
        for item in ("min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms"):
            if update.get(item) is not None:
                ref[item] = _catalog_id(update[item], item, 1)
        if name == "main" and metadata["current-snapshot-id"] != snapshot_id:
            metadata["current-snapshot-id"] = snapshot_id
            metadata["snapshot-log"].append({"snapshot-id": snapshot_id, "timestamp-ms": metadata["last-updated-ms"]})
        metadata["refs"][name] = ref

    @staticmethod
    def _remove_snapshots(metadata, update):
        removed = {
            _catalog_id(item, "snapshot ID", 1) for item in _catalog_array(update.get("snapshot-ids"), "Snapshot IDs")
        }
        for ref, value in metadata["refs"].items():
            if value["snapshot-id"] in removed:
                raise IcebergCatalogError(f"Snapshot is still referenced by {ref!r}")
        metadata["snapshots"] = [item for item in metadata["snapshots"] if item["snapshot-id"] not in removed]
        for key in ("snapshot-log", "statistics", "partition-statistics"):
            if key in metadata:
                metadata[key] = [item for item in metadata[key] if item["snapshot-id"] not in removed]

    def _apply_statistics(self, metadata, update):
        field = "partition-statistics" if "partition-statistics" in update["action"] else "statistics"
        if update["action"].startswith("set-"):
            value = copy.deepcopy(_catalog_object(update.get(field), field))
            snapshot_id = _catalog_id(value.get("snapshot-id"), "snapshot ID", 1)
            _catalog_snapshot(metadata, snapshot_id)
            self.local_path(value.get("statistics-path"))
        else:
            value = None
            snapshot_id = _catalog_id(update.get("snapshot-id"), "snapshot ID", 1)
        metadata[field] = [entry for entry in metadata.get(field, []) if entry["snapshot-id"] != snapshot_id]
        if value is not None:
            metadata[field].append(value)

    def _prepare_commit(self, connection, namespace, name, requirements, updates):
        requirements = [_catalog_object(value, "Requirement") for value in _catalog_array(requirements, "Requirements")]
        updates = [_catalog_object(value, "Table update") for value in _catalog_array(updates, "Updates")]
        row = self._table_row(connection, namespace, name)
        existing = row is not None
        if existing:
            old_pointer, version = row
            metadata = self._read_metadata(old_pointer)
        else:
            if not any(requirement.get("type") == "assert-create" for requirement in requirements):
                self._required_table_row(connection, namespace, name)
            old_pointer, version = None, -1
            metadata = None
        _catalog_check_requirements(metadata, requirements)
        if existing and not updates:
            return metadata, old_pointer, version, False
        if existing:
            old_timestamp = metadata["last-updated-ms"]
            metadata["metadata-log"].append({"timestamp-ms": old_timestamp, "metadata-file": old_pointer})
            metadata["last-updated-ms"] = max(_catalog_now_ms(), old_timestamp + 1)
        else:
            properties = self._namespace_row(connection, namespace)
            metadata = _catalog_metadata(self._new_table_location(namespace, name, properties))
        self._apply_updates(metadata, updates, creating=not existing)
        self._validate_metadata(metadata)
        return metadata, old_pointer, version + 1, True

    def _publish_commit(self, connection, namespace, name, prepared):
        metadata, old_pointer, version, changed = prepared
        if not changed:
            return _catalog_response(metadata, old_pointer)
        new_pointer = self._write_metadata(metadata, version)
        if old_pointer is None:
            self._insert_table(connection, namespace, name, new_pointer, version)
        else:
            connection.execute(
                "UPDATE iceberg_tables SET metadata_location = ?, version = ? WHERE namespace = ? AND name = ?",
                (new_pointer, version, self._namespace_key(namespace), name),
            )
        return _catalog_response(metadata, new_pointer)

    def commit_table(self, namespace, name, requirements, updates):
        namespace, name = _catalog_namespace(namespace), _catalog_component(name)
        with self._transaction(write=True) as connection:
            prepared = self._prepare_commit(connection, namespace, name, requirements, updates)
            return self._publish_commit(connection, namespace, name, prepared)

    def commit_transaction(self, changes):
        pending = {}
        with self._transaction(write=True) as connection:
            for change in _catalog_array(changes, "Table changes"):
                change = _catalog_object(change, "Table change")
                namespace, name = _catalog_identifier(change.get("identifier"))
                if (namespace, name) in pending:
                    raise IcebergCatalogError("A transaction cannot include the same table twice")
                pending[namespace, name] = self._prepare_commit(
                    connection, namespace, name, change.get("requirements"), change.get("updates"),
                )
            for (namespace, name), prepared in pending.items():
                self._publish_commit(connection, namespace, name, prepared)


def _catalog_identifier(value):
    value = _catalog_object(value, "Table identifier")
    return _catalog_namespace(value.get("namespace")), _catalog_component(value.get("name"))


def _catalog_page(items, query, key):
    size = query.get("pageSize", [None])[0]
    start = query.get("pageToken", [""])[0]
    if start and (len(start) > 12 or not start.isascii() or not start.isdecimal()):
        raise IcebergCatalogError("Invalid page token")
    start = int(start or 0)
    if size is None:
        return {key: items[start:]}
    if len(size) > 9 or not size.isascii() or not size.isdecimal() or int(size) <= 0:
        raise IcebergCatalogError("Invalid page size")
    stop = start + int(size)
    return {key: items[start:stop], "next-page-token": str(stop) if stop < len(items) else None}


_catalog_endpoints = [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/transactions/commit",
]


class _IcebergCatalogHandler(BaseHTTPRequestHandler):
    server_version = "jjap-iceberg/1"

    def _read_body(self):
        length = self.headers.get("Content-Length", "")
        if not length or len(length) > 9 or not length.isascii() or not length.isdecimal():
            raise IcebergCatalogError("A valid Content-Length is required")
        count = int(length)
        if count > 8 * 1024 * 1024:
            raise IcebergCatalogError("Request body exceeds 8 MiB", 413)
        raw = self.rfile.read(count)
        if len(raw) != count:
            raise IcebergCatalogError("Incomplete request body")
        return raw

    def _read_json(self):
        try:
            return _catalog_object(json.loads(self._read_body()), "Request body")
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise IcebergCatalogError("Invalid request JSON") from exc

    def _respond(self, status, data=None, headers=None):
        payload = b"" if data is None else json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        if payload:
            self.send_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD" and payload:
            self.wfile.write(payload)

    @staticmethod
    def _not_found():
        raise IcebergCatalogError("Unknown catalog endpoint", 404, "NoSuchEndpointException")

    def _route(self):
        try:
            url = urlsplit(self.path)
            parts = [unquote(part, errors="strict") for part in url.path.strip("/").split("/")]
            query = parse_qs(url.query, max_num_fields=64, errors="strict")
        except (UnicodeError, ValueError) as exc:
            raise IcebergCatalogError("Invalid request URI") from exc
        return parts, query

    def _dispatch(self):
        parts, query = self._route()
        method = self.command
        catalog = self.server.catalog
        if parts == ["v1", "config"] and method == "GET":
            self._respond(200, {
                "defaults": {"scan-planning-mode": "client"},
                "overrides": {"warehouse": catalog.warehouse.as_uri()},
                "endpoints": _catalog_endpoints,
            })
            return
        if not parts or parts[0] != "v1":
            self._not_found()
        self._catalog_request(catalog, method, parts[1:], query)

    def _catalog_request(self, catalog, method, parts, query):
        if parts == ["tables", "rename"] and method == "POST":
            body = self._read_json()
            catalog.rename_table(*_catalog_identifier(body.get("source")), *_catalog_identifier(body.get("destination")))
            self._respond(204)
            return
        if parts == ["transactions", "commit"] and method == "POST":
            catalog.commit_transaction(self._read_json().get("table-changes"))
            self._respond(204)
            return
        if not parts or parts[0] != "namespaces":
            self._not_found()
        if len(parts) == 1:
            if method == "GET":
                parent = query.get("parent", [""])[0]
                try:
                    namespaces = catalog.list_namespaces(parent.split("\x1f") if parent else ())
                except IcebergCatalogError as exc:
                    # PyIceberg can URL-encode each component before encoding the query string.
                    decoded = unquote(parent, errors="strict")
                    if exc.status != 404 or decoded == parent:
                        raise
                    namespaces = catalog.list_namespaces(decoded.split("\x1f"))
                self._respond(200, _catalog_page(namespaces, query, "namespaces"))
            elif method == "POST":
                body = self._read_json()
                self._respond(200, catalog.create_namespace(body.get("namespace"), body.get("properties")))
            else:
                self._not_found()
            return
        namespace = _catalog_namespace(parts[1].split("\x1f"))
        tail = parts[2:]
        if not tail:
            if method in {"GET", "HEAD"}:
                self._respond(200, catalog.load_namespace(namespace))
            elif method == "DELETE":
                catalog.drop_namespace(namespace)
                self._respond(204)
            else:
                self._not_found()
        elif tail == ["properties"] and method == "POST":
            body = self._read_json()
            self._respond(200, catalog.update_namespace(namespace, body.get("removals", []), body.get("updates", {})))
        elif tail == ["register"] and method == "POST":
            body = self._read_json()
            self._respond(200, catalog.register_table(
                namespace, body.get("name"), body.get("metadata-location"), body.get("overwrite", False),
            ))
        elif tail == ["tables"]:
            if method == "GET":
                self._respond(200, _catalog_page(catalog.list_tables(namespace), query, "identifiers"))
            elif method == "POST":
                body = self._read_json()
                self._respond(200, catalog.create_table(
                    namespace, body.get("name"), body.get("schema"), partition_spec=body.get("partition-spec"),
                    sort_order=body.get("write-order"), properties=body.get("properties"), location=body.get("location"),
                    stage_create=body.get("stage-create", False),
                ))
            else:
                self._not_found()
        elif tail[0] == "tables" and len(tail) in {2, 3}:
            self._table_request(catalog, method, namespace, tail[1], tail[2:], query)
        else:
            self._not_found()

    def _table_request(self, catalog, method, namespace, name, tail, query):
        if tail == ["metrics"] and method == "POST":
            self._read_json()
            self._respond(204)
        elif tail == ["unregister"] and method == "POST":
            self._respond(200, catalog.drop_table(namespace, name, return_metadata=True))
        elif tail:
            self._not_found()
        elif method == "HEAD":
            if not catalog.table_exists(namespace, name):
                raise IcebergCatalogError("Table was not found", 404, "NoSuchTableException")
            self._respond(200)
        elif method == "GET":
            result = catalog.load_table(namespace, name)
            snapshot_mode = query.get("snapshots", ["all"])[0]
            if snapshot_mode not in ("all", "refs"):
                raise IcebergCatalogError("snapshots must be all or refs")
            if snapshot_mode == "refs":
                metadata = result["metadata"]
                selected = {ref["snapshot-id"] for ref in metadata["refs"].values()}
                metadata["snapshots"] = [item for item in metadata["snapshots"] if item["snapshot-id"] in selected]
            digest = hashlib.sha256((result["metadata-location"] + snapshot_mode).encode()).hexdigest()
            etag = f'"{digest}"'
            conditions = {item.strip().removeprefix("W/") for item in self.headers.get("If-None-Match", "").split(",")}
            if etag in conditions or "*" in conditions:
                self._respond(304, headers={"ETag": etag})
            else:
                self._respond(200, result, {"ETag": etag})
        elif method == "POST":
            body = self._read_json()
            if body.get("identifier") is not None and _catalog_identifier(body["identifier"]) != (namespace, name):
                raise IcebergCatalogError("Commit identifier does not match its request path")
            self._respond(200, catalog.commit_table(namespace, name, body.get("requirements"), body.get("updates")))
        elif method == "DELETE":
            if query.get("purgeRequested", ["false"])[0].lower() == "true":
                raise IcebergCatalogError("Physical table purge is unsupported", 501, "UnsupportedOperationException")
            catalog.drop_table(namespace, name)
            self._respond(204)
        else:
            self._not_found()

    def _handle(self):
        try:
            self._dispatch()
        except IcebergCatalogError as exc:
            self._respond(exc.status, {"error": {"message": str(exc), "type": exc.error_type, "code": exc.status}})
        except (ValueError, TypeError, KeyError, RecursionError) as exc:
            self._respond(400, {"error": {"message": str(exc), "type": "BadRequestException", "code": 400}})
        except (BrokenPipeError, ConnectionResetError):
            return
        except (OSError, sqlite3.Error) as exc:
            self.log_error("Catalog operation failed: %s", exc)
            self._respond(500, {"error": {
                "message": "Catalog storage operation failed", "type": "InternalServerError", "code": 500,
            }})

    do_GET = do_HEAD = do_POST = do_DELETE = _handle


def iceberg_serve(warehouse, db_file=None, host="127.0.0.1", port=8181):
    """Serve an Iceberg catalog over HTTP until interrupted."""
    catalog = IcebergCatalog(warehouse, db_file)
    with ThreadingHTTPServer((host, port), _IcebergCatalogHandler) as server:
        server.catalog = catalog
        print(f"Iceberg test server listening on http://{host}:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


# Polaris management and OAuth2 for local FILE catalogs.


_POLARIS_NAMESPACE_PRIVILEGES = {
    "NAMESPACE_LIST", "NAMESPACE_CREATE", "NAMESPACE_DROP",
    "NAMESPACE_READ_PROPERTIES", "NAMESPACE_WRITE_PROPERTIES",
}
_POLARIS_TABLE_PRIVILEGES = {
    "TABLE_LIST", "TABLE_CREATE", "TABLE_DROP", "TABLE_READ_PROPERTIES",
    "TABLE_WRITE_PROPERTIES", "TABLE_READ_DATA", "TABLE_WRITE_DATA",
}
_POLARIS_PRIVILEGES = _POLARIS_NAMESPACE_PRIVILEGES | _POLARIS_TABLE_PRIVILEGES | {
    "CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA", "CATALOG_READ_CONFIG",
    "CATALOG_READ_PROPERTIES", "NAMESPACE_FULL_METADATA", "TABLE_FULL_METADATA",
}
_POLARIS_TOP_ENTITIES = {
    "catalogs": ("catalog", "catalogs", "polaris_catalogs"),
    "principals": ("principal", "principals", "polaris_principals"),
    "principal-roles": ("principalRole", "roles", "polaris_principal_roles"),
}
_POLARIS_CATALOG_ROLE = ("catalogRole", "roles", "polaris_catalog_roles")


def _polaris_name(value):
    value = _catalog_component(value)
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", value):
        raise IcebergCatalogError("Invalid Polaris entity name")
    return value


def _polaris_secret(value):
    return hashlib.sha256(value.encode("utf-8")).digest()


def _polaris_denied(message="Insufficient catalog privileges"):
    return IcebergCatalogError(message, 403, "ForbiddenException")


class _PolarisHandler(_IcebergCatalogHandler):
    server_version = "jjap-polaris/1"
    timeout = 15

    def _dispatch(self):
        parts, query = self._route()
        if self.command == "GET" and parts in (["q", "health"], ["q", "health", "ready"]):
            self._respond(200, {"status": "UP", "checks": []})
            return
        state = self.server.polaris
        if self.headers.get("Polaris-Realm", state.realm) != state.realm:
            raise IcebergCatalogError("Unknown Polaris realm", 404, "NotFoundException")
        # Keep authorization, catalog deletion, and catalog operations ordered on one node.
        with state.lock:
            if parts == ["api", "catalog", "v1", "oauth", "tokens"] and self.command == "POST":
                self._token_request(state)
            elif parts[:3] == ["api", "management", "v1"]:
                client, _ = state.authenticate(self.headers.get("Authorization", ""))
                if client != "root":
                    raise _polaris_denied("Service administration requires the root principal")
                with _catalog_transaction(state.db_file, self.command != "GET") as connection:
                    status, result = self._management(state, connection, parts[3:])
                self._respond(status, result)
            elif parts[:3] == ["api", "catalog", "v1"]:
                identity = state.authenticate(self.headers.get("Authorization", ""))
                self._polaris_catalog_request(state, identity, parts[3:], query)
            else:
                self._not_found()

    def _token_request(self, state):
        body = self._read_body()
        try:
            form = parse_qs(body.decode("utf-8"), max_num_fields=20, errors="strict")
            scheme, _, value = self.headers.get("Authorization", "").partition(" ")
            if scheme.lower() == "basic":
                client, separator, secret = base64.b64decode(value, validate=True).decode().partition(":")
                if not separator:
                    raise ValueError("missing credential separator")
            else:
                client = form.get("client_id", [""])[0]
                secret = form.get("client_secret", [""])[0]
        except ValueError:
            self._respond(400, {"error": "invalid_request"})
            return
        if form.get("grant_type") != ["client_credentials"]:
            self._respond(400, {"error": "unsupported_grant_type"})
            return
        scope = form.get("scope", ["PRINCIPAL_ROLE:ALL"])[0]
        try:
            token = state.issue_token(client, secret, scope)
        except IcebergCatalogError as exc:
            self._respond(exc.status, {"error": exc.error_type})
            return
        self._respond(200, {
            "access_token": token, "token_type": "Bearer", "expires_in": state.token_lifetime,
            "scope": scope,
        }, {"Cache-Control": "no-store"})

    def _polaris_catalog_request(self, state, identity, parts, query):
        if parts == ["config"] and self.command == "GET":
            name = _polaris_name(query.get("warehouse", [None])[0])
            catalog, privileges = state.catalog_access(name, identity)
            if not privileges:
                raise _polaris_denied()
            self._respond(200, {
                "defaults": {"scan-planning-mode": "client"},
                "overrides": {"prefix": name, "warehouse": catalog.warehouse.as_uri()},
                "endpoints": _catalog_endpoints,
            })
            return
        if len(parts) < 2:
            self._not_found()
        catalog, privileges = state.catalog_access(_polaris_name(parts[0]), identity)
        _polaris_authorize(privileges, self.command, parts[1:])
        self._catalog_request(catalog, self.command, parts[1:], query)

    def _management(self, state, connection, parts):
        method = self.command
        if not parts or parts[0] not in _POLARIS_TOP_ENTITIES:
            self._not_found()
        kind, *tail = parts
        if len(tail) < 2:
            return self._entities(state, connection, _POLARIS_TOP_ENTITIES[kind], tail)

        name = _polaris_name(tail[0])
        state.entity(connection, _POLARIS_TOP_ENTITIES[kind][2], name)
        if kind == "catalogs" and tail[1] == "catalog-roles":
            if len(tail) <= 3:
                return self._entities(state, connection, _POLARIS_CATALOG_ROLE, tail[2:], catalog=name)
            if len(tail) == 4 and tail[3] == "grants":
                return self._grants(state, connection, name, _polaris_name(tail[2]))
        if kind == "principals" and tail[1] == "principal-roles" and len(tail) <= 3:
            return self._assign_roles(state, connection, tail[2:], principal=name)
        if kind == "principal-roles" and tail[1] == "catalog-roles" and 3 <= len(tail) <= 4:
            catalog = _polaris_name(tail[2])
            state.entity(connection, "polaris_catalogs", catalog)
            return self._assign_roles(state, connection, tail[3:], principal_role=name, catalog=catalog)
        self._not_found()

    def _entities(self, state, connection, info, tail, catalog=None):
        wrapper, list_key, table = info
        method = self.command
        condition, params = ("catalog = ?", (catalog,)) if catalog is not None else ("1 = 1", ())
        if not tail and method == "GET":
            rows = connection.execute(f"SELECT payload FROM {table} WHERE {condition} ORDER BY name", params)
            return 200, {list_key: [json.loads(row[0]) for row in rows]}
        if not tail and method == "POST":
            body = self._read_json()
            return 201, state.create_entity(connection, body, wrapper, catalog)
        if not tail:
            self._not_found()
        name = _polaris_name(tail[0])
        entity = state.entity(connection, table, name, catalog)
        if method == "GET":
            return 200, entity
        if method == "DELETE":
            state.delete_entity(connection, table, name, entity, catalog)
            return 204, None
        self._not_found()

    def _assign_roles(self, state, connection, tail, *, principal=None, principal_role=None, catalog=None):
        if catalog is None:
            table = "polaris_principal_assignments"
            keys, values = ("principal",), (principal,)
            wrapper, role_table = "principalRole", "polaris_principal_roles"
            join = "r.name = a.role"
        else:
            table = "polaris_catalog_assignments"
            keys, values = ("principal_role", "catalog"), (principal_role, catalog)
            wrapper, role_table = "catalogRole", "polaris_catalog_roles"
            join = "r.name = a.role AND r.catalog = a.catalog"
        filter_sql = " AND ".join(f"a.{key} = ?" for key in keys)
        if not tail and self.command == "GET":
            rows = connection.execute(
                f"SELECT r.payload FROM {table} a JOIN {role_table} r ON {join} WHERE {filter_sql} ORDER BY a.role",
                values,
            )
            return 200, {"roles": [json.loads(row[0]) for row in rows]}
        if not tail and self.command == "PUT":
            role = _polaris_name(_catalog_object(self._read_json().get(wrapper), wrapper).get("name"))
            state.entity(connection, role_table, role, catalog)
            columns = ", ".join((*keys, "role"))
            placeholders = ", ".join("?" for _ in range(len(keys) + 1))
            connection.execute(f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})", (*values, role))
            return 201, None
        if tail and self.command == "DELETE":
            connection.execute(f"DELETE FROM {table} AS a WHERE {filter_sql} AND a.role = ?", (*values, _polaris_name(tail[0])))
            return 204, None
        self._not_found()

    def _grants(self, state, connection, catalog, role):
        state.entity(connection, "polaris_catalog_roles", role, catalog)
        if self.command == "GET":
            rows = connection.execute(
                "SELECT privilege FROM polaris_catalog_grants WHERE catalog = ? AND role = ? ORDER BY privilege",
                (catalog, role),
            )
            return 200, {"grants": [{"type": "catalog", "privilege": row[0]} for row in rows]}
        if self.command in ("PUT", "POST"):
            grant = _catalog_object(self._read_json().get("grant"), "Grant")
            privilege = grant.get("privilege")
            if grant.get("type") != "catalog" or not isinstance(privilege, str) or privilege not in _POLARIS_PRIVILEGES:
                raise IcebergCatalogError("Unsupported catalog grant")
            if self.command == "PUT":
                connection.execute("INSERT OR IGNORE INTO polaris_catalog_grants VALUES (?, ?, ?)", (catalog, role, privilege))
            else:
                connection.execute(
                    "DELETE FROM polaris_catalog_grants WHERE catalog = ? AND role = ? AND privilege = ?",
                    (catalog, role, privilege),
                )
            return 201, None
        self._not_found()

    do_PUT = _IcebergCatalogHandler._handle


class _PolarisState:
    token_lifetime = 3600

    def __init__(self, warehouse, db_file, password, realm):
        self.warehouse = _catalog_local_path(warehouse)
        self.warehouse.mkdir(parents=True, exist_ok=True)
        self.db_file = Path(db_file).resolve() if db_file else self.warehouse / ".polaris.sqlite3"
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        self.realm = _polaris_name(realm)
        self.root_secret = _polaris_secret(password)
        self.lock = threading.Lock()
        self.tokens = {}
        self.catalogs = {}
        with _catalog_transaction(self.db_file, True) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS polaris_catalogs (
                    name TEXT PRIMARY KEY, payload TEXT NOT NULL, location TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS polaris_principals (
                    name TEXT PRIMARY KEY, payload TEXT NOT NULL, client_id TEXT NOT NULL UNIQUE,
                    secret BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS polaris_principal_roles (name TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS polaris_catalog_roles (
                    catalog TEXT REFERENCES polaris_catalogs(name) ON DELETE CASCADE,
                    name TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (catalog, name)
                );
                CREATE TABLE IF NOT EXISTS polaris_principal_assignments (
                    principal TEXT REFERENCES polaris_principals(name) ON DELETE CASCADE,
                    role TEXT REFERENCES polaris_principal_roles(name) ON DELETE CASCADE,
                    PRIMARY KEY (principal, role)
                );
                CREATE TABLE IF NOT EXISTS polaris_catalog_assignments (
                    principal_role TEXT REFERENCES polaris_principal_roles(name) ON DELETE CASCADE,
                    catalog TEXT NOT NULL, role TEXT NOT NULL, PRIMARY KEY (principal_role, catalog, role),
                    FOREIGN KEY (catalog, role) REFERENCES polaris_catalog_roles(catalog, name) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS polaris_catalog_grants (
                    catalog TEXT NOT NULL, role TEXT NOT NULL, privilege TEXT NOT NULL,
                    PRIMARY KEY (catalog, role, privilege),
                    FOREIGN KEY (catalog, role) REFERENCES polaris_catalog_roles(catalog, name) ON DELETE CASCADE
                );
            """)

    @staticmethod
    def entity(connection, table, name, catalog=None):
        selector = "name = ?" if catalog is None else "name = ? AND catalog = ?"
        keys = (name,) if catalog is None else (name, catalog)
        row = connection.execute(f"SELECT payload FROM {table} WHERE {selector}", keys).fetchone()
        if row is None:
            raise IcebergCatalogError(f"Unknown Polaris entity {name!r}", 404, "NotFoundException")
        return json.loads(row[0])

    def create_entity(self, connection, body, wrapper, catalog):
        data = _catalog_object(body.get(wrapper), wrapper)
        name = _polaris_name(data.get("name"))
        properties = _catalog_properties(data.get("properties", {}))
        now = _catalog_now_ms()
        payload = {
            "name": name, "properties": properties, "createTimestamp": now,
            "lastUpdateTimestamp": now, "entityVersion": 1,
        }
        try:
            if wrapper == "catalog":
                self._create_catalog(connection, data, payload)
            elif wrapper == "principal":
                if name == "root" or body.get("credentialRotationRequired", False):
                    raise IcebergCatalogError("Reserved principal name or unsupported required credential rotation")
                client_id, secret = uuid.uuid4().hex, secrets.token_urlsafe(32)
                payload["clientId"] = client_id
                connection.execute(
                    "INSERT INTO polaris_principals VALUES (?, ?, ?, ?)",
                    (name, json.dumps(payload), client_id, _polaris_secret(secret)),
                )
                return {"principal": payload, "credentials": {"clientId": client_id, "clientSecret": secret}}
            elif wrapper == "principalRole":
                if data.get("federated", False):
                    raise IcebergCatalogError("Federated principal roles are unsupported")
                payload["federated"] = False
                connection.execute("INSERT INTO polaris_principal_roles VALUES (?, ?)", (name, json.dumps(payload)))
            else:
                connection.execute("INSERT INTO polaris_catalog_roles VALUES (?, ?, ?)", (catalog, name, json.dumps(payload)))
        except sqlite3.IntegrityError as exc:
            raise IcebergCatalogError(f"Entity {name!r} already exists", 409, "AlreadyExistsException") from exc
        return payload

    def _create_catalog(self, connection, data, payload):
        storage = _catalog_object(data.get("storageConfigInfo"), "Storage configuration")
        if data.get("type") != "INTERNAL" or storage.get("storageType") != "FILE":
            raise IcebergCatalogError("Only INTERNAL catalogs with FILE storage are supported")
        name = payload["name"]
        location = _catalog_local_path(payload["properties"].get("default-base-location", self.warehouse / name))
        if location == self.warehouse or not location.is_relative_to(self.warehouse) or self.db_file.is_relative_to(location):
            raise IcebergCatalogError("Catalog location must be a dedicated directory under the Polaris warehouse")
        for (previous,) in connection.execute("SELECT location FROM polaris_catalogs"):
            previous = Path(previous)
            if previous.is_relative_to(location) or location.is_relative_to(previous):
                raise IcebergCatalogError("Catalog storage locations overlap", 409, "AlreadyExistsException")
        allowed = _catalog_array(storage.get("allowedLocations", [location]), "Allowed locations")
        # Storage is rooted at the single default base location for these local catalogs.
        if {_catalog_local_path(path) for path in allowed} != {location}:
            raise IcebergCatalogError("FILE allowedLocations must only contain the default base location")
        payload["properties"]["default-base-location"] = location.as_uri()
        payload["type"] = "INTERNAL"
        payload["storageConfigInfo"] = {"storageType": "FILE", "allowedLocations": [location.as_uri()]}
        connection.execute("INSERT INTO polaris_catalogs VALUES (?, ?, ?)", (name, json.dumps(payload), str(location)))
        role = {
            "name": "catalog_admin", "properties": {}, "createTimestamp": payload["createTimestamp"],
            "lastUpdateTimestamp": payload["lastUpdateTimestamp"], "entityVersion": 1,
        }
        connection.execute("INSERT INTO polaris_catalog_roles VALUES (?, ?, ?)", (name, role["name"], json.dumps(role)))
        connection.execute("INSERT INTO polaris_catalog_grants VALUES (?, ?, ?)", (name, role["name"], "CATALOG_MANAGE_CONTENT"))
        self._iceberg(name, payload)

    def delete_entity(self, connection, table, name, payload, catalog):
        if table == "polaris_catalogs":
            extra = connection.execute(
                "SELECT 1 FROM polaris_catalog_roles WHERE catalog = ? AND name != 'catalog_admin'", (name,),
            ).fetchone()
            if extra or self._iceberg(name, payload).list_namespaces():
                raise IcebergCatalogError("Catalog is not empty", 409, "CatalogNotEmptyException")
        if catalog is None:
            connection.execute(f"DELETE FROM {table} WHERE name = ?", (name,))
        else:
            if name == "catalog_admin":
                raise IcebergCatalogError("The built-in catalog role cannot be deleted")
            connection.execute(f"DELETE FROM {table} WHERE catalog = ? AND name = ?", (catalog, name))
        if table == "polaris_catalogs":
            self.catalogs.pop(name, None)

    def _iceberg(self, name, payload):
        location = payload["properties"]["default-base-location"]
        catalog = self.catalogs.get(name)
        if catalog is None or catalog.warehouse.as_uri() != location:
            path = _catalog_local_path(location)
            if path.as_uri() != location or not path.is_relative_to(self.warehouse):
                raise IcebergCatalogError("Catalog storage location has changed", 409, "ConflictException")
            catalog = IcebergCatalog(path)
            self.catalogs[name] = catalog
        return catalog

    def issue_token(self, client, secret, scope):
        roles = None
        if scope not in ("PRINCIPAL_ROLE:ALL", "catalog"):
            if not scope.startswith("PRINCIPAL_ROLE:"):
                raise IcebergCatalogError("Unsupported token scope", 400, "invalid_scope")
            try:
                roles = {_polaris_name(role) for role in scope.split(":", 1)[1].split(",")}
            except IcebergCatalogError as exc:
                raise IcebergCatalogError("Invalid principal role scope", 400, "invalid_scope") from exc
        if client == "root":
            expected = self.root_secret
            if roles not in (None, {"service_admin"}):
                raise IcebergCatalogError("Unsupported root role scope", 400, "invalid_scope")
        else:
            with _catalog_transaction(self.db_file) as connection:
                row = connection.execute("SELECT secret FROM polaris_principals WHERE client_id = ?", (client,)).fetchone()
                expected = row[0] if row else b""
        if not hmac.compare_digest(_polaris_secret(secret), expected):
            raise IcebergCatalogError("Invalid client credentials", 401, "invalid_client")
        now = time.monotonic()
        self.tokens = {key: value for key, value in self.tokens.items() if value[0] > now}
        if len(self.tokens) >= 4096:
            raise IcebergCatalogError("Too many active tokens", 429, "temporarily_unavailable")
        token = secrets.token_urlsafe(32)
        self.tokens[_polaris_secret(token)] = (now + self.token_lifetime, client, roles)
        return token

    def authenticate(self, authorization):
        scheme, _, token = authorization.partition(" ")
        record = self.tokens.get(_polaris_secret(token)) if scheme.lower() == "bearer" else None
        if record is None or record[0] <= time.monotonic():
            raise IcebergCatalogError("Missing or expired bearer token", 401, "NotAuthorizedException")
        return record[1:]

    def catalog_access(self, name, identity):
        client, roles = identity
        with _catalog_transaction(self.db_file) as connection:
            payload = self.entity(connection, "polaris_catalogs", name)
            if client == "root":
                privileges = {"CATALOG_MANAGE_CONTENT"}
            else:
                rows = connection.execute("""
                    SELECT a.role, g.privilege
                    FROM polaris_principals p
                    JOIN polaris_principal_assignments a ON a.principal = p.name
                    JOIN polaris_catalog_assignments c ON c.principal_role = a.role
                    JOIN polaris_catalog_grants g ON g.catalog = c.catalog AND g.role = c.role
                    WHERE p.client_id = ? AND c.catalog = ?
                """, (client, name))
                privileges = {privilege for role, privilege in rows if roles is None or role in roles}
        if not privileges:
            raise _polaris_denied()
        return self._iceberg(name, payload), privileges


def _polaris_authorize(privileges, method, parts):
    if privileges & {"CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA"}:
        return
    if "NAMESPACE_FULL_METADATA" in privileges:
        privileges = privileges | _POLARIS_NAMESPACE_PRIVILEGES | {"TABLE_LIST"}
    if "TABLE_FULL_METADATA" in privileges:
        privileges = privileges | (_POLARIS_TABLE_PRIVILEGES - {"TABLE_READ_DATA", "TABLE_WRITE_DATA"})
    required = None
    if parts == ["transactions", "commit"] and method == "POST":
        required = "TABLE_FULL_METADATA"
    elif parts == ["namespaces"]:
        required = {"GET": "NAMESPACE_LIST", "POST": "NAMESPACE_CREATE"}.get(method)
    elif len(parts) >= 2 and parts[0] == "namespaces":
        tail = parts[2:]
        if not tail:
            required = {"GET": "NAMESPACE_READ_PROPERTIES", "HEAD": "NAMESPACE_READ_PROPERTIES",
                        "DELETE": "NAMESPACE_DROP"}.get(method)
        elif tail == ["properties"] and method == "POST":
            required = "NAMESPACE_WRITE_PROPERTIES"
        elif tail == ["register"] and method == "POST":
            required = "TABLE_FULL_METADATA"
        elif tail == ["tables"]:
            required = {"GET": "TABLE_LIST", "POST": "TABLE_CREATE"}.get(method)
        elif tail[0] == "tables" and len(tail) == 2:
            if method in ("GET", "HEAD"):
                if privileges & {"TABLE_READ_PROPERTIES", "TABLE_READ_DATA", "TABLE_WRITE_DATA"}:
                    return
            required = {"POST": "TABLE_FULL_METADATA", "DELETE": "TABLE_DROP"}.get(method)
        elif tail[0] == "tables" and len(tail) == 3 and method == "POST":
            if tail[2] == "metrics" and privileges & {"TABLE_READ_PROPERTIES", "TABLE_READ_DATA", "TABLE_WRITE_DATA"}:
                return
            if tail[2] == "unregister":
                required = "TABLE_DROP"
    if required is None or required not in privileges:
        raise _polaris_denied()


def polaris_serve(warehouse, db_file, host, port, password, realm):
    state = _PolarisState(warehouse, db_file, password, realm)
    with ThreadingHTTPServer((host, port), _PolarisHandler) as server:
        server.polaris = state
        print(f"Polaris test server listening on http://{host}:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


# Avro object containers and Iceberg v2 manifest schemas.


_AVRO_MAX_BLOCK = 128 * 1024 * 1024
_AVRO_MAX_ITEMS = 1_000_000
_AVRO_MAGIC = b"Obj\x01"
_AVRO_PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
_AVRO_NAMED = {"record", "enum", "fixed"}
_AVRO_METADATA = {"type": "map", "values": "bytes"}
_AVRO_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_AVRO_CODECS = {"null", "deflate", "snappy", "bzip2", "xz"}


def _iceberg_snappy_decode(data, max_output=_AVRO_MAX_BLOCK):
    """Read a raw Snappy block, used by both Avro and Parquet."""
    index = 0
    length = 0
    for shift in range(0, 35, 7):
        if index == len(data):
            raise ValueError("Truncated Snappy block header")
        byte = data[index]
        index += 1
        length |= (byte & 0x7f) << shift
        if byte < 0x80:
            break
    else:
        raise ValueError("Invalid Snappy block length")
    if length >= 2**32 or length > max_output:
        raise ValueError("Snappy block exceeds its allowed output size")
    result = bytearray()
    while index < len(data):
        tag = data[index]
        index += 1
        kind = tag & 3
        if kind == 0:
            size = tag >> 2
            if size >= 60:
                width = size - 59
                if index + width > len(data):
                    raise ValueError("Truncated Snappy literal length")
                size = int.from_bytes(data[index:index + width], "little")
                index += width
            size += 1
            if index + size > len(data) or len(result) + size > length:
                raise ValueError("Invalid Snappy literal size")
            result.extend(data[index:index + size])
            index += size
            continue

        width = (1, 2, 4)[kind - 1]
        if index + width > len(data):
            raise ValueError("Truncated Snappy copy offset")
        offset = int.from_bytes(data[index:index + width], "little")
        index += width
        if kind == 1:
            offset |= (tag >> 5) << 8
            size = ((tag >> 2) & 7) + 4
        else:
            size = (tag >> 2) + 1
        if not 0 < offset <= len(result) or len(result) + size > length:
            raise ValueError("Invalid Snappy copy")
        if offset >= size:
            start = len(result) - offset
            result.extend(result[start:start + size])
        else:
            pattern = result[-offset:]
            result.extend((pattern * ((size + offset - 1) // offset))[:size])
    if len(result) != length:
        raise ValueError("Snappy block has an incorrect decoded size")
    return bytes(result)


def _avro_uvarint(number):
    output = bytearray()
    while number >= 0x80:
        output.append((number & 0x7f) | 0x80)
        number >>= 7
    output.append(number)
    return bytes(output)


def _avro_snappy_encode(data):
    # Literal-only blocks are valid Snappy and keep this test writer simple.
    if not data:
        return b"\x00"
    size = len(data) - 1
    if size < 60:
        literal = bytes([size << 2])
    else:
        width = (size.bit_length() + 7) // 8
        literal = bytes([(59 + width) << 2]) + size.to_bytes(width, "little")
    return _avro_uvarint(len(data)) + literal + data


class _AvroReader:
    def __init__(self, stream):
        self.stream = stream

    def read(self, size):
        if not 0 <= size <= _AVRO_MAX_BLOCK:
            raise ValueError("Invalid Avro buffer size")
        data = self.stream.read(size)
        if len(data) != size:
            raise ValueError("Truncated Avro data")
        return data

    def long(self, first=None):
        value = 0
        for shift in range(0, 70, 7):
            byte = self.read(1)[0] if shift or first is None else first
            value |= (byte & 0x7f) << shift
            if byte < 0x80:
                if value >= 2**64:
                    break
                return (value >> 1) ^ -(value & 1)
        raise ValueError("Invalid Avro integer")

    def bytes(self):
        return self.read(self.long())

    def string(self):
        return self.bytes().decode("utf-8")


def _avro_fullname(schema, namespace):
    name = schema["name"]
    if "." in name:
        return name
    namespace = schema.get("namespace", namespace)
    return f"{namespace}.{name}" if namespace else name


class _AvroSchema:
    def __init__(self, schema):
        self.named = {}
        self._collect(schema, "", 0)

    def _collect(self, schema, namespace, depth):
        if depth > 80:
            raise ValueError("Avro schema is nested too deeply")
        if isinstance(schema, list):
            if not schema:
                raise ValueError("Avro union must have at least one branch")
            for option in schema:
                self._collect(option, namespace, depth + 1)
        elif isinstance(schema, dict):
            kind = schema["type"]
            if isinstance(kind, str) and kind in _AVRO_NAMED:
                fullname = _avro_fullname(schema, namespace)
                if fullname in self.named:
                    raise ValueError(f"Duplicate Avro schema name {fullname!r}")
                self.named[fullname] = schema
                if kind == "record":
                    namespace = fullname.rpartition(".")[0]
                    fields = [field["name"] for field in schema["fields"]]
                    if len(set(fields)) != len(fields):
                        raise ValueError(f"Duplicate field in Avro record {fullname!r}")
                    for field in schema["fields"]:
                        self._collect(field["type"], namespace, depth + 1)
            elif kind == "array":
                self._collect(schema["items"], namespace, depth + 1)
            elif kind == "map":
                self._collect(schema["values"], namespace, depth + 1)
            else:
                self._collect(kind, namespace, depth + 1)
        elif not isinstance(schema, str):
            raise ValueError("Invalid Avro schema")

    def resolve(self, schema, namespace):
        while True:
            if isinstance(schema, str):
                if schema in _AVRO_PRIMITIVES:
                    return schema, namespace
                fullname = schema if "." in schema or not namespace else f"{namespace}.{schema}"
                if fullname not in self.named:
                    raise ValueError(f"Unknown Avro schema {fullname!r}")
                return self.named[fullname], fullname.rpartition(".")[0]
            if isinstance(schema, list):
                return schema, namespace
            kind = schema["type"]
            if isinstance(kind, str) and kind in _AVRO_NAMED:
                return schema, _avro_fullname(schema, namespace).rpartition(".")[0]
            if kind in ("array", "map"):
                return schema, namespace
            schema = kind


def _avro_decode(reader, schema, definitions, namespace="", depth=0):
    if depth > 100:
        raise ValueError("Avro data is nested too deeply")
    schema, namespace = definitions.resolve(schema, namespace)

    def decode(child, source=reader):
        return _avro_decode(source, child, definitions, namespace, depth + 1)

    if isinstance(schema, list):
        tag = reader.long()
        if not 0 <= tag < len(schema):
            raise ValueError("Invalid Avro union branch")
        return decode(schema[tag])
    kind = schema if isinstance(schema, str) else schema["type"]
    if kind == "null":
        return None
    if kind == "boolean":
        value = reader.read(1)
        if value not in (b"\x00", b"\x01"):
            raise ValueError("Invalid Avro boolean")
        return value == b"\x01"
    if kind in ("int", "long"):
        value = reader.long()
        if kind == "int" and not -(2**31) <= value < 2**31:
            raise ValueError("Avro int is out of range")
        return value
    if kind in ("float", "double"):
        return struct.unpack("<f" if kind == "float" else "<d", reader.read(4 if kind == "float" else 8))[0]
    if kind == "bytes":
        return reader.bytes()
    if kind == "string":
        return reader.string()
    if kind == "fixed":
        return reader.read(schema["size"])
    if kind == "enum":
        tag = reader.long()
        if not 0 <= tag < len(schema["symbols"]):
            raise ValueError("Invalid Avro enum index")
        return schema["symbols"][tag]
    if kind == "record":
        return {field["name"]: decode(field["type"]) for field in schema["fields"]}

    # Arrays and maps may contain multiple positive-count or sized blocks.
    result = {} if kind == "map" else []
    count_total = 0
    while count := reader.long():
        sized = count < 0
        count = abs(count)
        count_total += count
        if count_total > _AVRO_MAX_ITEMS:
            raise ValueError("Avro collection exceeds the item limit")
        source = _AvroReader(io.BytesIO(reader.bytes())) if sized else reader
        if kind == "map":
            for _ in range(count):
                key = source.string()
                if key in result:
                    raise ValueError("Duplicate Avro map key")
                result[key] = decode(schema["values"], source)
        else:
            result.extend(decode(schema["items"], source) for _ in range(count))
        if sized and source.stream.read(1):
            raise ValueError("Invalid Avro collection block length")
    return result


def _avro_long(number, bits=64):
    if not isinstance(number, int) or isinstance(number, bool) or not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
        raise ValueError(f"Avro {bits}-bit integer is out of range: {number!r}")
    return _avro_uvarint((number << 1) ^ (number >> (bits - 1)))


def _avro_encode_bytes(value):
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("Expected an Avro byte string")
    value = bytes(value)
    return _avro_long(len(value)) + value


def _avro_matches(value, schema, definitions, namespace):
    schema, _ = definitions.resolve(schema, namespace)
    kind = schema if isinstance(schema, str) else schema["type"]
    if kind == "null":
        return value is None
    if kind == "boolean":
        return isinstance(value, bool)
    if kind in ("int", "long"):
        bits = 32 if kind == "int" else 64
        return isinstance(value, int) and not isinstance(value, bool) and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1)
    if kind in ("float", "double"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "string":
        return isinstance(value, str)
    if kind == "enum":
        return value in schema["symbols"]
    if kind in ("fixed", "bytes"):
        return isinstance(value, (bytes, bytearray, memoryview)) and (kind == "bytes" or len(value) == schema["size"])
    if kind == "record":
        return isinstance(value, dict) and all("default" in f or f["name"] in value for f in schema["fields"])
    if kind == "map":
        return isinstance(value, dict)
    return isinstance(value, (list, tuple)) or isinstance(value, dict) and schema.get("logicalType") == "map"


def _avro_encode(value, schema, definitions, namespace="", depth=0):
    if depth > 100:
        raise ValueError("Avro data is nested too deeply")
    schema, namespace = definitions.resolve(schema, namespace)

    def encode(item, child):
        return _avro_encode(item, child, definitions, namespace, depth + 1)

    if isinstance(schema, list):
        for tag, option in enumerate(schema):
            if _avro_matches(value, option, definitions, namespace):
                return _avro_long(tag) + encode(value, option)
        raise ValueError(f"Value does not match an Avro union branch: {value!r}")
    if not _avro_matches(value, schema, definitions, namespace):
        raise ValueError(f"Invalid Avro value for {schema!r}: {value!r}")

    kind = schema if isinstance(schema, str) else schema["type"]
    if kind == "null":
        return b""
    if kind == "boolean":
        return b"\x01" if value else b"\x00"
    if kind in ("int", "long"):
        return _avro_long(value, 32 if kind == "int" else 64)
    if kind in ("float", "double"):
        return struct.pack("<f" if kind == "float" else "<d", value)
    if kind == "bytes":
        return _avro_encode_bytes(value)
    if kind == "string":
        return _avro_encode_bytes(value.encode("utf-8"))
    if kind == "fixed":
        return bytes(value)
    if kind == "enum":
        return _avro_long(schema["symbols"].index(value))
    if kind == "record":
        return b"".join(encode(value.get(field["name"], field.get("default")), field["type"]) for field in schema["fields"])
    if len(value) > _AVRO_MAX_ITEMS:
        raise ValueError("Avro collection exceeds the item limit")
    if not value:
        return b"\x00"
    if kind == "map":
        payload = b"".join(encode(key, "string") + encode(item, schema["values"]) for key, item in value.items())
    else:
        entries = ({"key": key, "value": item} for key, item in value.items()) if isinstance(value, dict) else value
        payload = b"".join(encode(entry, schema["items"]) for entry in entries)
    return _avro_long(len(value)) + payload + b"\x00"


def _avro_decompress(codec, data):
    if codec == "null":
        return data
    if codec == "snappy":
        if len(data) < 5:
            raise ValueError("Truncated Avro Snappy block")
        result = _iceberg_snappy_decode(data[:-4])
        if binascii.crc32(result) != int.from_bytes(data[-4:], "big"):
            raise ValueError("Avro Snappy checksum mismatch")
        return result
    if codec == "deflate":
        decoder = zlib.decompressobj(-15)
        result = decoder.decompress(data, _AVRO_MAX_BLOCK + 1)
        if len(result) > _AVRO_MAX_BLOCK or not decoder.eof or decoder.unconsumed_tail:
            raise ValueError("Invalid or oversized Avro deflate block")
        # Some Avro writers leave the first three bytes of the Adler-32 trailer.
        trailer = (zlib.adler32(result) & 0xffffffff).to_bytes(4, "big")
        if decoder.unused_data and decoder.unused_data != trailer[:3]:
            raise ValueError("Unexpected data after Avro deflate stream")
        return result
    decoder = bz2.BZ2Decompressor() if codec == "bzip2" else lzma.LZMADecompressor(memlimit=_AVRO_MAX_BLOCK)
    result = decoder.decompress(data, max_length=_AVRO_MAX_BLOCK + 1)
    if len(result) > _AVRO_MAX_BLOCK or not decoder.eof or decoder.unused_data:
        raise ValueError(f"Invalid or oversized Avro {codec} block")
    return result


def _avro_compress(codec, data):
    if codec == "null":
        return data
    if codec == "deflate":
        encoder = zlib.compressobj(wbits=-15)
        return encoder.compress(data) + encoder.flush()
    if codec == "snappy":
        return _avro_snappy_encode(data) + binascii.crc32(data).to_bytes(4, "big")
    return bz2.compress(data) if codec == "bzip2" else lzma.compress(data)


def avro_read(path):
    """Read a local Avro object container. Logical types retain their wire values."""
    with open(path, "rb") as stream:
        reader = _AvroReader(stream)
        if reader.read(len(_AVRO_MAGIC)) != _AVRO_MAGIC:
            raise ValueError("File is not an Avro object container")
        metadata = _avro_decode(reader, _AVRO_METADATA, _AvroSchema(_AVRO_METADATA))
        sync = reader.read(16)
        if "avro.schema" not in metadata:
            raise ValueError("Avro container has no writer schema")
        schema = json.loads(metadata["avro.schema"])
        definitions = _AvroSchema(schema)
        codec = metadata.get("avro.codec", b"null").decode("ascii")
        if codec not in _AVRO_CODECS:
            raise ValueError(f"Unsupported Avro codec {codec!r}")
        records = []
        while first := stream.read(1):
            count = reader.long(first[0])
            if not 0 < count <= _AVRO_MAX_ITEMS - len(records):
                raise ValueError("Invalid or oversized Avro record block")
            compressed = reader.bytes()
            if reader.read(16) != sync:
                raise ValueError("Avro container sync marker mismatch")
            try:
                block = io.BytesIO(_avro_decompress(codec, compressed))
            except (zlib.error, lzma.LZMAError, OSError, EOFError) as exc:
                raise ValueError(f"Corrupt Avro {codec} block") from exc
            record_reader = _AvroReader(block)
            records.extend(_avro_decode(record_reader, schema, definitions) for _ in range(count))
            if block.read(1):
                raise ValueError("Avro block contains trailing data")
        return schema, metadata, records


def avro_write(path, schema, records, metadata=None, codec="null"):
    """Write a new local Avro object container without overwriting existing files."""
    if codec not in _AVRO_CODECS:
        raise ValueError(f"Unsupported Avro codec {codec!r}")
    definitions = _AvroSchema(schema)
    metadata = {
        key: value.encode("utf-8") if isinstance(value, str) else value
        for key, value in (metadata or {}).items()
    }
    if any(key.startswith("avro.") for key in metadata):
        raise ValueError("Avro container metadata cannot redefine reserved keys")
    metadata["avro.schema"] = json.dumps(schema, separators=(",", ":"), allow_nan=False).encode("utf-8")
    metadata["avro.codec"] = codec.encode("ascii")
    header = _avro_encode(metadata, _AVRO_METADATA, _AvroSchema(_AVRO_METADATA))
    sync = os.urandom(16)

    def write_block(stream, count, block):
        data = _avro_compress(codec, block)
        if len(data) > _AVRO_MAX_BLOCK:
            raise ValueError("Compressed Avro block exceeds the size limit")
        stream.write(_avro_long(count))
        stream.write(_avro_long(len(data)))
        stream.write(data)
        stream.write(sync)

    path = Path(path)
    with path.open("xb") as stream:
        try:
            stream.write(_AVRO_MAGIC + header + sync)
            block, count, total = bytearray(), 0, 0
            for record in records:
                encoded = _avro_encode(record, schema, definitions)
                if len(encoded) > _AVRO_MAX_BLOCK:
                    raise ValueError("Avro record exceeds the size limit")
                if count and (len(block) + len(encoded) > 1024 * 1024 or count >= 10_000):
                    write_block(stream, count, block)
                    block.clear()
                    count = 0
                block.extend(encoded)
                count += 1
                total += 1
                if total > _AVRO_MAX_ITEMS:
                    raise ValueError("Avro container exceeds the record limit")
            if count:
                write_block(stream, count, block)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            stream.close()
            path.unlink(missing_ok=True)
            raise


def _avro_field(field_id, name, schema, optional=False):
    if not _AVRO_NAME.fullmatch(name):
        raise ValueError(f"Invalid Avro field name {name!r}")
    if optional:
        return {"name": name, "field-id": field_id, "type": ["null", schema], "default": None}
    return {"name": name, "field-id": field_id, "type": schema}


def _avro_record(name, fields):
    return {"type": "record", "name": name, "fields": fields}


def _avro_iceberg_list(element_id, element):
    return {"type": "array", "items": element, "element-id": element_id}


def _avro_iceberg_map(key_id, value_id, value):
    # Iceberg integer-keyed maps are encoded as key/value records, not Avro maps.
    return {
        "type": "array",
        "items": _avro_record(f"k{key_id}_v{value_id}", [
            _avro_field(key_id, "key", "int"), _avro_field(value_id, "value", value),
        ]),
        "logicalType": "map",
    }


def iceberg_manifest_schema(partition_fields=()):
    """V2 manifest schema. Partition fields are (field_id, name, Avro type) tuples."""
    partition = _avro_record("r102", [
        _avro_field(field_id, name, field_type, True)
        for field_id, name, field_type in partition_fields
    ])
    data = _avro_record("r2", [
        _avro_field(134, "content", "int"),
        _avro_field(100, "file_path", "string"),
        _avro_field(101, "file_format", "string"),
        _avro_field(102, "partition", partition),
        _avro_field(103, "record_count", "long"),
        _avro_field(104, "file_size_in_bytes", "long"),
        _avro_field(108, "column_sizes", _avro_iceberg_map(117, 118, "long"), True),
        _avro_field(109, "value_counts", _avro_iceberg_map(119, 120, "long"), True),
        _avro_field(110, "null_value_counts", _avro_iceberg_map(121, 122, "long"), True),
        _avro_field(137, "nan_value_counts", _avro_iceberg_map(138, 139, "long"), True),
        _avro_field(125, "lower_bounds", _avro_iceberg_map(126, 127, "bytes"), True),
        _avro_field(128, "upper_bounds", _avro_iceberg_map(129, 130, "bytes"), True),
        _avro_field(131, "key_metadata", "bytes", True),
        _avro_field(132, "split_offsets", _avro_iceberg_list(133, "long"), True),
        _avro_field(135, "equality_ids", _avro_iceberg_list(136, "int"), True),
        _avro_field(140, "sort_order_id", "int", True),
        _avro_field(143, "referenced_data_file", "string", True),
    ])
    return _avro_record("manifest_entry", [
        _avro_field(0, "status", "int"),
        _avro_field(1, "snapshot_id", "long", True),
        _avro_field(3, "sequence_number", "long", True),
        _avro_field(4, "file_sequence_number", "long", True),
        _avro_field(2, "data_file", data),
    ])


def iceberg_manifest_list_schema():
    summary = _avro_record("r508", [
        _avro_field(509, "contains_null", "boolean"),
        _avro_field(518, "contains_nan", "boolean", True),
        _avro_field(510, "lower_bound", "bytes", True),
        _avro_field(511, "upper_bound", "bytes", True),
    ])
    return _avro_record("manifest_file", [
        _avro_field(500, "manifest_path", "string"),
        _avro_field(501, "manifest_length", "long"),
        _avro_field(502, "partition_spec_id", "int"),
        _avro_field(517, "content", "int"),
        _avro_field(515, "sequence_number", "long"),
        _avro_field(516, "min_sequence_number", "long"),
        _avro_field(503, "added_snapshot_id", "long"),
        _avro_field(504, "added_files_count", "int"),
        _avro_field(505, "existing_files_count", "int"),
        _avro_field(506, "deleted_files_count", "int"),
        _avro_field(512, "added_rows_count", "long"),
        _avro_field(513, "existing_rows_count", "long"),
        _avro_field(514, "deleted_rows_count", "long"),
        _avro_field(507, "partitions", _avro_iceberg_list(508, summary), True),
        _avro_field(519, "key_metadata", "bytes", True),
    ])


# Parquet files use compact Thrift for their footer and page headers.
# Format reference: https://parquet.apache.org/docs/file-format/


class ParquetError(ValueError):
    pass


_PQ_TRUE, _PQ_FALSE = 1, 2
_PQ_BYTE, _PQ_I16, _PQ_I32, _PQ_I64, _PQ_DOUBLE, _PQ_BINARY = range(3, 9)
_PQ_LIST, _PQ_SET, _PQ_MAP, _PQ_STRUCT = range(9, 13)
_PQ_MAX_SIZE = 256 * 1024 * 1024
_PQ_MAX_ROWS = 1_000_000
_PQ_EPOCH_DATE = datetime.date(1970, 1, 1)
_PQ_EPOCH_TIME = datetime.datetime(1970, 1, 1)
_PQ_DAY_MICROS = 86_400_000_000
_PQ_PHYSICAL = {"boolean": 0, "int": 1, "long": 2, "float": 4, "double": 5,
                "string": 6, "binary": 6, "date": 1, "time": 2, "timestamp": 2,
                "timestamptz": 2, "uuid": 7}
_PQ_PLAIN_FORMAT = {1: "<i", 2: "<q", 4: "<f", 5: "<d"}
_PQ_PRIMITIVE = {0: "boolean", 1: "int", 2: "long", 4: "float", 5: "double", 6: "binary"}


class _ParquetReader:
    def __init__(self, data, offset=0, limit=None):
        self.data = data
        self.pos = offset
        self.limit = len(data) if limit is None else limit

    def read(self, size):
        start = self.pos
        if size < 0 or size > self.limit - start:
            raise ParquetError("Truncated Parquet section")
        self.pos = start + size
        return self.data[start:self.pos]

    def byte(self):
        return self.read(1)[0]

    def uint(self):
        value = 0
        for shift in range(0, 70, 7):
            byte = self.byte()
            value |= (byte & 127) << shift
            if byte < 128:
                if value >= 1 << 64:
                    raise ParquetError("Parquet varint overflow")
                return value
        raise ParquetError("Parquet varint overflow")

    def sint(self):
        value = self.uint()
        return (value >> 1) ^ -(value & 1)

    def int32(self):
        return struct.unpack("<i", self.read(4))[0]

    def compact_struct(self, depth=0):
        result = {}
        previous = 0
        while True:
            tag = self.byte()
            if tag == 0:
                return result
            delta, kind = divmod(tag, 16)
            field = previous + delta if delta else self.sint()
            if not 0 < field <= 32767 or field in result:
                raise ParquetError("Invalid Parquet metadata field")
            if kind in (_PQ_TRUE, _PQ_FALSE):
                result[field] = kind == _PQ_TRUE
            else:
                result[field] = self.compact_value(kind, depth + 1)
            previous = field

    def compact_value(self, kind, depth):
        if depth > 32:
            raise ParquetError("Parquet metadata is nested too deeply")
        if kind in (_PQ_TRUE, _PQ_FALSE):
            byte = self.byte()
            if byte not in (_PQ_TRUE, _PQ_FALSE):
                raise ParquetError("Invalid Parquet metadata boolean")
            return byte == _PQ_TRUE
        if kind == _PQ_BYTE:
            return struct.unpack("b", self.read(1))[0]
        if kind in (_PQ_I16, _PQ_I32, _PQ_I64):
            return self.sint()
        if kind == _PQ_DOUBLE:
            return struct.unpack("<d", self.read(8))[0]
        if kind == _PQ_BINARY:
            return self.read(self.uint())
        if kind == _PQ_STRUCT:
            return self.compact_struct(depth)
        if kind in (_PQ_LIST, _PQ_SET):
            size, item_kind = divmod(self.byte(), 16)
            if size == 15:
                size = self.uint()
            if size > min(self.limit - self.pos, _PQ_MAX_ROWS):
                raise ParquetError("Invalid Parquet metadata list size")
            return [self.compact_value(item_kind, depth + 1) for _ in range(size)]
        if kind == _PQ_MAP:
            size = self.uint()
            if size == 0:
                return []
            key_kind, value_kind = divmod(self.byte(), 16)
            if size > min((self.limit - self.pos) // 2, _PQ_MAX_ROWS):
                raise ParquetError("Invalid Parquet metadata map size")
            return [(self.compact_value(key_kind, depth + 1), self.compact_value(value_kind, depth + 1))
                    for _ in range(size)]
        raise ParquetError(f"Unknown compact Thrift type {kind}")


def _pq_uint(value):
    output = bytearray()
    while value >= 128:
        output.append((value & 127) | 128)
        value >>= 7
    output.append(value)
    return bytes(output)


def _pq_sint(value):
    return _pq_uint(value * 2 if value >= 0 else -value * 2 - 1)


def _pq_compact_value(kind, value):
    if kind in (_PQ_I16, _PQ_I32, _PQ_I64):
        return _pq_sint(value)
    if kind == _PQ_BINARY:
        data = value.encode() if isinstance(value, str) else value
        return _pq_uint(len(data)) + data
    if kind == _PQ_STRUCT:
        return _pq_compact_struct(value)
    if kind == _PQ_LIST:
        item_kind, items = value
        if len(items) < 15:
            prefix = bytes([(len(items) << 4) | item_kind])
        else:
            prefix = bytes([240 | item_kind]) + _pq_uint(len(items))
        return prefix + b"".join(_pq_compact_value(item_kind, item) for item in items)
    raise AssertionError(f"Unsupported compact Thrift write type {kind}")


def _pq_compact_struct(fields):
    output = bytearray()
    previous = 0
    for field, (kind, value) in sorted(fields.items()):
        delta = field - previous
        if 0 < delta < 16:
            output.append((delta << 4) | kind)
        else:
            output.append(kind)
            output.extend(_pq_sint(field))
        if kind not in (_PQ_TRUE, _PQ_FALSE):
            output.extend(_pq_compact_value(kind, value))
        previous = field
    output.append(0)
    return bytes(output)


def _pq_type(iceberg_type):
    if not isinstance(iceberg_type, str):
        raise ParquetError("Parquet supports only flat primitive Iceberg types")
    physical = _PQ_PHYSICAL.get(iceberg_type)
    if physical is not None:
        return physical, 16 if iceberg_type == "uuid" else None, None
    fixed = re.fullmatch(r"fixed\[([0-9]{1,6})\]", iceberg_type)
    if fixed and int(fixed[1]) > 0:
        return 7, int(fixed[1]), None
    match = re.fullmatch(r"decimal\(([0-9]{1,2}),\s*([0-9]{1,2})\)", iceberg_type)
    if match and 0 <= int(match[2]) <= int(match[1]) <= 38 and int(match[1]) > 0:
        precision, scale = int(match[1]), int(match[2])
        width = ((10**precision - 1).bit_length() + 8) // 8
        return 7, width, (precision, scale)
    raise ParquetError(f"Unsupported Parquet type {iceberg_type}")


def _pq_schema_element(field):
    kind = field["type"]
    physical, fixed_length, decimal_type = _pq_type(kind)
    element = {1: (_PQ_I32, physical), 3: (_PQ_I32, 0 if field["required"] else 1),
               4: (_PQ_BINARY, field["name"]), 9: (_PQ_I32, field["id"])}
    if fixed_length is not None:
        element[2] = (_PQ_I32, fixed_length)
    if kind == "string":
        element[6], element[10] = (_PQ_I32, 0), (_PQ_STRUCT, {1: (_PQ_STRUCT, {})})
    if kind == "date":
        element[6], element[10] = (_PQ_I32, 6), (_PQ_STRUCT, {6: (_PQ_STRUCT, {})})
    if kind in {"timestamp", "timestamptz", "time"}:
        time_tag = 7 if kind == "time" else 8
        adjusted = _PQ_TRUE if kind == "timestamptz" else _PQ_FALSE
        time_struct = {1: (adjusted, None), 2: (_PQ_STRUCT, {2: (_PQ_STRUCT, {})})}
        element[6] = (_PQ_I32, 8 if kind == "time" else 10)
        element[10] = (_PQ_STRUCT, {time_tag: (_PQ_STRUCT, time_struct)})
    if kind == "uuid":
        element[10] = (_PQ_STRUCT, {14: (_PQ_STRUCT, {})})
    if decimal_type:
        precision, scale = decimal_type
        element[6] = (_PQ_I32, 5)
        element[7], element[8] = (_PQ_I32, scale), (_PQ_I32, precision)
        element[10] = (_PQ_STRUCT, {5: (_PQ_STRUCT, {1: (_PQ_I32, scale), 2: (_PQ_I32, precision)})})
    return element


def _pq_temporal_value(kind, value):
    if type(value) is int:
        return value
    if kind == "date":
        parsed = datetime.date.fromisoformat(value) if isinstance(value, str) else value
        if type(parsed) is not datetime.date:
            raise ParquetError("Expected ISO date or integer days")
        return (parsed - _PQ_EPOCH_DATE).days
    if kind == "time":
        parsed = datetime.time.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(parsed, datetime.time) or parsed.tzinfo is not None:
            raise ParquetError("Expected ISO local time or integer microseconds")
        return ((parsed.hour * 60 + parsed.minute) * 60 + parsed.second) * 1_000_000 + parsed.microsecond
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(parsed, datetime.datetime):
        raise ParquetError("Expected ISO timestamp or integer microseconds")
    if kind == "timestamptz":
        if parsed.utcoffset() is None:
            raise ParquetError("Expected timezone offset in timestamptz")
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    elif parsed.utcoffset() is not None:
        raise ParquetError("Expected timestamp without timezone")
    delta = parsed - _PQ_EPOCH_TIME
    return delta.days * _PQ_DAY_MICROS + delta.seconds * 1_000_000 + delta.microseconds


def _pq_physical_value(kind, value, physical, width, decimal_type):
    if kind == "string":
        if not isinstance(value, str):
            raise ParquetError("Expected string")
        return value.encode()
    if kind == "uuid":
        return (value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))).bytes
    if decimal_type:
        precision, scale = decimal_type
        number = decimal.Decimal(str(value))
        if not number.is_finite() or (number and abs(number.adjusted()) > 76):
            raise ParquetError("Decimal is out of range")
        with decimal.localcontext() as ctx:
            ctx.prec = max(len(number.as_tuple().digits), 38) + 2
            scaled = number.scaleb(scale)
            if scaled != scaled.to_integral_value() or abs(scaled) >= 10**precision:
                raise ParquetError(f"Decimal does not fit {kind}")
            return int(scaled).to_bytes(width, "big", signed=True)
    if kind in {"date", "time", "timestamp", "timestamptz"}:
        return _pq_temporal_value(kind, value)
    if physical in {6, 7}:
        if not isinstance(value, (bytes, bytearray)):
            raise ParquetError("Expected bytes for binary or fixed")
        if width is not None and len(value) != width:
            raise ParquetError(f"Expected exactly {width} bytes")
        return bytes(value)
    if physical == 0 and type(value) is not bool:
        raise ParquetError("Expected boolean")
    if physical in {1, 2} and type(value) is not int:
        raise ParquetError("Expected integer")
    if physical in {4, 5} and type(value) not in {float, int}:
        raise ParquetError("Expected number")
    return value


def _pq_encode_plain(values, physical):
    if physical == 0:
        result = bytearray((len(values) + 7) // 8)
        for index, value in enumerate(values):
            if value:
                result[index // 8] |= 1 << (index % 8)
        return bytes(result)
    if physical in _PQ_PLAIN_FORMAT:
        fmt = _PQ_PLAIN_FORMAT[physical]
        return b"".join(struct.pack(fmt, value) for value in values)
    if physical == 6:
        return b"".join(struct.pack("<i", len(value)) + value for value in values)
    return b"".join(values)


def _pq_encode_definition(values):
    body = bytearray()
    for available, group in itertools.groupby(value is not None for value in values):
        count = sum(1 for _ in group)
        body.extend(_pq_uint(count << 1))
        body.append(available)
    return struct.pack("<i", len(body)) + body


def _pq_write_rowgroup(output, fields, rows):
    start = len(output)
    columns = []
    for field in fields:
        name, kind = field["name"], field["type"]
        physical, width, decimal_type = _pq_type(kind)
        col_start = len(output)
        null_count = 0
        for first in range(0, len(rows), 8192):
            values = [row.get(name) for row in rows[first:first + 8192]]
            count = sum(value is None for value in values)
            if count and field["required"]:
                raise ParquetError(f"Required field {name!r} cannot be null")
            null_count += count
            try:
                present = [_pq_physical_value(kind, value, physical, width, decimal_type)
                           for value in values if value is not None]
                levels = b"" if field["required"] else _pq_encode_definition(values)
                body = levels + _pq_encode_plain(present, physical)
            except (ValueError, TypeError, OverflowError, struct.error, decimal.DecimalException) as exc:
                raise ParquetError(f"Invalid value in column {name!r}: {exc}") from exc
            page_header = {1: (_PQ_I32, 0), 2: (_PQ_I32, len(body)), 3: (_PQ_I32, len(body)),
                           5: (_PQ_STRUCT, {1: (_PQ_I32, len(values)), 2: (_PQ_I32, 0),
                                            3: (_PQ_I32, 3), 4: (_PQ_I32, 3),
                                            5: (_PQ_STRUCT, {3: (_PQ_I64, count)})})}
            output.extend(_pq_compact_struct(page_header))
            output.extend(body)
            if len(output) > _PQ_MAX_SIZE:
                raise ParquetError("Parquet file exceeds local test size limit")
        size = len(output) - col_start
        metadata = {1: (_PQ_I32, physical), 2: (_PQ_LIST, (_PQ_I32, [0, 3])),
                    3: (_PQ_LIST, (_PQ_BINARY, [name])), 4: (_PQ_I32, 0),
                    5: (_PQ_I64, len(rows)), 6: (_PQ_I64, size), 7: (_PQ_I64, size),
                    9: (_PQ_I64, col_start), 12: (_PQ_STRUCT, {3: (_PQ_I64, null_count)})}
        columns.append({2: (_PQ_I64, 0), 3: (_PQ_STRUCT, metadata)})
    size = len(output) - start
    return {1: (_PQ_LIST, (_PQ_STRUCT, columns)), 2: (_PQ_I64, size), 3: (_PQ_I64, len(rows)),
            5: (_PQ_I64, start), 6: (_PQ_I64, size)}


def parquet_write(path, fields, rows):
    """Write an interoperable flat Parquet file with Iceberg field IDs."""
    if not fields or len(fields) > 1024 or len(rows) > _PQ_MAX_ROWS:
        raise ParquetError("Invalid Parquet column or row count")
    ids, names = set(), set()
    elements = [{4: (_PQ_BINARY, "table"), 5: (_PQ_I32, len(fields))}]
    for field in fields:
        field_id, name = field["id"], field["name"]
        if type(field_id) is not int or not 0 < field_id < 1 << 31 or field_id in ids:
            raise ParquetError(f"Invalid or duplicate Parquet field ID {field_id!r}")
        if not isinstance(name, str) or not name or name in names or type(field["required"]) is not bool:
            raise ParquetError(f"Invalid or duplicate Parquet field name {name!r}")
        ids.add(field_id)
        names.add(name)
        elements.append(_pq_schema_element(field))
    if any(not isinstance(row, dict) or row.keys() - names for row in rows):
        raise ParquetError("Parquet rows must be objects keyed by schema fields")
    output = bytearray(b"PAR1")
    groups = [_pq_write_rowgroup(output, fields, rows[start:start + 65_536])
              for start in range(0, len(rows), 65_536)]
    footer = _pq_compact_struct({1: (_PQ_I32, 1), 2: (_PQ_LIST, (_PQ_STRUCT, elements)), 3: (_PQ_I64, len(rows)),
                                 4: (_PQ_LIST, (_PQ_STRUCT, groups)),
                                 6: (_PQ_BINARY, "jjap Iceberg test server")})
    output.extend(footer)
    output.extend(struct.pack("<I", len(footer)))
    output.extend(b"PAR1")
    if len(output) > _PQ_MAX_SIZE:
        raise ParquetError("Parquet file exceeds local test size limit")
    with Path(path).open("xb") as handle:
        handle.write(output)


def _pq_read_field(element):
    if element.get(3) not in (0, 1) or 1 not in element or 5 in element:
        raise ParquetError("Nested and repeated Parquet columns are outside this codec's scope")
    physical, width = element[1], element.get(2)
    logical, converted = element.get(10, {}), element.get(6)
    factor, scale = 1, None
    kind = _PQ_PRIMITIVE.get(physical, None)
    if 1 in logical or (not logical and converted == 0):
        kind = "string" if physical == 6 else None
    elif 6 in logical or (not logical and converted == 6):
        kind = "date" if physical == 1 else None
    elif 7 in logical or 8 in logical:
        tag = 7 if 7 in logical else 8
        unit = logical[tag][2]
        if unit not in ({1: {}}, {2: {}}):
            raise ParquetError("Unsupported Parquet temporal unit")
        factor = 1000 if 1 in unit else 1
        if tag == 7 and physical == (1 if factor == 1000 else 2):
            kind = "time"
        elif tag == 8 and physical == 2:
            kind = "timestamptz" if logical[tag][1] else "timestamp"
        else:
            kind = None
    elif not logical and converted in {7, 8, 9, 10}:
        kind = "time" if converted in {7, 8} else "timestamptz"
        factor = 1000 if converted in {7, 9} else 1
        if physical != (1 if converted == 7 else 2):
            kind = None
    elif 14 in logical and physical == 7 and width == 16:
        kind = "uuid"
    elif 5 in logical or (not logical and converted == 5):
        decimal_type = logical.get(5)
        scale, precision = (decimal_type[1], decimal_type[2]) if decimal_type else (element[7], element[8])
        kind = f"decimal({precision},{scale})"
        _pq_type(kind)
        if physical not in {1, 2, 6, 7}:
            kind = None
    elif 10 in logical:
        if not logical[10].get(2) or physical not in (1, 2):
            kind = None
    elif logical or converted not in {None, 15, 16, 17, 18}:
        kind = None
    elif physical == 7 and type(width) is int and width > 0:
        kind = f"fixed[{width}]"
    if kind is None or (physical == 7 and (type(width) is not int or width <= 0)):
        raise ParquetError(f"Unsupported Parquet annotation or physical type {physical!r}")
    field = {"id": element.get(9), "name": element[4].decode(), "required": element[3] == 0, "type": kind}
    return field, (physical, width, scale, factor)


def _pq_unpack_bits(data, width, count):
    if width == 0:
        return [0] * count
    mask = (1 << width) - 1
    # Decode small runs incrementally so large non-dictionary pages do not allocate giant integers.
    result = []
    consumed = 0
    bits = 0
    have = 0
    for _ in range(count):
        while have < width:
            bits |= data[consumed] << have
            consumed += 1
            have += 8
        result.append(bits & mask)
        bits >>= width
        have -= width
    return result


def _pq_decode_rle(data, width, count):
    if not 0 <= width <= 64 or not 0 <= count <= _PQ_MAX_ROWS:
        raise ParquetError("Invalid Parquet bit width or run count")
    raw = _ParquetReader(data)
    result = []
    while len(result) < count:
        header = raw.uint()
        size = header >> 1
        left = count - len(result)
        if not 0 < size <= _PQ_MAX_ROWS:
            raise ParquetError("Invalid Parquet RLE run length")
        if header & 1:
            if size * 8 > left + 7:
                raise ParquetError("Parquet packed run exceeds value count")
            packed = raw.read(size * width)
            result.extend(_pq_unpack_bits(packed, width, min(left, size * 8)))
        else:
            if size > left:
                raise ParquetError("Parquet repeated run exceeds value count")
            value = int.from_bytes(raw.read((width + 7) // 8), "little")
            if value >= 1 << width:
                raise ParquetError("Parquet repeated value out of range")
            result.extend([value] * size)
    if raw.pos != raw.limit:
        raise ParquetError("Unexpected bytes after Parquet RLE stream")
    return result


def _pq_decode_plain(data, physical, width, count):
    if not 0 <= count <= _PQ_MAX_ROWS:
        raise ParquetError("Invalid value count in Parquet page")
    if physical == 0:
        if len(data) != (count + 7) // 8:
            raise ParquetError("Invalid Parquet boolean length")
        return [bool(value) for value in _pq_unpack_bits(data, 1, count)]
    if physical in _PQ_PLAIN_FORMAT:
        fmt = _PQ_PLAIN_FORMAT[physical]
        if len(data) != struct.calcsize(fmt) * count:
            raise ParquetError("Invalid Parquet numeric length")
        return [value[0] for value in struct.iter_unpack(fmt, data)]
    raw = _ParquetReader(data)
    result = [raw.read(raw.int32() if physical == 6 else width) for _ in range(count)]
    if raw.pos != raw.limit:
        raise ParquetError("Unexpected bytes after Parquet PLAIN values")
    return result


def _pq_decompress(data, codec, expected):
    if not 0 <= expected <= _PQ_MAX_SIZE:
        raise ParquetError("Invalid decompressed Parquet size")
    if codec == 0:
        output = data
    elif codec == 1:
        output = _iceberg_snappy_decode(data, max_output=expected)
    elif codec == 2:
        # GZIP's trailer and any concatenated members are checked by zlib.
        pieces = bytearray()
        while data:
            decoder = zlib.decompressobj(31)
            pieces.extend(decoder.decompress(data, expected - len(pieces) + 1))
            if not decoder.eof or len(pieces) > expected:
                raise ParquetError("Invalid Parquet GZIP page")
            data = decoder.unused_data
        output = bytes(pieces)
    elif codec == 6:
        try:
            zstd = importlib.import_module("compression.zstd")
        except ImportError as exc:
            raise ParquetError("Reading ZSTD Parquet needs Python 3.14 with compression.zstd") from exc
        try:
            decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: 28})
            output = decoder.decompress(data, max_length=expected + 1)
            if not decoder.eof or decoder.unused_data:
                raise ParquetError("Invalid Parquet ZSTD page")
        except zstd.ZstdError as exc:
            raise ParquetError(f"Invalid Parquet ZSTD page: {exc}") from exc
    else:
        raise ParquetError(f"Unsupported Parquet compression codec {codec}")
    if len(output) != expected:
        raise ParquetError("Incorrect decompressed Parquet page size")
    return output


def _pq_logical_value(value, kind, scale, factor):
    if kind == "string":
        return value.decode()
    if kind == "uuid":
        return str(uuid.UUID(bytes=value))
    if scale is not None:
        number = int.from_bytes(value, "big", signed=True) if isinstance(value, bytes) else value
        with decimal.localcontext() as ctx:
            ctx.prec = 50
            return format(decimal.Decimal(number).scaleb(-scale), "f")
    if kind == "date":
        try:
            return (_PQ_EPOCH_DATE + datetime.timedelta(days=value)).isoformat()
        except OverflowError:
            return value
    if kind == "time":
        value *= factor
        if not 0 <= value < _PQ_DAY_MICROS:
            raise ParquetError("Invalid Parquet time of day")
        return (_PQ_EPOCH_TIME + datetime.timedelta(microseconds=value)).time().isoformat()
    if kind in {"timestamp", "timestamptz"}:
        micros = value * factor
        try:
            timestamp = _PQ_EPOCH_TIME + datetime.timedelta(microseconds=micros)
        except OverflowError:
            return micros
        if kind == "timestamptz":
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        return timestamp.isoformat()
    return value


def _pq_delta_integers(raw, count, max_bits):
    if not count and raw.pos == raw.limit:
        return []
    block_size, miniblocks, encoded_count = raw.uint(), raw.uint(), raw.uint()
    if not 0 < block_size <= _PQ_MAX_ROWS or block_size % 128 or not 0 < miniblocks <= block_size:
        raise ParquetError("Invalid Parquet delta block dimensions")
    width, extra = divmod(block_size, miniblocks)
    if extra or width % 32 or encoded_count != count:
        raise ParquetError("Invalid Parquet delta miniblock dimensions or count")

    first = raw.sint()
    if count == 0:
        return []
    half_range = 1 << (max_bits - 1)
    if not -half_range <= first < half_range:
        raise ParquetError("Invalid first value in Parquet delta stream")
    values = [first]
    mask = (1 << max_bits) - 1
    while len(values) < count:
        minimum = raw.sint()
        for bits in raw.read(miniblocks):
            left = count - len(values)
            if not left:
                break  # Unused trailing miniblock widths have no corresponding payload.
            if bits > max_bits:
                raise ParquetError("Invalid Parquet delta bit width")
            packed = raw.read(bits * (width // 8))
            for delta in _pq_unpack_bits(packed, bits, min(left, width)):
                value = values[-1] + minimum + delta
                values.append(((value + half_range) & mask) - half_range)
    return values


def _pq_delta_values(payload, encoding, count, physical, width):
    raw = _ParquetReader(payload)
    if encoding == 5:
        values = _pq_delta_integers(raw, count, 32 if physical == 1 else 64)
    else:
        prefixes = _pq_delta_integers(raw, count, 32) if encoding == 7 else None
        lengths = _pq_delta_integers(raw, count, 32)
        if prefixes is None:
            values = [raw.read(length) for length in lengths]
        else:
            values = []
            previous = b""
            total = 0
            for prefix, length in zip(prefixes, lengths):
                if not 0 <= prefix <= len(previous) or length < 0:
                    raise ParquetError("Invalid prefix or suffix length in Parquet delta byte array")
                total += prefix + length
                if total > _PQ_MAX_SIZE or (physical == 7 and prefix + length != width):
                    raise ParquetError("Invalid Parquet delta byte array output size")
                previous = previous[:prefix] + raw.read(length)
                values.append(previous)
    if raw.pos != raw.limit:
        raise ParquetError("Unexpected bytes after Parquet delta stream")
    return values


def _pq_data_values(payload, encoding, count, physical, width, dictionary):
    if encoding == 0:
        return _pq_decode_plain(payload, physical, width, count)
    if encoding in (2, 8) and dictionary is not None:
        if not payload:
            if count == 0:
                return []
            raise ParquetError("Truncated Parquet dictionary stream")
        indexes = _pq_decode_rle(payload[1:], payload[0], count)
        if any(index >= len(dictionary) for index in indexes):
            raise ParquetError("Parquet dictionary index out of range")
        return [dictionary[index] for index in indexes]
    if encoding == 3 and physical == 0:
        raw = _ParquetReader(payload)
        result = [bool(value) for value in _pq_decode_rle(raw.read(raw.int32()), 1, count)]
        if raw.pos != raw.limit:
            raise ParquetError("Unexpected bytes after Parquet boolean stream")
        return result
    if (encoding == 5 and physical in {1, 2}) or (encoding == 6 and physical == 6) or (
        encoding == 7 and physical in {6, 7}
    ):
        return _pq_delta_values(payload, encoding, count, physical, width)
    if encoding == 9 and physical in {1, 2, 4, 5, 7}:
        bytes_per_value = struct.calcsize(_PQ_PLAIN_FORMAT[physical]) if physical != 7 else width
        if len(payload) != bytes_per_value * count:
            raise ParquetError("Invalid Parquet byte-stream split length")
        transposed = bytes(value for row in zip(*(payload[i * count:(i + 1) * count]
                                                  for i in range(bytes_per_value))) for value in row)
        return _pq_decode_plain(transposed, physical, width, count)
    raise ParquetError(f"Unsupported Parquet data encoding {encoding}")


def _pq_read_column(data, meta, field, specification, row_count, footer_start):
    physical, width, scale, factor = specification
    if meta[1] != physical or meta[3] != [field["name"].encode()] or meta[5] != row_count:
        raise ParquetError("Parquet column metadata does not match schema or row count")
    if not row_count:
        return []
    start = min(meta[9], meta.get(11, meta[9]))
    end = start + meta[7]
    if not 4 <= start < end <= footer_start:
        raise ParquetError("Invalid Parquet column chunk offsets")
    raw = _ParquetReader(data, start, end)
    result, dictionary = [], None
    codec = meta[4]
    while len(result) < row_count:
        page = raw.compact_struct()
        page_type, expected, size = page[1], page[2], page[3]
        payload = raw.read(size)
        if 4 in page and (zlib.crc32(payload) & 0xFFFFFFFF) != (page[4] & 0xFFFFFFFF):
            raise ParquetError("Parquet page checksum mismatch")
        if page_type == 2:
            if dictionary is not None or result or page[7][2] not in (0, 2):
                raise ParquetError("Invalid Parquet dictionary page")
            dictionary = _pq_decode_plain(_pq_decompress(payload, codec, expected), physical, width, page[7][1])
            continue
        if page_type == 0:
            header = page[5]
            count, encoding = header[1], header[2]
            if not 0 < count <= row_count - len(result):
                raise ParquetError("Invalid Parquet page row count")
            body = _ParquetReader(_pq_decompress(payload, codec, expected))
            if field["required"]:
                levels = None
            elif header[3] == 3:
                levels = _pq_decode_rle(body.read(body.int32()), 1, count)
            elif header[3] == 4:
                packed = body.read((count + 7) // 8)
                levels = [(packed[i // 8] >> (7 - i % 8)) & 1 for i in range(count)]
            else:
                raise ParquetError("Unsupported Parquet definition-level encoding")
            values = body.read(body.limit - body.pos)
        elif page_type == 3:
            header = page[8]
            count, encoding, definitions, repetitions = header[1], header[4], header[5], header[6]
            if not 0 < count <= row_count - len(result) or count != header[3] or repetitions != 0:
                raise ParquetError("Invalid Parquet V2 page row count or repetitions")
            if not 0 <= definitions <= min(len(payload), expected) or (field["required"] and definitions):
                raise ParquetError("Invalid Parquet V2 definition levels")
            levels = None if field["required"] else _pq_decode_rle(payload[:definitions], 1, count)
            if header[2] != (count - sum(levels) if levels is not None else 0):
                raise ParquetError("Parquet V2 null count does not match definition levels")
            values = _pq_decompress(payload[definitions:], codec if header.get(7, True) else 0, expected - definitions)
        else:
            raise ParquetError(f"Unsupported Parquet page type {page_type}")
        count_present = count if levels is None else sum(levels)
        decoded = _pq_data_values(values, encoding, count_present, physical, width, dictionary)
        converted = [_pq_logical_value(value, field["type"], scale, factor) for value in decoded]
        if levels is not None:
            present = iter(converted)
            result.extend(next(present) if level else None for level in levels)
        else:
            result.extend(converted)
    if raw.pos != end:
        raise ParquetError("Unexpected bytes after Parquet column chunk")
    return result


def parquet_read(path):
    """Return (Iceberg fields, rows) from a flat Parquet file."""
    with Path(path).open("rb") as handle:
        data = handle.read(_PQ_MAX_SIZE + 1)
    if len(data) > _PQ_MAX_SIZE:
        raise ParquetError("Parquet file exceeds local test size limit")
    if len(data) < 12 or data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise ParquetError("Invalid or encrypted Parquet file")
    metadata_start = len(data) - 8 - struct.unpack_from("<I", data, len(data) - 8)[0]
    if not 4 <= metadata_start < len(data) - 8:
        raise ParquetError("Invalid Parquet metadata size")
    raw = _ParquetReader(data, metadata_start, len(data) - 8)
    try:
        metadata = raw.compact_struct()
        if raw.pos != raw.limit or metadata[1] not in (1, 2) or 8 in metadata:
            raise ParquetError("Invalid or encrypted Parquet metadata")
        schema = metadata[2]
        if not 2 <= len(schema) <= 1025 or 1 in schema[0] or schema[0].get(5) != len(schema) - 1:
            raise ParquetError("Only flat Parquet schemas are supported")
        fields_and_types = [_pq_read_field(element) for element in schema[1:]]
        fields = [field for field, _ in fields_and_types]
        names = [field["name"] for field in fields]
        ids = [field["id"] for field in fields if field["id"] is not None]
        if len(names) != len(set(names)) or len(ids) != len(set(ids)) or not 0 <= metadata[3] <= _PQ_MAX_ROWS:
            raise ParquetError("Invalid Parquet field IDs, names, or row count")
        rows = []
        for group in metadata[4]:
            count = group[3]
            if not 0 <= count <= metadata[3] - len(rows) or len(group[1]) != len(fields):
                raise ParquetError("Invalid Parquet row group")
            columns = []
            for chunk, (field, specification) in zip(group[1], fields_and_types):
                if 1 in chunk or 8 in chunk or 9 in chunk:
                    raise ParquetError("External or encrypted Parquet column chunks are unsupported")
                columns.append(_pq_read_column(data, chunk[3], field, specification, count, metadata_start))
            rows.extend(dict(zip(names, row)) for row in zip(*columns))
        if len(rows) != metadata[3]:
            raise ParquetError("Parquet file row count does not match row groups")
    except ParquetError:
        raise
    except (KeyError, IndexError, ValueError, TypeError, OverflowError, zlib.error) as exc:
        raise ParquetError(f"Invalid Parquet data or metadata: {exc}") from exc
    return fields, rows


class IcebergFileError(ValueError):
    pass


_ICE_EPOCH = datetime.datetime(1970, 1, 1)
_ICE_DAY = 86_400_000_000
_ICE_AVRO_TYPES = {
    "boolean": "boolean", "int": "int", "long": "long", "float": "float", "double": "double",
    "string": "string", "binary": "bytes", "date": "int", "time": "long", "timestamp": "long", "timestamptz": "long",
}



def _ice_primitive(primitive):
    _pq_type(primitive)
    return primitive.split("(", 1)[0].split("[", 1)[0]


def _ice_decimal(value, primitive):
    _, _, (precision, scale) = _pq_type(primitive)
    try:
        number = decimal.Decimal(str(value))
        if not number.is_finite() or (number and abs(number.adjusted()) > 76):
            raise ValueError
        with decimal.localcontext() as context:
            context.prec = max(len(number.as_tuple().digits), 38) + 2
            scaled = number.scaleb(scale)
            if scaled != scaled.to_integral_value() or abs(scaled) >= 10**precision:
                raise ValueError
            rounded = number.quantize(decimal.Decimal(1).scaleb(-scale))
            return format(rounded if rounded else rounded.copy_abs(), "f")
    except (ValueError, decimal.DecimalException) as exc:
        raise IcebergFileError(f"Invalid value for {primitive}: {value!r}") from exc


def _ice_time_micros(delta):
    return delta.days * _ICE_DAY + delta.seconds * 1_000_000 + delta.microseconds


def _ice_normalize(value, primitive):
    if value is None:
        return None
    base = _ice_primitive(primitive)
    if base == "decimal":
        return _ice_decimal(value, primitive)
    if base == "uuid":
        return str(uuid.UUID(bytes=value) if isinstance(value, bytes) else uuid.UUID(str(value)))
    if primitive == "string" and isinstance(value, str):
        return value
    if primitive == "boolean" and type(value) is bool:
        return value
    if primitive in {"int", "long"} and type(value) is int:
        bits = 32 if primitive == "int" else 64
        if -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            return value
    if primitive in {"float", "double"} and (
        type(value) in (int, float) or value in ("NaN", "Infinity", "-Infinity")
    ):
        result = float(value)
        return struct.unpack("<f", struct.pack("<f", result))[0] if primitive == "float" else result

    if base in {"binary", "fixed"}:
        if isinstance(value, str):
            try:
                value = base64.b64decode(value, validate=True)
            except binascii.Error as exc:
                raise IcebergFileError("Binary columns use base64 strings in JSON") from exc
        if isinstance(value, bytes):
            if base == "fixed" and len(value) != _pq_type(primitive)[1]:
                raise IcebergFileError(f"Invalid binary length for {primitive}")
            return value
    if primitive in {"date", "time", "timestamp", "timestamptz"}:
        if type(value) is int:
            limit = (-(1 << 31), (1 << 31)) if primitive == "date" else (-(1 << 63), (1 << 63))
            if primitive == "time":
                limit = (0, _ICE_DAY)
            if limit[0] <= value < limit[1]:
                return value
        if primitive == "date" and isinstance(value, (str, datetime.date)):
            date = datetime.date.fromisoformat(value) if isinstance(value, str) else value
            return (date - _ICE_EPOCH.date()).days
        if primitive == "time" and isinstance(value, (str, datetime.time)):
            clock = datetime.time.fromisoformat(value) if isinstance(value, str) else value
            if clock.tzinfo is None:
                return ((clock.hour * 60 + clock.minute) * 60 + clock.second) * 1_000_000 + clock.microsecond
        if primitive in {"timestamp", "timestamptz"} and isinstance(value, (str, datetime.datetime)):
            if isinstance(value, str):
                value = datetime.datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if
                                                           value.endswith("Z") else value)
            if primitive == "timestamp" and value.tzinfo is None:
                return _ice_time_micros(value - _ICE_EPOCH)
            if primitive == "timestamptz" and value.tzinfo is not None:
                utc = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                return _ice_time_micros(utc - _ICE_EPOCH)
    raise IcebergFileError(f"Invalid value for {primitive}: {value!r}")


def _ice_json_value(value, primitive):
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"
    if _ice_primitive(primitive) in {"binary", "fixed"}:
        return base64.b64encode(value).decode("ascii")
    try:
        if primitive == "date":
            return (_ICE_EPOCH.date() + datetime.timedelta(days=value)).isoformat()
        if primitive == "time":
            return (_ICE_EPOCH + datetime.timedelta(microseconds=value)).time().isoformat()
        if primitive in {"timestamp", "timestamptz"}:
            stamp = (_ICE_EPOCH + datetime.timedelta(microseconds=value)).isoformat()
            return stamp + "Z" if primitive == "timestamptz" else stamp
    except OverflowError:
        return value
    return value


def _ice_flat_fields(schema):
    if not isinstance(schema, dict) or schema.get("type") != "struct" or not isinstance(schema.get("fields"), list):
        raise IcebergFileError("Schema must be a struct with a fields array")
    fields = schema["fields"]
    if not fields:
        raise IcebergFileError("A file schema needs at least one column")
    for field in fields:
        if not isinstance(field, dict) or "type" not in field:
            raise IcebergFileError("Each field must contain a type")
        _ice_primitive(field["type"])
    return fields


def _ice_schema(metadata, schema_id=None):
    if schema_id is None:
        schema_id = metadata["current-schema-id"]
    for schema in metadata["schemas"]:
        if schema["schema-id"] == schema_id:
            return schema
    raise IcebergFileError(f"Unknown schema ID {schema_id}")


def _ice_partition_spec(metadata, spec_id=None):
    if spec_id is None:
        spec_id = metadata["default-spec-id"]
    for spec in metadata["partition-specs"]:
        if spec["spec-id"] == spec_id:
            return spec
    raise IcebergFileError(f"Unknown partition spec ID {spec_id}")


def _ice_murmur3(data):
    mask = 0xFFFFFFFF

    def rot(value, amount):
        return ((value << amount) | (value >> (32 - amount))) & mask

    result = 0
    for start in range(0, len(data), 4):
        chunk = data[start:start + 4]
        block = int.from_bytes(chunk, "little")
        result ^= (rot((block * 0xCC9E2D51) & mask, 15) * 0x1B873593) & mask
        if len(chunk) == 4:
            result = (rot(result, 13) * 5 + 0xE6546B64) & mask
    result ^= len(data)
    result = ((result ^ (result >> 16)) * 0x85EBCA6B) & mask
    result = ((result ^ (result >> 13)) * 0xC2B2AE35) & mask
    return result ^ (result >> 16)


def _ice_partition_layout(metadata):
    spec = _ice_partition_spec(metadata)
    current = _ice_schema(metadata)
    source_fields = {field["id"]: field for field in _ice_flat_fields(current)}
    for schema in reversed(metadata["schemas"]):
        for field in schema["fields"]:
            source_fields.setdefault(field["id"], field)
    layout, avro_fields = [], []
    for part in spec["fields"]:
        source = source_fields.get(part["source-id"])
        if source is None:
            raise IcebergFileError(f"Unknown partition source ID {part['source-id']}")
        primitive = source["type"]
        base = _ice_primitive(primitive)
        transform = part["transform"]
        name = part["name"]
        argument = None
        if match := re.fullmatch(r"(bucket|truncate)\[([1-9][0-9]{0,8})\]", transform):
            transform, argument = match[1], int(match[2])
        if transform in {"identity", "void"}:
            target = primitive
        elif transform == "truncate" and argument and base in {"int", "long", "string", "binary", "decimal"}:
            target = primitive
        elif transform == "bucket" and argument and base in {
            "int", "long", "string", "binary", "date", "time", "timestamp", "timestamptz", "uuid", "decimal", "fixed",
        }:
            target = "int"
        elif transform in {"year", "month", "day", "hour"} and primitive in {
            "date", "timestamp", "timestamptz",
        } and (transform != "hour" or primitive != "date"):
            target = "int"
        else:
            raise IcebergFileError(f"Unsupported partition transform {part['transform']!r} for {primitive}")
        layout.append((name, source["name"], primitive, transform, argument))
        avro_type = {"date": {"type": "int", "logicalType": "date"},
                     "time": {"type": "long", "logicalType": "time-micros"},
                     "timestamp": {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": False},
                     "timestamptz": {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": True}
                     }.get(target, _ICE_AVRO_TYPES.get(target))
        if avro_type is None:
            _, width, decimal_type = _pq_type(target)
            avro_type = {"type": "fixed", "size": width, "name": f"partition_{part['field-id']}"}
            if decimal_type:
                precision, scale = decimal_type
                avro_type.update(logicalType="decimal", precision=precision, scale=scale)
            elif target == "uuid":
                avro_type["logicalType"] = "uuid"
        avro_fields.append((part["field-id"], name, avro_type))
    return spec, layout, avro_fields


def _ice_partition_value(value, primitive, transform, argument):
    if value is None or transform == "void":
        return None
    base = _ice_primitive(primitive)
    decimal_width = None
    if base == "decimal":
        _, decimal_width, (_, scale) = _pq_type(primitive)
        with decimal.localcontext() as context:
            context.prec = 40
            value = int(decimal.Decimal(value).scaleb(scale))
    if base == "uuid":
        value = uuid.UUID(value).bytes
    if transform == "truncate":
        value = value - value % argument if isinstance(value, int) else value[:argument]
    if transform == "bucket":
        if decimal_width:
            data = value.to_bytes((8 + (value if value >= 0 else ~value).bit_length()) // 8, "big", signed=True)
        elif isinstance(value, str):
            data = value.encode("utf-8")
        elif isinstance(value, bytes):
            data = value
        else:
            data = struct.pack("<q", value)
        return (_ice_murmur3(data) & 0x7FFFFFFF) % argument
    if transform in {"identity", "truncate"}:
        return value.to_bytes(decimal_width, "big", signed=True) if decimal_width else value
    if transform == "hour":
        return value // 3_600_000_000
    days = value if primitive == "date" else value // _ICE_DAY
    if transform == "day":
        return days
    # The Gregorian calendar repeats every 400 years. Keep datetime within its valid range.
    cycles, remainder = divmod(days, 146097)
    date = _ICE_EPOCH.date() + datetime.timedelta(days=remainder)
    years = cycles * 400 + date.year - 1970
    return years if transform == "year" else years * 12 + date.month - 1


def _ice_frozen(value):
    if isinstance(value, float):
        return ("float", b"nan" if math.isnan(value) else struct.pack(">d", value))
    if isinstance(value, dict):
        return tuple((key, _ice_frozen(item)) for key, item in sorted(value.items()))
    return value


def _ice_equal(left, right):
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left):
        return math.isnan(right)
    return left == right


def _ice_snapshot(metadata, snapshot_id=None):
    if snapshot_id is None:
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id in (None, -1):
            return None
    for snapshot in metadata["snapshots"]:
        if snapshot["snapshot-id"] == snapshot_id:
            return snapshot
    raise IcebergFileError(f"Unknown snapshot ID {snapshot_id}")


def _ice_inventory(snapshot):
    if snapshot is None:
        return [], []
    _, _, manifests = avro_read(_catalog_local_path(snapshot["manifest-list"]))
    inventory = []
    live_paths = set()
    for manifest in manifests:
        schema, header, entries = avro_read(_catalog_local_path(manifest["manifest_path"]))
        live = []
        for entry in entries:
            if entry["status"] == 2:
                continue
            if entry["status"] not in (0, 1):
                raise IcebergFileError(f"Unsupported manifest entry status {entry['status']}")
            for member in ("sequence_number", "file_sequence_number"):
                if entry.get(member) is None:
                    entry[member] = manifest.get("sequence_number", 0)
            if entry.get("snapshot_id") is None:
                entry["snapshot_id"] = manifest["added_snapshot_id"]
            identity = (entry["data_file"].get("content", 0), entry["data_file"]["file_path"])
            if identity in live_paths:
                raise IcebergFileError(f"Duplicate live file in snapshot: {identity[1]}")
            live_paths.add(identity)
            live.append(entry)
        inventory.append((manifest, schema, header, live))
    return manifests, inventory


def _ice_parquet_file(data_file):
    if data_file["file_format"].upper() != "PARQUET":
        raise IcebergFileError(f"Unsupported data file format {data_file['file_format']}")
    fields, rows = parquet_read(_catalog_local_path(data_file["file_path"]))
    if len(rows) != data_file["record_count"]:
        raise IcebergFileError(f"Manifest row count does not match data file: {data_file['file_path']}")
    return fields, rows


def _ice_column_names(fields):
    return {field["id"]: field["name"] for field in fields if field.get("id") is not None}


def _ice_read_rows(metadata, schema, inventory):
    fields = _ice_flat_fields(schema)
    target_fields = {field["id"]: field for field in fields}
    for old_schema in reversed(metadata["schemas"]):
        for field in old_schema["fields"]:
            target_fields.setdefault(field["id"], field)
    name_mapping = {}
    if encoded := metadata.get("properties", {}).get("schema.name-mapping.default"):
        mapping = json.loads(encoded)
        if not isinstance(mapping, list):
            raise IcebergFileError("Invalid Iceberg column name mapping")
        for entry in mapping:
            if (not isinstance(entry, dict) or type(entry.get("field-id")) is not int
                    or not isinstance(entry.get("names"), list) or not all(isinstance(name, str) for name in entry["names"])):
                raise IcebergFileError("Invalid Iceberg column name mapping entry")
            name_mapping[entry["field-id"]] = entry["names"]
    position_deletes = []
    equality_deletes = []
    data_entries = []
    unpartitioned = {spec["spec-id"] for spec in metadata["partition-specs"] if not spec["fields"]}
    for manifest, _, _, entries in inventory:
        spec_id = manifest["partition_spec_id"]
        for entry in entries:
            data_file = entry["data_file"]
            content = data_file.get("content", 0)
            partition = _ice_frozen(data_file["partition"])
            sequence = entry["sequence_number"]
            if content == 0:
                data_entries.append((data_file, spec_id, partition, sequence))
                continue
            delete_fields, delete_rows = _ice_parquet_file(data_file)
            names = _ice_column_names(delete_fields)
            if content == 1:
                path_column = names.get(2147483546, "file_path")
                position_column = names.get(2147483545, "pos")
                positions = {}
                for row in delete_rows:
                    positions.setdefault(row[path_column], set()).add(row[position_column])
                position_deletes.append((spec_id, partition, sequence, positions))
            elif content == 2:
                ids = tuple(data_file.get("equality_ids") or ())
                if not ids or not set(ids) <= names.keys() or not set(ids) <= target_fields.keys():
                    raise IcebergFileError("Equality delete references missing or unsupported fields")
                keys = {tuple(_ice_frozen(_ice_normalize(row[names[i]], target_fields[i]["type"])) for i in ids)
                        for row in delete_rows}
                equality_deletes.append((spec_id, partition, sequence, ids, keys))
            else:
                raise IcebergFileError(f"Unsupported delete file type {content}")
    for data_file, spec_id, partition, sequence in data_entries:
        parquet_fields, rows = _ice_parquet_file(data_file)
        names = _ice_column_names(parquet_fields)
        available_names = {field["name"] for field in parquet_fields if field.get("id") is None}
        for field in target_fields.values():
            field_id = field["id"]
            if field_id not in names:
                aliases = name_mapping.get(field_id, [field["name"]])
                matches = available_names.intersection(aliases)
                if len(matches) > 1:
                    raise IcebergFileError(f"Ambiguous Parquet column mapping for field ID {field_id}")
                if matches:
                    names[field_id] = matches.pop()
        deleted_positions = set()
        for delete_spec, delete_partition, delete_seq, positions in position_deletes:
            if (delete_spec, delete_partition) == (spec_id, partition) and sequence <= delete_seq:
                deleted_positions.update(positions.get(data_file["file_path"], ()))
        applicable = [
            (ids, keys) for delete_spec, delete_partition, delete_seq, ids, keys in equality_deletes
            if sequence < delete_seq and ((delete_spec, delete_partition) == (spec_id, partition)
                                          or delete_spec in unpartitioned)
        ]
        needed = set().union(*(ids for ids, _ in applicable), (field["id"] for field in fields))
        for position, row in enumerate(rows):
            if position in deleted_positions:
                continue
            by_id = {}
            for field_id in needed:
                field = target_fields[field_id]
                key = names.get(field_id)
                value = row.get(key, field.get("initial-default"))
                if value is None and field["required"]:
                    raise IcebergFileError(f"Data file omits required column {field['name']!r}")
                by_id[field_id] = _ice_normalize(value, field["type"])
            if any(tuple(_ice_frozen(by_id[i]) for i in ids) in keys for ids, keys in applicable):
                continue
            yield {field["name"]: by_id[field["id"]] for field in fields}


def _ice_filter(fields, equals):
    if equals is None:
        return {}
    by_name = {field["name"]: field["type"] for field in fields}
    if not isinstance(equals, dict) or not equals.keys() <= by_name.keys():
        raise IcebergFileError("Filter must be a JSON object with known column names")
    return {name: _ice_normalize(value, by_name[name]) for name, value in equals.items()}


def _ice_matches(row, equals):
    return all(_ice_equal(row[name], value) for name, value in equals.items())


def _ice_validate_rows(schema, rows):
    fields = _ice_flat_fields(schema)
    names = {field["name"] for field in fields}
    result = []
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict) or not row.keys() <= names:
            raise IcebergFileError(f"Row {index} must be an object containing only table columns")
        values = {}
        for field in fields:
            name = field["name"]
            value = row.get(name, field.get("write-default"))
            if value is None and field["required"]:
                raise IcebergFileError(f"Row {index} needs required field {name!r}")
            values[name] = _ice_normalize(value, field["type"])
        result.append(values)
    return result


def _ice_new_path(directory, prefix, suffix, created):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}{secrets.token_hex(16)}{suffix}"
    created.append(path)
    return path


def _ice_write_manifest(directory, schema, header, entries, spec_id, content, snapshot, created):
    snapshot_id = snapshot["snapshot-id"]
    sequence = snapshot["sequence-number"]
    path = _ice_new_path(directory, "manifest-", ".avro", created)
    avro_write(path, schema, entries, header, codec="deflate")
    result = {
        "manifest_path": path.as_uri(),
        "manifest_length": path.stat().st_size,
        "partition_spec_id": spec_id,
        "content": content,
        "sequence_number": sequence,
        "min_sequence_number": min(
            (entry["sequence_number"] if entry["sequence_number"] is not None else sequence
             for entry in entries if entry["status"] != 2),
            default=sequence,
        ),
        "added_snapshot_id": snapshot_id,
    }
    for status, name in ((0, "existing"), (1, "added"), (2, "deleted")):
        records = [entry["data_file"]["record_count"] for entry in entries if entry["status"] == status]
        result[f"{name}_files_count"] = len(records)
        result[f"{name}_rows_count"] = sum(records)
    return result


def _ice_append_files(metadata, schema, rows, snapshot, location, created):
    spec, layout, avro_fields = _ice_partition_layout(metadata)
    fields = _ice_flat_fields(schema)
    groups = {}
    for row in rows:
        partition = {
            name: _ice_partition_value(row.get(source), primitive, transform, argument)
            for name, source, primitive, transform, argument in layout
        }
        _, group = groups.setdefault(_ice_frozen(partition), (partition, []))
        group.append(row)
    entries = []
    for partition, group in groups.values():
        path = _ice_new_path(location / "data", "data-", ".parquet", created)
        parquet_write(path, fields, group)
        data_file = {
            "content": 0, "file_path": path.as_uri(), "file_format": "PARQUET",
            "partition": partition, "record_count": len(group), "file_size_in_bytes": path.stat().st_size,
            "value_counts": {field["id"]: len(group) for field in fields},
            "null_value_counts": {field["id"]: sum(row[field["name"]] is None for row in group) for field in fields},
            "nan_value_counts": {field["id"]: sum(
                isinstance(row[field["name"]], float) and math.isnan(row[field["name"]]) for row in group
            ) for field in fields if field["type"] in {"float", "double"}},
            "sort_order_id": 0,
        }
        entries.append({
            "status": 1,
            "snapshot_id": snapshot["snapshot-id"],
            "sequence_number": None,
            "file_sequence_number": None,
            "data_file": data_file,
        })
    if not entries:
        return []
    header = {
        "schema": json.dumps(schema, separators=(",", ":")),
        "schema-id": str(schema["schema-id"]),
        "partition-spec": json.dumps(spec["fields"], separators=(",", ":")),
        "partition-spec-id": str(spec["spec-id"]),
        "format-version": "2",
        "content": "data",
    }
    return [_ice_write_manifest(
        location / "metadata", iceberg_manifest_schema(avro_fields), header, entries, spec["spec-id"],
        0, snapshot, created,
    )]


def _ice_delete_manifests(inventory, snapshot, location, created):
    result = []
    for manifest, schema, header, live in inventory:
        if not live:
            continue
        entries = [dict(entry, status=2, snapshot_id=snapshot["snapshot-id"]) for entry in live]
        user_header = {key: value for key, value in header.items() if not key.startswith("avro.")}
        result.append(_ice_write_manifest(
            location / "metadata", schema, user_header, entries, manifest["partition_spec_id"],
            manifest.get("content", 0), snapshot, created,
        ))
    return result


def _ice_flush_files(paths):
    parents = set()
    for path in paths:
        with path.open("rb") as file:
            os.fsync(file.fileno())
        parents.add(path.parent)
    if hasattr(os, "O_DIRECTORY"):
        for path in parents:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


class IcebergFiles:
    def __init__(self, catalog):
        self.catalog = catalog

    def _metadata(self, namespace, table):
        metadata = self.catalog.load_table(namespace, table)["metadata"]
        if metadata["format-version"] != 2:
            raise IcebergFileError("File operations require Iceberg format version 2")
        return metadata

    def append(self, namespace, table, rows):
        metadata = self._metadata(namespace, table)
        schema = _ice_schema(metadata)
        rows = _ice_validate_rows(schema, rows)
        if not rows:
            raise IcebergFileError("Append requires at least one row")
        return self._write_snapshot(namespace, table, metadata, schema, rows)

    def delete(self, namespace, table, equals):
        metadata = self._metadata(namespace, table)
        schema = _ice_schema(metadata)
        predicate = _ice_filter(_ice_flat_fields(schema), equals)
        if not predicate:
            raise IcebergFileError("Delete requires a non-empty equality filter")
        _, inventory = _ice_inventory(_ice_snapshot(metadata))
        rows = list(_ice_read_rows(metadata, schema, inventory))
        kept = [row for row in rows if not _ice_matches(row, predicate)]
        deleted = len(rows) - len(kept)
        if not deleted:
            return {"deleted-rows": 0, "snapshot-id": metadata.get("current-snapshot-id")}
        snapshot = self._write_snapshot(namespace, table, metadata, schema, kept, inventory)
        return {"deleted-rows": deleted, "snapshot-id": snapshot["snapshot-id"]}

    def scan(self, namespace, table, *, snapshot_id=None, columns=None, equals=None, limit=None):
        metadata = self._metadata(namespace, table)
        snapshot = _ice_snapshot(metadata, snapshot_id)
        schema_id = snapshot.get("schema-id") if snapshot_id is not None else None
        schema = _ice_schema(metadata, schema_id)
        fields = _ice_flat_fields(schema)
        wanted = {field["name"]: field["type"] for field in fields}
        if columns is not None:
            if not set(columns) <= wanted.keys():
                raise IcebergFileError("Projection refers to unknown columns")
            wanted = {name: wanted[name] for name in columns}
        predicate = _ice_filter(fields, equals)
        if limit is not None and limit < 0:
            raise IcebergFileError("Scan limit must not be negative")
        if limit == 0:
            return
        _, inventory = _ice_inventory(snapshot)
        yielded = 0
        for row in _ice_read_rows(metadata, schema, inventory):
            if _ice_matches(row, predicate):
                yield {name: _ice_json_value(row[name], primitive) for name, primitive in wanted.items()}
                yielded += 1
                if limit is not None and yielded >= limit:
                    break

    def _write_snapshot(self, namespace, table, metadata, schema, rows, replaced=None):
        parent = _ice_snapshot(metadata)
        parent_id = parent["snapshot-id"] if parent else None
        snapshot_ids = {item["snapshot-id"] for item in metadata["snapshots"]}
        while (snapshot_id := (secrets.randbits(63) or 1)) in snapshot_ids:
            pass
        sequence = metadata["last-sequence-number"] + 1
        snapshot = {
            "snapshot-id": snapshot_id,
            "sequence-number": sequence,
            "timestamp-ms": max(time.time_ns() // 1_000_000, metadata["last-updated-ms"] + 1),
            "schema-id": schema["schema-id"],
        }
        if parent_id is not None:
            snapshot["parent-snapshot-id"] = parent_id
        previous = [] if parent is None else avro_read(_catalog_local_path(parent["manifest-list"]))[2]
        location = self.catalog.local_path(metadata["location"])
        created = []
        try:
            added = _ice_append_files(metadata, schema, rows, snapshot, location, created)
            deleted = [] if replaced is None else _ice_delete_manifests(replaced, snapshot, location, created)
            manifests = [*(previous if replaced is None else []), *added, *deleted]
            path = _ice_new_path(location / "metadata", f"snap-{snapshot_id}-", ".avro", created)
            avro_write(path, iceberg_manifest_list_schema(), manifests, {
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent_id) if parent_id is not None else "null",
                "sequence-number": str(sequence),
                "format-version": "2",
            }, codec="deflate")
            snapshot["manifest-list"] = path.as_uri()

            def total(items, key):
                return sum(item.get(key, 0) or 0 for item in items if item.get("content", 0) == 0)

            def count(items, unit):
                return total(items, f"added_{unit}_count") + total(items, f"existing_{unit}_count")

            operation = "append" if replaced is None else "overwrite" if added else "delete"
            snapshot["summary"] = {
                "operation": operation,
                "added-data-files": str(total(added, "added_files_count")),
                "added-records": str(total(added, "added_rows_count")),
                "deleted-data-files": str(total(deleted, "deleted_files_count")),
                "deleted-records": str(total(deleted, "deleted_rows_count")),
                "total-data-files": str(count(manifests, "files")),
                "total-records": str(count(manifests, "rows")),
            }
            _ice_flush_files(created)
        except BaseException:
            for path in created:
                path.unlink(missing_ok=True)
            raise
        requirements = [
            {"type": "assert-table-uuid", "uuid": metadata["table-uuid"]},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent_id},
            {"type": "assert-current-schema-id", "current-schema-id": schema["schema-id"]},
            {"type": "assert-default-spec-id", "default-spec-id": metadata["default-spec-id"]},
        ]
        updates = [
            {"action": "add-snapshot", "snapshot": snapshot},
            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": snapshot_id, "type": "branch"},
        ]
        try:
            self.catalog.commit_table(namespace, table, requirements, updates)
        except IcebergCatalogError as exc:
            if exc.status in {400, 404, 409, 422}:
                for path in created:
                    path.unlink(missing_ok=True)
            raise
        return snapshot


def _ice_identifier(value):
    parts = value.split(".")
    if len(parts) < 2 or any(not part for part in parts):
        raise argparse.ArgumentTypeError("Use namespace.table or parent.namespace.table")
    return tuple(parts[:-1]), parts[-1]


def _ice_json_file(filename):
    with open(filename, encoding="utf-8") as file:
        return json.load(file)


def _ice_json_rows(stream):
    for line_number, line in enumerate(stream, 1):
        if line.strip():
            try:
                yield json.loads(line)
            except ValueError as exc:
                raise IcebergFileError(f"Invalid JSON on line {line_number}: {exc}") from exc


def iceberg_main(argv=None):
    parser = argparse.ArgumentParser(description="Iceberg v2 REST catalog and local Parquet/Avro table tools")
    parser.add_argument("--warehouse", default="iceberg-warehouse", help="local warehouse directory or file URI")
    parser.add_argument("--db-file", help="SQLite catalog path (default: in the warehouse)")
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="start the Iceberg REST Catalog")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", default=8181, type=int, help="REST port; 0 chooses an available port")

    polaris = commands.add_parser("serve-polaris", help="start the Polaris REST Catalog and Management API")
    polaris.add_argument("--host", default="127.0.0.1")
    polaris.add_argument("--port", default=8181, type=int, help="REST port; 0 chooses an available port")
    polaris.add_argument("--realm", default="POLARIS")
    polaris.add_argument("--password-file", required=True, type=Path, help="file containing the root client secret")

    create = commands.add_parser("create-table", help="create a v2 table from an Iceberg schema JSON file")
    create.add_argument("table", type=_ice_identifier)
    create.add_argument("--schema", required=True)
    create.add_argument("--partition-spec", help="Iceberg partition spec JSON file")

    append = commands.add_parser("append", help="write newline-delimited JSON rows to Parquet and commit")
    append.add_argument("table", type=_ice_identifier)
    append.add_argument("input", help="JSON Lines file, or - to read stdin")

    scan = commands.add_parser("scan", help="read a snapshot as newline-delimited JSON")
    scan.add_argument("table", type=_ice_identifier)
    scan.add_argument("--snapshot-id", type=int)
    scan.add_argument("--columns", help="comma-separated column names")
    scan.add_argument("--where", type=json.loads, help='JSON equality filter, e.g. {"category":"Books"}')
    scan.add_argument("--limit", type=int)

    delete = commands.add_parser("delete", help="delete matching rows by rewriting the current snapshot")
    delete.add_argument("table", type=_ice_identifier)
    delete.add_argument("--where", type=json.loads, required=True)

    snapshots = commands.add_parser("snapshots", help="list snapshot metadata as JSON")
    snapshots.add_argument("table", type=_ice_identifier)

    args = parser.parse_args(argv)
    try:
        if args.command in ("serve", "serve-polaris"):
            if not 0 <= args.port <= 65535:
                parser.error("--port must be between 0 and 65535")
            if args.command == "serve-polaris":
                password = args.password_file.read_text(encoding="utf-8").rstrip("\r\n")
                if not password or len(password) > 4096:
                    parser.error("--password-file must contain a root secret of 1 to 4096 characters")
                polaris_serve(args.warehouse, args.db_file, args.host, args.port, password, args.realm)
            else:
                iceberg_serve(args.warehouse, args.db_file, args.host, args.port)
            return 0
        namespace, table = args.table
        catalog = IcebergCatalog(args.warehouse, args.db_file)
        files = IcebergFiles(catalog)
        if args.command == "create-table":
            schema = _ice_json_file(args.schema)
            if isinstance(schema, list):
                schema = {"type": "struct", "schema-id": 0, "fields": schema}
            _ice_flat_fields(schema)
            spec = _ice_json_file(args.partition_spec) if args.partition_spec else None
            for depth in range(1, len(namespace) + 1):
                try:
                    catalog.create_namespace(namespace[:depth])
                except IcebergCatalogError as exc:
                    if exc.status != 409:
                        raise
            result = catalog.create_table(namespace, table, schema, partition_spec=spec)
        elif args.command == "append":
            if args.input == "-":
                result = files.append(namespace, table, _ice_json_rows(sys.stdin))
            else:
                with open(args.input, encoding="utf-8") as stream:
                    result = files.append(namespace, table, _ice_json_rows(stream))
        elif args.command == "scan":
            columns = [name.strip() for name in args.columns.split(",")] if args.columns is not None else None
            for row in files.scan(namespace, table, snapshot_id=args.snapshot_id, columns=columns,
                                  equals=args.where, limit=args.limit):
                print(json.dumps(row, ensure_ascii=False, allow_nan=False))
            return 0
        elif args.command == "delete":
            result = files.delete(namespace, table, args.where)
        else:
            result = catalog.load_table(namespace, table)["metadata"]["snapshots"]
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    except (OSError, ValueError, OverflowError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(iceberg_main())
