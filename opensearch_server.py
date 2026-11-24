#!/usr/bin/env python3
"""Single-node OpenSearch REST subset for integration tests."""

from __future__ import annotations

import argparse
import base64
import fnmatch
import gzip
import http.client
import io
import json
import math
import os
import re
import sqlite3
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib
from datetime import datetime, timezone
from functools import cmp_to_key
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit


VERSION = "2.19.0"
PRIMARY_TERM = 1
INTEGER_TYPES = {"byte", "short", "integer", "long", "unsigned_long"}
FLOAT_TYPES = {"half_float", "float", "double", "scaled_float"}
SHARDS = {"total": 1, "successful": 1, "failed": 0}
INDEX_NAME = re.compile(r"[^\sA-Z\\/*?\"<>|,#:]+\Z")
TOKENS = re.compile(r"[^\W_]+", re.UNICODE)


class Error(Exception):
    def __init__(self, status, kind, reason, **details):
        super().__init__(reason)
        self.status = status
        self.body = {"type": kind, "reason": reason, **details}


def invalid(reason):
    return Error(400, "illegal_argument_exception", reason)


def missing_index(name):
    return Error(404, "index_not_found_exception", f"no such index [{name}]", index=name)


def object_value(value, label):
    if not isinstance(value, dict):
        raise invalid(f"[{label}] must be an object")
    return value


def list_value(value, label):
    if not isinstance(value, list):
        raise invalid(f"[{label}] must be an array")
    return value


def integer(value, label, minimum=0):
    if not isinstance(value, (int, str)) or isinstance(value, bool):
        raise invalid(f"[{label}] must be an integer >= {minimum}")
    try:
        result = int(value)
    except ValueError:
        raise invalid(f"[{label}] must be an integer >= {minimum}") from None
    if result < minimum or result > 2 ** 63 - 1:
        raise invalid(f"[{label}] is out of range")
    return result


def boolean(value, label):
    if value is True or value == "true":
        return True
    if value is False or value == "false":
        return False
    raise invalid(f"[{label}] must be true or false")


def json_text(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def parse_json(data):
    def invalid_constant(value):
        raise ValueError(value)

    try:
        parsed = json.loads(data, parse_constant=invalid_constant)
        json_text(parsed)
        return parsed
    except (ValueError, RecursionError):
        raise Error(400, "parse_exception", "Invalid JSON content") from None


def check_index_name(name):
    if (not isinstance(name, str) or not INDEX_NAME.fullmatch(name) or name in {".", ".."}
            or name != name.lower() or name.startswith(("_", "-", "+")) or len(name.encode("utf-8", "replace")) > 255):
        raise Error(400, "invalid_index_name_exception", f"invalid index or alias name [{name}]")


def refresh_seconds(value):
    if str(value) == "-1":
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", str(value))
    if not match:
        raise invalid("[index.refresh_interval] must be -1 or a duration such as 1s")
    seconds = float(match[1]) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[match[2]]
    if not math.isfinite(seconds):
        raise invalid("[index.refresh_interval] is out of range")
    return max(0.001, seconds)


def date_value(value):
    if type(value) in {int, float}:
        return value
    if not isinstance(value, str):
        raise ValueError(value)
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.timestamp() * 1000


def infer_field(value):
    if isinstance(value, dict):
        return {"properties": {}}
    if type(value) is bool:
        return {"type": "boolean"}
    if type(value) is int:
        return {"type": "long"}
    if type(value) is float:
        return {"type": "float"}
    return {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}


def field_type(field):
    return field.get("type", "object" if "properties" in field else "text")


def typed_value(value, field):
    kind = field_type(field)
    if value is None:
        return None
    if kind in {"object", "nested"}:
        if isinstance(value, dict):
            return value
    elif kind in {"keyword", "text", "constant_keyword", "wildcard"}:
        if type(value) in {str, bool, int, float}:
            return str(value).lower() if type(value) is bool else str(value)
    elif kind in INTEGER_TYPES | FLOAT_TYPES:
        if type(value) in {int, float} or isinstance(value, str) and field.get("coerce", True):
            if kind in INTEGER_TYPES:
                if not field.get("coerce", True) and type(value) is not int:
                    raise ValueError(value)
                result = int(float(value)) if isinstance(value, str) and not re.fullmatch(r"[+-]?\d+", value) else int(value)
                if kind == "unsigned_long" and 0 <= result < 2 ** 64:
                    return result
                bits = {"byte": 8, "short": 16, "integer": 32, "long": 64}.get(kind)
                if bits is not None and -(2 ** (bits - 1)) <= result < 2 ** (bits - 1):
                    return result
            else:
                result = float(value)
                if math.isfinite(result):
                    return result
    elif kind == "boolean":
        if type(value) is bool:
            return value
        if value in ("true", "false"):
            return value == "true"
    elif kind in {"date", "date_nanos"}:
        return date_value(value)
    else:
        return value
    raise ValueError(value)


def flatten(value):
    if isinstance(value, list):
        for item in value:
            yield from flatten(item)
    elif value is not None:
        yield value


def update_dynamic_mapping(source, mapping, mode=True, prefix=""):
    mode = mapping.get("dynamic", mode)
    properties = mapping.setdefault("properties", {})
    for name, raw in source.items():
        values = list(flatten(raw))
        if not values:
            continue
        path = prefix + name
        if name not in properties:
            if mode == "strict":
                raise Error(400, "strict_dynamic_mapping_exception", f"dynamic introduction of [{path}] is not allowed")
            if mode in {False, "false"}:
                continue
            properties[name] = infer_field(values[0])
        field = properties[name]
        for value in values:
            try:
                typed = typed_value(value, field)
            except (ValueError, OverflowError):
                raise Error(400, "mapper_parsing_exception", f"failed to parse field [{path}] of type [{field_type(field)}]") from None
            if field_type(field) in {"object", "nested"} and field.get("enabled", True):
                update_dynamic_mapping(typed, field, mode, path + ".")


def merge_objects(previous, changes):
    merged = dict(previous)
    for key, value in changes.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_objects(merged[key], value)
        else:
            merged[key] = value
    return merged


def merge_mapping(previous, changes):
    result = dict(previous)
    if "type" in changes and previous and field_type(previous) != changes["type"]:
        raise invalid("a field's mapping type cannot be changed")
    for section, updates in changes.items():
        if section in {"properties", "fields"}:
            fields = dict(previous.get(section, {}))
            for name, definition in object_value(updates, section).items():
                fields[name] = merge_mapping(fields.get(name, {}), object_value(definition, name))
            result[section] = fields
        elif section == "dynamic" and (not isinstance(updates, (str, bool)) or updates not in {True, False, "true", "false", "strict"}):
            raise invalid("[dynamic] must be true, false, or strict")
        elif section == "type" and not isinstance(updates, str):
            raise invalid("[type] must be a string")
        else:
            result[section] = updates
    return result


def settings_values(source):
    settings = dict(object_value(source.get("settings", {}), "settings"))
    flat = {key.removeprefix("index."): value for key, value in settings.items() if key != "index"}
    flat.update(object_value(settings.get("index", {}), "settings.index"))
    return flat


def health_name(status):
    return {"green": 0, "yellow": 1, "red": 2}.get(status, -1)


class Store:
    def __init__(self, db_file):
        self.changed = threading.Condition(threading.RLock())
        self.db = sqlite3.connect(db_file, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS opensearch_indexes (
                name TEXT PRIMARY KEY, uuid TEXT NOT NULL, settings TEXT NOT NULL,
                mapping TEXT NOT NULL, next_seq INTEGER NOT NULL DEFAULT 0,
                visible_seq INTEGER NOT NULL DEFAULT -1, last_refresh REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS opensearch_aliases (
                name TEXT NOT NULL, idx TEXT NOT NULL REFERENCES opensearch_indexes(name) ON DELETE CASCADE,
                options TEXT NOT NULL, PRIMARY KEY (name, idx)
            );
            CREATE TABLE IF NOT EXISTS opensearch_documents (
                idx TEXT NOT NULL REFERENCES opensearch_indexes(name) ON DELETE CASCADE,
                id TEXT NOT NULL, source TEXT, version INTEGER NOT NULL, seq INTEGER NOT NULL,
                PRIMARY KEY (idx, id)
            );
            CREATE TABLE IF NOT EXISTS opensearch_visible (
                idx TEXT NOT NULL REFERENCES opensearch_indexes(name) ON DELETE CASCADE,
                id TEXT NOT NULL, source TEXT NOT NULL, version INTEGER NOT NULL, seq INTEGER NOT NULL,
                PRIMARY KEY (idx, id)
            );
        """)

    def index(self, name):
        return self.db.execute("SELECT * FROM opensearch_indexes WHERE name = ?", (name,)).fetchone()

    def aliases(self, *, name=None, index=None):
        return self.db.execute("""
            SELECT * FROM opensearch_aliases WHERE (? IS NULL OR name = ?) AND (? IS NULL OR idx = ?)
            ORDER BY name, idx
        """, (name, name, index, index)).fetchall()

    def add_alias(self, name, index, options):
        check_index_name(name)
        if self.index(name):
            raise Error(400, "invalid_alias_name_exception", f"an index already exists with the same name [{name}]")
        options = dict(object_value(options, "alias options"))
        if "is_write_index" in options:
            options["is_write_index"] = boolean(options["is_write_index"], "is_write_index")
        if "filter" in options:
            compile_query(options["filter"])
        self.db.execute("INSERT OR REPLACE INTO opensearch_aliases VALUES (?, ?, ?)", (name, index, json_text(options)))

    def check_write_aliases(self):
        writable = set()
        for alias in self.aliases():
            if json.loads(alias["options"]).get("is_write_index"):
                name = alias["name"]
                if name in writable:
                    raise invalid(f"alias [{name}] has more than one write index")
                writable.add(name)

    def resolve(self, pattern=None, *, allow_missing=False):
        indexes = [row[0] for row in self.db.execute("SELECT name FROM opensearch_indexes ORDER BY name")]
        aliases = self.aliases()
        targets = {}
        for selector in (pattern or "_all").split(","):
            if selector == "_all":
                selector = "*"
            matched = False
            for name in indexes:
                if fnmatch.fnmatchcase(name, selector):
                    targets[name] = None
                    matched = True
            for row in aliases:
                if fnmatch.fnmatchcase(row["name"], selector):
                    name = row["idx"]
                    query = json.loads(row["options"]).get("filter")
                    if query is None:
                        targets[name] = None
                    elif name not in targets or targets[name] is not None:
                        targets.setdefault(name, []).append(query)
                    matched = True
            if not matched and not allow_missing and not any(char in selector for char in "*?"):
                raise missing_index(selector)
        return targets

    def create_index(self, name, settings):
        check_index_name(name)
        if row := self.index(name):
            raise Error(400, "resource_already_exists_exception", f"index [{name}] already exists", index=name, index_uuid=row["uuid"])
        if self.aliases(name=name):
            raise invalid(f"an alias already exists with the same name [{name}]")
        config = settings_values(settings)
        if integer(config.get("number_of_shards", 1), "number_of_shards", 1) != 1:
            raise invalid("this server supports one shard per index")
        config["number_of_shards"] = "1"
        config["number_of_replicas"] = str(integer(config.get("number_of_replicas", 1), "number_of_replicas"))
        config["refresh_interval"] = str(config.get("refresh_interval", "1s"))
        refresh_seconds(config["refresh_interval"])
        mapping = merge_mapping({}, object_value(settings.get("mappings", {}), "mappings"))
        self.db.execute(
            "INSERT INTO opensearch_indexes (name, uuid, settings, mapping, last_refresh) VALUES (?, ?, ?, ?, ?)",
            (name, uuid.uuid4().hex, json_text(config), json_text(mapping), time.time()),
        )
        aliases = object_value(settings.get("aliases", {}), "aliases")
        for alias, options in aliases.items():
            self.add_alias(alias, name, options)
        self.check_write_aliases()

    def describe(self, names):
        result = {}
        for name in names:
            row = self.index(name)
            aliases = {alias["name"]: json.loads(alias["options"]) for alias in self.aliases(index=name)}
            settings = json.loads(row["settings"])
            settings.update(uuid=row["uuid"], provided_name=name)
            result[name] = {"aliases": aliases, "mappings": json.loads(row["mapping"]), "settings": {"index": settings}}
        return result

    def change_settings(self, names, changes):
        for name in names:
            config = json.loads(self.index(name)["settings"])
            for key, value in changes.items():
                if key == "number_of_shards":
                    raise invalid("[index.number_of_shards] cannot be changed")
                if key == "number_of_replicas":
                    value = str(integer(value if value is not None else 1, key))
                if key == "refresh_interval":
                    value = str(value if value is not None else "1s")
                    refresh_seconds(value)
                if value is None:
                    config.pop(key, None)
                else:
                    config[key] = value
            self.db.execute("UPDATE opensearch_indexes SET settings = ? WHERE name = ?", (json_text(config), name))

    def write_index(self, target, *, create=False, require_alias=False, alias_write=True):
        if aliases := self.aliases(name=target):
            if len(aliases) == 1 and (not alias_write or json.loads(aliases[0]["options"]).get("is_write_index") is not False):
                return aliases[0]["idx"]
            writable = [row["idx"] for row in aliases if json.loads(row["options"]).get("is_write_index")]
            if alias_write and len(writable) == 1:
                return writable[0]
            raise invalid(f"alias [{target}] does not resolve to a single {'write ' if alias_write else ''}index")
        if require_alias:
            raise Error(404, "index_not_found_exception", f"require_alias is true and [{target}] is not an alias", index=target)
        if self.index(target) is None:
            if not create:
                raise missing_index(target)
            self.create_index(target, {})
        return target

    def document(self, index, doc_id, realtime=True):
        table = "opensearch_documents" if realtime else "opensearch_visible"
        return self.db.execute(f"SELECT * FROM {table} WHERE idx = ? AND id = ?", (index, doc_id)).fetchone()

    def mutate(self, target, doc_id, action, body, params):
        require_alias = boolean(params.get("require_alias", False), "require_alias")
        conditional = "if_seq_no" in params or "if_primary_term" in params
        if conditional and not ("if_seq_no" in params and "if_primary_term" in params):
            raise invalid("if_seq_no and if_primary_term must be used together")
        if action == "create" and conditional:
            raise invalid("create does not support if_seq_no or if_primary_term")
        if "version" in params or "version_type" in params:
            raise invalid("external and explicit versions are unsupported")
        if not doc_id or len(doc_id.encode("utf-8", "replace")) > 512:
            raise invalid("document ID must be non-empty and at most 512 bytes")
        index = self.write_index(target, create=action != "delete", require_alias=require_alias)
        previous = self.document(index, doc_id)
        exists = previous is not None and previous["source"] is not None
        if conditional:
            seq = integer(params["if_seq_no"], "if_seq_no")
            term = integer(params["if_primary_term"], "if_primary_term", 1)
            if not exists or previous["seq"] != seq or term != PRIMARY_TERM:
                raise Error(409, "version_conflict_engine_exception", f"[{doc_id}]: sequence number or primary term does not match", index=index)
        if action == "create" and exists:
            raise Error(409, "version_conflict_engine_exception", f"[{doc_id}]: document already exists", index=index)
        if action == "update":
            if "script" in body or "scripted_upsert" in body:
                raise invalid("scripted updates are unsupported")
            if "doc" not in body and "upsert" not in body:
                raise invalid("an update needs a document or upsert")
            changes = object_value(body.get("doc", {}), "doc")
            if exists:
                source = merge_objects(json.loads(previous["source"]), changes)
                if boolean(body.get("detect_noop", True), "detect_noop") and source == json.loads(previous["source"]):
                    return 200, self.write_result(previous, "noop"), index, previous["seq"]
            elif boolean(body.get("doc_as_upsert", False), "doc_as_upsert") and "doc" in body:
                source = changes
            elif "upsert" in body:
                source = object_value(body["upsert"], "upsert")
            else:
                raise Error(404, "document_missing_exception", f"[{doc_id}]: document missing", index=index)
        else:
            source = body
        row = self.index(index)
        if source is not None:
            mapping = json.loads(row["mapping"])
            update_dynamic_mapping(source, mapping)
            self.db.execute("UPDATE opensearch_indexes SET mapping = ? WHERE name = ?", (json_text(mapping), index))
        seq = row["next_seq"]
        version = previous["version"] + 1 if previous else 1
        self.db.execute("UPDATE opensearch_indexes SET next_seq = ? WHERE name = ?", (seq + 1, index))
        self.db.execute(
            "INSERT OR REPLACE INTO opensearch_documents VALUES (?, ?, ?, ?, ?)",
            (index, doc_id, None if source is None else json_text(source), version, seq),
        )
        if action == "delete":
            status, state = (200, "deleted") if exists else (404, "not_found")
        else:
            status, state = (200, "updated") if exists else (201, "created")
        result = self.write_result(self.document(index, doc_id), state)
        return status, result, index, seq

    @staticmethod
    def write_result(row, state):
        return {"_index": row["idx"], "_id": row["id"], "_version": row["version"], "result": state,
                "_shards": SHARDS, "_seq_no": row["seq"], "_primary_term": PRIMARY_TERM}

    def refresh(self, names):
        for name in names:
            row = self.index(name)
            if row is None or row["visible_seq"] == row["next_seq"] - 1:
                continue
            self.db.execute("DELETE FROM opensearch_visible WHERE idx = ?", (name,))
            self.db.execute("""
                INSERT INTO opensearch_visible
                SELECT * FROM opensearch_documents WHERE idx = ? AND source IS NOT NULL
            """, (name,))
            self.db.execute("UPDATE opensearch_indexes SET visible_seq = next_seq - 1, last_refresh = ? WHERE name = ?",
                            (time.time(), name))
        self.changed.notify_all()

    def periodic_refresh(self):
        with self.changed, self.db:
            now = time.time()
            due = []
            for row in self.db.execute("SELECT * FROM opensearch_indexes WHERE visible_seq < next_seq - 1"):
                interval = refresh_seconds(json.loads(row["settings"])["refresh_interval"])
                if interval is not None and now >= row["last_refresh"] + interval:
                    due.append(row["name"])
            if due:
                self.refresh(due)

    def wait_visible(self, indexes, timeout):
        deadline = time.monotonic() + timeout
        with self.changed:
            while any((row := self.index(name)) and row["uuid"] == index_uuid and row["visible_seq"] < seq
                      for name, (index_uuid, seq) in indexes.items()):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Error(408, "timeout_exception", "timed out waiting for a refresh")
                self.changed.wait(min(remaining, 0.25))


def split_tokens(value):
    return TOKENS.findall(value.lower())


def mapping_field(mapping, name):
    remaining = name
    for part in name.split("."):
        properties = mapping.get("properties", mapping.get("fields", {}))
        if remaining in properties:
            return properties[remaining]
        if part not in properties:
            return None
        mapping = properties[part]
        remaining = remaining[len(part) + 1:]
    return mapping


def raw_values(value, name):
    for item in flatten(value):
        if isinstance(item, dict):
            if name in item:
                yield from flatten(item[name])
            elif "." in name:
                head, rest = name.split(".", 1)
                yield from raw_values(item.get(head), rest)


def field_values(source, mapping, name):
    field = mapping_field(mapping, name)
    if field is None or field.get("index", True) is False and field.get("doc_values", True) is False:
        return ()
    values = list(raw_values(source, name))
    if not values and "." in name:
        parent, leaf = name.rsplit(".", 1)
        if leaf in (mapping_field(mapping, parent) or {}).get("fields", {}):
            values = list(raw_values(source, parent))
    result = []
    ignore_above = field.get("ignore_above")
    for value in values:
        try:
            typed = typed_value(value, field)
        except (ValueError, OverflowError):
            continue
        if isinstance(typed, str) and type(ignore_above) is int and len(typed) > ignore_above:
            continue
        if not isinstance(typed, (dict, list)):
            result.append(typed)
    return result


def should_count(value, size):
    if value is None:
        return None
    if isinstance(value, str) and re.fullmatch(r"-?\d+%", value):
        percent = int(value[:-1])
        count = abs(percent) * size // 100
        return max(0, size - count if percent < 0 else count)
    if type(value) is int or isinstance(value, str) and re.fullmatch(r"-?\d+", value):
        return max(0, int(value) if int(value) >= 0 else size + int(value))
    raise invalid("unsupported minimum_should_match expression")


def compile_bool(body):
    if set(body) - {"must", "filter", "must_not", "should", "minimum_should_match", "boost"}:
        raise invalid("unsupported bool parameter")

    def load(key):
        clauses = body.get(key, [])
        return [compile_query(clause) for clause in (clauses if isinstance(clauses, list) else [clauses])]

    must, filters, excluded, should = (load(name) for name in ("must", "filter", "must_not", "should"))
    required = should_count(body.get("minimum_should_match"), len(should))
    if required is None:
        required = 1 if should and not (must or filters) else 0

    def score(source, mapping, doc_id):
        for clause in filters:
            if clause(source, mapping, doc_id) is None:
                return None
        for clause in excluded:
            if clause(source, mapping, doc_id) is not None:
                return None
        total = 0.0
        for clause in must:
            match = clause(source, mapping, doc_id)
            if match is None:
                return None
            total += match
        matches = [match for clause in should if (match := clause(source, mapping, doc_id)) is not None]
        return total + sum(matches) if len(matches) >= required else None

    return score


def compile_query(query):
    query = object_value(query, "query")
    if len(query) != 1:
        raise invalid("each query clause must have one query type")
    kind, body = next(iter(query.items()))
    body = object_value(body, kind)
    if kind == "bool":
        return compile_bool(body)
    if kind == "match_all":
        return lambda _source, _mapping, _doc_id: 1.0
    if kind == "match_none":
        return lambda _source, _mapping, _doc_id: None
    if kind == "ids":
        values = {str(value) for value in list_value(body.get("values"), "values")}
        return lambda _source, _mapping, doc_id: 1.0 if doc_id in values else None
    if kind == "exists":
        name = body.get("field")
        if not isinstance(name, str):
            raise invalid("[exists] needs a field name")
        return lambda source, mapping, doc_id: 1.0 if name == "_id" or field_values(source, mapping, name) else None
    if kind not in {"term", "terms", "range", "prefix", "wildcard", "match", "match_phrase"}:
        raise Error(400, "parsing_exception", f"unknown query [{kind}]")
    fields = [name for name in body if name != "boost"]
    if len(fields) != 1:
        raise invalid(f"[{kind}] needs one field")
    name = fields[0]
    criteria = body[name]
    options = criteria if isinstance(criteria, dict) else {}
    if kind == "range":
        limits = [(key, value) for key, value in object_value(criteria, kind).items() if key in {"lt", "lte", "gt", "gte"}]
        if not limits:
            raise invalid("[range] needs one or more limits")
    elif kind == "terms":
        expected = list_value(criteria, kind)
    else:
        expected = options.get("query" if kind in {"match", "match_phrase"} else "value") if options else criteria
        if type(expected) not in {str, int, float, bool}:
            raise invalid(f"[{kind}] needs a scalar value")
        operator = options.get("operator", "or")
        if not isinstance(operator, str) or operator.lower() not in {"and", "or"}:
            raise invalid("[operator] must be 'and' or 'or'")
    if kind == "match" and "minimum_should_match" in options:
        should_count(options["minimum_should_match"], 1)

    def score(source, mapping, doc_id):
        field = {"type": "keyword"} if name == "_id" else mapping_field(mapping, name)
        if field is None:
            return None
        values = [doc_id] if name == "_id" else field_values(source, mapping, name)
        if not values:
            return None
        if kind in {"match", "match_phrase"} and field_type(field) == "text":
            query_tokens = split_tokens(str(expected))
            if not query_tokens:
                return 1.0 if options.get("zero_terms_query") == "all" else None
            candidates = [split_tokens(value) for value in values]
            if kind == "match_phrase":
                width = len(query_tokens)
                return 1.0 if any(tokens[start:start + width] == query_tokens
                                  for tokens in candidates for start in range(len(tokens) - width + 1)) else None
            distinct = set(query_tokens)
            combined = {term for tokens in candidates for term in tokens}
            matches = len(distinct & combined)
            required = should_count(options.get("minimum_should_match"), len(distinct))
            if required is None:
                required = len(distinct) if options.get("operator", "or").lower() == "and" else 1
            return float(matches) if matches >= max(1, required) else None
        if field_type(field) == "text" and kind in {"term", "terms", "prefix", "wildcard"}:
            values = [word for value in values for word in split_tokens(value)]
        try:
            if kind == "range":
                bounds = [(op, typed_value(limit, field)) for op, limit in limits]
                def matches_range(value):
                    checks = {"lt": lambda limit: value < limit, "lte": lambda limit: value <= limit,
                              "gt": lambda limit: value > limit, "gte": lambda limit: value >= limit}
                    return all(checks[op](limit) for op, limit in bounds)
                matches = any(matches_range(value) for value in values)
            elif kind == "terms":
                choices = [typed_value(value, field) for value in expected]
                matches = any(value in choices for value in values)
            elif kind in {"prefix", "wildcard"}:
                needle = str(expected)
                if options.get("case_insensitive"):
                    needle, values = needle.lower(), [str(value).lower() for value in values]
                if kind == "prefix":
                    matches = any(str(value).startswith(needle) for value in values)
                else:
                    matches = any(fnmatch.fnmatchcase(str(value), needle) for value in values)
            else:
                needle = typed_value(expected, field)
                if options.get("case_insensitive") and isinstance(needle, str):
                    needle, values = needle.lower(), [str(value).lower() for value in values]
                matches = needle in values
        except (ValueError, TypeError, OverflowError):
            matches = False
        return 1.0 if matches else None

    return score


def parse_sort(items):
    if items is None:
        return [("_score", "desc", "_last", None)]
    if not isinstance(items, list):
        items = [items]
    result = []
    for item in items:
        if isinstance(item, str):
            name, options = item, {}
        elif isinstance(item, dict) and len(item) == 1:
            name, options = next(iter(item.items()))
            if isinstance(options, str):
                options = {"order": options}
            object_value(options, "sort options")
        else:
            raise invalid("invalid sort field")
        order = options.get("order", "desc" if name == "_score" else "asc")
        mode = options.get("mode")
        if order not in ("asc", "desc") or mode not in (None, "min", "max"):
            raise invalid("unsupported sort order or mode")
        result.append((name, order, options.get("missing", "_last"), mode))
    return result


def sort_values(hit, mapping, sort):
    values = []
    for name, order, missing, mode in sort:
        if name == "_doc":
            value = hit["_seq_no"]
        elif name in {"_id", "_index", "_score"}:
            value = hit[name]
        else:
            parts = field_values(hit["_source"], mapping, name)
            use_max = mode == "max" or mode is None and order == "desc"
            value = (max(parts) if use_max else min(parts)) if parts else None
        values.append(missing if value is None and missing not in ("_first", "_last") else value)
    return values


def compare_sort(left, right, sort):
    for a, b, (_, order, missing, _) in zip(left, right, sort):
        if a == b:
            continue
        if a is None or b is None:
            result = 1 if a is None else -1
            return -result if missing == "_first" else result
        try:
            result = -1 if a < b else 1
        except TypeError:
            result = -1 if str(a) < str(b) else 1 if str(a) > str(b) else 0
        if result:
            return result if order == "asc" else -result
    return 0


def project_source(source, options):
    if options is True or options is None:
        return source
    if options is False:
        return None
    if isinstance(options, (str, list)):
        options = {"includes": options}
    options = object_value(options, "_source")

    def patterns(key):
        value = options.get(key, options.get(key.removesuffix("s"), []))
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise invalid(f"invalid _source {key}")
        return value

    includes, excludes = patterns("includes"), patterns("excludes")
    removed = object()

    def select(value, path, selected):
        if path and any(fnmatch.fnmatchcase(path, pattern) for pattern in excludes):
            return removed
        selected = selected or (bool(path) and any(fnmatch.fnmatchcase(path, pattern) for pattern in includes))
        if isinstance(value, dict):
            result = {}
            for key, child in value.items():
                filtered = select(child, f"{path}.{key}" if path else key, selected)
                if filtered is not removed:
                    result[key] = filtered
            return result if result or selected or not path else removed
        if isinstance(value, list):
            result = [child for item in value if (child := select(item, path, selected)) is not removed]
            return result if result or selected else removed
        return value if selected else removed

    return select(source, "", not includes)


def search_aggregations(specifications, documents):
    result = {}
    for name, spec in object_value(specifications, "aggs").items():
        spec = object_value(spec, name)
        fields = [key for key in spec if key not in {"aggs", "aggregations", "meta"}]
        if len(fields) != 1:
            raise invalid("each aggregation must have one aggregation type")
        kind = fields[0]
        options = object_value(spec[kind], kind)
        field = options.get("field")
        if kind == "filter":
            query = compile_query(options)
            selected = [(hit, mapping) for hit, mapping in documents if query(hit["_source"], mapping, hit["_id"]) is not None]
            result[name] = {"doc_count": len(selected), **search_aggregations(spec.get("aggs", spec.get("aggregations", {})), selected)}
        elif not isinstance(field, str):
            raise invalid(f"[{kind}] aggregation needs a field name")
        elif kind == "terms":
            size = integer(options.get("size", 10), "size", 1)
            grouped = {}
            for hit, mapping in documents:
                for value in set(field_values(hit["_source"], mapping, field)):
                    grouped.setdefault(value, []).append((hit, mapping))
            buckets = sorted(grouped.items(), key=lambda item: (-len(item[1]), str(item[0])))
            result[name] = {"doc_count_error_upper_bound": 0, "sum_other_doc_count": sum(len(group) for _, group in buckets[size:]),
                            "buckets": [{"key": key, "doc_count": len(group),
                                         **search_aggregations(spec.get("aggs", spec.get("aggregations", {})), group)}
                                        for key, group in buckets[:size]]}
        elif kind in {"min", "max", "sum", "avg", "stats", "cardinality", "value_count"}:
            values = [value for hit, mapping in documents for value in field_values(hit["_source"], mapping, field)]
            if kind == "value_count":
                aggregate = {"value": len(values)}
            elif kind == "cardinality":
                aggregate = {"value": len(set(values))}
            else:
                if any(type(value) not in {int, float} for value in values):
                    raise invalid(f"[{kind}] aggregation needs a numeric field")
                total, count = sum(values), len(values)
                functions = {"min": min(values, default=None), "max": max(values, default=None),
                             "sum": total, "avg": total / count if count else None}
                aggregate = {"count": count, **functions} if kind == "stats" else {"value": functions[kind]}
            result[name] = aggregate
        else:
            raise invalid(f"unsupported aggregation [{kind}]")
        if "meta" in spec:
            result[name]["meta"] = spec["meta"]
    return result


def search(store, targets, body, count_only=False):
    started = time.monotonic()
    query = compile_query(body.get("query", {"match_all": {}}))
    post_filter = compile_query(body["post_filter"]) if "post_filter" in body else None
    documents = []
    for index, filters in targets.items():
        mapping = json.loads(store.index(index)["mapping"])
        alias_filters = [compile_query(item) for item in filters] if filters else []
        for row in store.db.execute("SELECT * FROM opensearch_visible WHERE idx = ? ORDER BY id", (index,)):
            source = json.loads(row["source"])
            if alias_filters and not any(alias(source, mapping, row["id"]) is not None for alias in alias_filters):
                continue
            score = query(source, mapping, row["id"])
            if score is not None:
                hit = {"_index": index, "_id": row["id"], "_score": score, "_source": source,
                       "_seq_no": row["seq"], "_primary_term": PRIMARY_TERM, "_version": row["version"]}
                documents.append((hit, mapping))
    filtered = documents if post_filter is None else [(hit, mapping) for hit, mapping in documents
                if post_filter(hit["_source"], mapping, hit["_id"]) is not None]
    shards = {"total": len(targets), "successful": len(targets), "skipped": 0, "failed": 0}
    if count_only:
        return {"count": len(filtered), "_shards": shards}
    limit = integer(body.get("size", 10), "size")
    start = integer(body.get("from", 0), "from")
    sort = parse_sort(body.get("sort"))
    after = body.get("search_after")
    if after is not None and ("sort" not in body or len(list_value(after, "search_after")) != len(sort) or start != 0):
        raise invalid("search_after requires a sort value for each field and from=0")
    compute_score = boolean(body.get("track_scores", False), "track_scores") or any(key == "_score" for key, *_ in sort)
    requested_total = body.get("track_total_hits", 10_000)
    total = len(filtered)
    if requested_total is not True and requested_total is not False:
        total = min(total, integer(requested_total, "track_total_hits"))
    ranked = [(sort_values(hit, mapping, sort), hit) for hit, mapping in filtered]
    ranked.sort(key=cmp_to_key(lambda left, right: compare_sort(left[0], right[0], sort)))
    if after is not None:
        ranked = [(values, hit) for values, hit in ranked if compare_sort(values, after, sort) > 0]
    hits = []
    versions = boolean(body.get("version", False), "version")
    sequence = boolean(body.get("seq_no_primary_term", False), "seq_no_primary_term")
    for values, full_hit in ranked[start:start + limit]:
        hit = dict(full_hit)
        source = project_source(hit.pop("_source"), body.get("_source"))
        if source is not None:
            hit["_source"] = source
        if "sort" in body:
            hit["sort"] = values
        if not compute_score:
            hit["_score"] = None
        if not versions:
            hit.pop("_version")
        if not sequence:
            hit.pop("_seq_no")
            hit.pop("_primary_term")
        hits.append(hit)
    maximum = max((hit["_score"] for hit in hits), default=None) if compute_score else None
    response = {"took": 0, "timed_out": False, "_shards": shards, "hits": {"max_score": maximum, "hits": hits}}
    if requested_total is not False:
        response["hits"]["total"] = {"value": total, "relation": "eq" if total == len(filtered) else "gte"}
    if "aggs" in body or "aggregations" in body:
        response["aggregations"] = search_aggregations(body.get("aggs", body.get("aggregations")), documents)
    response["took"] = int((time.monotonic() - started) * 1000)
    return response


def refresh_option(params):
    option = params.get("refresh", "false")
    if option not in ("", "true", "false", "wait_for"):
        raise invalid("[refresh] must be true, false, or wait_for")
    return "true" if option == "" else option


def source_option(params, default=None):
    source = params.get("_source", default)
    if source == "true":
        source = True
    if source == "false":
        source = False
    if source is False:
        return False
    include = params.get("_source_includes")
    exclude = params.get("_source_excludes")
    if include is not None or exclude is not None:
        return {"includes": include.split(",") if include else [], "excludes": exclude.split(",") if exclude else []}
    return source.split(",") if isinstance(source, str) else source


def parse_bulk(data):
    if not data.endswith(b"\n"):
        raise invalid("the bulk request must end with a newline")
    lines = iter(data.split(b"\n")[:-1])
    for line in lines:
        if not line.strip():
            continue
        header = object_value(parse_json(line), "bulk action")
        if len(header) != 1:
            raise invalid("bulk action must have exactly one operation")
        action, options = next(iter(header.items()))
        if action not in {"index", "create", "update", "delete"}:
            raise invalid(f"unknown bulk action [{action}]")
        options = object_value(options, "bulk action metadata")
        if action == "delete":
            source = None
        else:
            source_line = next(lines, None)
            if source_line is None:
                raise invalid(f"bulk action [{action}] requires a source line")
            source = object_value(parse_json(source_line), "bulk source")
        yield action, options, source


class Handler(BaseHTTPRequestHandler):
    server_version = "jjap-opensearch"
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        if self.server.verbose:
            super().log_message(*args)

    @property
    def store(self):
        return self.server.store

    def read_body(self):
        values = self.headers.get_all("Content-Length") or ["0"]
        if self.headers.get("Transfer-Encoding") or len(values) != 1 or not re.fullmatch(r"[0-9]{1,18}", values[0]):
            self.close_connection = True
            raise invalid("unsupported transfer encoding or invalid content length")
        length = int(values[0])
        limit = self.server.max_body
        if length > limit:
            self.close_connection = True
            raise Error(413, "content_too_long_exception", "request body exceeds maximum size")
        body = self.rfile.read(length)
        if len(body) != length:
            self.close_connection = True
            raise invalid("incomplete request body")
        encoding = self.headers.get("Content-Encoding", "identity").lower()
        if encoding == "gzip":
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                    body = stream.read(limit + 1)
            except (OSError, EOFError, zlib.error):
                raise invalid("invalid gzip request body") from None
            if len(body) > limit:
                raise Error(413, "content_too_long_exception", "decompressed request body exceeds maximum size")
        elif encoding != "identity":
            raise Error(415, "illegal_argument_exception", "unsupported content encoding")
        return body

    def send_result(self, status, result):
        if isinstance(result, str):
            body, content_type = result.encode("utf-8"), "text/plain; charset=UTF-8"
        else:
            body, content_type = json_text(result).encode("utf-8"), "application/json; charset=UTF-8"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def handle_rest(self):
        try:
            url = urlsplit(self.path)
            path = [unquote(part, errors="strict") for part in url.path.strip("/").split("/") if part]
            params = {name: values[-1] for name, values in parse_qs(url.query, keep_blank_values=True).items()}
            raw = self.read_body()
            if len(path) in (1, 2) and path[-1] == "_bulk" and self.command in ("PUT", "POST"):
                body = list(parse_bulk(raw))
            else:
                body = object_value(parse_json(raw), "request body") if raw else {}
            timeout = 30.0
            if params.get("refresh") == "wait_for":
                timeout = refresh_seconds(params.get("timeout", "30s"))
                if timeout is None:
                    raise invalid("timeout must be a duration")
            with self.store.changed, self.store.db:
                self.store.db.execute("BEGIN")
                result, status, waiting = self.dispatch(path, params, body)
            if waiting:
                self.store.wait_visible(waiting, min(timeout, 3600))
            self.send_result(status, result)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            self.close_connection = True
        except Error as exc:
            self.send_result(exc.status, {"error": {"root_cause": [exc.body], **exc.body}, "status": exc.status})
        except (ValueError, RecursionError, TypeError, OverflowError):
            self.send_result(400, {"error": {"type": "illegal_argument_exception", "reason": "invalid request value"}, "status": 400})

    do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = handle_rest

    def dispatch(self, path, params, body):
        method = self.command
        read = method in ("GET", "HEAD")
        if not path and read:
            return self.info(), 200, None
        if path[:2] == ["_cluster", "health"] and len(path) in (2, 3) and read:
            return self.health(path[2] if len(path) == 3 else None, params), 200, None
        if path[:2] == ["_cat", "indices"] and len(path) in (2, 3) and read:
            records = self.cat_indexes(path[2] if len(path) == 3 else None)
            if params.get("format") == "json":
                return records, 200, None
            names = ["health", "status", "index", "uuid", "pri", "rep", "docs.count", "docs.deleted", "store.size", "pri.store.size"]
            if "h" in params:
                names = params["h"].split(",")
            lines = [" ".join(names)] if params.get("v") in ("", "true") else []
            lines.extend(" ".join(record.get(name, "") for name in names) for record in records)
            return "\n".join(lines + [""]), 200, None
        if len(path) in (1, 2) and path[-1] == "_bulk" and method in ("PUT", "POST"):
            result, wait = self.bulk(path[0] if len(path) == 2 else None, body, params)
            return result, 200, wait
        if len(path) in (1, 2) and path[-1] in {"_search", "_count", "_refresh", "_mget"}:
            target = path[0] if len(path) == 2 else None
            operation = path[-1]
            if operation == "_refresh" and (read or method == "POST"):
                names = self.targets(target, params)
                self.store.refresh(names)
                return {"_shards": {"total": len(names), "successful": len(names), "failed": 0}}, 200, None
            if operation == "_mget" and (read or method == "POST"):
                return self.multiget(target, params, body), 200, None
            if operation in {"_search", "_count"} and (read or method == "POST"):
                if "q" in params:
                    raise invalid("the query string query parameter is unsupported, use the JSON query DSL")
                body = dict(body)
                for option in ("size", "from", "track_total_hits", "track_scores", "version", "seq_no_primary_term"):
                    if option in params:
                        value = params[option]
                        body[option] = boolean(value, option) if value in ("true", "false") else value
                if "sort" in params:
                    body["sort"] = [{name: order or "asc"} for item in params["sort"].split(",")
                                    for name, _, order in [item.partition(":")]]
                body["_source"] = source_option(params, body.get("_source"))
                return search(self.store, self.targets(target, params), body, operation == "_count"), 200, None
        if path == ["_aliases"] and method == "POST":
            return self.change_aliases(body), 200, None
        if any(part in {"_alias", "_aliases"} for part in path[:2]):
            result, status = self.alias_route(path, body)
            return result, status, None
        if len(path) in (2, 3) and path[1] in {"_doc", "_create", "_update", "_source"}:
            target, endpoint = path[:2]
            doc_id = path[2] if len(path) == 3 else None
            if doc_id is not None and endpoint in {"_doc", "_source"} and read:
                result = self.get_document(target, doc_id, params)
                status = 200 if result.get("found") else 404
                if endpoint == "_source" and status == 200:
                    result = result.get("_source", {})
                return result, status, None
            action = None
            if endpoint == "_doc" and (method == "POST" or method == "PUT" and doc_id):
                action = params.get("op_type", "index" if doc_id else "create")
                if action not in {"create", "index"}:
                    raise invalid("[op_type] must be index or create")
                doc_id = doc_id or uuid.uuid4().hex
            elif doc_id and endpoint in {"_create", "_update"} and method in {"PUT", "POST"}:
                action = endpoint[1:]
            elif doc_id and endpoint == "_doc" and method == "DELETE":
                action, body = "delete", None
            if action:
                refresh = refresh_option(params)
                status, result, index, seq = self.store.mutate(target, doc_id, action, body, params)
                return result, status, self.finish_writes({index: seq}, refresh)
        if len(path) in (1, 2) and path[-1] in {"_settings", "_mapping"}:
            return self.index_details(path, params, body), 200, None
        if len(path) == 1 and not path[0].startswith("_"):
            target = path[0]
            if read:
                names = self.targets(target, params)
                if not names and method == "HEAD":
                    return {}, 404, None
                return self.store.describe(names), 200, None
            if method == "PUT":
                self.store.create_index(target, body)
                return {"acknowledged": True, "shards_acknowledged": True, "index": target}, 200, None
            if method == "DELETE":
                for name in target.split(","):
                    if self.store.index(name) is None:
                        raise missing_index(name)
                    self.store.db.execute("DELETE FROM opensearch_indexes WHERE name = ?", (name,))
                return {"acknowledged": True}, 200, None
        raise Error(404, "resource_not_found_exception", f"no handler found for [{self.path}] and method [{method}]")

    def targets(self, pattern, params):
        return self.store.resolve(pattern, allow_missing=boolean(params.get("ignore_unavailable", False), "ignore_unavailable"))

    def info(self):
        return {"name": "jjap", "cluster_name": "jjap-opensearch", "cluster_uuid": "jjap-opensearch",
                "version": {"distribution": "opensearch", "number": VERSION, "build_type": "tar", "build_hash": "jjap",
                            "build_date": "1970-01-01T00:00:00Z", "build_snapshot": False,
                            "lucene_version": "9.12.1", "minimum_wire_compatibility_version": "7.10.0",
                            "minimum_index_compatibility_version": "7.0.0"},
                "tagline": "The OpenSearch Project: https://opensearch.org/"}

    def health(self, target, params):
        indexes = self.store.describe(self.targets(target, params))
        replicas = sum(int(detail["settings"]["index"]["number_of_replicas"]) for detail in indexes.values())
        status = "yellow" if replicas else "green"
        desired = params.get("wait_for_status", "red")
        if health_name(desired) < 0:
            raise invalid("[wait_for_status] must be green, yellow, or red")
        active = len(indexes)
        result = {"cluster_name": "jjap-opensearch", "status": status, "timed_out": health_name(status) > health_name(desired),
                  "number_of_nodes": 1, "number_of_data_nodes": 1, "active_primary_shards": active,
                  "active_shards": active, "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": replicas,
                  "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0, "number_of_in_flight_fetch": 0,
                  "task_max_waiting_in_queue_millis": 0, "active_shards_percent_as_number": (100 * active / (active + replicas) if replicas else 100.0)}
        if params.get("level") == "indices":
            result["indices"] = {}
            for name, detail in indexes.items():
                count = int(detail["settings"]["index"]["number_of_replicas"])
                result["indices"][name] = {"status": "yellow" if count else "green", "number_of_shards": 1,
                    "number_of_replicas": count, "active_primary_shards": 1, "active_shards": 1,
                    "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": count}
        return result

    def cat_indexes(self, pattern):
        records = []
        for name in self.store.resolve(pattern):
            index = self.store.index(name)
            replicas = json.loads(index["settings"])["number_of_replicas"]
            count, size = self.store.db.execute("SELECT COUNT(*), COALESCE(SUM(LENGTH(source)), 0) FROM opensearch_visible WHERE idx = ?", (name,)).fetchone()
            records.append({"health": "green" if replicas == "0" else "yellow", "status": "open", "index": name, "uuid": index["uuid"],
                            "pri": "1", "rep": replicas, "docs.count": str(count), "docs.deleted": "0",
                            "store.size": f"{size}b", "pri.store.size": f"{size}b"})
        return records

    def index_details(self, path, params, body):
        method = self.command
        kind = path[-1][1:]
        names = self.targets(path[0] if len(path) == 2 else None, params)
        if method == "PUT":
            if kind == "mapping":
                for name in names:
                    field_mapping = merge_mapping(json.loads(self.store.index(name)["mapping"]), body)
                    self.store.db.execute("UPDATE opensearch_indexes SET mapping = ? WHERE name = ?", (json_text(field_mapping), name))
            else:
                self.store.change_settings(names, settings_values({"settings": body}))
            return {"acknowledged": True}
        if method in ("GET", "HEAD"):
            key = "mappings" if kind == "mapping" else "settings"
            return {name: {key: details[key]} for name, details in self.store.describe(names).items()}
        raise invalid(f"unsupported method for [{path[-1]}]")

    def change_aliases(self, body):
        for operation in list_value(body.get("actions"), "actions"):
            operation = object_value(operation, "action")
            if len(operation) != 1:
                raise invalid("alias action must have exactly one operation")
            kind, spec = next(iter(operation.items()))
            spec = dict(object_value(spec, kind))
            indexes = spec.pop("indices", None)
            if indexes is None:
                indexes = [spec.pop("index", None)]
            patterns = list_value(indexes, "indices")
            if not patterns or not all(isinstance(pattern, str) and pattern for pattern in patterns):
                raise invalid("alias action requires an index or indices")
            names = self.store.resolve(",".join(patterns))
            if kind == "remove_index":
                for name in names:
                    self.store.db.execute("DELETE FROM opensearch_indexes WHERE name = ?", (name,))
                continue
            aliases = spec.pop("aliases", None)
            if aliases is None:
                aliases = [spec.pop("alias", None)]
            if not list_value(aliases, "aliases") or not all(isinstance(alias, str) and alias for alias in aliases):
                raise invalid("alias action requires an alias or aliases")
            for name in names:
                for alias in aliases:
                    if kind == "add":
                        self.store.add_alias(alias, name, spec)
                    elif kind == "remove":
                        matching = [row["name"] for row in self.store.aliases(index=name) if fnmatch.fnmatchcase(row["name"], alias)]
                        if not matching and spec.get("must_exist") is True:
                            raise Error(404, "aliases_not_found_exception", f"aliases [{alias}] missing")
                        self.store.db.executemany("DELETE FROM opensearch_aliases WHERE name = ? AND idx = ?",
                                                  [(match, name) for match in matching])
                    else:
                        raise invalid(f"unknown alias action [{kind}]")
        self.store.check_write_aliases()
        return {"acknowledged": True}

    def alias_route(self, path, body):
        marker = 0 if path[0] in {"_alias", "_aliases"} else 1
        if len(path) not in (marker + 1, marker + 2) or path[marker] not in {"_alias", "_aliases"}:
            raise invalid("invalid alias path")
        pattern = path[0] if marker else None
        names = self.store.resolve(pattern)
        alias = path[-1] if len(path) == marker + 2 else None
        if self.command in ("GET", "HEAD"):
            selectors = (alias or "*").split(",")
            result = {}
            for name in names:
                matches = {row["name"]: json.loads(row["options"]) for row in self.store.aliases(index=name)
                           if any(fnmatch.fnmatchcase(row["name"], expression) for expression in selectors)}
                if matches or alias is None:
                    result[name] = {"aliases": matches}
            if not result and alias is not None:
                raise Error(404, "aliases_not_found_exception", f"aliases [{alias}] missing")
            return result, 200
        if pattern and alias and self.command in ("PUT", "POST", "DELETE"):
            kind = "remove" if self.command == "DELETE" else "add"
            spec = {**body, "index": pattern, "alias": alias}
            if kind == "remove":
                spec["must_exist"] = True
            return self.change_aliases({"actions": [{kind: spec}]}), 200
        raise invalid("alias modification requires an index and alias in the path")

    def get_document(self, target, doc_id, params):
        index = self.store.write_index(target, alias_write=False)
        realtime = boolean(params.get("realtime", True), "realtime")
        if boolean(params.get("refresh", False), "refresh"):
            self.store.refresh([index])
        row = self.store.document(index, doc_id, realtime)
        result = {"_index": index, "_id": doc_id, "found": row is not None and row["source"] is not None}
        if result["found"]:
            result.update(_version=row["version"], _seq_no=row["seq"], _primary_term=PRIMARY_TERM)
            source = project_source(json.loads(row["source"]), source_option(params))
            if source is not None:
                result["_source"] = source
        return result

    def multiget(self, default_index, params, body):
        docs = body.get("docs")
        if docs is None:
            docs = [{"_id": doc_id} for doc_id in list_value(body.get("ids"), "ids")]
        result = []
        for entry in list_value(docs, "docs"):
            entry = object_value(entry, "document")
            target, doc_id = entry.get("_index", default_index), entry.get("_id")
            if not isinstance(target, str) or type(doc_id) not in (str, int):
                raise invalid("each document requires an index and ID")
            try:
                options = {**params, "_source": entry["_source"]} if "_source" in entry else params
                result.append(self.get_document(target, str(doc_id), options))
            except Error as exc:
                result.append({"_index": target, "_id": str(doc_id), "error": exc.body})
        return {"docs": result}

    def finish_writes(self, affected, refresh):
        if refresh == "true":
            self.store.refresh(affected)
        if refresh == "wait_for":
            return {name: (self.store.index(name)["uuid"], seq) for name, seq in affected.items()}
        return None

    def bulk(self, default_index, items, params):
        started = time.monotonic()
        refresh = refresh_option(params)
        results, affected = [], {}
        has_errors = False
        for action, metadata, source in items:
            target = metadata.get("_index", default_index)
            doc_id = metadata.get("_id")
            try:
                self.store.db.execute("SAVEPOINT bulk_action")
                if not isinstance(target, str):
                    raise invalid("bulk action is missing an index")
                if doc_id is None and action in {"index", "create"}:
                    doc_id = uuid.uuid4().hex
                if type(doc_id) not in (str, int):
                    raise invalid("bulk action needs a document ID")
                keys = ("if_seq_no", "if_primary_term", "version", "version_type")
                options = {key: metadata[key] for key in keys if key in metadata}
                options["require_alias"] = metadata.get("_require_alias", metadata.get("require_alias", params.get("require_alias", False)))
                status, result, index, seq = self.store.mutate(target, str(doc_id), action, source, options)
                affected[index] = seq if index not in affected else max(seq, affected[index])
            except Error as exc:
                self.store.db.execute("ROLLBACK TO bulk_action")
                status = exc.status
                result = {"_index": exc.body.get("index", target), "_id": str(doc_id) if doc_id is not None else None, "error": exc.body}
                has_errors = True
            self.store.db.execute("RELEASE bulk_action")
            results.append({action: {**result, "status": status}})
        return {"took": int((time.monotonic() - started) * 1000), "errors": has_errors, "items": results}, self.finish_writes(affected, refresh)


class Server(ThreadingHTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(30)
        return connection, address

    def service_actions(self):
        self.store.periodic_refresh()


# Export as OpenSearch REST request bodies and replay them into a running cluster

SERVER_SETTINGS = ("uuid", "creation_date", "creation_date_string", "provided_name", "version", "history.uuid",
                   "resize.source", "routing.allocation.initial_recovery", "verified_before_close")
ALIAS_OPTIONS = {"filter", "routing", "index_routing", "search_routing", "is_write_index", "is_hidden"}


def warn(message):
    print(f"warning: {message}", file=sys.stderr)


def portable_settings(config, prefix=""):
    result = {}
    for key, value in config.items():
        path = prefix + key
        if any(path == name or path.startswith(name + ".") for name in SERVER_SETTINGS):
            continue
        result[key] = portable_settings(value, path + ".") if isinstance(value, dict) else value
    return result


class Snapshot:
    """One read transaction over a persistence file, opened read-only."""

    def __init__(self, filename):
        source = Path(filename).expanduser().resolve(strict=True)
        if not source.is_file():
            raise ValueError(f"Expected a SQLite persistence file: {source}")
        self.db = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute("BEGIN")
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            if not {"opensearch_indexes", "opensearch_aliases", "opensearch_documents", "opensearch_visible"} <= tables:
                raise ValueError("Expected an OpenSearch test server SQLite persistence file")
        except BaseException:
            self.db.close()
            raise

    def indexes(self):
        result = []
        for row in self.db.execute("SELECT name, settings, mapping FROM opensearch_indexes ORDER BY name").fetchall():
            name = row["name"]
            aliases = {}
            for alias in self.db.execute("SELECT name, options FROM opensearch_aliases WHERE idx = ? ORDER BY name", (name,)):
                options = json.loads(alias["options"])
                for key in sorted(set(options) - ALIAS_OPTIONS):
                    warn(f"alias [{alias['name']}] on [{name}]: option [{key}] is not an OpenSearch alias property; skipped")
                aliases[alias["name"]] = {key: value for key, value in options.items() if key in ALIAS_OPTIONS}
            mappings = json.loads(row["mapping"])
            if isinstance(mappings.get("_routing"), dict) and mappings["_routing"].get("required") in (True, "true"):
                warn(f"[{name}] requires _routing, but document routing values are not stored; OpenSearch will reject its documents")
            count, = self.db.execute("SELECT COUNT(*) FROM opensearch_documents WHERE idx = ? AND source IS NOT NULL", (name,)).fetchone()
            body = {"settings": {"index": portable_settings(json.loads(row["settings"]))}, "mappings": mappings, "aliases": aliases}
            result.append({"name": name, "body": body, "documents": count})
        return result

    def bulk_lines(self, index, preserve_versions):
        """Yield one action and source pair per live document, including writes not yet refreshed."""
        for doc_id, source, version in self.db.execute("""
            SELECT id, source, version FROM opensearch_documents WHERE idx = ? AND source IS NOT NULL ORDER BY seq
        """, (index,)):
            action = {"_index": index, "_id": doc_id}
            if preserve_versions:
                action.update(version=version, version_type="external")
            yield f"{json_text({'index': action})}\n{source}\n".encode("utf-8")


def export_opensearch(filename, destination, preserve_versions):
    snapshot = Snapshot(filename)
    try:
        indexes = snapshot.indexes()
        destination.mkdir(mode=0o700)
        manifest = {"format": "jjap-opensearch-export", "formatVersion": 1, "opensearchVersion": VERSION,
                    "preserveVersions": preserve_versions, "indexes": []}
        total = 0
        for position, index in enumerate(indexes, 1):
            stem = f"{position:04d}-{quote(index['name'], safe='')[:200]}"
            with (destination / f"{stem}.index.json").open("x", encoding="utf-8") as file:
                file.write(json.dumps(index["body"], indent=2, ensure_ascii=False, allow_nan=False) + "\n")
            count = 0
            with (destination / f"{stem}.bulk.ndjson").open("xb") as file:
                for lines in snapshot.bulk_lines(index["name"], preserve_versions):
                    file.write(lines)
                    count += 1
            total += count
            manifest["indexes"].append({"index": index["name"], "create": f"{stem}.index.json", "bulk": f"{stem}.bulk.ndjson",
                                        "documents": count, "aliases": list(index["body"]["aliases"])})
        with (destination / "manifest.json").open("x", encoding="utf-8") as file:
            file.write(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
        return len(indexes), total
    finally:
        snapshot.db.close()


def publish_directory(staged, destination):
    if os.name == "nt":
        staged.rename(destination)
        return
    destination.mkdir(mode=0o700)
    try:
        staged.replace(destination)
    except BaseException:
        destination.rmdir()
        raise


def export_main(argv):
    parser = argparse.ArgumentParser(prog="opensearch_server.py export",
                                     description="Write create-index bodies and _bulk files for a real OpenSearch cluster.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--preserve-versions", action="store_true",
                        help="carry each _version with version_type=external (the test server itself rejects these)")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".opensearch-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "export"
            indexes, documents = export_opensearch(args.db_file, staged, args.preserve_versions)
            publish_directory(staged, destination)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"OpenSearch export failed: {exc}\n")
    print(f"Exported {documents} documents across {indexes} indexes to {destination}")
    return 0


def error_text(payload):
    error = payload.get("error", payload) if isinstance(payload, dict) else payload
    if isinstance(error, dict):
        return f"{error.get('type', 'error')}: {error.get('reason', json_text(error)[:300])}"
    return str(error)[:300]


class Cluster:
    def __init__(self, url, headers, context, timeout, retries):
        self.url, self.headers, self.context, self.timeout, self.retries = url, headers, context, timeout, retries

    def call(self, method, path, body=None, content_type="application/json", compress=False):
        data = body if body is None or isinstance(body, bytes) else json_text(body).encode()
        headers = dict(self.headers)
        if data is not None:
            headers["Content-Type"] = content_type
            if compress:
                data = gzip.compress(data)
                headers["Content-Encoding"] = "gzip"
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout, context=self.context) as response:
                    status, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                status, raw = error.code, error.read()
            except (urllib.error.URLError, OSError, http.client.HTTPException) as error:
                if attempt == self.retries:
                    raise ConnectionError(f"{method} {self.url + path}: {getattr(error, 'reason', error)}") from None
                time.sleep(min(0.25 * 2 ** attempt, 4))
                continue
            if status in {429, 502, 503, 504} and attempt < self.retries:
                time.sleep(min(0.25 * 2 ** attempt, 4))
                continue
            try:
                return status, json.loads(raw) if raw else {}
            except ValueError:
                return status, {"error": raw[:300].decode("utf-8", "replace")}

    def bulk(self, lines, compress, failures):
        """Send one chunk; retry only items the cluster rejected with 429, and record every other item failure."""
        loaded = 0
        for attempt in range(self.retries + 1):
            status, payload = self.call("POST", "/_bulk", b"".join(lines), "application/x-ndjson", compress)
            items = payload.get("items") if isinstance(payload, dict) else None
            if status != 200 or not isinstance(items, list) or len(items) != len(lines):
                failures.append(f"bulk request of {len(lines)} documents: HTTP {status} {error_text(payload)}")
                return loaded
            retry = []
            for line, item in zip(lines, items):
                result = next(iter(item.values())) if isinstance(item, dict) and len(item) == 1 else {}
                code = result.get("status") if isinstance(result, dict) else None
                if code == 429 and attempt < self.retries:
                    retry.append(line)
                elif type(code) is int and 200 <= code < 300 and "error" not in result:
                    loaded += 1
                else:
                    failures.append(f"document [{result.get('_index')}][{result.get('_id')}]: HTTP {code} {error_text(result)}")
            if not retry:
                return loaded
            lines = retry
            time.sleep(min(0.25 * 2 ** attempt, 4))
        return loaded


def migrate_main(argv):
    parser = argparse.ArgumentParser(prog="opensearch_server.py migrate",
                                     description="Create the stored indexes in a running OpenSearch cluster and bulk-load their documents.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--target", required=True, help="cluster URL, for example https://localhost:9200")
    parser.add_argument("--username", help="HTTP basic authentication user")
    parser.add_argument("--password-file", type=Path, help="file containing the password for --username")
    parser.add_argument("--ca-file", type=Path, help="PEM CA bundle for verifying an https target")
    parser.add_argument("--index", action="extend", nargs="+", metavar="NAME", help="migrate only these indexes")
    parser.add_argument("--skip-existing", action="store_true",
                        help="leave indexes that already exist on the target untouched instead of refusing to start")
    parser.add_argument("--preserve-versions", action="store_true", help="send each _version with version_type=external")
    parser.add_argument("--chunk-docs", type=int, default=1000, help="documents per _bulk request (default: %(default)s)")
    parser.add_argument("--chunk-bytes", type=int, default=5 * 1024 * 1024, help="bytes per _bulk request (default: %(default)s)")
    parser.add_argument("--gzip", action="store_true", help="gzip _bulk request bodies")
    parser.add_argument("--retries", type=int, default=3, help="retries on 429, 502-504, or connection errors")
    parser.add_argument("--timeout", type=float, default=60, help="seconds per HTTP request")
    args = parser.parse_args(argv)
    target = args.target.rstrip("/")
    if urlsplit(target).scheme not in {"http", "https"}:
        parser.error("--target must be an http or https URL")
    if (args.username is None) != (args.password_file is None):
        parser.error("--username and --password-file must be used together")
    if args.chunk_docs < 1 or args.chunk_bytes < 1 or args.retries < 0 or args.timeout <= 0:
        parser.error("--chunk-docs, --chunk-bytes and --timeout must be positive and --retries not negative")
    failures, created, documents, aliases, aborted = [], [], 0, 0, None
    try:
        headers = {"Accept": "application/json"}
        if args.username is not None:
            password = args.password_file.expanduser().read_text(encoding="utf-8").rstrip("\r\n")
            credentials = base64.b64encode(f"{args.username}:{password}".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {credentials}"
        context = ssl.create_default_context(cafile=str(args.ca_file.expanduser())) if args.ca_file else None
        cluster = Cluster(target, headers, context, args.timeout, args.retries)
        snapshot = Snapshot(args.db_file)
        try:
            indexes = snapshot.indexes()
            if args.index:
                available = {index["name"] for index in indexes}
                if missing := sorted(set(args.index) - available):
                    raise ValueError(f"Index not found in the snapshot: {', '.join(missing)}")
                indexes = [index for index in indexes if index["name"] in args.index]
            existing = []
            for index in indexes:
                status, payload = cluster.call("HEAD", "/" + quote(index["name"], safe=""))
                if status not in {200, 404}:
                    raise ValueError(f"checking [{index['name']}] on the target returned HTTP {status} {error_text(payload)}")
                if status == 200:
                    existing.append(index["name"])
            if existing and not args.skip_existing:
                raise ValueError(f"the target already has {', '.join(existing)}; use --skip-existing to leave them untouched")
            for index in indexes:
                name, path = index["name"], "/" + quote(index["name"], safe="")
                if name in existing:
                    warn(f"[{name}] already exists on the target; skipped with its documents and aliases")
                    continue
                status, payload = cluster.call("PUT", path, {key: value for key, value in index["body"].items() if key != "aliases"})
                if status != 200:
                    failures.append(f"create index [{name}]: HTTP {status} {error_text(payload)}")
                    continue
                created.append(name)
                chunk, size = [], 0
                for lines in snapshot.bulk_lines(name, args.preserve_versions):
                    if chunk and (len(chunk) >= args.chunk_docs or size + len(lines) > args.chunk_bytes):
                        documents += cluster.bulk(chunk, args.gzip, failures)
                        chunk, size = [], 0
                    chunk.append(lines)
                    size += len(lines)
                if chunk:
                    documents += cluster.bulk(chunk, args.gzip, failures)
                if index["body"]["aliases"]:
                    actions = [{"add": {"index": name, "alias": alias, **options}} for alias, options in index["body"]["aliases"].items()]
                    status, payload = cluster.call("POST", "/_aliases", {"actions": actions})
                    if status == 200 and payload.get("acknowledged") is True:
                        aliases += len(actions)
                    else:
                        failures.append(f"aliases of [{name}]: HTTP {status} {error_text(payload)}")
            for name in created:
                status, payload = cluster.call("POST", f"/{quote(name, safe='')}/_refresh")
                if status != 200:
                    failures.append(f"refresh [{name}]: HTTP {status} {error_text(payload)}")
        finally:
            snapshot.db.close()
    except (OSError, ValueError, sqlite3.Error) as exc:
        aborted = exc
    for failure in failures[:50]:
        print(f"OpenSearch migrate: {failure}", file=sys.stderr)
    if len(failures) > 50:
        print(f"OpenSearch migrate: ... and {len(failures) - 50} more failures", file=sys.stderr)
    if aborted is not None:
        parser.exit(1, f"OpenSearch migrate failed: {aborted}\n")
    summary = f"{documents} documents into {len(created)} indexes with {aliases} aliases at {target}"
    if failures:
        parser.exit(1, f"OpenSearch migrate failed: {len(failures)} failures; migrated {summary}\n")
    print(f"Migrated {summary}")
    return 0


def serve_main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=9200, help="listen port, 0 chooses a free port (default: %(default)s)")
    parser.add_argument("--db-file", type=Path, help="SQLite path for persistent indexes and documents")
    parser.add_argument("--max-body", type=int, default=32 * 1024 * 1024, help="maximum request bytes")
    parser.add_argument("--verbose", action="store_true", help="log HTTP requests")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535 or args.max_body <= 0:
        parser.error("--port or --max-body is out of range")
    try:
        store = Store(args.db_file or ":memory:")
        try:
            with Server((args.host, args.port), Handler) as server:
                server.store = store
                server.max_body = args.max_body
                server.verbose = args.verbose
                print(f"OpenSearch test server listening on http://{args.host}:{server.server_port}", flush=True)
                try:
                    server.serve_forever(poll_interval=0.1)
                except KeyboardInterrupt:
                    pass
        finally:
            with store.changed:
                store.db.close()
    except (OSError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["export"]:
        return export_main(argv[1:])
    if argv[:1] == ["migrate"]:
        return migrate_main(argv[1:])
    return serve_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
