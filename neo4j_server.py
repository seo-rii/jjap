#!/usr/bin/env python3
"""A single-node Neo4j-compatible test server for Python 3.10+.

Run: python3 neo4j_server.py --port 7687
Connect over bolt://127.0.0.1:7687 or neo4j://127.0.0.1:7687.
Data is in memory unless --db-file graph.sqlite3 is supplied. If
--password-file is supplied, use basic authentication with --username
(neo4j by default). Use --tls-cert and --tls-key for TLS connections.

Protocol references: https://neo4j.com/docs/bolt/current/
Cypher reference: https://neo4j.com/docs/cypher-manual/current/
"""

from __future__ import annotations

import argparse
import copy
import datetime
import hmac
import itertools
import math
import os
import re
import signal
import socket
import socketserver
import sqlite3
import ssl
import struct
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar
from zoneinfo import ZoneInfo

MAX_MESSAGE = 32 * 1024 * 1024
SERVER_AGENT = "Neo4j/5.26.0"
VERSIONS = ((5, 4), (5, 3), (5, 2), (5, 1), (5, 0), (4, 4))
SYNTAX = "Neo.ClientError.Statement.SyntaxError"
TYPE_ERROR = "Neo.ClientError.Statement.TypeError"
CONSTRAINT = "Neo.ClientError.Schema.ConstraintValidationFailed"


class ProtocolError(Exception):
    pass


class CypherError(Exception):
    def __init__(self, message, code=SYNTAX):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Structure:
    tag: int
    fields: tuple


def size_header(size, tiny, markers):
    if tiny is not None and size < 16:
        return bytes((tiny + size,))
    for marker, fmt, maximum in zip(markers, ("!B", "!H", "!I"), (255, 65535, 4294967295)):
        if size <= maximum:
            return bytes((marker,)) + struct.pack(fmt, size)
    raise ProtocolError("PackStream value is too large")


def pack(value):
    if value is None:
        return b"\xc0"
    if value is False:
        return b"\xc2"
    if value is True:
        return b"\xc3"
    if isinstance(value, int):
        if -16 <= value <= 127:
            return bytes((value & 255,))
        for marker, bits, fmt in ((0xC8, 8, "!b"), (0xC9, 16, "!h"),
                                  (0xCA, 32, "!i"), (0xCB, 64, "!q")):
            if -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
                return bytes((marker,)) + struct.pack(fmt, value)
        raise ProtocolError("integer outside signed 64-bit range")
    if isinstance(value, float):
        return b"\xc1" + struct.pack("!d", value)
    if isinstance(value, bytes):
        return size_header(len(value), None, (0xCC, 0xCD, 0xCE)) + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return size_header(len(encoded), 0x80, (0xD0, 0xD1, 0xD2)) + encoded
    if isinstance(value, (list, tuple)):
        return size_header(len(value), 0x90, (0xD4, 0xD5, 0xD6)) + b"".join(map(pack, value))
    if isinstance(value, dict):
        body = b"".join(pack(key) + pack(item) for key, item in value.items())
        return size_header(len(value), 0xA0, (0xD8, 0xD9, 0xDA)) + body
    if isinstance(value, Structure) and len(value.fields) < 16:
        return bytes((0xB0 + len(value.fields), value.tag)) + b"".join(map(pack, value.fields))
    raise ProtocolError(f"Unsupported PackStream type {type(value).__name__}")


class Decoder:
    SIZED: ClassVar[dict] = {
        marker + index: (tiny, fmt)
        for marker, tiny in ((0xCC, 0xC0), (0xD0, 0x80), (0xD4, 0x90), (0xD8, 0xA0))
        for index, fmt in enumerate(("!B", "!H", "!I"))
    }

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read(self, count):
        end = self.pos + count
        if end > len(self.data):
            raise ProtocolError("Truncated PackStream value")
        data = self.data[self.pos:end]
        self.pos = end
        return data

    def number(self, fmt):
        return struct.unpack(fmt, self.read(struct.calcsize(fmt)))[0]

    def value(self, depth=0):
        if depth > 64:
            raise ProtocolError("PackStream nesting limit exceeded")
        marker = self.number("!B")
        if marker <= 127:
            return marker
        if marker >= 240:
            return marker - 256
        if marker == 0xC0:
            return None
        if marker == 0xC1:
            return self.number("!d")
        if marker in (0xC2, 0xC3):
            return marker == 0xC3
        if 0xC8 <= marker <= 0xCB:
            return self.number(("!b", "!h", "!i", "!q")[marker - 0xC8])
        kind, size = marker & 0xF0, marker & 15
        if marker in self.SIZED:
            kind, fmt = self.SIZED[marker]
            size = self.number(fmt)
        if kind == 0xC0 and marker in self.SIZED:
            return self.read(size)
        if kind == 0x80:
            return self.read(size).decode("utf-8")
        if kind == 0x90:
            return [self.value(depth + 1) for _ in range(size)]
        if kind == 0xA0:
            result = {}
            for _ in range(size):
                key = self.value(depth + 1)
                if not isinstance(key, str) or key in result:
                    raise ProtocolError("Invalid PackStream map key")
                result[key] = self.value(depth + 1)
            return result
        if kind == 0xB0:
            tag = self.number("!B")
            return Structure(tag, tuple(self.value(depth + 1) for _ in range(size)))
        raise ProtocolError(f"Unsupported PackStream marker 0x{marker:02x}")

    @classmethod
    def decode(cls, data):
        decoder = cls(data)
        result = decoder.value()
        if decoder.pos != len(data):
            raise ProtocolError("Trailing bytes in PackStream message")
        return result


@dataclass(eq=False)
class Node:
    id: int
    labels: set
    props: dict

    def __hash__(self):
        return hash((Node, self.id))

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id

    @property
    def element_id(self):
        return f"4:jjap:{self.id}"


@dataclass(eq=False)
class Edge:
    id: int
    start: int
    end: int
    type: str
    props: dict

    def __hash__(self):
        return hash((Edge, self.id))

    def __eq__(self, other):
        return isinstance(other, Edge) and self.id == other.id

    @property
    def element_id(self):
        return f"5:jjap:{self.id}"


@dataclass
class PathValue:
    nodes: list
    edges: list


def wire_value(value, bolt_version):
    if isinstance(value, Node):
        fields = (value.id, sorted(value.labels), value.props)
        return Structure(0x4E, fields + (value.element_id,) if bolt_version[0] >= 5 else fields)
    if isinstance(value, Edge):
        fields = (value.id, value.start, value.end, value.type, value.props)
        if bolt_version[0] >= 5:
            fields += (value.element_id, f"4:jjap:{value.start}", f"4:jjap:{value.end}")
        return Structure(0x52, fields)
    if isinstance(value, PathValue):
        node_map = {}
        edge_map = {}
        sequences = []
        for index, node in enumerate(value.nodes):
            node_map.setdefault(node, len(node_map))
            if index:
                edge = value.edges[index - 1]
                edge_index = edge_map.setdefault(edge, len(edge_map) + 1)
                direction = 1 if edge.start == value.nodes[index - 1].id else -1
                sequences.extend((direction * edge_index, node_map[node]))
        nodes = [wire_value(node, bolt_version) for node in node_map]
        relationships = []
        for edge in edge_map:
            fields = (edge.id, edge.type, edge.props)
            if bolt_version[0] >= 5:
                fields += (edge.element_id,)
            relationships.append(Structure(0x72, fields))
        return Structure(0x50, (nodes, relationships, sequences))
    if isinstance(value, (list, tuple)):
        return [wire_value(item, bolt_version) for item in value]
    if isinstance(value, dict):
        return {key: wire_value(item, bolt_version) for key, item in value.items()}
    return value


@dataclass
class Graph:
    nodes: dict = field(default_factory=dict)
    edges: dict = field(default_factory=dict)
    schema: dict = field(default_factory=dict)
    next_node: int = 0
    next_edge: int = 0

    def add_node(self, labels, props):
        node = Node(self.next_node, set(labels), props)
        self.nodes[node.id] = node
        self.next_node += 1
        return node

    def add_edge(self, start, end, name, props):
        edge = Edge(self.next_edge, start.id, end.id, name, props)
        self.edges[edge.id] = edge
        self.next_edge += 1
        return edge

    def serialize(self):
        return pack({
            "nodes": [[node.id, sorted(node.labels), node.props] for node in self.nodes.values()],
            "edges": [[edge.id, edge.start, edge.end, edge.type, edge.props]
                      for edge in self.edges.values()],
            "schema": self.schema,
            "next_node": self.next_node,
            "next_edge": self.next_edge,
        })

    @classmethod
    def load(cls, data):
        raw = Decoder.decode(data)
        nodes = {node_id: Node(node_id, set(labels), props) for node_id, labels, props in raw["nodes"]}
        edges = {fields[0]: Edge(*fields) for fields in raw["edges"]}
        return cls(nodes, edges, raw["schema"], raw["next_node"], raw["next_edge"])

    def validate_constraints(self):
        for schema in self.schema.values():
            if schema["kind"] != "constraint":
                continue
            seen = set()
            for node in self.nodes.values():
                if schema["label"] not in node.labels:
                    continue
                values = [node.props.get(key) for key in schema["properties"]]
                if any(value is None for value in values):
                    if schema.get("required"):
                        raise CypherError(f"Constraint {schema['name']} requires a property", CONSTRAINT)
                    continue
                if schema.get("unique"):
                    key = freeze(values)
                    if key in seen:
                        raise CypherError(f"Constraint {schema['name']} requires unique values", CONSTRAINT)
                    seen.add(key)


class Store:
    def __init__(self, filename):
        self.lock = threading.Lock()
        self.connection = None
        self.graph = Graph()
        self.revision = 0
        if filename is not None:
            self.connection = sqlite3.connect(filename, check_same_thread=False)
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS jjap_neo4j "
                "(id INTEGER PRIMARY KEY CHECK (id=1), revision INTEGER, graph BLOB)"
            )
            self.reload()

    def reload(self):
        row = self.connection.execute("SELECT revision, graph FROM jjap_neo4j WHERE id=1").fetchone()
        if row is not None and row[0] != self.revision:
            self.graph = Graph.load(row[1])
            self.revision = row[0]

    def snapshot(self):
        with self.lock:
            if self.connection is not None:
                self.reload()
            return copy.deepcopy(self.graph), self.revision

    def commit(self, graph, revision, changed):
        with self.lock:
            if changed:
                if revision != self.revision:
                    raise CypherError("A concurrent write changed this graph. Retry the transaction.",
                                      "Neo.TransientError.Transaction.Conflict")
                next_revision = self.revision + 1
                if self.connection is not None:
                    with self.connection:
                        cursor = self.connection.execute(
                            "INSERT INTO jjap_neo4j (id, revision, graph) VALUES (1, ?, ?) "
                            "ON CONFLICT(id) DO UPDATE SET revision=excluded.revision, graph=excluded.graph "
                            "WHERE jjap_neo4j.revision=?",
                            (next_revision, graph.serialize(), revision),
                        )
                        if cursor.rowcount != 1:
                            raise CypherError("A concurrent write changed this graph. Retry the transaction.",
                                              "Neo.TransientError.Transaction.Conflict")
                self.graph, self.revision = graph, next_revision
            return f"jjap:{self.revision}"

    def close(self):
        with self.lock:
            if self.connection is not None:
                self.connection.close()


# Cypher lexer and parser. Expressions are tagged tuples, never Python code.
TOKEN = re.compile(
    r"(?P<skip>\s+|//[^\n]*|/\*.*?\*/)|"
    r"(?P<string>'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")|"
    r"(?P<escaped>`(?:[^`]|``)*`)|"
    r"(?P<number>0[xX][0-9a-fA-F]+|(?:\d+\.\d+|\.\d+|\d+)(?:[eE][+-]?\d+)?)|"
    r"(?P<name>[^\W\d]\w*|_\w*)|"
    r"(?P<symbol><>|!=|<=|>=|=~|\+=|->|<-|\.\.|\|\||[\[\](){},.*:;=+/%^|\-<>$])",
    re.DOTALL,
)
ESCAPE = re.compile(r"\\(?:u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}|.)", re.DOTALL)
ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f", "\\": "\\", "'": "'", '"': '"'}
PRECEDENCE = {
    "OR": 1, "XOR": 2, "AND": 3, "=": 5, "<>": 5, "!=": 5, "<": 5, ">": 5,
    "<=": 5, ">=": 5, "=~": 5, "IN": 5, "IS": 5, "CONTAINS": 5, "STARTS": 5,
    "ENDS": 5, ":": 5, "+": 6, "-": 6, "||": 6, "*": 7, "/": 7, "%": 7, "^": 8,
}
AGGREGATES = {"count", "collect", "sum", "avg", "min", "max", "stdev", "stdevp"}


def decode_string(token):
    def replace(match):
        escaped = match[0][1:]
        if len(escaped) > 1:
            return chr(int(escaped[1:], 16))
        if escaped not in ESCAPES:
            raise CypherError(f"Invalid string escape \\{escaped}")
        return ESCAPES[escaped]

    decoded = ESCAPE.sub(replace, token[1:-1])
    try:
        decoded.encode("utf-8")
    except UnicodeError as exc:
        raise CypherError("String contains an invalid Unicode scalar") from exc
    return decoded


@dataclass
class PatternNode:
    name: str | None
    labels: list
    props: tuple | None


@dataclass
class PatternEdge:
    name: str | None
    types: list
    props: tuple | None
    direction: int
    bounds: tuple | None


@dataclass
class Pattern:
    name: str | None
    start: PatternNode
    links: list

    def variables(self):
        names = [self.name, self.start.name]
        for edge, node in self.links:
            names.extend((edge.name, node.name))
        return [name for name in names if name is not None]


class Parser:
    def __init__(self, text):
        self.text = text
        self.tokens = []
        pos = 0
        while pos < len(text):
            token = TOKEN.match(text, pos)
            if token is None:
                raise CypherError(f"Unexpected character at offset {pos}")
            if token.lastgroup != "skip":
                self.tokens.append((token.lastgroup, token[0], pos, token.end()))
            pos = token.end()
        self.tokens.append(("end", "", pos, pos))
        self.pos = 0

    def ahead(self, distance=0):
        kind, value, _, _ = self.tokens[min(self.pos + distance, len(self.tokens) - 1)]
        return value.upper() if kind == "name" else value if kind == "symbol" else ""

    def take(self):
        token = self.tokens[self.pos]
        if token[0] == "end":
            raise CypherError("Unexpected end of query")
        self.pos += 1
        return token[:2]

    def accept(self, *values):
        if self.ahead() in values:
            return self.take()[1].upper()
        return None

    def require(self, value):
        if not self.accept(value):
            _, raw, start, _ = self.tokens[self.pos]
            raise CypherError(f"Expected {value} at offset {start}, found {raw!r}")

    def identifier(self):
        kind, value = self.take()
        if kind == "escaped":
            return value[1:-1].replace("``", "`")
        if kind != "name":
            raise CypherError(f"Expected a name, found {value!r}")
        return value

    def peek_name(self):
        return self.tokens[self.pos][0] in {"name", "escaped"}

    def raw(self, start):
        return self.text[self.tokens[start][2]:self.tokens[self.pos - 1][3]]

    def comma_list(self, read):
        values = [read()]
        while self.accept(","):
            values.append(read())
        return values

    def map(self):
        entries = []
        if not self.accept("}"):
            while True:
                kind = self.tokens[self.pos][0]
                key = decode_string(self.take()[1]) if kind == "string" else self.identifier()
                self.require(":")
                entries.append((key, self.expression()))
                if not self.accept(","):
                    break
            self.require("}")
        return ("map", entries)

    def map_projection(self, target):
        fields = []
        if self.accept("}"):
            return "map_projection", target, fields
        while True:
            if self.accept("."):
                if self.accept("*"):
                    fields.append((None, None))
                else:
                    name = self.identifier()
                    fields.append((name, ("property", target, name)))
            else:
                name = self.identifier()
                value = self.expression() if self.accept(":") else ("var", name)
                fields.append((name, value))
            if not self.accept(","):
                break
        self.require("}")
        return "map_projection", target, fields

    def expression(self, minimum=0):
        if (op := self.accept("NOT", "+", "-")) is not None:
            left = ("unary", op, self.expression(4 if op == "NOT" else 8))
        elif self.accept("("):
            left = self.expression()
            self.require(")")
        elif self.accept("["):
            if self.peek_name() and self.ahead(1) == "IN":
                name = self.identifier()
                self.require("IN")
                source = self.expression()
                where = self.expression() if self.accept("WHERE") else None
                value = self.expression() if self.accept("|") else ("var", name)
                left = ("comprehension", source, name, where, value)
                self.require("]")
            elif self.accept("]"):
                left = ("list", [])
            else:
                left = ("list", self.comma_list(self.expression))
                self.require("]")
        elif self.accept("{"):
            left = self.map()
        elif self.accept("$"):
            left = ("param", self.identifier())
        elif self.accept("CASE"):
            choice = None if self.ahead() == "WHEN" else self.expression()
            cases = []
            while self.accept("WHEN"):
                check = self.expression()
                self.require("THEN")
                cases.append((check, self.expression()))
            default = self.expression() if self.accept("ELSE") else ("literal", None)
            self.require("END")
            left = ("case", choice, cases, default)
        elif (constant := self.accept("TRUE", "FALSE", "NULL")) is not None:
            left = ("literal", {"TRUE": True, "FALSE": False, "NULL": None}[constant])
        else:
            kind, value = self.tokens[self.pos][:2]
            if kind == "number":
                self.take()
                if value.lower().startswith("0x"):
                    number = int(value, 16)
                elif any(marker in value.lower() for marker in ".e"):
                    number = float(value)
                else:
                    number = int(value)
                left = ("literal", number)
            elif kind == "string":
                self.take()
                left = ("literal", decode_string(value))
            else:
                name = self.identifier()
                if self.accept("("):
                    if name.lower() in {"all", "any", "none", "single"} and self.peek_name() and self.ahead(1) == "IN":
                        variable = self.identifier()
                        self.require("IN")
                        source = self.expression()
                        self.require("WHERE")
                        left = ("quantifier", name.lower(), source, variable, self.expression())
                    else:
                        distinct = bool(self.accept("DISTINCT"))
                        if self.accept("*"):
                            args = [("star",)]
                        else:
                            args = [] if self.ahead() == ")" else self.comma_list(self.expression)
                        left = ("call", name.lower(), args, distinct)
                    self.require(")")
                else:
                    left = ("var", name)

        while True:
            if left[0] == "var" and self.accept("{"):
                left = self.map_projection(left)
                continue
            if self.accept("."):
                left = ("property", left, self.identifier())
                continue
            if self.accept("["):
                start = None if self.ahead() == ".." else self.expression()
                if self.accept(".."):
                    end = None if self.ahead() == "]" else self.expression()
                    left = ("slice", left, start, end)
                else:
                    left = ("index", left, start)
                self.require("]")
                continue
            op = self.ahead()
            precedence = PRECEDENCE.get(op, -1)
            if precedence < minimum:
                return left
            self.take()
            if op == "IS":
                is_not = bool(self.accept("NOT"))
                self.require("NULL")
                left = ("isnull", left, is_not)
            elif op == ":":
                left = ("label", left, self.identifier())
            else:
                if op in {"STARTS", "ENDS"}:
                    self.require("WITH")
                left = ("binary", op, left, self.expression(precedence + (op != "^")))

    def props(self):
        if self.accept("{"):
            return self.map()
        if self.accept("$"):
            return ("param", self.identifier())
        return None

    def node(self):
        self.require("(")
        name = self.identifier() if self.peek_name() else None
        labels = []
        while self.accept(":"):
            labels.append(self.identifier())
        props = self.props()
        self.require(")")
        return PatternNode(name, labels, props)

    def hops(self):
        kind, value = self.tokens[self.pos][:2]
        if kind == "number" and value.isdecimal():
            self.take()
            return int(value)
        return None

    def pattern(self):
        name = None
        if self.peek_name() and self.ahead(1) == "=":
            name = self.identifier()
            self.require("=")
        start = self.node()
        links = []
        while (direction := self.accept("-", "<-")) is not None:
            edge_name, types, bounds, props = None, [], None, None
            if self.accept("["):
                edge_name = self.identifier() if self.peek_name() else None
                if self.accept(":"):
                    types.append(self.identifier())
                    while self.accept("|"):
                        self.accept(":")
                        types.append(self.identifier())
                if self.accept("*"):
                    low = self.hops()
                    if self.accept(".."):
                        high = self.hops()
                    else:
                        high = low
                    bounds = (1 if low is None else low, high)
                    if high is not None and high < bounds[0]:
                        raise CypherError("Invalid relationship length bounds")
                props = self.props()
                self.require("]")
            right = self.accept("->", "-")
            if right is None or (direction == "<-" and right == "->"):
                raise CypherError("Invalid relationship direction")
            edge = PatternEdge(edge_name, types, props, -1 if direction == "<-" else int(right == "->"), bounds)
            links.append((edge, self.node()))
        return Pattern(name, start, links)

    def assign(self):
        name = self.identifier()
        if self.accept("."):
            key = self.identifier()
            self.require("=")
            return ("property", name, key, self.expression())
        if self.accept(":"):
            labels = [self.identifier()]
            while self.accept(":"):
                labels.append(self.identifier())
            return ("labels", name, labels, None)
        op = self.accept("=", "+=")
        if op is None:
            raise CypherError("Expected a property, label, or map assignment")
        return (op, name, None, self.expression())

    def remove(self):
        name = self.identifier()
        if self.accept("."):
            return ("property", name, self.identifier(), ("literal", None))
        self.require(":")
        labels = [self.identifier()]
        while self.accept(":"):
            labels.append(self.identifier())
        return ("remove_labels", name, labels, None)

    def select_item(self):
        if self.accept("*"):
            return None, "*"
        start = self.pos
        expr = self.expression()
        name = self.raw(start)
        if self.accept("AS"):
            name = self.identifier()
        elif expr[0] == "var":
            name = expr[1]
        return expr, name

    def sort_item(self):
        expr = self.expression()
        descending = self.accept("DESC", "DESCENDING") is not None
        if not descending:
            self.accept("ASC", "ASCENDING")
        return expr, descending

    def projection(self):
        distinct = bool(self.accept("DISTINCT"))
        items = self.comma_list(self.select_item)
        order = []
        if self.accept("ORDER"):
            self.require("BY")
            order = self.comma_list(self.sort_item)
        skip = self.expression() if self.accept("SKIP", "OFFSET") else None
        limit = self.expression() if self.accept("LIMIT") else None
        return items, distinct, order, skip, limit

    def schema_create(self, kind):
        name = None
        conditional = False
        if self.ahead() not in {"FOR", "IF"}:
            name = self.identifier()
        if self.accept("IF"):
            self.require("NOT")
            self.require("EXISTS")
            conditional = True
        self.require("FOR")
        self.require("(")
        variable = self.identifier()
        self.require(":")
        label = self.identifier()
        self.require(")")
        self.require("REQUIRE" if kind == "constraint" else "ON")
        grouped = bool(self.accept("("))

        def key():
            if self.identifier() != variable:
                raise CypherError("Schema property must use the declared variable")
            self.require(".")
            return self.identifier()

        properties = self.comma_list(key) if grouped else [key()]
        if grouped:
            self.require(")")
        unique, required = False, False
        if kind == "constraint":
            self.require("IS")
            if self.accept("UNIQUE"):
                unique = True
            elif self.accept("NODE"):
                self.require("KEY")
                unique, required = True, True
            else:
                self.require("NOT")
                self.require("NULL")
                required = True
        if name is None:
            name = "_".join((kind, label, *properties))
        schema = {"name": name, "kind": kind, "label": label, "properties": properties,
                  "unique": unique, "required": required}
        return "schema_create", schema, conditional

    def yield_projection(self):
        projection = self.projection() if self.accept("YIELD") else None
        where = self.expression() if self.accept("WHERE") else None
        return projection, where

    def clause(self):
        if self.accept("OPTIONAL"):
            self.require("MATCH")
            patterns = self.comma_list(self.pattern)
            return "match", patterns, self.expression() if self.accept("WHERE") else None, True
        if self.accept("MATCH"):
            patterns = self.comma_list(self.pattern)
            return "match", patterns, self.expression() if self.accept("WHERE") else None, False
        if self.accept("UNWIND"):
            expr = self.expression()
            self.require("AS")
            return "unwind", expr, self.identifier()
        if self.accept("CREATE"):
            if kind := self.accept("CONSTRAINT", "INDEX"):
                return self.schema_create(kind.lower())
            if self.accept("RANGE"):
                self.require("INDEX")
                return self.schema_create("index")
            return "create", self.comma_list(self.pattern)
        if self.accept("MERGE"):
            patterns = self.comma_list(self.pattern)
            on_match, on_create = [], []
            while self.accept("ON"):
                target = self.accept("MATCH", "CREATE")
                if target is None:
                    raise CypherError("MERGE ON expects MATCH or CREATE")
                self.require("SET")
                (on_match if target == "MATCH" else on_create).extend(self.comma_list(self.assign))
            return "merge", patterns, on_match, on_create
        if self.accept("SET"):
            return "set", self.comma_list(self.assign)
        if self.accept("REMOVE"):
            return "set", self.comma_list(self.remove)
        detach = bool(self.accept("DETACH"))
        if detach or self.ahead() == "DELETE":
            self.require("DELETE")
            return "delete", self.comma_list(self.expression), detach
        if self.accept("WITH"):
            projection = self.projection()
            return "with", projection, self.expression() if self.accept("WHERE") else None
        if self.accept("RETURN"):
            return "return", self.projection()
        if self.accept("FOREACH"):
            self.require("(")
            name = self.identifier()
            self.require("IN")
            source = self.expression()
            self.require("|")
            body = [self.clause()]
            while self.ahead() != ")":
                body.append(self.clause())
            self.require(")")
            if any(clause[0] not in {"create", "merge", "set", "delete", "foreach"} for clause in body):
                raise CypherError("FOREACH accepts only update clauses")
            return "foreach", name, source, body
        if self.accept("CALL"):
            if self.ahead() in {"(", "{"}:
                imports = None
                if self.accept("("):
                    if self.accept("*"):
                        imports = "*"
                    else:
                        imports = [] if self.ahead() == ")" else self.comma_list(self.identifier)
                    self.require(")")
                self.require("{")
                branches, unions = self.branches("}")
                self.require("}")
                return "subquery", imports, branches, unions
            names = [self.identifier()]
            while self.accept("."):
                names.append(self.identifier())
            args = []
            if self.accept("(") and not self.accept(")"):
                args = self.comma_list(self.expression)
                self.require(")")
            return "call", ".".join(names).lower(), args, *self.yield_projection()
        if self.accept("SHOW"):
            self.accept("ALL")
            name = self.accept("DATABASES", "INDEXES", "CONSTRAINTS", "PROCEDURES", "FUNCTIONS")
            if name is None:
                raise CypherError("Unsupported SHOW command")
            return "show", name, *self.yield_projection()
        if self.accept("DROP"):
            kind = self.accept("CONSTRAINT", "INDEX")
            if kind is None:
                raise CypherError("Unsupported DROP command")
            name = self.identifier()
            conditional = bool(self.accept("IF"))
            if conditional:
                self.require("EXISTS")
            return "schema_drop", kind.lower(), name, conditional
        _, raw, start, _ = self.tokens[self.pos]
        raise CypherError(f"Unsupported or misplaced clause {raw!r} at offset {start}")

    def branches(self, closer):
        branches = []
        unions = []
        while True:
            clauses = [self.clause()]
            while self.tokens[self.pos][0] != "end" and self.ahead() not in {closer, "UNION"}:
                if clauses[-1][0] == "return":
                    raise CypherError("RETURN must be the last clause")
                clauses.append(self.clause())
            branches.append(clauses)
            if not self.accept("UNION"):
                break
            unions.append(bool(self.accept("ALL")))
            if not unions[-1]:
                self.accept("DISTINCT")
        return branches, unions

    def parse(self):
        explain = bool(self.accept("EXPLAIN"))
        profile = bool(self.accept("PROFILE"))
        branches, unions = self.branches(";")
        self.accept(";")
        if self.tokens[self.pos][0] != "end":
            raise CypherError("Only one statement per RUN is supported")
        return branches, unions, explain, profile


def freeze(value):
    """Hashable type-aware key for DISTINCT, grouping, and constraints."""
    if isinstance(value, bool):
        return bool, value
    if isinstance(value, dict):
        return dict, tuple((key, freeze(item)) for key, item in sorted(value.items()))
    if isinstance(value, list):
        return list, tuple(map(freeze, value))
    if isinstance(value, PathValue):
        return PathValue, tuple(value.nodes), tuple(value.edges)
    if isinstance(value, Structure):
        return Structure, value.tag, tuple(map(freeze, value.fields))
    return value


def expression_children(expr):
    match expr:
        case ("unary", _, item) | ("property", item, _) | ("label", item, _) | ("isnull", item, _):
            return [item]
        case ("binary", _, left, right) | ("index", left, right):
            return [left, right]
        case ("slice", item, start, end):
            return [value for value in (item, start, end) if value is not None]
        case ("call", _, args, _) | ("list", args):
            return args
        case ("map", entries):
            return [item for _, item in entries]
        case ("map_projection", target, entries):
            return [target, *(item for _, item in entries if item is not None)]
        case ("comprehension", source, _, where, value):
            return [source, value] if where is None else [source, where, value]
        case ("quantifier", _, source, _, where):
            return [source, where]
        case ("case", choice, pairs, default):
            return [expr for expr in (choice, *itertools.chain.from_iterable(pairs), default)
                    if expr is not None]
        case _:
            return []


def has_aggregate(expr):
    return ((expr[0] == "call" and expr[1] in AGGREGATES)
            or any(map(has_aggregate, expression_children(expr))))


def query_parameters(tree):
    if isinstance(tree, (PatternNode, PatternEdge)):
        yield from query_parameters(tree.props)
    elif isinstance(tree, Pattern):
        yield from query_parameters((tree.start, tree.links))
    elif isinstance(tree, tuple) and len(tree) == 2 and tree[0] == "param" and isinstance(tree[1], str):
        yield tree[1]
    elif isinstance(tree, (list, tuple)):
        for item in tree:
            yield from query_parameters(item)


def order_key(value):
    if value is None:
        return 10, 0
    if isinstance(value, bool):
        return 8, value
    if isinstance(value, (int, float)):
        return 9, value
    if isinstance(value, str):
        return 7, value
    if isinstance(value, bytes):
        return 6, value
    if isinstance(value, Structure):
        return 5, (value.tag, tuple(map(order_key, value.fields)))
    if isinstance(value, PathValue):
        return 4, (tuple(node.id for node in value.nodes), tuple(edge.id for edge in value.edges))
    if isinstance(value, list):
        return 3, tuple(map(order_key, value))
    if isinstance(value, Edge):
        return 2, value.id
    if isinstance(value, Node):
        return 1, value.id
    return 0, tuple((name, order_key(item)) for name, item in sorted(value.items()))


def props(value):
    if isinstance(value, (Node, Edge)):
        return value.props
    if isinstance(value, dict):
        return value
    raise CypherError("Expected a node, relationship, or map", TYPE_ERROR)


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def equality(left, right):
    if left is None or right is None:
        return None
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return and_values(equality(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return and_values(equality(left[key], right[key]) for key in left)
    return freeze(left) == freeze(right)


def and_values(values):
    saw_null = False
    for value in values:
        if value is False:
            return False
        saw_null |= value is None
    return None if saw_null else True


def predicate(value):
    if value is not None and not isinstance(value, bool):
        raise CypherError("Expected a boolean expression", TYPE_ERROR)
    return value


def compare(op, left, right):
    if op == "IN":
        if right is None:
            return None
        if not isinstance(right, list):
            raise CypherError("IN requires a list on the right side", TYPE_ERROR)
        compared = [equality(left, item) for item in right]
        return True if True in compared else None if None in compared else False
    if left is None or right is None:
        return None
    if op in {"=", "<>", "!="}:
        equal = equality(left, right)
        return equal if op == "=" or equal is None else not equal
    if op in {"<", ">", "<=", ">="}:
        if not (is_number(left) and is_number(right)) and type(left) is not type(right):
            return None
        a, b = order_key(left), order_key(right)
        return {"<": a < b, ">": a > b, "<=": a <= b, ">=": a >= b}[op]
    if op in {"CONTAINS", "STARTS", "ENDS", "=~"}:
        if not isinstance(left, str) or not isinstance(right, str):
            return None
        if op == "CONTAINS":
            return right in left
        if op == "STARTS":
            return left.startswith(right)
        if op == "ENDS":
            return left.endswith(right)
        return re.fullmatch(right, left) is not None
    if op in {"+", "||"}:
        if isinstance(left, list) or isinstance(right, list):
            if op == "||" and not (isinstance(left, list) and isinstance(right, list)):
                raise CypherError("|| requires two lists or two strings", TYPE_ERROR)
            return (left if isinstance(left, list) else [left]) + (right if isinstance(right, list) else [right])
        if isinstance(left, str) and isinstance(right, str):
            return left + right
        if op == "||":
            raise CypherError("|| requires two lists or two strings", TYPE_ERROR)
    if not (is_number(left) and is_number(right)):
        raise CypherError(f"{op} requires numeric operands", TYPE_ERROR)
    if op == "+":
        return left + right
    if op == "-":
        return left - right
    if op == "*":
        return left * right
    if op == "^":
        return math.pow(left, right)
    if isinstance(left, int) and isinstance(right, int):
        quotient = (abs(left) // abs(right)) * (-1 if (left < 0) != (right < 0) else 1)
        return quotient if op == "/" else left - quotient * right
    return left / right if op == "/" else math.fmod(left, right)


def temporal_function(name, args):
    if name in {"date", "datetime", "localdatetime"} and len(args) > 1:
        raise CypherError(f"{name}() takes at most one argument")
    if name == "date":
        value = datetime.date.today() if not args else datetime.date.fromisoformat(args[0])
        return Structure(0x44, ((value - datetime.date(1970, 1, 1)).days,))
    if name == "datetime":
        value = datetime.datetime.now(datetime.timezone.utc)
        if args:
            value = datetime.datetime.fromisoformat(args[0].replace("Z", "+00:00"))
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
        return Structure(0x49, (math.floor(value.timestamp()), value.microsecond * 1000,
                                int(value.utcoffset().total_seconds())))
    if name == "localdatetime":
        value = datetime.datetime.now() if not args else datetime.datetime.fromisoformat(args[0])
        epoch = datetime.datetime(1970, 1, 1)
        return Structure(0x64, (math.floor((value - epoch).total_seconds()), value.microsecond * 1000))
    point, = args
    crs = point.get("crs")
    geo = "latitude" in point or "longitude" in point
    z = point.get("height" if geo else "z")
    srid = point.get("srid", (4979 if z is not None else 4326) if geo else (9157 if z is not None else 7203))
    if crs is not None:
        srid = {"wgs-84": 4326, "wgs-84-3d": 4979, "cartesian": 7203, "cartesian-3d": 9157}[crs]
    x, y = (point["longitude"], point["latitude"]) if geo else (point["x"], point["y"])
    return Structure(0x58, (srid, float(x), float(y))) if z is None else Structure(
        0x59, (srid, float(x), float(y), float(z))
    )


def scalar_function(name, args):
    if name in {"date", "datetime", "localdatetime", "point"}:
        return None if None in args else temporal_function(name, args)
    if name == "timestamp":
        if args:
            raise CypherError("timestamp() does not take arguments")
        return time.time_ns() // 1_000_000
    if name == "range":
        if None in args:
            return None
        first, last, *step_arg = args
        if len(step_arg) > 1:
            raise CypherError("range() takes two or three arguments")
        step = step_arg[0] if step_arg else 1
        result = range(first, last + (1 if step > 0 else -1), step)
        if len(result) > 1_000_000:
            raise CypherError("range() exceeded the test server list limit")
        return list(result)

    arities = {
        "id": 1, "elementid": 1, "labels": 1, "type": 1, "properties": 1, "keys": 1,
        "head": 1, "last": 1, "size": 1, "length": 1, "nodes": 1, "relationships": 1,
        "exists": 1, "tostring": 1, "tointeger": 1, "tofloat": 1, "toboolean": 1,
        "toupper": 1, "tolower": 1, "trim": 1, "ltrim": 1, "rtrim": 1, "reverse": 1,
        "split": 2, "left": 2, "right": 2, "replace": 3, "abs": 1, "ceil": 1, "floor": 1,
        "round": 1, "sqrt": 1,
    }
    if name not in arities and name != "substring":
        raise CypherError(f"Unknown function {name!r}")
    if (name == "substring" and len(args) not in {2, 3}) or (name != "substring" and len(args) != arities[name]):
        raise CypherError(f"Incorrect number of arguments for {name}()")
    if name == "exists":
        return args[0] is not None
    if None in args:
        return None
    item = args[0]
    if name == "id":
        return item.id
    if name == "elementid":
        return item.element_id
    if name == "labels":
        return sorted(item.labels)
    if name == "type":
        return item.type
    if name == "properties":
        return props(item).copy()
    if name == "keys":
        return list(props(item))
    if name == "head":
        return item[0] if item else None
    if name == "last":
        return item[-1] if item else None
    if name == "size":
        return len(item)
    if name == "length":
        return len(item.edges)
    if name == "nodes":
        return item.nodes.copy()
    if name == "relationships":
        return item.edges.copy()
    if name == "tostring":
        return str(item).lower() if isinstance(item, bool) else str(item)
    if name in {"tointeger", "tofloat", "toboolean"}:
        if name == "toboolean":
            if isinstance(item, str):
                return {"true": True, "false": False}.get(item.lower())
            if isinstance(item, (bool, int)):
                return bool(item)
            raise CypherError("Cannot convert value to boolean", TYPE_ERROR)
        try:
            return (int if name == "tointeger" else float)(item)
        except ValueError:
            return None
    if name == "substring":
        start = args[1]
        return item[start:] if len(args) == 2 else item[start:start + args[2]]
    if name == "left":
        return item[:args[1]]
    if name == "right":
        return item[-args[1]:] if args[1] else ""
    if name == "replace":
        return item.replace(*args[1:])
    if name == "split":
        return item.split(args[1]) if args[1] else list(item)
    if name == "reverse":
        return item[::-1]
    conversions = {
        "toupper": str.upper, "tolower": str.lower, "trim": str.strip,
        "ltrim": str.lstrip, "rtrim": str.rstrip, "abs": abs, "ceil": math.ceil,
        "floor": math.floor, "round": round, "sqrt": math.sqrt,
    }
    return conversions[name](item)


def aggregate(name, exprs, distinct, params, rows):
    if len(exprs) != 1:
        raise CypherError(f"{name}() expects one argument")
    expr, = exprs
    if expr[0] == "star":
        if name != "count" or distinct:
            raise CypherError(f"Incorrect {name}(*)")
        return len(rows)
    values = [evaluate(expr, row, params) for row in rows]
    values = [value for value in values if value is not None]
    if distinct:
        distinct_values = {}
        for value in values:
            distinct_values.setdefault(freeze(value), value)
        values = list(distinct_values.values())
    if name == "count":
        return len(values)
    if name == "collect":
        return values
    if name == "min":
        return min(values, default=None, key=order_key)
    if name == "max":
        return max(values, default=None, key=order_key)
    if not all(map(is_number, values)):
        raise CypherError(f"{name}() requires numeric values", TYPE_ERROR)
    if name == "sum":
        return sum(values)
    if name == "avg":
        return sum(values) / len(values) if values else None
    divisor = len(values) - (1 if name == "stdev" else 0)
    if divisor <= 0:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / divisor)


def evaluate(expr, row, params, group=None):
    def ev(item):
        return evaluate(item, row, params, group)

    match expr:
        case ("literal", value):
            return value
        case ("var", name):
            if name in row:
                return row[name]
            raise CypherError(f"Variable {name!r} is not defined")
        case ("param", name):
            if name in params:
                return params[name]
            raise CypherError("Expected parameter $" + name, "Neo.ClientError.Statement.ParameterMissing")
        case ("property", item, key):
            target = ev(item)
            return None if target is None else props(target).get(key)
        case ("index", item, index):
            target, key = ev(item), ev(index)
            if target is None or key is None:
                return None
            if isinstance(target, (dict, Node, Edge)):
                return props(target).get(key)
            if type(key) is not int:
                raise CypherError("A list index must be an integer", TYPE_ERROR)
            try:
                return target[key]
            except IndexError:
                return None
        case ("slice", item, start, end):
            target = ev(item)
            if target is None:
                return None
            return target[ev(start) if start is not None else None:ev(end) if end is not None else None]
        case ("isnull", item, is_not):
            return (ev(item) is None) != is_not
        case ("label", item, label):
            target = ev(item)
            if target is None:
                return None
            if not isinstance(target, Node):
                raise CypherError("Expected a node in the label predicate", TYPE_ERROR)
            return label in target.labels
        case ("unary", op, item):
            result = ev(item)
            if result is None:
                return None
            if op == "NOT":
                return not predicate(result)
            if not is_number(result):
                raise CypherError("Expected a numeric operand", TYPE_ERROR)
            return result if op == "+" else -result
        case ("binary", op, left, right):
            a = ev(left)
            if op == "AND":
                return False if predicate(a) is False else and_values((a, predicate(ev(right))))
            if op == "OR":
                if predicate(a) is True:
                    return True
                b = predicate(ev(right))
                return b if a is False or b is True else None
            if op == "XOR":
                b = predicate(ev(right))
                return None if a is None or b is None else predicate(a) != b
            return compare(op, a, ev(right))
        case ("list", items):
            return list(map(ev, items))
        case ("map", entries):
            return {key: ev(item) for key, item in entries}
        case ("map_projection", item, entries):
            target = ev(item)
            if target is None:
                return None
            result = {}
            for key, value in entries:
                if key is None:
                    result.update(props(target))
                else:
                    result[key] = ev(value)
            return result
        case ("comprehension", source, variable, where, value):
            items = ev(source)
            if items is None:
                return None
            if not isinstance(items, list):
                raise CypherError("List comprehension requires a list", TYPE_ERROR)
            bound = (row | {variable: item} for item in items)
            return [evaluate(value, item, params, group) for item in bound
                    if where is None or predicate(evaluate(where, item, params, group)) is True]
        case ("quantifier", name, source, variable, where):
            items = ev(source)
            if items is None:
                return None
            if not isinstance(items, list):
                raise CypherError(f"{name}() requires a list", TYPE_ERROR)
            checks = [predicate(evaluate(where, row | {variable: item}, params, group)) for item in items]
            if name == "all":
                return and_values(checks)
            matches = checks.count(True)
            if name == "single":
                return False if matches > 1 else None if None in checks else matches == 1
            if matches:
                return name == "any"
            return None if None in checks else name == "none"
        case ("case", choice, pairs, default):
            a = ev(choice) if choice is not None else None
            for condition, result in pairs:
                b = ev(condition)
                if (predicate(b) if choice is None else equality(a, b)) is True:
                    return ev(result)
            return ev(default)
        case ("call", name, args, distinct):
            if name in AGGREGATES:
                if group is None:
                    raise CypherError(f"Aggregate {name}() is not valid here")
                return aggregate(name, args, distinct, params, group)
            if distinct or any(item[0] == "star" for item in args):
                raise CypherError(f"Invalid argument for {name}()")
            if name == "coalesce":
                for item in args:
                    value = ev(item)
                    if value is not None:
                        return value
                return None
            return scalar_function(name, list(map(ev, args)))
        case _:
            raise CypherError("Unsupported expression")


def map_value(expr, row, params):
    result = evaluate(expr, row, params)
    if not isinstance(result, dict):
        raise CypherError("Expected a property map", TYPE_ERROR)
    return result


def property_value(value):
    if isinstance(value, list):
        items = [property_value(item) for item in value if not isinstance(item, list)]
        kinds = {float if is_number(item) else type(item) for item in items}
        if len(kinds) <= 1 and len(items) == len(value):
            return items
    elif isinstance(value, (str, int, float, bool, bytes, Structure)):
        if isinstance(value, int) and not -(1 << 63) <= value < (1 << 63):
            raise CypherError("Integer outside signed 64-bit range", TYPE_ERROR)
        return value
    raise CypherError("Properties must be scalar values or homogeneous scalar lists", TYPE_ERROR)


def create_properties(expr, row, params, merging):
    if expr is None:
        return {}
    result = {}
    for key, value in map_value(expr, row, params).items():
        if value is not None:
            result[key] = property_value(value)
        elif merging:
            raise CypherError(f"Cannot MERGE with null property {key!r}", CONSTRAINT)
    return result


def match_props(entity, expr, row, params):
    if expr is None:
        return True
    return all(equality(entity.props.get(key), value) is True
               for key, value in map_value(expr, row, params).items())


def bind(row, name, value):
    if name is None:
        return row
    if name in row:
        return row if row[name] == value else None
    return dict(row, **{name: value})


def node_matches(node, pattern, row, params):
    if not isinstance(node, Node) or not set(pattern.labels).issubset(node.labels):
        return None
    bound = bind(row, pattern.name, node)
    if bound is not None and match_props(node, pattern.props, bound, params):
        return bound
    return None


def neighbors(graph, node, spec, row, params):
    for edge in graph.edges.values():
        if spec.types and edge.type not in spec.types:
            continue
        if spec.direction != -1 and edge.start == node.id:
            other = graph.nodes[edge.end]
        elif spec.direction != 1 and edge.end == node.id:
            other = graph.nodes[edge.start]
        else:
            continue
        if match_props(edge, spec.props, row, params):
            yield other, edge


def walks(graph, node, spec, row, params, used):
    minimum, maximum = spec.bounds or (1, 1)
    if maximum is None:
        maximum = len(graph.edges)
    stack = [(node, [], [], used)]
    while stack:
        last, nodes, edges, walked = stack.pop()
        if len(edges) >= minimum:
            yield last, nodes, edges, walked
        if len(edges) < maximum:
            for other, edge in reversed(list(neighbors(graph, last, spec, row, params))):
                if edge.id not in walked:
                    stack.append((other, nodes + [other], edges + [edge], walked | {edge.id}))


def match_pattern(graph, pattern, row, params):
    starts = [row[pattern.start.name]] if pattern.start.name in row else graph.nodes.values()
    for start in starts:
        first = node_matches(start, pattern.start, row, params)
        if first is None:
            continue
        stack = [(0, start, [start], [], set(), first)]
        while stack:
            index, last, nodes, edges, used, current = stack.pop()
            if index == len(pattern.links):
                out = current if pattern.name is None else bind(current, pattern.name, PathValue(nodes, edges))
                if out is not None:
                    yield out
                continue
            spec, node_spec = pattern.links[index]
            additions = []
            for end, new_nodes, new_edges, walked in walks(graph, last, spec, current, params, used):
                new_edge = new_edges[0] if spec.bounds is None else new_edges
                matched = bind(current, spec.name, new_edge)
                if matched is None:
                    continue
                matched = node_matches(end, node_spec, matched, params)
                if matched is not None:
                    additions.append((index + 1, end, nodes + new_nodes, edges + new_edges, walked, matched))
            stack.extend(reversed(additions))


def distinct_rows(items, values):
    seen = set()
    distinct = []
    for item in items:
        key = freeze(values(item))
        if key not in seen:
            seen.add(key)
            distinct.append(item)
    return distinct


def project(rows, scope, projection, params):
    items, distinct, order, skip, limit = projection
    expanded = []
    for expr, name in items:
        if expr is None:
            expanded.extend((("var", key), key) for key in scope)
        else:
            expanded.append((expr, name))
    columns = [name for _, name in expanded]
    if len(set(columns)) != len(columns):
        raise CypherError("A projection cannot contain duplicate field names")
    aggregate_flags = [has_aggregate(expr) for expr, _ in expanded]
    if any(aggregate_flags) or any(has_aggregate(expr) for expr, _ in order):
        grouped = {}
        grouping = [expr for (expr, _), aggregate_flag in zip(expanded, aggregate_flags) if not aggregate_flag]
        if not grouping:
            grouped[()] = rows
        else:
            for row in rows:
                key = tuple(freeze(evaluate(expr, row, params)) for expr in grouping)
                grouped.setdefault(key, []).append(row)
        groups = [(group[0] if group else {}, group) for group in grouped.values()]
    else:
        groups = [(row, None) for row in rows]
    projected = [
        ({name: evaluate(expr, row, params, group) for expr, name in expanded}, row, group)
        for row, group in groups
    ]
    if distinct:
        projected = distinct_rows(projected, lambda item: item[0])
    for expr, descending in reversed(order):
        projected.sort(key=lambda item: order_key(evaluate(expr, item[1] | item[0], params, item[2])),
                       reverse=descending)

    def page(expr, label):
        amount = evaluate(expr, {}, params)
        if type(amount) is not int or amount < 0:
            raise CypherError(f"{label} must be a nonnegative integer", TYPE_ERROR)
        return amount

    if skip is not None:
        projected = projected[page(skip, "SKIP"):]
    if limit is not None:
        projected = projected[:page(limit, "LIMIT")]
    return [value for value, _, _ in projected], columns


@dataclass
class Result:
    fields: list
    rows: list
    stats: dict
    mode: str
    plan: dict | None = None
    profile: dict | None = None


class Query:
    def __init__(self, graph, params, database):
        self.graph = graph
        self.params = params
        self.database = database
        self.stats = {}
        self.write_query = False

    def record(self, key, count=1):
        if count:
            self.stats[key] = self.stats.get(key, 0) + count

    def where(self, expr, row):
        return expr is None or predicate(evaluate(expr, row, self.params)) is True

    def match(self, patterns, rows, where, optional):
        result = []
        for row in rows:
            matched = [row]
            for pattern in patterns:
                matched = [candidate for item in matched
                           for candidate in match_pattern(self.graph, pattern, item, self.params)]
            matched = [candidate for candidate in matched if self.where(where, candidate)]
            if not matched and optional:
                missing = dict.fromkeys(name for pattern in patterns for name in pattern.variables())
                matched = [missing | row]
            result.extend(matched)
        return result

    def create(self, pattern, row, merging):
        copy_row = row.copy()
        graph = self.graph

        def create_node(spec):
            if spec.name in copy_row:
                node = copy_row[spec.name]
                if not isinstance(node, Node) or node.id not in graph.nodes:
                    raise CypherError(f"{spec.name!r} must refer to an existing node", TYPE_ERROR)
                if spec.labels or spec.props is not None:
                    raise CypherError(f"Cannot redeclare the labels or properties of {spec.name!r}")
                return node
            values = create_properties(spec.props, copy_row, self.params, merging)
            node = graph.add_node(spec.labels, values)
            self.record("nodes-created")
            self.record("labels-added", len(node.labels))
            self.record("properties-set", len(values))
            if spec.name is not None:
                copy_row[spec.name] = node
            return node

        nodes = [create_node(pattern.start)]
        edges = []
        for spec, node_spec in pattern.links:
            if spec.bounds is not None or not spec.direction or len(spec.types) != 1:
                raise CypherError("Creating a relationship requires a single type and a direction")
            if spec.name in copy_row:
                raise CypherError(f"Cannot create the already bound relationship {spec.name!r}")
            node = create_node(node_spec)
            start, end = (nodes[-1], node) if spec.direction == 1 else (node, nodes[-1])
            values = create_properties(spec.props, copy_row, self.params, merging)
            edge = graph.add_edge(start, end, spec.types[0], values)
            self.record("relationships-created")
            self.record("properties-set", len(values))
            nodes.append(node)
            edges.append(edge)
            if spec.name is not None:
                copy_row[spec.name] = edge
        if pattern.name is not None:
            copy_row[pattern.name] = PathValue(nodes, edges)
        return copy_row

    def assign(self, row, assignments):
        for op, name, key, expr in assignments:
            if name not in row:
                raise CypherError(f"Variable {name!r} is not defined")
            entity = row[name]
            if entity is None:
                continue
            if not isinstance(entity, (Node, Edge)):
                raise CypherError(f"Cannot modify {name!r}. Expected a node or relationship", TYPE_ERROR)
            if entity.id not in (self.graph.nodes if isinstance(entity, Node) else self.graph.edges):
                raise CypherError("Cannot modify a deleted entity", TYPE_ERROR)
            if op in {"labels", "remove_labels"}:
                if not isinstance(entity, Node):
                    raise CypherError("Expected a node for label assignment", TYPE_ERROR)
                if op == "labels":
                    self.record("labels-added", len(set(key) - entity.labels))
                    entity.labels.update(key)
                else:
                    self.record("labels-removed", len(set(key) & entity.labels))
                    entity.labels.difference_update(key)
                continue
            if op == "property":
                values = {key: evaluate(expr, row, self.params)}
            else:
                value = evaluate(expr, row, self.params)
                values = dict(props(value))
                if op == "=":
                    for key in entity.props.keys() - values.keys():
                        values[key] = None
            for prop, value in values.items():
                if value is None:
                    if prop in entity.props:
                        del entity.props[prop]
                        self.record("properties-set")
                else:
                    entity.props[prop] = property_value(value)
                    self.record("properties-set")

    def delete(self, rows, items, detach):
        delete_nodes, delete_edges = set(), set()
        for row in rows:
            for item in items:
                value = evaluate(item, row, self.params)
                if isinstance(value, Node):
                    delete_nodes.add(value.id)
                elif isinstance(value, Edge):
                    delete_edges.add(value.id)
                elif isinstance(value, PathValue):
                    delete_nodes.update(node.id for node in value.nodes)
                    delete_edges.update(edge.id for edge in value.edges)
                elif value is not None:
                    raise CypherError("DELETE expects nodes, relationships, or paths", TYPE_ERROR)
        for edge in self.graph.edges.values():
            if edge.start in delete_nodes or edge.end in delete_nodes:
                if detach:
                    delete_edges.add(edge.id)
                elif edge.id not in delete_edges:
                    raise CypherError("Cannot delete a node with relationships. Use DETACH DELETE.", CONSTRAINT)
        for edge_id in delete_edges:
            self.record("relationships-deleted", self.graph.edges.pop(edge_id, None) is not None)
        for node_id in delete_nodes:
            self.record("nodes-deleted", self.graph.nodes.pop(node_id, None) is not None)

    def schema(self, clause):
        kind = clause[0]
        if kind == "schema_create":
            definition, conditional = clause[1:]
            name = definition["name"]
            same_kind = [item for item in self.graph.schema.values() if item["kind"] == definition["kind"]]
            duplicate = any(item["label"] == definition["label"] and item["properties"] == definition["properties"]
                            and item["unique"] == definition["unique"] and item["required"] == definition["required"]
                            for item in same_kind)
            if name in self.graph.schema or duplicate:
                if conditional and (duplicate or self.graph.schema[name]["kind"] == definition["kind"]):
                    return
                raise CypherError(f"Schema object {name!r} conflicts with an existing object",
                                  "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists")
            self.graph.schema[name] = definition
            self.record("constraints-added" if definition["kind"] == "constraint" else "indexes-added")
        else:
            object_kind, name, conditional = clause[1:]
            item = self.graph.schema.get(name)
            if item is None or item["kind"] != object_kind:
                if conditional:
                    return
                raise CypherError(f"Unknown {object_kind} {name!r}",
                                  "Neo.ClientError.Schema.SchemaRuleNotFound")
            del self.graph.schema[name]
            self.record("constraints-removed" if object_kind == "constraint" else "indexes-removed")

    def metadata(self, name, args, default_database):
        graph = self.graph
        if args:
            raise CypherError(f"{name} does not take arguments")
        if name == "db.labels":
            return ["label"], [{"label": label} for label in sorted(set().union(
                *(node.labels for node in graph.nodes.values())
            ))]
        if name == "db.relationshiptypes":
            return ["relationshipType"], [{"relationshipType": item} for item in sorted(
                {edge.type for edge in graph.edges.values()}
            )]
        if name == "db.propertykeys":
            keys = {key for item in itertools.chain(graph.nodes.values(), graph.edges.values()) for key in item.props}
            return ["propertyKey"], [{"propertyKey": key} for key in sorted(keys)]
        if name == "dbms.components":
            return ["name", "versions", "edition"], [
                {"name": "Neo4j Kernel", "versions": [SERVER_AGENT.split("/")[1]], "edition": "community"}
            ]
        if name == "DATABASES":
            columns = ["name", "type", "aliases", "access", "address", "role", "writer",
                       "requestedStatus", "currentStatus", "statusMessage", "default", "home"]
            return columns, [
                {"name": db, "type": db_type, "aliases": [], "access": "read-write", "address": None,
                 "role": "primary", "writer": True, "requestedStatus": "online", "currentStatus": "online",
                 "statusMessage": "", "default": db_type == "standard", "home": db_type == "standard"}
                for db, db_type in ((default_database, "standard"), ("system", "system"))
            ]
        if name in {"INDEXES", "db.indexes"}:
            columns = ["id", "name", "state", "populationPercent", "type", "entityType",
                       "labelsOrTypes", "properties", "indexProvider", "owningConstraint"]
            rows = []
            for index, item in enumerate(graph.schema.values()):
                if item["kind"] == "index" or item["unique"]:
                    rows.append({
                        "id": index, "name": item["name"], "state": "ONLINE", "populationPercent": 100.0,
                        "type": "RANGE", "entityType": "NODE", "labelsOrTypes": [item["label"]],
                        "properties": item["properties"], "indexProvider": "range-1.0",
                        "owningConstraint": item["name"] if item["unique"] else None,
                    })
            return columns, rows
        if name in {"CONSTRAINTS", "db.constraints"}:
            columns = ["id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex"]
            rows = []
            for index, item in enumerate(graph.schema.values()):
                if item["kind"] == "constraint":
                    kind = ("NODE_KEY" if item["required"] else "UNIQUENESS") if item["unique"] else "NODE_PROPERTY_EXISTENCE"
                    rows.append({"id": index, "name": item["name"], "type": kind, "entityType": "NODE",
                                 "labelsOrTypes": [item["label"]], "properties": item["properties"],
                                 "ownedIndex": item["name"] if item["unique"] else None})
            return columns, rows
        if name == "PROCEDURES":
            return ["name"], [
                {"name": item} for item in (
                    "db.labels", "db.relationshipTypes", "db.propertyKeys", "dbms.components",
                    "db.indexes", "db.constraints"
                )
            ]
        if name == "FUNCTIONS":
            return ["name"], [{"name": item} for item in sorted(AGGREGATES | {
                "id", "elementId", "labels", "type", "properties", "keys", "size", "length",
                "range", "coalesce", "date", "datetime", "point",
            })]
        raise CypherError(f"Unknown procedure {name!r}", "Neo.ClientError.Procedure.ProcedureNotFound")

    def branch(self, clauses, default_database, initial=None):
        first = {} if initial is None else initial
        rows, scope = [first], list(first)
        output = None
        graph_clauses = {"match", "create", "merge", "set", "delete", "schema_create", "schema_drop", "foreach"}
        for clause in clauses:
            kind = clause[0]
            if kind in graph_clauses and self.database == "system":
                raise CypherError("Run graph queries in the user database")
            if kind in graph_clauses - {"match"}:
                self.write_query = True
            if kind == "match":
                patterns, where, optional = clause[1:]
                rows = self.match(patterns, rows, where, optional)
                scope = list(dict.fromkeys(scope + list(itertools.chain.from_iterable(
                    pattern.variables() for pattern in patterns
                ))))
            elif kind == "unwind":
                expr, name = clause[1:]
                next_rows = []
                for row in rows:
                    items = evaluate(expr, row, self.params)
                    if items is None:
                        continue
                    if not isinstance(items, list):
                        items = [items]
                    next_rows.extend(row | {name: item} for item in items)
                rows = next_rows
                if name not in scope:
                    scope.append(name)
            elif kind in {"create", "merge"}:
                patterns = clause[1]
                for pattern in patterns:
                    next_rows = []
                    for row in rows:
                        matched = [] if kind == "create" else list(match_pattern(self.graph, pattern, row, self.params))
                        if matched:
                            for item in matched:
                                self.assign(item, clause[2])
                            next_rows.extend(matched)
                        else:
                            created = self.create(pattern, row, kind == "merge")
                            if kind == "merge":
                                self.assign(created, clause[3])
                            next_rows.append(created)
                    rows = next_rows
                    scope = list(dict.fromkeys(scope + pattern.variables()))
            elif kind == "set":
                for row in rows:
                    self.assign(row, clause[1])
            elif kind == "delete":
                self.delete(rows, *clause[1:])
            elif kind == "foreach":
                _, name, source, body = clause
                for row in rows:
                    values = evaluate(source, row, self.params)
                    if values is not None:
                        if not isinstance(values, list):
                            raise CypherError("FOREACH requires a list", TYPE_ERROR)
                        for value in values:
                            self.branch(body, default_database, row | {name: value})
            elif kind in {"with", "return"}:
                rows, scope = project(rows, scope, clause[1], self.params)
                if kind == "with":
                    rows = [row for row in rows if self.where(clause[2], row)]
                else:
                    output = scope
            elif kind == "call":
                _, name, args, projection, where = clause
                columns = None
                calls = []
                for row in rows:
                    fields, records = self.metadata(name, [evaluate(expr, row, self.params) for expr in args],
                                                   default_database)
                    if projection is not None:
                        records, fields = project(records, fields, projection, self.params)
                    if set(row) & set(fields):
                        raise CypherError("Procedure YIELD variable already exists")
                    columns = fields
                    calls.extend(row | record for record in records if self.where(where, row | record))
                if columns is not None:
                    scope += columns
                rows = calls
                output = scope
            elif kind == "subquery":
                _, imports, branches, unions = clause
                selected = scope if imports == "*" else imports or []
                if set(selected) - set(scope):
                    raise CypherError("Subquery imports an undefined variable")
                calls, fields = [], []
                for row in rows:
                    initial = {name: row[name] for name in selected}
                    fields, records = self.execute_branches(
                        branches, unions, default_database, initial, row if imports is None else None,
                    )
                    if set(fields) & set(scope):
                        raise CypherError("Subquery return variable already exists")
                    calls.extend(row | record for record in records if fields)
                    if not fields:
                        calls.append(row)
                rows = calls
                scope += fields
                output = scope
            elif kind == "show":
                _, name, projection, where = clause
                scope, rows = self.metadata(name, [], default_database)
                if projection is not None:
                    rows, scope = project(rows, scope, projection, self.params)
                rows = [row for row in rows if self.where(where, row)]
                output = scope
            elif kind in {"schema_create", "schema_drop"}:
                self.schema(clause)
            if kind not in {"return", "call", "show", "subquery"}:
                output = None
        if output is None and not any(clause[0] in graph_clauses - {"match"} for clause in clauses):
            raise CypherError("A query must conclude with RETURN, a procedure, or an update")
        return ([], []) if output is None else (output, rows)

    def execute_branches(self, clauses, unions, default_database, initial=None, legacy_row=None):
        def execute(branch):
            bindings = legacy_row if legacy_row is not None and branch[0][0] == "with" else initial
            return self.branch(branch, default_database, bindings)

        fields, rows = execute(clauses[0])
        for branch, all_rows in zip(clauses[1:], unions):
            other_fields, other_rows = execute(branch)
            if other_fields != fields:
                raise CypherError("UNION branches must have identical return column names")
            rows.extend(other_rows)
            if not all_rows:
                rows = distinct_rows(rows, lambda row: row)
        return fields, rows

    def run(self, clauses, unions, explain, profile, default_database):
        fields, rows = self.execute_branches(clauses, unions, default_database)
        if self.stats and not explain:
            self.graph.validate_constraints()
        plan = {"operatorType": "ProduceResults", "args": {}, "identifiers": fields, "children": []}
        profiled = plan | {"rows": len(rows), "dbHits": 0} if profile else None
        query_type = ("rw" if fields else "w") if self.write_query else "r"
        return Result(fields, [[row[key] for key in fields] for row in rows] if fields and not explain else [],
                      self.stats if not explain else {}, query_type, plan if explain else None, profiled)


def version_bytes(version):
    major, minor = version
    return bytes((0, 0, minor, major))


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError
        data.extend(part)
    return bytes(data)


def handshake(sock):
    hello = read_exact(sock, 20)
    if hello[:4] != b"\x60\x60\xb0\x17":
        raise ProtocolError("Expected Bolt identification")
    for offset in range(4, 20, 4):
        proposal = hello[offset:offset + 4]
        if proposal == b"\x00\x00\x01\xff":
            ranges = b"\x00\x04\x04\x05" + version_bytes((4, 4))
            sock.sendall(proposal + b"\x02" + ranges + b"\x00")
            selected = read_exact(sock, 4)
            if selected not in [version_bytes(version) for version in VERSIONS] or read_exact(sock, 1) != b"\x00":
                raise ProtocolError("Invalid Bolt manifest selection")
            return selected[3], selected[2]
        reserved, count, minor, major = proposal
        if reserved or count > minor:
            continue
        for version in VERSIONS:
            if version[0] == major and minor - count <= version[1] <= minor:
                sock.sendall(version_bytes(version))
                return version
    sock.sendall(b"\x00\x00\x00\x00")
    raise ProtocolError("No mutual Bolt version")


def recv_message(sock):
    data = bytearray()
    while True:
        size = struct.unpack("!H", read_exact(sock, 2))[0]
        if not size:
            if data:
                message = Decoder.decode(bytes(data))
                if not isinstance(message, Structure):
                    raise ProtocolError("Bolt messages must be structures")
                return message
            continue
        if len(data) + size > MAX_MESSAGE:
            raise ProtocolError("Bolt message exceeded the test server limit")
        data.extend(read_exact(sock, size))


def send_message(sock, data):
    for offset in range(0, len(data), 65535):
        chunk = data[offset:offset + 65535]
        sock.sendall(struct.pack("!H", len(chunk)) + chunk)
    sock.sendall(b"\x00\x00")


def endpoint(host, port):
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


@dataclass
class Transaction:
    graph: Graph
    revision: int
    database: str
    readonly: bool = False
    changed: bool = False


@dataclass
class Stream:
    records: list
    summary: dict
    auto_tx: Transaction | None = None
    offset: int = 0


class Bolt:
    def __init__(self, sock, server, version):
        self.socket = sock
        self.server = server
        self.version = version
        self.transaction = None
        self.streams = {}
        self.ids = itertools.count()
        self.last_qid = None
        self.initialized = False
        self.authenticated = False
        self.failed = False
        self.utc_patch = version[0] >= 5

    def send(self, tag, *fields):
        send_message(self.socket, pack(Structure(tag, fields)))

    def success(self, data=None):
        self.send(0x70, {} if data is None else data)

    def authenticate(self, token):
        if self.server.password is not None:
            username, password = token.get("principal"), token.get("credentials")
            if (token.get("scheme") != "basic" or not isinstance(password, str)
                    or not hmac.compare_digest(password.encode("utf-8"), self.server.password)
                    or username != self.server.username):
                raise CypherError("Invalid authentication credentials", "Neo.ClientError.Security.Unauthorized")
        self.authenticated = True

    def database(self, options, default=None):
        if options.get("imp_user") is not None:
            raise CypherError("User impersonation is unavailable", "Neo.ClientError.Security.Forbidden")
        database = options.get("db")
        if database is None or database == "":
            return default or self.server.database
        if database not in (self.server.database, "system"):
            raise CypherError(f"Database {database!r} does not exist", "Neo.ClientError.Database.DatabaseNotFound")
        return database

    def begin(self, options):
        database = self.database(options)
        graph, revision = self.server.store.snapshot()
        return Transaction(graph, revision, database, options.get("mode") == "r")

    def normalize_params(self, value):
        if isinstance(value, Structure):
            if value.tag == 0x46 and len(value.fields) == 3:
                seconds, nanos, offset = value.fields
                return Structure(0x49, (seconds - offset, nanos, offset))
            if value.tag == 0x66 and len(value.fields) == 3:
                seconds, nanos, zone = value.fields
                local = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
                aware = local.replace(tzinfo=ZoneInfo(zone))
                return Structure(0x69, (int(aware.timestamp()), nanos, zone))
        if isinstance(value, list):
            return [self.normalize_params(item) for item in value]
        if isinstance(value, dict):
            return {key: self.normalize_params(item) for key, item in value.items()}
        return value

    def encode_result(self, value):
        if isinstance(value, Structure):
            fields = tuple(self.encode_result(item) for item in value.fields)
            if not self.utc_patch:
                if value.tag == 0x49:
                    seconds, nanos, offset = fields
                    return Structure(0x46, (seconds + offset, nanos, offset))
                if value.tag == 0x69:
                    seconds, nanos, zone = fields
                    offset = datetime.datetime.fromtimestamp(seconds, ZoneInfo(zone)).utcoffset()
                    return Structure(0x66, (seconds + int(offset.total_seconds()), nanos, zone))
            return Structure(value.tag, fields)
        if isinstance(value, list):
            return [self.encode_result(item) for item in value]
        if isinstance(value, dict):
            return {key: self.encode_result(item) for key, item in value.items()}
        return value

    def run(self, text, params, options):
        first = time.monotonic()
        explicit = self.transaction is not None
        if not explicit and self.streams:
            raise ProtocolError("Previous result has not been consumed")
        transaction = self.transaction if explicit else self.begin(options)
        database = self.database(options, transaction.database)
        if database != transaction.database:
            raise CypherError("Cannot change the database of an open transaction")
        try:
            clauses, unions, explain, profile = Parser(text).parse()
            missing = sorted(set(query_parameters(clauses)) - params.keys())
            if missing:
                raise CypherError("Expected parameter(s): " + ", ".join(missing),
                                  "Neo.ClientError.Statement.ParameterMissing")
            graph = copy.deepcopy(transaction.graph) if explain else transaction.graph
            result = Query(graph, self.normalize_params(params), database).run(
                clauses, unions, explain, profile, self.server.database
            )
            if result.mode != "r" and transaction.readonly and not explain:
                raise CypherError("Updates are unavailable in a read transaction",
                                  "Neo.ClientError.Statement.AccessMode")
            records = []
            for row in result.rows:
                wire = self.encode_result(wire_value(row, self.version))
                data = pack(Structure(0x71, (wire,)))
                if len(data) > MAX_MESSAGE:
                    raise CypherError("A result record exceeded the test server limit")
                records.append(data)
        except (AttributeError, KeyError, OverflowError, ProtocolError, RecursionError, TypeError, ValueError, re.error,
                ZeroDivisionError) as exc:
            raise CypherError(str(exc), TYPE_ERROR) from exc
        transaction.changed |= bool(result.stats)
        summary = {"db": database, "type": result.mode}
        if result.stats:
            summary["stats"] = result.stats
        if result.plan:
            summary["plan"] = result.plan
        if result.profile:
            summary["profile"] = result.profile
        qid = next(self.ids)
        self.last_qid = qid
        self.streams[qid] = Stream(records, summary, None if explicit else transaction)
        meta = {"fields": result.fields, "t_first": int((time.monotonic() - first) * 1000)}
        if explicit:
            meta["qid"] = qid
        self.success(meta)

    def pull(self, options, send_records):
        qid = options.get("qid", -1)
        if qid == -1:
            qid = self.last_qid
        if type(qid) is not int or qid not in self.streams:
            raise ProtocolError("No result stream with that id")
        stream = self.streams[qid]
        count = options.get("n", -1)
        if type(count) is not int or count < -1 or count == 0:
            raise ProtocolError("PULL and DISCARD require a positive count or -1")
        start = stream.offset
        finish = len(stream.records) if count == -1 else min(start + count, len(stream.records))
        stream.offset = finish
        has_more = finish < len(stream.records)
        if has_more:
            summary = {"has_more": True}
        else:
            summary = stream.summary | {"t_last": 0}
            if stream.auto_tx is not None:
                tx = stream.auto_tx
                summary["bookmark"] = self.server.store.commit(tx.graph, tx.revision, tx.changed)
            del self.streams[qid]
        if send_records:
            for data in stream.records[start:finish]:
                send_message(self.socket, data)
        self.success(summary)

    def route(self, options):
        database = self.database(options)
        address = self.server.advertised
        if address is None:
            host, port, *_ = self.socket.getsockname()
            address = endpoint(host, port)
        roles = [{"role": role, "addresses": [address]} for role in ("ROUTE", "READ", "WRITE")]
        self.success({"rt": {"db": database, "ttl": 300, "servers": roles}})

    def dispatch(self, message):
        tag, fields = message.tag, message.fields
        if tag == 0x02:
            return False
        if tag == 0x0F and self.initialized:
            self.transaction = None
            self.streams.clear()
            self.last_qid = None
            self.failed = False
            self.success()
            return True
        if self.failed:
            self.send(0x7E)
            return True
        if tag == 0x01 and not self.initialized:
            if len(fields) != 1 or not isinstance(fields[0], dict):
                raise ProtocolError("HELLO requires a metadata map")
            info, = fields
            if self.version < (5, 1):
                self.authenticate(info)
            self.initialized = True
            metadata = {"server": SERVER_AGENT, "connection_id": f"bolt-{next(self.server.ids)}"}
            patches = info.get("patch_bolt")
            if self.version == (4, 4) and isinstance(patches, list) and "utc" in patches:
                metadata["patch_bolt"] = ["utc"]
                self.utc_patch = True
            self.success(metadata)
            return True
        if not self.initialized:
            raise ProtocolError("HELLO required")
        if tag == 0x6A and self.version >= (5, 1) and not self.authenticated:
            if len(fields) != 1 or not isinstance(fields[0], dict):
                raise ProtocolError("LOGON requires a metadata map")
            self.authenticate(fields[0])
            self.success()
            return True
        if not self.authenticated:
            raise CypherError("LOGON required", "Neo.ClientError.Security.Unauthorized")

        match tag, fields:
            case 0x10, (str(text), dict(params), dict(options)):
                self.run(text, params, options)
            case (0x2F | 0x3F), (dict(options),):
                self.pull(options, send_records=tag == 0x3F)
            case 0x11, (dict(options),) if self.transaction is None and not self.streams:
                self.transaction = self.begin(options)
                self.success()
            case 0x12, () if self.transaction is not None:
                tx = self.transaction
                bookmark = self.server.store.commit(tx.graph, tx.revision, tx.changed)
                self.transaction = None
                self.streams.clear()
                self.success({"bookmark": bookmark})
            case 0x13, () if self.transaction is not None:
                self.transaction = None
                self.streams.clear()
                self.success()
            case 0x66, (dict(), list(), dict(options)) if not self.streams and self.transaction is None:
                self.route(options)
            case 0x54, (int(),) if self.version >= (5, 4):
                self.success()
            case 0x6B, () if self.version >= (5, 1) and self.transaction is None and not self.streams:
                self.authenticated = False
                self.success()
            case _:
                raise ProtocolError(f"Unsupported Bolt message 0x{tag:02x} or invalid session state")
        return True

    def serve(self):
        while True:
            message = recv_message(self.socket)
            try:
                if not self.dispatch(message):
                    return
            except (CypherError, ProtocolError, sqlite3.Error) as exc:
                code = exc.code if isinstance(exc, CypherError) else (
                    "Neo.DatabaseError.General.UnknownError" if isinstance(exc, sqlite3.Error)
                    else "Neo.ClientError.Request.Invalid"
                )
                self.send(0x7F, {"code": code, "message": str(exc)})
                if not self.authenticated:
                    return
                self.failed = True


class BoltHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            self.request.settimeout(15)
            if self.server.tls is not None:
                self.request.do_handshake()
            version = handshake(self.request)
            self.request.settimeout(None)
            Bolt(self.request, self.server, version).serve()
        except (EOFError, OSError, ProtocolError, UnicodeError):
            pass


class BoltServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, store, database, username, password, tls, advertised):
        self.store = store
        self.database = database
        self.username = username
        self.password = password
        self.tls = tls
        self.advertised = advertised
        self.ids = itertools.count(1)
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, BoltHandler)

    def get_request(self):
        connection, address = super().get_request()
        if self.tls is not None:
            try:
                connection = self.tls.wrap_socket(connection, server_side=True, do_handshake_on_connect=False)
            except OSError:
                connection.close()
                raise
        return connection, address


def serve_main(argv):
    parser = argparse.ArgumentParser(
        description="Single-file Neo4j-compatible graph test server",
        epilog="See 'export --help' to write the graph for cypher-shell or neo4j-admin import.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=7687, help="listen port, 0 chooses a free port")
    parser.add_argument("--db-file", type=Path, help="persist the graph in this SQLite file")
    parser.add_argument("--database", default="neo4j", help="default graph database name")
    parser.add_argument("--username", default="neo4j", help="basic auth username (default: %(default)s)")
    parser.add_argument("--password-file", type=Path, help="read a basic auth password from this file")
    parser.add_argument("--tls-cert", type=Path, help="PEM certificate, optionally with the private key")
    parser.add_argument("--tls-key", type=Path, help="PEM private key for --tls-cert")
    parser.add_argument("--advertised-address", help="host:port to advertise to routed Bolt clients")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if not args.database or args.database.lower() == "system":
        parser.error("--database must name a graph database other than system")
    if args.tls_key is not None and args.tls_cert is None:
        parser.error("--tls-key requires --tls-cert")

    password, tls = None, None
    try:
        if args.password_file is not None:
            password = args.password_file.read_bytes().rstrip(b"\r\n")
            if not password:
                parser.error("--password-file must contain a password")
        if args.tls_cert is not None:
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(args.tls_cert, args.tls_key)
        store = Store(args.db_file)
        try:
            with BoltServer((args.host, args.port), store, args.database, args.username,
                            password, tls, args.advertised_address) as server:
                host, port, *_ = server.server_address
                scheme = "bolt" if tls is None else "bolt+s"
                def stop(signum, frame):
                    raise KeyboardInterrupt

                signal.signal(signal.SIGTERM, stop)
                try:
                    print(f"Neo4j test server listening on {scheme}://{endpoint(host, port)}", flush=True)
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
        finally:
            store.close()
    except (OSError, ProtocolError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")


# Export for cypher-shell scripts and neo4j-admin database import CSV files

EXPORT_LABEL = "__jjap_export"
EXPORT_KEY = "__jjap_export_id"
EXPORT_ROWS = 1000
EXPORT_STATEMENT_BYTES = 1 << 20
UNIX_EPOCH = datetime.datetime(1970, 1, 1)
UTC_EPOCH = UNIX_EPOCH.replace(tzinfo=datetime.timezone.utc)
CYPHER_ESCAPES = {ord("\\"): "\\\\", ord("'"): "\\'", **{code: f"\\u{code:04x}" for code in (*range(32), 127)}}
STRUCTURE_TYPES = {0x44: "date", 0x54: "time", 0x74: "localtime", 0x64: "localdatetime", 0x49: "datetime",
                   0x69: "datetime", 0x45: "duration", 0x58: "point", 0x59: "point"}
STRUCTURE_ARITY = {0x44: 1, 0x54: 2, 0x74: 1, 0x64: 2, 0x49: 3, 0x69: 3, 0x45: 4, 0x58: 3, 0x59: 4}
CRS_NAMES = {4326: "wgs-84", 4979: "wgs-84-3d", 7203: "cartesian", 9157: "cartesian-3d"}
ARRAY_DELIMITERS = ";|\x1f\x1e"


def read_graph_snapshot(filename):
    source = Path(filename).resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"Expected a SQLite persistence file: {source}")
    connection = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True)
    try:
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='jjap_neo4j'").fetchone() is None:
            raise ValueError("Expected a Neo4j test server SQLite persistence file")
        row = connection.execute("SELECT graph FROM jjap_neo4j WHERE id=1").fetchone()
    finally:
        connection.close()
    graph = Graph() if row is None else Graph.load(row[0])
    for edge in graph.edges.values():
        if edge.start not in graph.nodes or edge.end not in graph.nodes:
            raise ValueError(f"Relationship {edge.id} refers to a missing node")
    return graph


def iso_fraction(nanos):
    return f".{nanos:09d}".rstrip("0") if nanos else ""


def iso_offset(seconds):
    if not seconds:
        return "Z"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, extra = divmod(rest, 60)
    return f"{'-' if seconds < 0 else '+'}{hours:02d}:{minutes:02d}" + (f":{extra:02d}" if extra else "")


def iso_time(nanos):
    seconds, fraction = divmod(nanos, 1_000_000_000)
    hours, rest = divmod(seconds, 3600)
    return f"{hours:02d}:{rest // 60:02d}:{rest % 60:02d}{iso_fraction(fraction)}"


def structure_text(value):
    """Return the ISO text Neo4j parses for a stored temporal value, or the coordinate map of a point."""
    tag, fields = value.tag, value.fields
    if STRUCTURE_ARITY.get(tag) != len(fields):
        raise ValueError(f"Unsupported property structure 0x{tag:02x} with {len(fields)} fields")
    try:
        if tag == 0x44:
            return (UNIX_EPOCH + datetime.timedelta(days=fields[0])).date().isoformat()
        if tag == 0x74:
            return iso_time(fields[0])
        if tag == 0x54:
            return iso_time(fields[0]) + iso_offset(fields[1])
        if tag == 0x64:
            return (UNIX_EPOCH + datetime.timedelta(seconds=fields[0])).isoformat() + iso_fraction(fields[1])
        if tag == 0x49:
            seconds, nanos, offset = fields
            local = UNIX_EPOCH + datetime.timedelta(seconds=seconds + offset)
            return local.isoformat() + iso_fraction(nanos) + iso_offset(offset)
        if tag == 0x69:
            seconds, nanos, zone = fields
            local = (UTC_EPOCH + datetime.timedelta(seconds=seconds)).astimezone(ZoneInfo(zone))
            offset = iso_offset(int(local.utcoffset().total_seconds()))
            return local.replace(tzinfo=None).isoformat() + iso_fraction(nanos) + offset + f"[{zone}]"
    except (OverflowError, ValueError, KeyError) as exc:
        raise ValueError(f"Cannot export {STRUCTURE_TYPES[tag]} value {fields!r}: {exc}") from None
    if tag == 0x45:
        months, days, seconds, nanos = fields
        total = seconds * 1_000_000_000 + nanos
        whole, fraction = divmod(abs(total), 1_000_000_000)
        return f"P{months}M{days}DT{'-' if total < 0 else ''}{whole}{iso_fraction(fraction)}S"
    srid, *coordinates = fields
    if srid not in CRS_NAMES:
        raise ValueError(f"Neo4j has no coordinate reference system with SRID {srid}")
    if not all(math.isfinite(item) for item in coordinates):
        raise ValueError(f"Neo4j points need finite coordinates, not {coordinates!r}")
    names = ("longitude", "latitude", "height") if srid in (4326, 4979) else ("x", "y", "z")
    return [("crs", CRS_NAMES[srid]), *zip(names, map(float, coordinates))]


def storable_list(name, values, warnings):
    """Neo4j stores homogeneous arrays; the test server also accepts ints mixed with floats."""
    integers = [item for item in values if is_number(item) and isinstance(item, int)]
    if integers and any(isinstance(item, float) for item in values):
        if any(float(item) != item for item in integers):
            raise ValueError(f"Property {name!r} mixes floats with integers that a float list cannot hold exactly")
        warnings.add(f"Property {name!r} mixes integers and floats; it is exported as a float list")
        values = [float(item) for item in values]
    kinds = {STRUCTURE_TYPES.get(item.tag) if isinstance(item, Structure) else type(item) for item in values}
    if len(kinds) > 1 or bytes in kinds:
        raise ValueError(f"Property {name!r} holds a list that Neo4j cannot store as an array")
    return values


def cypher_name(name):
    return "`" + name.replace("`", "``") + "`"


def cypher_float(value):
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    text = repr(value)
    if "e" in text:
        mantissa, exponent = text.split("e")
        text = f"{mantissa if '.' in mantissa else mantissa + '.0'}e{int(exponent)}"
    return text


def cypher_literal(value, name, warnings):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value) if value != -(1 << 63) else "(-9223372036854775807 - 1)"
    if isinstance(value, float):
        return cypher_float(value)
    if isinstance(value, str):
        return "'" + value.translate(CYPHER_ESCAPES) + "'"
    if isinstance(value, list):
        items = storable_list(name, value, warnings)
        return "[" + ", ".join(cypher_literal(item, name, warnings) for item in items) + "]"
    if isinstance(value, Structure):
        text = structure_text(value)
        if value.tag == 0x45:
            months, days, seconds, nanos = value.fields
            return f"duration({{months: {months}, days: {days}, seconds: {seconds}, nanoseconds: {nanos}}})"
        if isinstance(text, list):
            return "point({" + ", ".join(f"{key}: {cypher_literal(item, name, warnings)}" for key, item in text) + "})"
        return f"{STRUCTURE_TYPES[value.tag]}('{text}')"
    if isinstance(value, bytes):
        raise ValueError(f"Property {name!r} is a byte array, which has no Cypher literal; use --format csv")
    raise ValueError(f"Property {name!r} has unsupported type {type(value).__name__}")


def cypher_map(values, warnings):
    return "{" + ", ".join(f"{cypher_name(key)}: {cypher_literal(item, key, warnings)}"
                           for key, item in values.items()) + "}"


def schema_statements(graph, warnings):
    ordinary, enterprise = [], []
    for item in graph.schema.values():
        label = cypher_name(item["label"])
        keys = [f"n.{cypher_name(key)}" for key in item["properties"]]
        head = f"CREATE {item['kind'].upper()} {cypher_name(item['name'])} IF NOT EXISTS FOR (n:{label})"
        if item["kind"] == "index":
            ordinary.append(f"{head} ON ({', '.join(keys)});")
        elif item["unique"]:
            target = keys[0] if len(keys) == 1 else f"({', '.join(keys)})"
            (enterprise if item["required"] else ordinary).append(
                f"{head} REQUIRE {target} {'IS NODE KEY' if item['required'] else 'IS UNIQUE'};"
            )
        else:
            # Neo4j property existence constraints cover a single property each.
            for key, target in zip(item["properties"], keys):
                name = item["name"] if len(keys) == 1 else f"{item['name']}_{key}"
                enterprise.append(f"CREATE CONSTRAINT {cypher_name(name)} IF NOT EXISTS FOR (n:{label}) "
                                  f"REQUIRE {target} IS NOT NULL;")
    if enterprise:
        warnings.add(f"{len(enterprise)} node key or property existence constraint(s) require Neo4j Enterprise "
                     "Edition; they are created last")
        enterprise.insert(0, "// Node key and property existence constraints require Neo4j Enterprise Edition.")
    return ordinary + enterprise


def batched_rows(entities, group, row):
    """Yield (group key, rows) for consecutive entities that share a group, bounded in count and size."""
    for key, members in itertools.groupby(entities, group):
        rows, size = [], 0
        for entity in members:
            text = row(entity)
            if rows and (len(rows) == EXPORT_ROWS or size + len(text) > EXPORT_STATEMENT_BYTES):
                yield key, rows
                rows, size = [], 0
            rows.append(text)
            size += len(text) + 2
        if rows:
            yield key, rows


def check_export_names(graph):
    for node in graph.nodes.values():
        if EXPORT_LABEL in node.labels or EXPORT_KEY in node.props:
            raise ValueError(f"Node {node.id} uses the reserved export name {EXPORT_LABEL!r} or {EXPORT_KEY!r}")


def write_cypher_export(graph, directory, warnings):
    check_export_names(graph)
    nodes = sorted(graph.nodes.values(), key=lambda node: node.id)
    edges = sorted(graph.edges.values(), key=lambda edge: edge.id)
    position = {node.id: index for index, node in enumerate(nodes)}
    label, key = cypher_name(EXPORT_LABEL), cypher_name(EXPORT_KEY)
    with (directory / "neo4j.cypher").open("x", encoding="utf-8", newline="\n") as script:
        script.write("// Generated by neo4j_server.py export. Load into an empty database with:\n"
                     "// cypher-shell -d <database> -f neo4j.cypher\n")
        if nodes:
            script.write(f"CREATE CONSTRAINT {key} FOR (n:{label}) REQUIRE n.{key} IS UNIQUE;\n")
        for labels, rows in batched_rows(
            nodes, lambda node: tuple(sorted(node.labels)),
            lambda node: f"{{i: {position[node.id]}, p: {cypher_map(node.props, warnings)}}}",
        ):
            names = "".join(f":{cypher_name(name)}" for name in (EXPORT_LABEL, *labels))
            script.write(f"UNWIND [{', '.join(rows)}] AS row CREATE (n{names} {{{key}: row.i}}) SET n += row.p;\n")
        for kind, rows in batched_rows(
            edges, lambda edge: edge.type,
            lambda edge: f"{{s: {position[edge.start]}, e: {position[edge.end]}, p: {cypher_map(edge.props, warnings)}}}",
        ):
            script.write(f"UNWIND [{', '.join(rows)}] AS row MATCH (a:{label} {{{key}: row.s}}) "
                         f"MATCH (b:{label} {{{key}: row.e}}) CREATE (a)-[r:{cypher_name(kind)}]->(b) SET r += row.p;\n")
        for start in range(0, len(nodes), EXPORT_ROWS):
            script.write(f"MATCH (n:{label}) WHERE n.{key} >= {start} AND n.{key} < {start + EXPORT_ROWS} "
                         f"REMOVE n:{label}, n.{key};\n")
        if nodes:
            script.write(f"DROP CONSTRAINT {key};\n")
        for statement in schema_statements(graph, warnings):
            script.write(statement + "\n")


def csv_float(value):
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return repr(value)


def csv_scalar(value, name):
    if isinstance(value, bool):
        return "boolean", "true" if value else "false"
    if isinstance(value, int):
        return "long", str(value)
    if isinstance(value, float):
        return "double", csv_float(value)
    if isinstance(value, str):
        return "string", value
    if isinstance(value, Structure):
        text = structure_text(value)
        if isinstance(text, list):
            text = "{" + ", ".join(f"{key}: '{item}'" if key == "crs" else f"{key}: {csv_float(item)}"
                                   for key, item in text) + "}"
        return STRUCTURE_TYPES[value.tag], text
    raise ValueError(f"Property {name!r} has unsupported type {type(value).__name__}")


def csv_property(name, value, warnings):
    """Return the neo4j-admin header type and the field text, or a list of array element texts."""
    if ":" in name:
        raise ValueError(f"Property {name!r} contains ':', which neo4j-admin reads as a type separator; "
                         "use --format cypher")
    if isinstance(value, bytes):
        return "byte[]", [str(item - 256 if item > 127 else item) for item in value]
    if not isinstance(value, list):
        return csv_scalar(value, name)
    if not value or value == [""]:
        raise ValueError(f"Property {name!r} holds {value!r}, which neo4j-admin cannot tell apart from a missing "
                         "value; use --format cypher")
    items = [csv_scalar(item, name) for item in storable_list(name, value, warnings)]
    return items[0][0] + "[]", [text for _, text in items]


def csv_field(text):
    return '"' + text.replace('"', '""') + '"'


def csv_header(name):
    return csv_field(name) if any(char in name for char in ',"\r\n') else name


def write_csv_files(prefix, entities, fixed, directory, delimiter, warnings):
    """Group entities whose property types agree, since one header types a whole file.

    Returns the file names and whether any field spans lines."""
    groups = []
    for entity in entities:
        values = {key: csv_property(key, item, warnings) for key, item in entity.props.items()}
        signature = {key: kind for key, (kind, _) in values.items()}
        for columns, members in groups:
            if all(columns.get(key, kind) == kind for key, kind in signature.items()):
                columns.update(signature)
                members.append((entity, values))
                break
        else:
            groups.append((dict(signature), [(entity, values)]))
    if not groups:
        groups.append(({}, []))
    names, multiline = [], False
    for number, (columns, members) in enumerate(groups, 1):
        keys = sorted(columns)
        filename = f"{prefix}-{number}.csv"
        names.append(filename)
        with (directory / filename).open("x", encoding="utf-8", newline="") as stream:
            header = [*(name for name, _ in fixed), *(csv_header(f"{key}:{columns[key]}") for key in keys)]
            stream.write(",".join(header) + "\n")
            for entity, values in members:
                fields = [value(entity) for _, value in fixed]
                for key in keys:
                    _, text = values.get(key, (None, None))
                    fields.append("" if text is None else csv_field(
                        delimiter.join(text) if isinstance(text, list) else text))
                line = ",".join(fields)
                multiline = multiline or "\n" in line or "\r" in line
                stream.write(line + "\n")
    return names, multiline


def array_delimiter(graph):
    used = set()
    for node in graph.nodes.values():
        used.update(*node.labels)
    for entity in itertools.chain(graph.nodes.values(), graph.edges.values()):
        for value in entity.props.values():
            if isinstance(value, list):
                used.update(*(item for item in value if isinstance(item, str)))
    for candidate in ARRAY_DELIMITERS:
        if candidate not in used:
            return candidate
    raise ValueError("Every candidate array delimiter occurs in a label or string list")


def write_csv_export(graph, directory, warnings):
    delimiter = array_delimiter(graph)
    nodes = sorted(graph.nodes.values(), key=lambda node: node.id)
    edges = sorted(graph.edges.values(), key=lambda edge: edge.id)
    node_files, multiline = write_csv_files("nodes", nodes, [
        (":ID", lambda node: str(node.id)),
        (":LABEL", lambda node: csv_field(delimiter.join(sorted(node.labels))) if node.labels else ""),
    ], directory, delimiter, warnings)
    edge_files = []
    if edges:
        edge_files, spans = write_csv_files("relationships", edges, [
            (":START_ID", lambda edge: str(edge.start)),
            (":END_ID", lambda edge: str(edge.end)),
            (":TYPE", lambda edge: csv_field(edge.type)),
        ], directory, delimiter, warnings)
        multiline = multiline or spans
    options = [f"--array-delimiter=U+{ord(delimiter):04X}", "--bad-tolerance=0"]
    if multiline:
        options.append("--multiline-fields=true")
    options += [f"--nodes={name}" for name in node_files] + [f"--relationships={name}" for name in edge_files]
    with (directory / "import.args").open("x", encoding="utf-8", newline="\n") as stream:
        # The database name goes before @import.args: --relationships would take a trailing name as a file.
        stream.write("# From this directory: neo4j-admin database import full <database> @import.args\n")
        stream.write("\n".join(options) + "\n")
    with (directory / "schema.cypher").open("x", encoding="utf-8", newline="\n") as stream:
        stream.write("// Run after the first start with: cypher-shell -d <database> -f schema.cypher\n")
        for statement in schema_statements(graph, warnings):
            stream.write(statement + "\n")


def publish_export(staged, destination):
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
    parser = argparse.ArgumentParser(description="Export a Neo4j test server SQLite snapshot for Neo4j 5.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--format", choices=("cypher", "csv"), default="cypher",
                        help="cypher-shell script or neo4j-admin import CSV files (default: %(default)s)")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    warnings = set()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        graph = read_graph_snapshot(args.db_file.expanduser())
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".neo4j-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "export"
            staged.mkdir(mode=0o700)
            (write_cypher_export if args.format == "cypher" else write_csv_export)(graph, staged, warnings)
            publish_export(staged, destination)
    except (OSError, ValueError, sqlite3.Error, ProtocolError) as exc:
        parser.exit(1, f"Neo4j export failed: {exc}\n")
    for warning in sorted(warnings):
        print(f"warning: {warning}", file=sys.stderr)
    print(f"Exported {len(graph.nodes)} nodes, {len(graph.edges)} relationships, and "
          f"{len(graph.schema)} indexes/constraints to {destination}")
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["export"]:
        return export_main(argv[1:])
    return serve_main(argv[1:] if argv[:1] == ["serve"] else argv)


if __name__ == "__main__":
    raise SystemExit(main())
