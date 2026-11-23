#!/usr/bin/env python3
"""A standalone MongoDB test server using only the Python 3.10+ standard library.

Run: python3 mongo_test_server.py --port 27017
URI: mongodb://127.0.0.1:27017
Add --db-file test.sqlite3 to retain databases between runs. Use a dedicated file.
Export: python3 mongo_test_server.py export --db-file test.sqlite3 --out dump
Add --format dbpath to export a WiredTiger directory using mongod and mongorestore.
The default listener is loopback. Use --password to prompt for a password,
--password-file to read one from a file, or set MONGO_TEST_PASSWORD. These enable
one test account with access to every database. The default username and
authentication database are both admin. See --username and --auth-db to change
them. --tls-cert accepts a PEM certificate and an optional separate --tls-key.

Connect: python3 mongo_test_server.py shell -u admin -p -d test
Run a command: python3 mongo_test_server.py shell --eval '{"ping": 1}'
The shell accepts MongoDB Extended JSON, simple commands, and piped input.

Supports the MongoDB 8.0 wire interface for CRUD, cursors, collection and client
bulk writes, indexes, validators, capped collections, views, and aggregation.
Queries scan small test data sets. Index metadata, uniqueness, and TTL are
enforced. Cursors hold snapshots of the documents returned by each read.

Coverage excludes replica sets, sharding, transactions, change streams, time
series, non-simple collations, specialized indexes, JavaScript evaluation, and
user/role administration. Only the implemented operators are available.

Protocol references:
https://www.mongodb.com/docs/v8.0/reference/mongodb-wire-protocol/
https://bsonspec.org/spec.html
"""

from __future__ import annotations

import argparse
import base64
import calendar
import decimal
import getpass
import hashlib
import hmac
import itertools
import json
import logging
import math
import os
import platform
import random
import re
import secrets
import shutil
import signal
import socket
import socketserver
import sqlite3
import ssl
import stringprep
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from collections import deque
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import (
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DecimalException,
    localcontext,
)
from enum import Enum
from functools import cmp_to_key, lru_cache
from pathlib import Path
from unicodedata import ucd_3_2_0
from urllib.parse import parse_qsl, quote, quote_plus, unquote, urlsplit
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MAX_BSON_SIZE = 16 * 1024 * 1024
MAX_MESSAGE_SIZE = 48_000_000
MAX_WRITE_BATCH = 100_000
WIRE_VERSION = 25
CURSOR_TIMEOUT = 600
LOGGER = logging.getLogger("mongo_test_server")

ERROR_NAMES = {
    1: "InternalError",
    2: "BadValue",
    9: "FailedToParse",
    13: "Unauthorized",
    14: "TypeMismatch",
    18: "AuthenticationFailed",
    20: "IllegalOperation",
    26: "NamespaceNotFound",
    27: "IndexNotFound",
    40: "ConflictingUpdateOperators",
    43: "CursorNotFound",
    48: "NamespaceExists",
    50: "MaxTimeMSExpired",
    59: "CommandNotFound",
    61: "ShardKeyNotFound",
    66: "ImmutableField",
    67: "CannotCreateIndex",
    72: "InvalidOptions",
    73: "InvalidNamespace",
    85: "IndexOptionsConflict",
    86: "IndexKeySpecsConflict",
    100: "UnsatisfiableWriteConcern",
    103: "NewReplicaSetConfigurationIncompatible",
    115: "CommandNotSupported",
    121: "DocumentValidationFailure",
    130: "RoleNotFound",
    136: "CappedPositionLost",
    166: "CommandNotSupportedOnView",
    171: "CannotIndexParallelArrays",
    180: "NoSuchSession",
    197: "InvalidIndexSpecificationOption",
    286: "ChangeStreamHistoryLost",
    322: "APIVersionError",
    323: "APIStrictError",
    40415: "IDLUnknownField",
    40573: "Location40573",
    40324: "Location40324",
    11000: "DuplicateKey",
    17280: "KeyTooLong",
    17419: "Location17419",
    17420: "Location17420",
    10334: "BSONObjectTooLarge",
    51183: "Location51183",
}


class MongoError(Exception):
    def __init__(self, message, code=2, **details):
        super().__init__(message)
        self.code = code
        self.details = details

    def document(self):
        return {
            "ok": 0.0,
            "errmsg": str(self),
            "code": self.code,
            "codeName": ERROR_NAMES.get(self.code, f"Location{self.code}"),
            **self.details,
        }


class _Missing(Enum):
    TOKEN = 0


MISSING = _Missing.TOKEN


# BSON


class BSONError(ValueError):
    pass


class Int64(int):
    """Keep the BSON int64 type when a small integer is decoded and re-encoded."""


class Binary(bytes):
    def __new__(cls, data=b"", subtype=0):
        if not isinstance(subtype, int) or not 0 <= subtype <= 255:
            raise BSONError("BSON binary subtype must be an unsigned byte")
        result = super().__new__(cls, data)
        object.__setattr__(result, "subtype", subtype)
        return result

    def __setattr__(self, name, value):
        raise AttributeError("Binary values are immutable")

    def __delattr__(self, name):
        raise AttributeError("Binary values are immutable")

    def __eq__(self, other):
        if isinstance(other, Binary):
            return self.subtype == other.subtype and bytes.__eq__(self, other)
        if isinstance(other, bytes):
            return self.subtype == 0 and bytes.__eq__(self, other)
        return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)
        return NotImplemented if equal is NotImplemented else not equal

    def __hash__(self):
        raw = bytes(self)
        return hash(raw) if self.subtype == 0 else hash((self.subtype, raw))

    def __getnewargs__(self):
        return bytes(self), self.subtype

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


_object_id_lock = threading.Lock()
_object_id_pid = os.getpid()
_object_id_random = secrets.token_bytes(5)
_object_id_counter = secrets.randbits(24)


@dataclass(frozen=True)
class ObjectId:
    data: bytes

    def __post_init__(self):
        if not isinstance(self.data, bytes) or len(self.data) != 12:
            raise BSONError("ObjectId requires 12 bytes")

    def __str__(self):
        return self.data.hex()

    @classmethod
    def new(cls):
        global _object_id_pid, _object_id_random, _object_id_counter
        with _object_id_lock:
            pid = os.getpid()
            if pid != _object_id_pid:
                _object_id_pid = pid
                _object_id_random = secrets.token_bytes(5)
                _object_id_counter = secrets.randbits(24)
            timestamp = int(time.time()) & 0xFFFFFFFF
            data = timestamp.to_bytes(4, "big") + _object_id_random + _object_id_counter.to_bytes(3, "big")
            _object_id_counter = (_object_id_counter + 1) & 0xFFFFFF
        return cls(data)


_BSON_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class DateTime:
    ms: int

    @classmethod
    def now(cls):
        return cls(time.time_ns() // 1_000_000)

    @classmethod
    def from_datetime(cls, value):
        if value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        delta = value - _BSON_EPOCH
        return cls(delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000)

    def to_datetime(self):
        return _BSON_EPOCH + timedelta(milliseconds=self.ms)


@dataclass(frozen=True)
class Regex:
    pattern: str
    options: str = ""

    def __post_init__(self):
        object.__setattr__(self, "options", "".join(sorted(self.options)))


@dataclass(frozen=True)
class Timestamp:
    time: int
    increment: int


# BSON Decimal128 stores the IEEE 754 Binary Integer Decimal representation.
_DECIMAL128_CONTEXT = decimal.Context(
    prec=34,
    rounding=decimal.ROUND_HALF_EVEN,
    Emin=-6143,
    Emax=6144,
    clamp=1,
    traps=[decimal.InvalidOperation, decimal.Overflow, decimal.Inexact],
)
_DECIMAL128_BIAS = 6176
_DECIMAL128_EXPONENT_MASK = (1 << 14) - 1
_DECIMAL128_COEFFICIENT_MASK = (1 << 113) - 1


@dataclass(frozen=True)
class Decimal128:
    data: bytes

    def __post_init__(self):
        if not isinstance(self.data, bytes) or len(self.data) != 16:
            raise BSONError("Decimal128 requires 16 bytes")

    @classmethod
    def from_decimal(cls, value):
        with decimal.localcontext(_DECIMAL128_CONTEXT) as context:
            value = context.create_decimal(value)
        sign, digits, exponent = value.as_tuple()
        bits = sign << 127
        if value.is_infinite():
            bits |= 0x78 << 120
        elif value.is_nan():
            if digits:
                raise BSONError("Decimal128 NaN payloads are unsupported")
            bits |= (0x7E if value.is_snan() else 0x7C) << 120
        else:
            coefficient = int("".join(map(str, digits)))
            bits |= (exponent + _DECIMAL128_BIAS) << 113
            bits |= coefficient
        return cls(bits.to_bytes(16, "little"))

    def to_decimal(self):
        bits = int.from_bytes(self.data, "little")
        sign = bits >> 127
        special = (bits >> 120) & 0x7E
        if special >= 0x78:
            exponent = {0x78: "F", 0x7A: "F", 0x7C: "n", 0x7E: "N"}[special]
            return decimal.Decimal((sign, (), exponent))
        if (bits >> 125) & 3 == 3:
            # The alternate significand layout encodes values above 10**34.
            exponent = ((bits >> 111) & _DECIMAL128_EXPONENT_MASK) - _DECIMAL128_BIAS
            coefficient = 0
        else:
            exponent = ((bits >> 113) & _DECIMAL128_EXPONENT_MASK) - _DECIMAL128_BIAS
            coefficient = bits & _DECIMAL128_COEFFICIENT_MASK
            if coefficient >= 10**34:
                coefficient = 0
        return decimal.Decimal((sign, tuple(map(int, str(coefficient))), exponent))


@dataclass(frozen=True)
class MinKey:
    pass


@dataclass(frozen=True)
class MaxKey:
    pass


@dataclass(frozen=True)
class Undefined:
    pass


@dataclass(frozen=True)
class JavaScript:
    code: str
    scope: dict | None = None


@dataclass(frozen=True)
class Symbol:
    value: str


@dataclass(frozen=True)
class DBPointer:
    namespace: str
    object_id: ObjectId


_BSON_DOUBLE = struct.Struct("<d")
_BSON_I32 = struct.Struct("<i")
_BSON_I64 = struct.Struct("<q")
_BSON_TIMESTAMP = struct.Struct("<II")
_BSON_MAX_DEPTH = 100


def _encode_bson_cstring(value):
    if not isinstance(value, str) or "\0" in value:
        raise BSONError("BSON cstrings must be strings without NUL")
    return value.encode("utf-8") + b"\0"


def _encode_bson_string(value):
    raw = value.encode("utf-8")
    return _BSON_I32.pack(len(raw) + 1) + raw + b"\0"


def _encode_bson_fields(fields, depth):
    if depth >= _BSON_MAX_DEPTH:
        raise BSONError("BSON document exceeds the nesting limit")
    payload = bytearray()
    for name, value in fields:
        tag, raw = _encode_bson_value(value, depth)
        payload.append(tag)
        payload.extend(_encode_bson_cstring(name))
        payload.extend(raw)
    return _BSON_I32.pack(len(payload) + 5) + payload + b"\0"


def _encode_bson_value(value, depth):
    if value is None:
        return 10, b""
    if isinstance(value, bool):
        return 8, bytes([value])
    if isinstance(value, Int64):
        return 18, _BSON_I64.pack(value)
    if isinstance(value, int):
        if -(1 << 31) <= value < 1 << 31:
            return 16, _BSON_I32.pack(value)
        return 18, _BSON_I64.pack(value)
    if isinstance(value, float):
        return 1, _BSON_DOUBLE.pack(value)
    if isinstance(value, str):
        return 2, _encode_bson_string(value)
    if isinstance(value, Mapping):
        return 3, _encode_bson_fields(value.items(), depth + 1)
    if isinstance(value, (list, tuple)):
        fields = ((str(index), item) for index, item in enumerate(value))
        return 4, _encode_bson_fields(fields, depth + 1)
    if isinstance(value, (bytes, bytearray, memoryview)):
        subtype = getattr(value, "subtype", 0)
        raw = bytes(value)
        if subtype == 2:
            raw = _BSON_I32.pack(len(raw)) + raw
        return 5, _BSON_I32.pack(len(raw)) + bytes([subtype]) + raw
    if isinstance(value, ObjectId):
        return 7, value.data
    if isinstance(value, datetime):
        value = DateTime.from_datetime(value)
    if isinstance(value, DateTime):
        return 9, _BSON_I64.pack(value.ms)
    if isinstance(value, Regex):
        return 11, _encode_bson_cstring(value.pattern) + _encode_bson_cstring(value.options)
    if isinstance(value, Timestamp):
        return 17, _BSON_TIMESTAMP.pack(value.increment, value.time)
    if isinstance(value, decimal.Decimal):
        value = Decimal128.from_decimal(value)
    if isinstance(value, Decimal128):
        return 19, value.data
    if isinstance(value, MinKey):
        return 255, b""
    if isinstance(value, MaxKey):
        return 127, b""
    if isinstance(value, Undefined):
        return 6, b""
    if isinstance(value, JavaScript):
        code = _encode_bson_string(value.code)
        if value.scope is None:
            return 13, code
        scope = _encode_bson_fields(value.scope.items(), depth + 1)
        return 15, _BSON_I32.pack(4 + len(code) + len(scope)) + code + scope
    if isinstance(value, Symbol):
        return 14, _encode_bson_string(value.value)
    if isinstance(value, DBPointer):
        return 12, _encode_bson_string(value.namespace) + value.object_id.data
    raise BSONError(f"Cannot encode a value of type {type(value).__name__}")


def encode_bson(document):
    if not isinstance(document, Mapping):
        raise BSONError("BSON document must be a mapping")
    try:
        return _encode_bson_fields(document.items(), 0)
    except (UnicodeError, struct.error, decimal.DecimalException, OverflowError) as exc:
        raise BSONError(str(exc)) from exc


def _check_bson_extent(start, size, end):
    if size < 0 or start + size > end:
        raise BSONError("Truncated BSON value")
    return start + size


def _read_bson_cstring(data, start, end):
    stop = data.find(b"\0", start, end)
    if stop < 0:
        raise BSONError("Unterminated BSON cstring")
    return data[start:stop].decode("utf-8"), stop + 1


def _read_bson_string(data, start, end):
    start_length = _check_bson_extent(start, 4, end)
    size = _BSON_I32.unpack_from(data, start)[0]
    stop = _check_bson_extent(start_length, size, end)
    if size < 1 or data[stop - 1] != 0:
        raise BSONError("Invalid BSON string")
    return data[start_length : stop - 1].decode("utf-8"), stop


def _read_bson_document(data, start, end, depth, array=False):
    if depth >= _BSON_MAX_DEPTH:
        raise BSONError("BSON document exceeds the nesting limit")
    index = _check_bson_extent(start, 4, end)
    size = _BSON_I32.unpack_from(data, start)[0]
    stop = _check_bson_extent(start, size, end)
    if size < 5 or data[stop - 1] != 0:
        raise BSONError("Invalid BSON document")
    content_end = stop - 1
    result = {}
    while index < content_end:
        tag = data[index]
        name, index = _read_bson_cstring(data, index + 1, content_end)
        if name in result or (array and name != str(len(result))):
            raise BSONError("Invalid or duplicate BSON key")
        result[name], index = _read_bson_value(tag, data, index, content_end, depth)
    return (list(result.values()) if array else result), stop


def _read_bson_value(tag, data, start, end, depth):
    if tag == 3 or tag == 4:
        return _read_bson_document(data, start, end, depth + 1, array=tag == 4)
    if tag == 2:
        return _read_bson_string(data, start, end)
    if tag == 10:
        return None, start
    if tag == 8:
        stop = _check_bson_extent(start, 1, end)
        if data[start] > 1:
            raise BSONError("Invalid BSON boolean")
        return bool(data[start]), stop
    if tag == 16:
        stop = _check_bson_extent(start, 4, end)
        return _BSON_I32.unpack_from(data, start)[0], stop
    if tag == 1:
        stop = _check_bson_extent(start, 8, end)
        return _BSON_DOUBLE.unpack_from(data, start)[0], stop
    if tag in (9, 17, 18):
        stop = _check_bson_extent(start, 8, end)
        if tag == 17:
            increment, timestamp = _BSON_TIMESTAMP.unpack_from(data, start)
            return Timestamp(timestamp, increment), stop
        number = _BSON_I64.unpack_from(data, start)[0]
        return (Int64(number) if tag == 18 else DateTime(number)), stop
    if tag == 7:
        stop = _check_bson_extent(start, 12, end)
        return ObjectId(bytes(data[start:stop])), stop
    if tag == 5:
        payload = _check_bson_extent(start, 5, end)
        size = _BSON_I32.unpack_from(data, start)[0]
        subtype = data[start + 4]
        stop = _check_bson_extent(payload, size, end)
        if subtype == 2:
            legacy_start = _check_bson_extent(payload, 4, stop)
            if _BSON_I32.unpack_from(data, payload)[0] != size - 4:
                raise BSONError("Invalid legacy BSON binary length")
            payload = legacy_start
        return Binary(data[payload:stop], subtype), stop
    if tag == 11:
        pattern, next_index = _read_bson_cstring(data, start, end)
        options, stop = _read_bson_cstring(data, next_index, end)
        return Regex(pattern, options), stop
    if tag == 19:
        stop = _check_bson_extent(start, 16, end)
        return Decimal128(bytes(data[start:stop])), stop
    if tag == 255:
        return MinKey(), start
    if tag == 127:
        return MaxKey(), start
    if tag == 6:
        return Undefined(), start
    if tag == 12:
        name, next_index = _read_bson_string(data, start, end)
        object_id, stop = _read_bson_value(7, data, next_index, end, depth)
        return DBPointer(name, object_id), stop
    if tag in (13, 14):
        value, stop = _read_bson_string(data, start, end)
        return (JavaScript(value) if tag == 13 else Symbol(value)), stop
    if tag == 15:
        position = _check_bson_extent(start, 4, end)
        size = _BSON_I32.unpack_from(data, start)[0]
        stop = _check_bson_extent(start, size, end)
        code, position = _read_bson_string(data, position, stop)
        scope, position = _read_bson_document(data, position, stop, depth + 1)
        if position != stop:
            raise BSONError("Invalid BSON code-with-scope length")
        return JavaScript(code, scope), stop
    raise BSONError(f"Unknown BSON element type 0x{tag:02x}")


def decode_bson(data, start=0, end=None):
    if end is None:
        end = len(data)
    if not 0 <= start <= end <= len(data):
        raise BSONError("Invalid BSON extent")
    try:
        return _read_bson_document(data, start, end, 0)
    except UnicodeError as exc:
        raise BSONError("Invalid BSON UTF-8") from exc


# MongoDB wire protocol

_OP_REPLY = 1
_OP_QUERY = 2004
_OP_COMPRESSED = 2012
_OP_MSG = 2013
_WIRE_HEADER = struct.Struct("<iiii")
_WIRE_COMPRESSED = struct.Struct("<iiB")
_WIRE_OP_REPLY = struct.Struct("<iqii")
_WIRE_U32 = struct.Struct("<I")
_WIRE_LOG = logging.getLogger("mongo_test_server.wire")
_UNCOMPRESSIBLE_COMMANDS = {
    "hello",
    "ismaster",
    "saslstart",
    "saslcontinue",
    "getnonce",
    "authenticate",
    "createuser",
    "updateuser",
    "copydbsaslstart",
    "copydbgetnonce",
    "copydb",
}


class _WireError(ValueError):
    pass


def _make_crc32c_table():
    entries = []
    for entry in range(256):
        for _ in range(8):
            entry = (entry >> 1) ^ (0x82F63B78 if entry & 1 else 0)
        entries.append(entry)
    return entries


_CRC32C_TABLE = _make_crc32c_table()


def _wire_crc32c(*parts):
    checksum = 0xFFFFFFFF
    table = _CRC32C_TABLE
    for part in parts:
        for byte in part:
            checksum = table[(checksum ^ byte) & 0xFF] ^ (checksum >> 8)
    return checksum ^ 0xFFFFFFFF


def _insert_document_sequence(command, name, documents):
    parts = name.split(".")
    if "" in parts:
        raise _WireError("Invalid OP_MSG document sequence identifier")
    for part in parts[:-1]:
        command = command.setdefault(part, {})
        if not isinstance(command, dict):
            raise _WireError("Conflicting OP_MSG document sequence path")
    if parts[-1] in command:
        raise _WireError("Duplicate OP_MSG document sequence")
    command[parts[-1]] = documents


def _read_op_msg(header, body, *, reply=False):
    if len(body) < 5:
        raise _WireError("Truncated OP_MSG")
    flags = _WIRE_U32.unpack_from(body)[0]
    if flags & 0xFFFC:
        raise _WireError("Unknown required OP_MSG flags")
    limit = len(body)
    if flags & 1:
        limit -= 4
        measured = _wire_crc32c(header, memoryview(body)[:limit])
        if measured != _WIRE_U32.unpack_from(body, limit)[0]:
            raise _WireError("Invalid OP_MSG checksum")
    index = 4
    command = None
    sequences = []
    telemetry_seen = False
    while index < limit:
        section_type = body[index]
        index += 1
        if section_type == 0:
            if command is not None:
                raise _WireError("Multiple OP_MSG body sections")
            command, index = decode_bson(body, index, limit)
        elif section_type == 1:
            sequence_start = _check_bson_extent(index, 4, limit)
            size = _BSON_I32.unpack_from(body, index)[0]
            sequence_end = _check_bson_extent(index, size, limit)
            name, index = _read_bson_cstring(body, sequence_start, sequence_end)
            documents = []
            while index < sequence_end:
                document, index = decode_bson(body, index, sequence_end)
                documents.append(document)
                if len(documents) > MAX_WRITE_BATCH:
                    raise _WireError("OP_MSG sequence exceeds maximum write batch size")
            sequences.append((name, documents))
        elif section_type == 3 and not telemetry_seen and not reply:
            # Telemetry is auxiliary context. It has no effect on command semantics.
            _, index = decode_bson(body, index, limit)
            telemetry_seen = True
        else:
            raise _WireError("Unknown or duplicate OP_MSG section type")
    if not command:
        raise _WireError("Missing OP_MSG command body")
    database = None if reply else command.get("$db")
    if not reply and (not isinstance(database, str) or not database or "\0" in database):
        raise _WireError("Missing or invalid OP_MSG database name")
    for name, documents in sequences:
        _insert_document_sequence(command, name, documents)
    return database, command, bool(flags & 2)


def _read_op_query(body):
    if len(body) < 4:
        raise _WireError("Truncated OP_QUERY")
    namespace, index = _read_bson_cstring(body, 4, len(body))
    database, separator, collection = namespace.partition(".")
    if not database or not separator or collection != "$cmd":
        raise _WireError("OP_QUERY is supported for legacy commands")
    index = _check_bson_extent(index, 8, len(body))  # skip and numberToReturn
    command, index = decode_bson(body, index)
    if index != len(body):
        _, index = decode_bson(body, index)  # optional fields selector
        if index != len(body):
            raise _WireError("Unexpected trailing data in OP_QUERY")
    if "$query" in command:
        wrapped = command["$query"]
        if not isinstance(wrapped, dict):
            raise _WireError("Invalid legacy command wrapper")
        command = wrapped
    if not command:
        raise _WireError("Missing OP_QUERY command")
    return database, command, False


def _unpack_compressed_wire_message(body):
    if len(body) < _WIRE_COMPRESSED.size:
        raise _WireError("Truncated OP_COMPRESSED")
    opcode, size, compressor = _WIRE_COMPRESSED.unpack_from(body)
    if opcode not in (_OP_MSG, _OP_QUERY) or not 0 <= size <= MAX_MESSAGE_SIZE - _WIRE_HEADER.size:
        raise _WireError("Invalid OP_COMPRESSED header")
    compressed = body[_WIRE_COMPRESSED.size :]
    if compressor == 0:
        payload = compressed
    elif compressor == 2:
        decoder = zlib.decompressobj()
        payload = decoder.decompress(compressed, size + 1)
        if not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
            raise _WireError("Invalid zlib stream in OP_COMPRESSED")
    else:
        raise _WireError("Unsupported OP_COMPRESSED compressor")
    if len(payload) != size:
        raise _WireError("OP_COMPRESSED size mismatch")
    return opcode, payload, compressor


def _wire_receive(sock, size):
    data = bytearray(size)
    remaining = memoryview(data)
    while remaining:
        received = sock.recv_into(remaining)
        if not received:
            if len(remaining) == size:
                return None
            raise _WireError("Truncated wire message")
        remaining = remaining[received:]
    return data


class _MongoWireHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            if self.server.tls_context is None:
                self._serve_messages(self.request)
            else:
                self.request.settimeout(10)
                with self.server.tls_context.wrap_socket(self.request, server_side=True) as sock:
                    sock.settimeout(None)
                    self._serve_messages(sock)
        except (OSError, _WireError, BSONError, zlib.error, UnicodeError) as exc:
            _WIRE_LOG.debug("MongoDB connection closed: %s", exc)

    def _serve_messages(self, sock):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        state = {"address": self.client_address, "id": next(self.server._connection_ids)}
        while True:
            header = _wire_receive(sock, _WIRE_HEADER.size)
            if header is None:
                return
            message_length, request_id, response_to, opcode = _WIRE_HEADER.unpack(header)
            if not _WIRE_HEADER.size <= message_length <= MAX_MESSAGE_SIZE or response_to != 0:
                raise _WireError("Invalid wire message header")
            body = _wire_receive(sock, message_length - _WIRE_HEADER.size)
            if body is None:
                raise _WireError("Missing wire message body")
            compressor = None
            if opcode == _OP_COMPRESSED:
                opcode, body, compressor = _unpack_compressed_wire_message(body)
                header = _WIRE_HEADER.pack(_WIRE_HEADER.size + len(body), request_id, response_to, opcode)
            if opcode == _OP_MSG:
                database, command, no_reply = _read_op_msg(header, body)
            elif opcode == _OP_QUERY:
                database, command, no_reply = _read_op_query(body)
            else:
                raise _WireError(f"Unsupported wire opcode {opcode}")
            command_name = next(iter(command)).lower()
            if compressor is not None and command_name in _UNCOMPRESSIBLE_COMMANDS:
                raise _WireError("Compression is unavailable for this command")
            reply = self._dispatch(database, command, state)
            if no_reply:
                continue
            if command_name in ("hello", "ismaster") and reply.get("ok"):
                offered = command.get("compression", [])
                if isinstance(offered, list) and "zlib" in offered:
                    reply = {**reply, "compression": ["zlib"]}
            self._send_reply(sock, request_id, opcode, reply, compressor)

    def _dispatch(self, database, command, state):
        try:
            return self.server.dispatch(database, command, state)
        except MongoError as exc:
            return exc.document()
        except Exception:
            _WIRE_LOG.exception("Unexpected MongoDB command failure")
            return MongoError("Internal server error", 1).document()

    def _send_reply(self, sock, response_to, request_opcode, reply, compressor):
        if request_opcode == _OP_QUERY:
            opcode = _OP_REPLY
            prefix = _WIRE_OP_REPLY.pack(0, 0, 0, 1)
        else:
            opcode = _OP_MSG
            prefix = b"\0\0\0\0\0"  # flagBits and BSON body kind
        try:
            payload = prefix + encode_bson(reply)
        except BSONError:
            _WIRE_LOG.exception("Could not encode MongoDB response")
            payload = prefix + encode_bson(MongoError("Could not encode server response", 1).document())
        if len(payload) > MAX_MESSAGE_SIZE - _WIRE_HEADER.size:
            payload = prefix + encode_bson(
                MongoError("Response exceeds maximum message size", 10334).document()
            )
        if compressor == 2:
            compressed = zlib.compress(payload)
            if len(compressed) + _WIRE_COMPRESSED.size < len(payload):
                payload = _WIRE_COMPRESSED.pack(opcode, len(payload), compressor) + compressed
                opcode = _OP_COMPRESSED
        request_id = next(self.server._request_ids) & 0x7FFFFFFF
        header = _WIRE_HEADER.pack(_WIRE_HEADER.size + len(payload), request_id, response_to, opcode)
        sock.sendall(header)
        sock.sendall(payload)


class MongoTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, dispatch, tls_context=None):
        self.dispatch = dispatch
        self.tls_context = tls_context
        self._request_ids = itertools.count(1)
        self._connection_ids = itertools.count(1)
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, _MongoWireHandler)


# Query predicates, projections, and update modifiers.


def numeric(value):
    return type(value) in (int, float, Int64) or isinstance(value, Decimal128)


def bson_type(value):
    if value is MISSING:
        return "missing"
    if value is None:
        return "null"
    if isinstance(value, JavaScript):
        return "javascript" if value.scope is None else "javascriptWithScope"
    if type(value) is int and not -(1 << 31) <= value < (1 << 31):
        return "long"
    return _Q_TYPE_NAMES[type(value)]


_Q_TYPE_NAMES = {
    float: "double",
    str: "string",
    dict: "object",
    list: "array",
    Binary: "binData",
    bytes: "binData",
    Undefined: "undefined",
    ObjectId: "objectId",
    bool: "bool",
    DateTime: "date",
    Regex: "regex",
    DBPointer: "dbPointer",
    Symbol: "symbol",
    int: "int",
    Timestamp: "timestamp",
    Int64: "long",
    Decimal128: "decimal",
    MinKey: "minKey",
    MaxKey: "maxKey",
}
_Q_TYPE_CODES = {
    -1: "minKey",
    1: "double",
    2: "string",
    3: "object",
    4: "array",
    5: "binData",
    6: "undefined",
    7: "objectId",
    8: "bool",
    9: "date",
    10: "null",
    11: "regex",
    12: "dbPointer",
    13: "javascript",
    14: "symbol",
    15: "javascriptWithScope",
    16: "int",
    17: "timestamp",
    18: "long",
    19: "decimal",
    127: "maxKey",
}
_Q_RANK = {
    "minKey": -1,
    "missing": 0,
    "undefined": 0,
    "null": 1,
    "int": 2,
    "long": 2,
    "double": 2,
    "decimal": 2,
    "string": 3,
    "symbol": 3,
    "object": 4,
    "array": 5,
    "binData": 6,
    "objectId": 7,
    "bool": 8,
    "date": 9,
    "timestamp": 10,
    "regex": 11,
    "dbPointer": 12,
    "javascript": 13,
    "javascriptWithScope": 14,
    "maxKey": 15,
}


def _q_decimal(value):
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return Decimal(value)


def value_key(value):
    alias = bson_type(value)
    rank = _Q_RANK[alias]
    if rank == 2:
        number = _q_decimal(value)
        return rank, 0 if number.is_nan() else 1, Decimal(0) if number.is_nan() else number
    if rank == 4:
        entries = []
        for field, item in value.items():
            item_key = value_key(item)
            entries.append((item_key[0], field, item_key[1:]))
        return rank, tuple(entries)
    if rank == 5:
        return rank, tuple(map(value_key, value))
    if rank == 6:
        return rank, len(value), getattr(value, "subtype", 0), bytes(value)
    if alias in ("string", "bool"):
        return rank, value
    if alias == "symbol":
        return rank, value.value
    if alias == "objectId":
        return rank, value.data
    if alias == "date":
        return rank, value.ms
    if alias == "timestamp":
        return rank, value.time, value.increment
    if alias == "regex":
        return rank, value.pattern, "".join(sorted(value.options))
    if alias == "dbPointer":
        return rank, len(value.namespace.encode()), value.namespace, value.object_id.data
    if alias == "javascript":
        return rank, value.code
    if alias == "javascriptWithScope":
        return rank, value.code, value_key(value.scope)
    return (rank,)


def compare(left, right):
    a, b = value_key(left), value_key(right)
    return (a > b) - (a < b)


def bson_equal(left, right):
    return value_key(left) == value_key(right)


def _q_index(part):
    if part.isascii() and part.isdecimal() and len(part) <= 10:
        return int(part)
    return None


@dataclass(frozen=True)
class _QAtomicArray:
    value: list


def _q_query_values(value, parts, *, missing=False):
    if not parts:
        return [value]
    head, *tail = parts
    if isinstance(value, dict):
        if head in value:
            return _q_query_values(value[head], tail, missing=missing)
        return [MISSING] if missing else []
    if isinstance(value, list):
        values = []
        index = _q_index(head)
        if index is not None and str(index) == head and index < len(value):
            picked = value[index]
            if not tail and isinstance(picked, list):
                values.append(_QAtomicArray(picked))
            elif not tail or isinstance(picked, (dict, list)):
                values += _q_query_values(picked, tail, missing=missing)
        for item in value:
            if isinstance(item, dict):
                values += _q_query_values(item, parts, missing=missing)
        return values
    return [MISSING] if missing else []


def _q_unwrap(value):
    return value.value if isinstance(value, _QAtomicArray) else value


def path_values(document, path):
    return [_q_unwrap(value) for value in _q_query_values(document, path.split("."))]


def get_path(document, path):
    """Resolve an aggregation field path, preserving arrays along the path."""

    def walk(value, parts):
        if not parts:
            return value
        if isinstance(value, dict):
            head, *tail = parts
            return walk(value[head], tail) if head in value else MISSING
        if isinstance(value, list):
            values = [walk(item, parts) for item in value if isinstance(item, dict)]
            return [item for item in values if item is not MISSING]
        return MISSING

    return walk(document, path.split("."))


def set_path(document, path, value):
    """Assign a dotted path with aggregation semantics."""

    def walk(current, parts):
        if isinstance(current, list):
            for i, item in enumerate(current):
                if not isinstance(item, (dict, list)):
                    current[i] = {}
                walk(current[i], parts)
            return
        head, *tail = parts
        if not tail:
            current[head] = deepcopy(value)
            return
        if not isinstance(current.get(head), (dict, list)):
            current[head] = {}
        walk(current[head], tail)

    walk(document, path.split("."))


def unset_path(document, path):
    def walk(current, parts):
        if isinstance(current, list):
            for item in current:
                walk(item, parts)
        elif isinstance(current, dict):
            head, *tail = parts
            if tail and head in current:
                walk(current[head], tail)
            elif not tail:
                current.pop(head, None)

    walk(document, path.split("."))


@lru_cache(maxsize=256)
def _q_regex(pattern, options):
    flags = 0
    allowed = {"i": re.I, "m": re.M, "s": re.S, "x": re.X, "u": re.U}
    for option in options:
        if option not in allowed:
            raise MongoError(f"Unsupported regular expression option {option!r}")
        flags |= allowed[option]
    try:
        return re.compile(pattern, flags)
    except re.error as exc:
        raise MongoError(f"Invalid regular expression: {exc}") from exc


def _q_regex_search(value, pattern, options):
    if isinstance(value, Regex):
        return bson_equal(value, Regex(pattern, options))
    if isinstance(value, Symbol):
        value = value.value
    return isinstance(value, str) and _q_regex(pattern, options).search(value) is not None


def _q_predicate_form(value):
    if not isinstance(value, dict) or not value:
        return False
    first = next(iter(value))
    if first == "$ref" and "$id" in value:
        return False
    return first.startswith("$")


def _q_require_obj(value, subject):
    if not isinstance(value, dict):
        raise MongoError(f"{subject} requires a document")
    return value


def _q_require_list(value, subject):
    if not isinstance(value, list):
        raise MongoError(f"{subject} requires an array")
    return value


def _q_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _q_type_names(spec):
    names = []
    for kind in spec if isinstance(spec, list) else [spec]:
        if _q_integer(kind):
            kind = _Q_TYPE_CODES.get(kind)
        if not isinstance(kind, str) or kind not in (*_Q_TYPE_CODES.values(), "number"):
            raise MongoError(f"Unknown BSON type {kind!r}")
        names.append(kind)
    if not names:
        raise MongoError("$type requires at least one type")
    return names


_Q_COMPARE = {"$gt", "$gte", "$lt", "$lte"}
_Q_BIT_PREDICATES = {"$bitsAllSet", "$bitsAllClear", "$bitsAnySet", "$bitsAnyClear"}
_Q_FIELD_OPERATORS = (
    _Q_COMPARE
    | _Q_BIT_PREDICATES
    | {
        "$eq",
        "$ne",
        "$in",
        "$nin",
        "$regex",
        "$options",
        "$not",
        "$exists",
        "$type",
        "$size",
        "$all",
        "$elemMatch",
        "$mod",
    }
)


def _q_elem_value_form(spec):
    return bool(spec) and all(field in _Q_FIELD_OPERATORS for field in spec)


def _q_validate_elem(spec):
    _q_require_obj(spec, "$elemMatch")
    if _q_elem_value_form(spec):
        _q_validate_condition(spec)
    else:
        validate_filter(spec)


def _q_bit_positions(operand):
    if isinstance(operand, bytes):
        return [
            offset * 8 + bit for offset, byte in enumerate(operand) for bit in range(8) if byte & (1 << bit)
        ]
    if isinstance(operand, list):
        if any(not _q_integer(pos) or not 0 <= pos < (1 << 31) for pos in operand):
            raise MongoError("Bit test positions must be nonnegative integers")
        return operand
    if _q_integer(operand) and 0 <= operand < (1 << 63):
        return [position for position in range(operand.bit_length()) if operand & (1 << position)]
    raise MongoError("Bit tests require a nonnegative integer, binary, or array of positions")


def _q_validate_condition(condition):
    if isinstance(condition, Regex):
        _q_regex(condition.pattern, condition.options)
        return
    if not _q_predicate_form(condition):
        return
    for op, operand in condition.items():
        if op not in _Q_FIELD_OPERATORS:
            raise MongoError(f"Unknown query operator {op!r}")
        if op in ("$in", "$nin", "$all"):
            for item in _q_require_list(operand, op):
                if isinstance(item, Regex):
                    _q_regex(item.pattern, item.options)
                elif _q_predicate_form(item):
                    if op == "$all" and set(item) == {"$elemMatch"}:
                        _q_validate_elem(item["$elemMatch"])
                    else:
                        raise MongoError(f"Cannot nest an operator inside {op}")
        elif op == "$regex":
            if not isinstance(operand, (str, Regex)):
                raise MongoError("$regex requires a string or BSON regular expression")
            pattern = operand.pattern if isinstance(operand, Regex) else operand
            options = condition.get("$options", operand.options if isinstance(operand, Regex) else "")
            if not isinstance(options, str):
                raise MongoError("$options requires a string")
            _q_regex(pattern, options)
        elif op == "$options" and "$regex" not in condition:
            raise MongoError("$options requires $regex")
        elif op == "$not":
            if not isinstance(operand, Regex) and not _q_predicate_form(operand):
                raise MongoError("$not requires a regular expression or operator document")
            _q_validate_condition(operand)
        elif op == "$type":
            _q_type_names(operand)
        elif op == "$size" and (not _q_integer(operand) or operand < 0):
            raise MongoError("$size requires a nonnegative integer")
        elif op == "$elemMatch":
            _q_validate_elem(operand)
        elif op == "$mod":
            if (
                not isinstance(operand, list)
                or len(operand) != 2
                or any(not _q_integer(arg) for arg in operand)
                or operand[0] == 0
            ):
                raise MongoError("$mod requires [nonzero integer divisor, integer remainder]")
        elif op in _Q_BIT_PREDICATES:
            _q_bit_positions(operand)


def validate_filter(query):
    _q_require_obj(query, "Query")
    for field, condition in query.items():
        if field in ("$and", "$or", "$nor"):
            if not _q_require_list(condition, field):
                raise MongoError(f"{field} requires a nonempty array")
            for part in condition:
                validate_filter(part)
        elif field == "$expr":
            validate_expression(condition)
        elif field == "$jsonSchema":
            validate_json_schema(condition)
        elif field == "$comment":
            continue
        elif field.startswith("$"):
            raise MongoError(f"Unknown top-level query operator {field!r}")
        else:
            _q_validate_condition(condition)


def _q_with_elements(values, traverse=True):
    for value in values:
        yield _q_unwrap(value)
        if traverse and isinstance(value, list):
            yield from value


def _q_match_equality(values, operand, *, regex=False, traverse=True):
    if regex and isinstance(operand, Regex):
        return any(
            _q_regex_search(value, operand.pattern, operand.options)
            for value in _q_with_elements(values, traverse)
        )
    return any(
        (operand is None and value is MISSING) or bson_equal(value, operand)
        for value in _q_with_elements(values, traverse)
    )


def _q_match_comparison(value, operand, op):
    if value is MISSING:
        value = None if operand is None else value
        if value is MISSING:
            return False
    rank = _Q_RANK[bson_type(operand)]
    if rank not in (-1, 15) and _Q_RANK[bson_type(value)] != rank:
        return False
    if rank == 2:
        value_nan, operand_nan = _q_decimal(value).is_nan(), _q_decimal(operand).is_nan()
        if value_nan or operand_nan:
            return value_nan and operand_nan and op in ("$gte", "$lte")
    order = compare(value, operand)
    return {"$gt": order > 0, "$gte": order >= 0, "$lt": order < 0, "$lte": order <= 0}[op]


def _q_match_elem(item, condition, variables):
    if _q_elem_value_form(condition):
        return _q_match_condition([item], condition, variables, traverse=False)
    if isinstance(item, list):
        item = {str(i): value for i, value in enumerate(item)}
    return isinstance(item, dict) and matches(item, condition, variables)


def _q_match_bits(value, positions, op):
    if isinstance(value, bytes):

        def bit(pos):
            return (pos // 8 < len(value)) and bool(value[pos // 8] & (1 << (pos % 8)))
    elif numeric(value):
        number = _q_decimal(value)
        if not number.is_finite() or not -(1 << 63) <= number < (1 << 63) or number != int(number):
            return False
        integer = int(number)

        def bit(pos):
            return bool(integer & (1 << pos)) if pos < 64 else integer < 0
    else:
        return False
    if op == "$bitsAllSet":
        return all(bit(pos) for pos in positions)
    if op == "$bitsAllClear":
        return not any(bit(pos) for pos in positions)
    if op == "$bitsAnySet":
        return any(bit(pos) for pos in positions)
    return not all(bit(pos) for pos in positions)


def _q_match_mod(value, operand):
    if not numeric(value):
        return False
    number = _q_decimal(value)
    if not number.is_finite() or not -(1 << 63) <= number < (1 << 63):
        return False
    divisor, expected = operand
    integer = int(number)
    remainder = abs(integer) % abs(divisor)
    return (-remainder if integer < 0 else remainder) == expected


def _q_match_condition(values, condition, variables=None, *, traverse=True):
    if not _q_predicate_form(condition):
        return _q_match_equality(values, condition, regex=True, traverse=traverse)
    for op, operand in condition.items():
        if op in ("$eq", "$ne"):
            matched = _q_match_equality(values, operand, traverse=traverse) == (op == "$eq")
        elif op in _Q_COMPARE:
            matched = any(
                _q_match_comparison(value, operand, op) for value in _q_with_elements(values, traverse)
            )
        elif op in ("$in", "$nin"):
            selected = any(
                _q_match_equality(values, probe, regex=True, traverse=traverse) for probe in operand
            )
            matched = selected == (op == "$in")
        elif op == "$not":
            matched = not _q_match_condition(values, operand, variables, traverse=traverse)
        elif op == "$exists":
            matched = any(value is not MISSING for value in values) == bool(operand)
        elif op == "$regex":
            pattern = operand.pattern if isinstance(operand, Regex) else operand
            options = condition.get("$options", operand.options if isinstance(operand, Regex) else "")
            matched = any(
                _q_regex_search(value, pattern, options) for value in _q_with_elements(values, traverse)
            )
        elif op == "$options":
            continue
        elif op == "$type":
            names = _q_type_names(operand)
            matched = any(
                bson_type(value) in names or ("number" in names and numeric(value))
                for value in _q_with_elements(values, traverse)
            )
        elif op == "$size":
            matched = any(
                isinstance(value, list) and len(value) == operand for value in map(_q_unwrap, values)
            )
        elif op == "$elemMatch":
            matched = any(
                _q_match_elem(item, operand, variables)
                for value in map(_q_unwrap, values)
                if isinstance(value, list)
                for item in value
            )
        elif op == "$all":
            matched = bool(operand) and all(
                _q_match_condition(values, item, variables, traverse=traverse)
                if isinstance(item, dict) and set(item) == {"$elemMatch"}
                else _q_match_equality(values, item, regex=True, traverse=traverse)
                for item in operand
            )
        elif op == "$mod":
            matched = any(_q_match_mod(value, operand) for value in _q_with_elements(values, traverse))
        elif op in _Q_BIT_PREDICATES:
            positions = _q_bit_positions(operand)
            matched = any(_q_match_bits(value, positions, op) for value in _q_with_elements(values, traverse))
        else:
            raise MongoError(f"Unknown query operator {op!r}")
        if not matched:
            return False
    return True


def matches(document, query, variables=None):
    for field, condition in query.items():
        if field == "$and":
            matched = all(matches(document, part, variables) for part in condition)
        elif field in ("$or", "$nor"):
            matched = any(matches(document, part, variables) for part in condition) == (field == "$or")
        elif field == "$expr":
            matched = agg_truth(evaluate(condition, document, variables))
        elif field == "$jsonSchema":
            matched = matches_json_schema(document, condition)
        elif field == "$comment":
            continue
        elif field.startswith("$"):
            raise MongoError(f"Unknown top-level query operator {field!r}")
        else:
            values = _q_query_values(document, field.split("."), missing=True)
            matched = _q_match_condition(values, condition, variables)
        if not matched:
            return False
    return True


def validate_sort(spec):
    _q_require_obj(spec, "sort")
    for field, direction in spec.items():
        _q_check_path(field)
        if not numeric(direction) or _q_decimal(direction).is_nan() or _q_decimal(direction) not in (1, -1):
            raise MongoError(f"Unsupported sort specification for {field!r}")
    if "$natural" in spec and len(spec) != 1:
        raise MongoError("$natural cannot be combined with another sort field")


def _q_sort_candidates(value, parts, trail=(), *, expand=True):
    if not parts:
        if isinstance(value, list) and expand:
            if not value:
                yield (0,), (*trail, (id(value), -1))
            for i, item in enumerate(value):
                yield value_key(item), (*trail, (id(value), i))
        else:
            yield value_key(None if value is MISSING else value), trail
    elif isinstance(value, dict):
        head, *tail = parts
        yield from _q_sort_candidates(value.get(head, MISSING), tail, trail)
    elif isinstance(value, list):
        head, *tail = parts
        index = _q_index(head)
        if index is not None and str(index) == head:
            if index < len(value) and any(isinstance(item, dict) and head in item for item in value):
                raise MongoError(f"Ambiguous numeric sort field {head!r} in array", 16746)
            if index < len(value):
                yield from _q_sort_candidates(value[index], tail, trail, expand=False)
            else:
                yield value_key(None), trail
        elif value:
            for i, item in enumerate(value):
                new_trail = (*trail, (id(value), i))
                if isinstance(item, dict):
                    yield from _q_sort_candidates(item, parts, new_trail)
                else:
                    yield value_key(None), new_trail
        else:
            yield value_key(None), (*trail, (id(value), -1))
    else:
        yield value_key(None), trail


def _q_shared_sort_trail(left, right):
    for (a, i), (b, j) in zip(left, right):
        if a != b:
            raise MongoError("Cannot sort with parallel arrays")
        if i != j:
            return None
    return left if len(left) >= len(right) else right


def _q_sort_key(document, fields, directions):
    combinations = [((), ())]
    for field in fields:
        choices = list(_q_sort_candidates(document, field.split(".")))
        if all(not trail for _, trail in choices):
            key = choices[0][0]
            combinations = [((*prefix, key), trail) for prefix, trail in combinations]
            continue
        joined = []
        for prefix, previous in combinations:
            for key, trail in choices:
                shared = _q_shared_sort_trail(previous, trail)
                if shared is not None:
                    joined.append(((*prefix, key), shared))
        combinations = joined
    keys = [key for key, _ in combinations]
    for offset, descending in reversed(list(enumerate(directions))):
        keys.sort(key=lambda key: key[offset], reverse=descending)
    return keys[0]


def sort_documents(documents, spec):
    validate_sort(spec)
    ordered = list(documents)
    if "$natural" in spec:
        return ordered[::-1] if _q_decimal(spec["$natural"]) < 0 else ordered
    fields = tuple(spec)
    directions = tuple(_q_decimal(direction) < 0 for direction in spec.values())
    decorated = [(document, _q_sort_key(document, fields, directions)) for document in ordered]
    for offset, descending in reversed(list(enumerate(directions))):
        decorated.sort(key=lambda element: element[1][offset], reverse=descending)
    return [document for document, _ in decorated]


def _q_positional_index(document, path, array, query, variables=None):
    """Find the array element bound by a positive query predicate."""

    def candidates(query_part):
        found = []
        for field, condition in query_part.items():
            if field == "$and":
                found.extend(indexes for part in condition for indexes in candidates(part))
            elif field == "$or":
                for part in condition:
                    if matches(document, part, variables):
                        found.extend(candidates(part))
            elif field == path:
                if _q_predicate_form(condition) and "$elemMatch" in condition:
                    found.append(
                        {
                            i
                            for i, item in enumerate(array)
                            if _q_match_elem(item, condition["$elemMatch"], variables)
                        }
                    )
                else:
                    positive = _q_positional_condition(condition)
                    if positive is not MISSING:
                        found.append(
                            {
                                i
                                for i, item in enumerate(array)
                                if _q_match_condition([item], positive, variables, traverse=False)
                            }
                        )
            elif field.startswith(path + "."):
                subpath = field[len(path) + 1 :]
                positive = _q_positional_condition(condition)
                if positive is not MISSING:
                    found.append(
                        {
                            i
                            for i, item in enumerate(array)
                            if _q_match_condition(
                                _q_query_values(item, subpath.split("."), missing=True), positive, variables
                            )
                        }
                    )
        return found

    possible = set()
    for indexes in candidates(query or {}):
        if indexes:
            possible = possible.intersection(indexes) or indexes
    if not possible:
        raise MongoError("The positional operator did not find the match needed from the query", 2)
    return min(possible)


def _q_positional_condition(condition):
    if not _q_predicate_form(condition):
        return condition
    positive = {
        op: operand
        for op, operand in condition.items()
        if op not in ("$ne", "$nin", "$not", "$exists", "$size")
    }
    return positive or MISSING


def _q_projection_fields(spec, prefix=""):
    for field, value in spec.items():
        path = prefix + field
        if isinstance(value, dict) and value and not _q_predicate_form(value):
            yield from _q_projection_fields(value, path + ".")
        else:
            yield path, value


def _q_projection_flag(value):
    if type(value) is bool:
        return int(value)
    if numeric(value):
        number = _q_decimal(value)
        return int(not number.is_zero())
    return None


def _q_assert_no_path_overlap(paths, code=40):
    seen = {}
    for path in sorted(paths):
        node = seen
        for part in path.split("."):
            if None in node:
                raise MongoError(f"Conflicting paths {node[None]!r} and {path!r}", code)
            node = node.setdefault(part, {})
        if node:
            raise MongoError(f"Conflicting projection or update path {path!r}", code)
        node[None] = path


def _q_check_path(path):
    if not path or any(not part for part in path.split(".")):
        raise MongoError(f"Invalid empty component in path {path!r}")


def _q_slice(array, operand):
    if isinstance(operand, list):
        start, count = operand
        if start < 0:
            start = max(0, len(array) + start)
        return array[start : start + count]
    if operand < 0:
        return array[operand:]
    return array[:operand]


def _q_validate_slice(operand):
    if _q_integer(operand):
        return
    if (
        isinstance(operand, list)
        and len(operand) == 2
        and all(_q_integer(item) for item in operand)
        and operand[1] > 0
    ):
        return
    raise MongoError("$slice projection requires an integer or [offset, positive limit]")


def _q_parse_projection(spec, aggregation):
    _q_require_obj(spec, "Projection")
    if aggregation and not spec:
        raise MongoError("$project requires at least one field")
    mode = None
    entries = []
    positions = 0
    for path, value in _q_projection_fields(spec):
        if path.endswith(".$"):
            if aggregation or _q_projection_flag(value) != 1:
                raise MongoError("Invalid positional projection")
            path = path[:-2]
            kind = "position"
            positions += 1
        elif not aggregation and isinstance(value, dict) and set(value) == {"$slice"}:
            _q_validate_slice(value["$slice"])
            kind = "slice"
        elif not aggregation and isinstance(value, dict) and set(value) == {"$elemMatch"}:
            if "." in path:
                raise MongoError("$elemMatch projection requires a top-level field")
            _q_validate_elem(value["$elemMatch"])
            kind = "element"
        else:
            if isinstance(value, dict) and not value:
                raise MongoError(f"Invalid empty sub-projection: {path}", 51270)
            projection_flag = _q_projection_flag(value)
            if projection_flag is not None:
                kind = "include" if projection_flag else "exclude"
            else:
                validate_expression(value)
                kind = "expression"
        _q_check_path(path)
        if any(part.startswith("$") for part in path.split(".")):
            raise MongoError(f"Invalid projection path {path!r}")
        if (path != "_id" or kind == "expression") and kind != "slice":
            new_mode = "exclusion" if kind == "exclude" else "inclusion"
            if mode is not None and mode != new_mode:
                raise MongoError("Cannot mix inclusion and exclusion in projection", 31254)
            mode = new_mode
        entries.append((path, kind, value))
    if positions > 1:
        raise MongoError("Only one positional projection is supported")
    _q_assert_no_path_overlap([path for path, _, _ in entries], 31249)
    if mode is None:
        mode = "inclusion" if any(kind == "include" for _, kind, _ in entries) else "exclusion"
    return mode, entries


def validate_projection(spec, *, aggregation=False):
    _q_parse_projection(spec, aggregation)


def _q_projection_tree(paths):
    result = {}
    for path in paths:
        node = result
        for part in path.split("."):
            node = node.setdefault(part, {})
    return result


def _q_pick_fields(value, tree, computed):
    if not tree and not computed:
        return deepcopy(value)
    if isinstance(value, dict):
        result = {}
        for field, item in value.items():
            if field in tree:
                selected = _q_pick_fields(item, tree[field], computed.get(field, {}))
                if selected is not MISSING:
                    result[field] = selected
            elif computed.get(field):
                result[field] = _q_pick_fields(item, {}, computed[field])
        for field, children in computed.items():
            if children and field not in result:
                result[field] = _q_pick_fields({}, {}, children)
        return result
    if isinstance(value, list):
        result = [_q_pick_fields(item, tree, computed) for item in value]
        return [item for item in result if item is not MISSING]
    return _q_pick_fields({}, {}, computed) if computed else MISSING


def _q_project_slice(value, tree):
    if not tree:
        return
    if isinstance(value, list):
        for item in value:
            _q_project_slice(item, tree)
    elif isinstance(value, dict):
        for field, nested in tree.items():
            if field in value and nested:
                if "$slice" in nested and isinstance(value[field], list):
                    value[field] = _q_slice(value[field], nested["$slice"])
                else:
                    _q_project_slice(value[field], nested)


def project(document, spec, variables=None, query=None, *, aggregation=False):
    mode, entries = _q_parse_projection(spec, aggregation)
    slices = {}
    if mode == "inclusion":
        paths = [path for path, kind, _ in entries if kind in ("include", "slice")]
        if not any(path == "_id" or path.startswith("_id.") for path, _, _ in entries):
            paths.append("_id")
        computed = [path for path, kind, _ in entries if kind == "expression"]
        if paths or computed:
            result = _q_pick_fields(document, _q_projection_tree(paths), _q_projection_tree(computed))
        else:
            result = {}
    else:
        result = deepcopy(document)
    for path, kind, value in entries:
        if kind == "exclude":
            unset_path(result, path)
        elif kind == "expression":
            evaluated = evaluate(value, document, variables)
            if evaluated is not MISSING:
                set_path(result, path, evaluated)
        elif kind == "slice":
            current = slices
            for part in path.split("."):
                current = current.setdefault(part, {})
            current["$slice"] = value["$slice"]
        elif kind in ("position", "element"):
            array = get_path(document, path)
            if isinstance(array, list):
                if kind == "position":
                    index = _q_positional_index(document, path, array, query, variables)
                    set_path(result, path, [array[index]])
                else:
                    selected = next(
                        (item for item in array if _q_match_elem(item, value["$elemMatch"], variables)),
                        MISSING,
                    )
                    if selected is not MISSING:
                        set_path(result, path, [selected])
    _q_project_slice(result, slices)
    return result


_Q_SCHEMA_ANNOTATIONS = {"description", "title"}
_Q_SCHEMA_COUNTS = {"maxItems", "minItems", "maxLength", "minLength", "maxProperties", "minProperties"}
_Q_SCHEMA_BOOLEANS = {"uniqueItems", "exclusiveMinimum", "exclusiveMaximum"}
_Q_JSON_TYPES = {"object", "array", "string", "number", "boolean", "null"}
_Q_SCHEMA_NUMBERS = {"minimum", "maximum", "multipleOf"}


def validate_json_schema(spec):
    _q_require_obj(spec, "$jsonSchema")
    for field, value in spec.items():
        if field in _Q_SCHEMA_ANNOTATIONS or field == "pattern":
            if not isinstance(value, str):
                raise MongoError(f"JSON schema {field} requires a string")
            if field == "pattern":
                _q_regex(value, "")
        elif field in _Q_SCHEMA_COUNTS:
            if not _q_integer(value) or value < 0:
                raise MongoError(f"JSON schema {field} requires a nonnegative integer")
        elif field in _Q_SCHEMA_BOOLEANS:
            if type(value) is not bool:
                raise MongoError(f"JSON schema {field} requires a boolean")
            required = {"exclusiveMinimum": "minimum", "exclusiveMaximum": "maximum"}.get(field)
            if required and required not in spec:
                raise MongoError(f"JSON schema {field} requires {required}")
        elif field in _Q_SCHEMA_NUMBERS:
            if (
                not numeric(value)
                or not _q_decimal(value).is_finite()
                or (field == "multipleOf" and compare(value, 0) <= 0)
            ):
                raise MongoError(f"Invalid JSON schema {field}")
        elif field == "bsonType":
            _q_type_names(value)
        elif field == "type":
            types = value if isinstance(value, list) else [value]
            if not types or any(not isinstance(t, str) or t not in _Q_JSON_TYPES for t in types):
                raise MongoError("Invalid JSON schema type")
        elif field == "required":
            if not isinstance(value, list) or not value or any(not isinstance(key, str) for key in value):
                raise MongoError("JSON schema required needs an array of property names")
        elif field in ("enum", "allOf", "anyOf", "oneOf"):
            if not isinstance(value, list) or not value:
                raise MongoError(f"JSON schema {field} requires a nonempty array")
            if field != "enum":
                for part in value:
                    validate_json_schema(part)
        elif field == "not":
            validate_json_schema(value)
        elif field in ("properties", "patternProperties", "dependencies"):
            for key, part in _q_require_obj(value, field).items():
                if field == "patternProperties":
                    _q_regex(key, "")
                if field == "dependencies" and isinstance(part, list):
                    if any(not isinstance(dep, str) for dep in part):
                        raise MongoError("JSON schema dependency needs an array of property names")
                else:
                    validate_json_schema(part)
        elif field in ("additionalProperties", "additionalItems"):
            if type(value) is not bool:
                validate_json_schema(value)
        elif field == "items":
            for part in value if isinstance(value, list) else [value]:
                validate_json_schema(part)
        else:
            raise MongoError(f"Unsupported JSON schema keyword {field!r}")
    if "bsonType" in spec and "type" in spec:
        raise MongoError("JSON schema cannot specify both type and bsonType")


def _q_schema_type_matches(value, names):
    return bson_type(value) in names or ("number" in names and numeric(value))


def _q_schema_extra(items, constraint):
    if constraint is False:
        return not items
    if isinstance(constraint, dict):
        return all(matches_json_schema(item, constraint) for item in items)
    return True


def _q_schema_object(value, spec):
    if len(value) < spec.get("minProperties", 0) or len(value) > spec.get("maxProperties", len(value)):
        return False
    if any(field not in value for field in spec.get("required", ())):
        return False
    properties = spec.get("properties", {})
    patterns = spec.get("patternProperties", {})
    extra = []
    for field, item in value.items():
        matching_patterns = [
            rule for pattern, rule in patterns.items() if _q_regex(pattern, "").search(field)
        ]
        if field in properties:
            matching_patterns.append(properties[field])
        if not matching_patterns:
            extra.append(item)
        elif any(not matches_json_schema(item, rule) for rule in matching_patterns):
            return False
    if not _q_schema_extra(extra, spec.get("additionalProperties", True)):
        return False
    for field, dependency in spec.get("dependencies", {}).items():
        if field in value:
            if isinstance(dependency, list):
                if any(key not in value for key in dependency):
                    return False
            elif not matches_json_schema(value, dependency):
                return False
    return True


def _q_schema_array(value, spec):
    if len(value) < spec.get("minItems", 0) or len(value) > spec.get("maxItems", len(value)):
        return False
    if spec.get("uniqueItems") and len({value_key(item) for item in value}) != len(value):
        return False
    items = spec.get("items")
    if isinstance(items, dict):
        return all(matches_json_schema(item, items) for item in value)
    if isinstance(items, list):
        return all(matches_json_schema(item, rule) for item, rule in zip(value, items)) and _q_schema_extra(
            value[len(items) :], spec.get("additionalItems", True)
        )
    return True


def matches_json_schema(value, spec):
    if "bsonType" in spec and not _q_schema_type_matches(value, _q_type_names(spec["bsonType"])):
        return False
    if "type" in spec:
        requested = spec["type"]
        requested = requested if isinstance(requested, list) else [requested]
        if not _q_schema_type_matches(value, {"bool" if name == "boolean" else name for name in requested}):
            return False
    if "enum" in spec and not any(bson_equal(value, item) for item in spec["enum"]):
        return False
    if "not" in spec and matches_json_schema(value, spec["not"]):
        return False
    for op in ("allOf", "anyOf", "oneOf"):
        if op in spec:
            matched = sum(matches_json_schema(value, rule) for rule in spec[op])
            if (
                (op == "allOf" and matched != len(spec[op]))
                or (op == "anyOf" and not matched)
                or (op == "oneOf" and matched != 1)
            ):
                return False
    if isinstance(value, dict):
        return _q_schema_object(value, spec)
    if isinstance(value, list):
        return _q_schema_array(value, spec)
    if isinstance(value, str):
        return spec.get("minLength", 0) <= len(value) <= spec.get("maxLength", len(value)) and (
            "pattern" not in spec or _q_regex(spec["pattern"], "").search(value) is not None
        )
    if numeric(value):
        if _q_decimal(value).is_nan() and _Q_SCHEMA_NUMBERS.intersection(spec):
            return False
        if "minimum" in spec:
            order = compare(value, spec["minimum"])
            if order < int(spec.get("exclusiveMinimum", False)):
                return False
        if "maximum" in spec:
            order = compare(value, spec["maximum"])
            if order > -int(spec.get("exclusiveMaximum", False)):
                return False
        if "multipleOf" in spec:
            number = _q_decimal(value)
            if not number.is_finite():
                return False
            num, den = number.as_integer_ratio()
            multiple_num, multiple_den = _q_decimal(spec["multipleOf"]).as_integer_ratio()
            if (num * multiple_den) % (den * multiple_num):
                return False
    return True


_Q_ARRAY_IDENTIFIER = re.compile(r"[a-z][a-zA-Z0-9]*\Z")
_Q_UPDATE_OPERATORS = {
    "$set",
    "$setOnInsert",
    "$unset",
    "$inc",
    "$mul",
    "$min",
    "$max",
    "$rename",
    "$currentDate",
    "$push",
    "$addToSet",
    "$pop",
    "$pull",
    "$pullAll",
    "$bit",
}
_Q_NO_CREATE = {"$unset", "$pop", "$pull", "$pullAll"}


def _q_validate_update_path(path):
    _q_check_path(path)
    marker_count = 0
    for i, part in enumerate(path.split(".")):
        if not part.startswith("$"):
            continue
        if i == 0:
            raise MongoError(f"Invalid top-level update field {part!r}")
        if part == "$":
            marker_count += 1
        elif part != "$[]" and not (
            part.startswith("$[") and part.endswith("]") and _Q_ARRAY_IDENTIFIER.fullmatch(part[2:-1])
        ):
            raise MongoError(f"Invalid positional placeholder {part!r}")
    if marker_count > 1:
        raise MongoError(f"Too many positional placeholders in {path!r}")


def _q_array_filter_fields(query):
    for field, condition in query.items():
        if field in ("$and", "$or", "$nor"):
            for part in condition:
                yield from _q_array_filter_fields(part)
        elif field.startswith("$"):
            raise MongoError(f"{field} is not supported in array filters")
        else:
            yield field.split(".")[0]


def _q_array_filter_map(array_filters):
    if array_filters is None:
        return {}
    result = {}
    for item in _q_require_list(array_filters, "arrayFilters"):
        validate_filter(item)
        ids = set(_q_array_filter_fields(item))
        if len(ids) != 1:
            raise MongoError("Every array filter must reference exactly one placeholder identifier")
        identifier = ids.pop()
        if identifier in result or not _Q_ARRAY_IDENTIFIER.fullmatch(identifier):
            raise MongoError(f"Duplicate or invalid array filter identifier {identifier!r}")
        result[identifier] = item
    return result


def _q_validate_push(op, operand):
    if not _q_predicate_form(operand):
        return
    if "$each" not in operand:
        return  # A document without $each is an array element.
    _q_require_list(operand["$each"], "$each")
    for modifier, value in operand.items():
        if modifier == "$each":
            continue
        if op != "$push" or modifier not in ("$position", "$slice", "$sort"):
            raise MongoError(f"Unsupported {op} modifier {modifier!r}")
        if modifier == "$sort" and isinstance(value, dict):
            if not value or "$natural" in value:
                raise MongoError("$push $sort requires a nonempty field ordering")
            validate_sort(value)
        elif modifier == "$sort" and (not _q_integer(value) or value not in (-1, 1)):
            raise MongoError("$push $sort requires 1, -1, or a document")
        elif modifier in ("$position", "$slice") and not _q_integer(value):
            raise MongoError(f"{modifier} requires an integer")


def _q_validate_update_operand(op, operand):
    if op in ("$inc", "$mul") and not numeric(operand):
        raise MongoError(f"{op} requires a numeric operand", 14)
    if op == "$currentDate":
        if operand is not True and not (
            isinstance(operand, dict)
            and set(operand) == {"$type"}
            and operand["$type"] in ("date", "timestamp")
        ):
            raise MongoError("$currentDate requires true or a date/timestamp type")
    elif op in ("$push", "$addToSet"):
        _q_validate_push(op, operand)
    elif op == "$pop" and (not _q_integer(operand) or operand not in (-1, 1)):
        raise MongoError("$pop requires either -1 or 1")
    elif op == "$pullAll":
        _q_require_list(operand, op)
    elif op == "$pull":
        if isinstance(operand, dict) and not _q_predicate_form(operand):
            validate_filter(operand)
        else:
            _q_validate_condition(operand)
    elif op == "$bit":
        _q_require_obj(operand, op)
        if not operand or any(
            kind not in ("and", "or", "xor") or not _q_integer(mask) for kind, mask in operand.items()
        ):
            raise MongoError("$bit needs and, or, or xor operations with integer masks")


def validate_update(update, array_filters=None):
    filters = _q_array_filter_map(array_filters)
    if isinstance(update, list):
        if array_filters is not None:
            raise MongoError("arrayFilters cannot be used with a pipeline update")
        validate_update_pipeline(update)
        return
    _q_require_obj(update, "Update")
    if not _q_predicate_form(update):
        if array_filters is not None:
            raise MongoError("arrayFilters cannot be used with a replacement")
        if any(key.startswith("$") for key in update):
            raise MongoError("A replacement document cannot contain update operators")
        return
    paths = []
    referenced = set()
    for op, fields in update.items():
        if op not in _Q_UPDATE_OPERATORS:
            raise MongoError(f"Unknown update operator {op!r}")
        for path, operand in _q_require_obj(fields, op).items():
            _q_validate_update_path(path)
            paths.append(path)
            referenced.update(
                part[2:-1] for part in path.split(".") if part.startswith("$[") and part != "$[]"
            )
            if op == "$rename":
                if not isinstance(operand, str):
                    raise MongoError("$rename requires a string field path")
                _q_check_path(operand)
                if any(part.startswith("$") for field in (path, operand) for part in field.split(".")):
                    raise MongoError("$rename does not support positional placeholders")
                paths.append(operand)
            else:
                _q_validate_update_operand(op, operand)
    _q_assert_no_path_overlap(paths)
    missing = referenced - filters.keys()
    unused = filters.keys() - referenced
    if missing:
        raise MongoError(f"No array filter found for {min(missing)!r}")
    if unused:
        raise MongoError(f"Array filter {min(unused)!r} was not used")


def _q_update_child(value, part):
    if isinstance(value, dict):
        return value.get(part, MISSING)
    if isinstance(value, list):
        index = _q_index(part)
        if index is not None and index < len(value):
            return value[index]
    return MISSING


def _q_update_value(document, parts, *, forbid_arrays=False):
    for part in parts:
        if forbid_arrays and isinstance(document, list):
            raise MongoError("$rename cannot traverse an array")
        document = _q_update_child(document, part)
    return document


def _q_expand_update_parts(document, path, query, filters, inserting, variables):
    def walk(current, remaining, resolved):
        if not remaining:
            yield tuple(resolved)
            return
        head, *tail = remaining
        if not head.startswith("$"):
            yield from walk(_q_update_child(current, head), tail, [*resolved, head])
            return
        if current is MISSING:
            raise MongoError(f"The path {'.'.join(resolved)!r} must exist for a positional update")
        if not isinstance(current, list):
            raise MongoError(f"The path {'.'.join(resolved)!r} must contain an array for a positional update")
        if head == "$":
            if inserting:
                raise MongoError("The positional operator cannot be used to insert an upserted document")
            indexes = [_q_positional_index(document, ".".join(resolved), current, query, variables)]
        elif head == "$[]":
            indexes = range(len(current))
        else:
            identifier = head[2:-1]
            indexes = [
                i
                for i, item in enumerate(current)
                if matches({identifier: item}, filters[identifier], variables)
            ]
        for index in indexes:
            yield from walk(current[index], tail, [*resolved, str(index)])

    return list(walk(document, path.split("."), []))


def _q_write_update_path(document, parts, value):
    current = document
    for offset, part in enumerate(parts):
        last = offset == len(parts) - 1
        if isinstance(current, list):
            key = _q_index(part)
            if key is None:
                if value is MISSING:
                    return
                raise MongoError(f"Cannot create field {part!r} in an array", 28)
            if key >= len(current):
                if value is MISSING:
                    return
                if key - len(current) > 1_500_000:
                    raise MongoError(
                        "Updating the array at this index would exceed the document limit", 10334
                    )
                next_value = value if last else {}
                current.extend([None] * (key - len(current)))
                current.append(next_value)
            elif last:
                current[key] = None if value is MISSING else value
        elif isinstance(current, dict):
            key = part
            if last:
                if value is MISSING:
                    current.pop(key, None)
                else:
                    current[key] = value
            elif key not in current:
                if value is MISSING:
                    return
                current[key] = {}
        else:
            if value is MISSING:
                return
            raise MongoError(f"Cannot create field {part!r} in a scalar value", 28)
        current = current[key] if not last else None


def _q_update_arithmetic(op, left, right):
    if left is MISSING:
        if op == "$inc":
            return right
        if isinstance(right, Decimal128):
            return Decimal128.from_decimal(Decimal(0))
        return type(right)(0)
    if not numeric(left):
        raise MongoError(f"Cannot apply {op} to a nonnumeric field", 14)
    if isinstance(left, Decimal128) or isinstance(right, Decimal128):
        with localcontext() as context:
            context.prec = 34
            a, b = _q_decimal(left), _q_decimal(right)
            result = a + b if op == "$inc" else a * b
            return Decimal128.from_decimal(result)
    result = left + right if op == "$inc" else left * right
    if isinstance(result, float):
        return result
    if not -(1 << 63) <= result < (1 << 63):
        raise MongoError(f"Integer overflow applying {op}", 14)
    if isinstance(left, Int64) or isinstance(right, Int64) or not -(1 << 31) <= result < (1 << 31):
        return Int64(result)
    return result


def _q_pop_slice_sort(array, instruction):
    values = deepcopy(instruction["$each"])
    if "$position" in instruction:
        position = instruction["$position"]
        position = min(position, len(array)) if position >= 0 else max(0, len(array) + position)
        array[position:position] = values
    else:
        array.extend(values)
    order = instruction.get("$sort")
    if isinstance(order, dict):
        array = sort_documents(array, order)
    elif order in (-1, 1):
        array.sort(key=value_key, reverse=order == -1)
    if "$slice" in instruction:
        array = _q_slice(array, instruction["$slice"])
    return array


def _q_array_update(op, current, operand, variables):
    if current is MISSING:
        if op in _Q_NO_CREATE:
            return MISSING
        current = []
    if not isinstance(current, list):
        raise MongoError(f"Cannot apply {op} to a non-array field", 2)
    if op == "$push":
        if isinstance(operand, dict) and "$each" in operand:
            return _q_pop_slice_sort(list(current), operand)
        return [*current, deepcopy(operand)]
    if op == "$addToSet":
        values = operand["$each"] if isinstance(operand, dict) and "$each" in operand else [operand]
        seen = {value_key(item) for item in current}
        result = list(current)
        for item in values:
            key = value_key(item)
            if key not in seen:
                result.append(deepcopy(item))
                seen.add(key)
        return result
    if op == "$pop":
        return current[1:] if operand < 0 else current[:-1]
    if op == "$pullAll":
        keys = {value_key(item) for item in operand}
        return [item for item in current if value_key(item) not in keys]
    if isinstance(operand, dict) and not _q_predicate_form(operand):
        return [
            item for item in current if not isinstance(item, dict) or not matches(item, operand, variables)
        ]
    traverse = _q_predicate_form(operand) or isinstance(operand, Regex)
    return [item for item in current if not _q_match_condition([item], operand, variables, traverse=traverse)]


def _q_apply_update_modifier(op, current, operand, now, stamp, variables):
    if op in ("$set", "$setOnInsert"):
        return deepcopy(operand)
    if op == "$unset":
        return MISSING
    if op in ("$inc", "$mul"):
        return _q_update_arithmetic(op, current, operand)
    if op == "$currentDate":
        return stamp if isinstance(operand, dict) and operand["$type"] == "timestamp" else now
    if op == "$min":
        return deepcopy(operand) if current is MISSING or compare(operand, current) < 0 else current
    if op == "$max":
        return deepcopy(operand) if current is MISSING or compare(operand, current) > 0 else current
    if op == "$bit":
        if current is MISSING:
            current = 0
        if not _q_integer(current):
            raise MongoError("Cannot apply $bit to a noninteger field", 14)
        use_long = isinstance(current, Int64) or any(isinstance(mask, Int64) for mask in operand.values())
        value = current
        for kind, mask in operand.items():
            if kind == "and":
                value &= mask
            elif kind == "or":
                value |= mask
            else:
                value ^= mask
        return Int64(value) if use_long else value
    return _q_array_update(op, current, operand, variables)


def _q_update_order(path):
    result = []
    for part in path:
        index = _q_index(part)
        result.append((0, index) if index is not None else (1, part))
    return result


def modified_document(original, update, *, query=None, array_filters=None, inserting=False, variables=None):
    original_id = original.get("_id", MISSING)
    if isinstance(update, list):
        result = update_pipeline(deepcopy(original), update, variables)
    elif not _q_predicate_form(update):
        result = deepcopy(update)
        if original_id is not MISSING and "_id" not in result:
            result = {"_id": original_id, **result}
    else:
        result = deepcopy(original)
        filter_map = _q_array_filter_map(array_filters)
        resolved = []
        touched_paths = []
        for op, fields in update.items():
            if op == "$setOnInsert" and not inserting:
                continue
            for path, operand in fields.items():
                for parts in _q_expand_update_parts(original, path, query, filter_map, inserting, variables):
                    resolved.append((parts, op, operand))
                    touched_paths.append(".".join(parts))
                if op == "$rename":
                    touched_paths.append(operand)
        _q_assert_no_path_overlap(touched_paths)
        now = DateTime.now()
        stamp = Timestamp(now.ms // 1000, 0)
        for parts, op, operand in sorted(resolved, key=lambda entry: _q_update_order(entry[0])):
            current = _q_update_value(result, parts, forbid_arrays=op == "$rename")
            if op == "$rename":
                if current is not MISSING:
                    destination = operand.split(".")
                    _q_update_value(result, destination, forbid_arrays=True)
                    _q_write_update_path(result, parts, MISSING)
                    _q_write_update_path(result, destination, current)
            elif current is not MISSING or op not in _Q_NO_CREATE:
                value = _q_apply_update_modifier(op, current, operand, now, stamp, variables)
                _q_write_update_path(result, parts, value)
    if original_id is not MISSING and ("_id" not in result or not bson_equal(original_id, result["_id"])):
        raise MongoError("Updating the document would modify the immutable field '_id'", 66)
    return result


def upsert_base(query):
    equality = {}

    def collect(document):
        for path, condition in document.items():
            if path == "$and":
                for part in condition:
                    collect(part)
                continue
            if path.startswith("$") or isinstance(condition, Regex):
                continue
            value = condition
            if _q_predicate_form(condition):
                if "$eq" not in condition:
                    continue
                value = condition["$eq"]
            if path.startswith("_id."):
                raise MongoError("Cannot use dotted _id equality in an upsert", 111)
            _q_check_path(path)
            if path in equality and not bson_equal(equality[path], value):
                raise MongoError(f"Cannot derive a value for {path!r} from conflicting query equalities", 54)
            equality[path] = value

    collect(query)
    _q_assert_no_path_overlap(equality, 54)
    result = {}
    for path, value in equality.items():
        _q_write_update_path(result, tuple(path.split(".")), deepcopy(value))
    return result


# Aggregation expressions


_AGG_DECIMAL = Context(prec=34, Emin=-6143, Emax=6144, clamp=1)
_AGG_MAX_ARRAY = 1_000_000
_AGG_MIN_INT = -(1 << 31)
_AGG_MAX_INT = (1 << 31) - 1
_AGG_MIN_LONG = -(1 << 63)
_AGG_MAX_LONG = (1 << 63) - 1
_AGG_UTC = timezone.utc


def _agg_null(value):
    return value is MISSING or value is None or isinstance(value, Undefined)


def agg_truth(value):
    if _agg_null(value) or value is False:
        return False
    if numeric(value):
        return _agg_number(value) != 0
    return True


def _agg_number(value):
    if not numeric(value):
        raise MongoError("Expression requires a numeric argument", 14)
    return value.to_decimal() if isinstance(value, Decimal128) else value


def _agg_integer(value):
    number = _agg_number(value)
    result = int(number)
    if result != number or not _AGG_MIN_LONG <= result <= _AGG_MAX_LONG:
        raise MongoError("Expression requires an integer argument")
    return result


def _agg_int_result(value, args):
    if _AGG_MIN_INT <= value <= _AGG_MAX_INT and not any(isinstance(arg, Int64) for arg in args):
        return int(value)
    if _AGG_MIN_LONG <= value <= _AGG_MAX_LONG:
        return Int64(value)
    return float(value)


def _agg_numeric_result(value, args):
    if isinstance(value, Decimal):
        return Decimal128.from_decimal(value)
    if isinstance(value, int):
        return _agg_int_result(value, args)
    return value


def _agg_decimal_double(value):
    return Decimal(format(value, ".14e")) if value else Decimal(str(value)).quantize(Decimal(1))


def _agg_numeric_operands(args, *, exact=False):
    numbers = [_agg_number(arg) for arg in args]
    if any(isinstance(number, Decimal) for number in numbers):
        return [
            Decimal(number) if exact or not isinstance(number, float) else _agg_decimal_double(number)
            for number in numbers
        ]
    return numbers


def _agg_comparison(op, left, right):
    order = compare(left, right)
    return {
        "$cmp": order,
        "$eq": order == 0,
        "$ne": order != 0,
        "$lt": order < 0,
        "$lte": order <= 0,
        "$gt": order > 0,
        "$gte": order >= 0,
    }[op]


def _agg_array(value):
    if not isinstance(value, list):
        raise MongoError("Expression requires an array", 14)
    return value


def _agg_string(value):
    if not isinstance(value, str):
        raise MongoError("Expression requires a string", 14)
    return value


def _agg_object(value):
    if not isinstance(value, dict):
        raise MongoError("Expression requires a document", 14)
    return value


def _agg_unique(values):
    seen = set()
    result = []
    for value in values:
        key = value_key(value)
        if key not in seen:
            result.append(value)
            seen.add(key)
    return result


def _agg_arg_range(op, raw):
    values = raw if isinstance(raw, list) else [raw]
    minimum, maximum = _AGG_ARITY[op]
    if len(values) < minimum or (maximum is not None and len(values) > maximum):
        if op in _AGG_CONVERSIONS:
            raise MongoError(f"{op} requires a single argument, got {len(values)}", 50723)
        expected = f"at least {minimum}" if maximum is None else f"{minimum} to {maximum}"
        if minimum == maximum:
            expected = str(minimum)
        raise MongoError(f"{op} requires {expected} argument(s)", 16020)
    return values


def _agg_check_fields(value, required, optional, label):
    if not isinstance(value, dict):
        raise MongoError(f"{label} requires an object")
    unknown = value.keys() - required - optional
    missing = required - value.keys()
    if unknown:
        raise MongoError(f"{label} has an unrecognized option: {sorted(unknown)[0]}")
    if missing:
        raise MongoError(f"{label} is missing the option: {sorted(missing)[0]}")


def _agg_field_name(name, *, dotted=True):
    if not isinstance(name, str) or not name or "\x00" in name or name.startswith("$"):
        raise MongoError("Expected a valid field path")
    parts = name.split(".")
    if any(not part or part.startswith("$") for part in parts) or (not dotted and len(parts) != 1):
        raise MongoError(f"Invalid field path: {name}")
    return name


def _agg_variable_name(name):
    if not isinstance(name, str) or not (
        name == "CURRENT" or re.fullmatch(r"[a-z\u0080-\U0010ffff][\w]*", name)
    ):
        raise MongoError(f"Invalid aggregation variable name: {name}")
    return name


_AGG_COMPARISONS = {"$cmp", "$eq", "$ne", "$lt", "$lte", "$gt", "$gte"}
_AGG_DATE_PARTS = {
    "$year",
    "$month",
    "$dayOfMonth",
    "$dayOfWeek",
    "$dayOfYear",
    "$hour",
    "$minute",
    "$second",
    "$millisecond",
    "$week",
    "$isoWeek",
    "$isoWeekYear",
    "$isoDayOfWeek",
}
_AGG_CONVERSIONS = {
    "$toString": "string",
    "$toInt": "int",
    "$toLong": "long",
    "$toDouble": "double",
    "$toDecimal": "decimal",
    "$toBool": "bool",
    "$toDate": "date",
    "$toObjectId": "objectId",
}


def _agg_arities():
    groups = {
        (0, 0): "$rand",
        (0, None): "$and $or $add $multiply $concat $concatArrays $sum $avg $min $max "
        "$mergeObjects $stdDevPop $stdDevSamp $setUnion $setIntersection",
        (1, 1): "$not $type $isNumber $isArray $abs $ceil $floor $sqrt $exp $ln $log10 "
        "$sin $cos $tan $asin $acos $atan $sinh $cosh $tanh $asinh $acosh $atanh "
        "$degreesToRadians $radiansToDegrees $size $first $last $reverseArray "
        "$anyElementTrue $allElementsTrue $objectToArray $arrayToObject "
        "$toLower $toUpper $strLenBytes $strLenCP $tsSecond $tsIncrement $binarySize "
        "$bsonSize $bitNot",
        (1, 2): "$round $trunc",
        (1, None): "$bitAnd $bitOr $bitXor",
        (2, 2): "$subtract $divide $mod $pow $log $atan2 $arrayElemAt $in $setDifference "
        "$setIsSubset $split $strcasecmp",
        (2, 3): "$range $slice",
        (2, 4): "$indexOfArray $indexOfBytes $indexOfCP",
        (2, None): "$ifNull $setEquals",
        (3, 3): "$cond $substr $substrBytes $substrCP",
    }
    result = {op: count for count, names in groups.items() for op in names.split()}
    result.update({op: (2, 2) for op in _AGG_COMPARISONS})
    result.update({op: (1, 1) for op in (*_AGG_CONVERSIONS, *_AGG_DATE_PARTS)})
    return result


_AGG_ARITY = _agg_arities()
_AGG_NAMED = {
    "$cond": ({"if", "then", "else"}, set()),
    "$switch": ({"branches"}, {"default"}),
    "$let": ({"vars", "in"}, set()),
    "$map": ({"input", "in"}, {"as"}),
    "$filter": ({"input", "cond"}, {"as", "limit"}),
    "$reduce": ({"input", "initialValue", "in"}, set()),
    "$zip": ({"inputs"}, {"useLongestLength", "defaults"}),
    "$sortArray": ({"input", "sortBy"}, set()),
    **{op: ({"input", "n"}, set()) for op in ("$firstN", "$lastN", "$minN", "$maxN")},
    "$getField": ({"field"}, {"input"}),
    "$setField": ({"field", "input", "value"}, set()),
    "$unsetField": ({"field", "input"}, set()),
    "$convert": ({"input", "to"}, {"format", "onError", "onNull"}),
    "$trim": ({"input"}, {"chars"}),
    "$ltrim": ({"input"}, {"chars"}),
    "$rtrim": ({"input"}, {"chars"}),
    "$replaceOne": ({"input", "find", "replacement"}, set()),
    "$replaceAll": ({"input", "find", "replacement"}, set()),
    "$regexMatch": ({"input", "regex"}, {"options"}),
    "$regexFind": ({"input", "regex"}, {"options"}),
    "$regexFindAll": ({"input", "regex"}, {"options"}),
    "$dateToString": ({"date"}, {"format", "timezone", "onNull"}),
    "$dateFromString": ({"dateString"}, {"format", "timezone", "onNull", "onError"}),
    "$dateFromParts": (
        set(),
        {
            "year",
            "month",
            "day",
            "isoWeekYear",
            "isoWeek",
            "isoDayOfWeek",
            "hour",
            "minute",
            "second",
            "millisecond",
            "timezone",
        },
    ),
    "$dateToParts": ({"date"}, {"timezone", "iso8601"}),
    "$dateAdd": ({"startDate", "unit", "amount"}, {"timezone"}),
    "$dateSubtract": ({"startDate", "unit", "amount"}, {"timezone"}),
    "$dateDiff": ({"startDate", "endDate", "unit"}, {"timezone", "startOfWeek"}),
    "$dateTrunc": ({"date", "unit"}, {"timezone", "binSize", "startOfWeek"}),
    **{op: ({"date"}, {"timezone"}) for op in _AGG_DATE_PARTS},
}


def _agg_parse(op, raw):
    if op == "$rand" and raw == {}:
        return []
    if op in _AGG_DATE_PARTS:
        if isinstance(raw, list) and len(raw) == 1:
            raw = raw[0]
        if not isinstance(raw, dict) or "date" not in raw:
            return _agg_arg_range(op, raw)
    if op == "$getField" and (not isinstance(raw, dict) or "field" not in raw):
        return {"field": raw}
    if op in _AGG_NAMED and (isinstance(raw, dict) or op not in _AGG_ARITY):
        required, optional = _AGG_NAMED[op]
        _agg_check_fields(raw, required, optional, op)
        return raw
    if op in _AGG_ARITY:
        return _agg_arg_range(op, raw)
    raise MongoError(f"Unrecognized expression '{op}'", 168)


def validate_expression(expr):
    if isinstance(expr, str) and expr.startswith("$"):
        path = expr[2:] if expr.startswith("$$") else expr[1:]
        _agg_field_name(path)
    elif isinstance(expr, list):
        for value in expr:
            validate_expression(value)
    elif isinstance(expr, dict) and expr:
        first = next(iter(expr))
        if not first.startswith("$"):
            for key, value in expr.items():
                _agg_field_name(key, dotted=False)
                validate_expression(value)
            return
        if len(expr) != 1:
            raise MongoError("An expression object must contain exactly one operator", 15983)
        if first in ("$literal", "$const"):
            return
        args = _agg_parse(first, expr[first])
        if isinstance(args, list):
            for value in args:
                validate_expression(value)
            return
        for key, value in args.items():
            if first == "$switch" and key == "branches":
                if not isinstance(value, list) or not value:
                    raise MongoError("$switch requires a nonempty array of branches")
                for branch in value:
                    _agg_check_fields(branch, {"case", "then"}, set(), "$switch branch")
                    validate_expression(branch["case"])
                    validate_expression(branch["then"])
            elif first == "$let" and key == "vars":
                for name, binding in _agg_object(value).items():
                    _agg_variable_name(name)
                    validate_expression(binding)
            elif first in ("$map", "$filter") and key == "as":
                _agg_variable_name(value)
            elif first == "$convert" and key == "to" and isinstance(value, dict) and "type" in value:
                _agg_check_fields(value, {"type"}, {"subtype"}, "$convert to")
                for expression in value.values():
                    validate_expression(expression)
            elif first == "$sortArray" and key == "sortBy":
                if not (numeric(value) and (compare(value, 1) == 0 or compare(value, -1) == 0)):
                    validate_sort(value)
            else:
                validate_expression(value)


def _agg_evaluate(expr, doc, variables):
    if isinstance(expr, str):
        if expr.startswith("$$"):
            name, _, path = expr[2:].partition(".")
            if name == "ROOT":
                value = doc
            elif name == "CURRENT":
                value = variables.get("CURRENT", doc)
            elif name == "REMOVE":
                value = MISSING
            elif name == "NOW":
                value = variables["NOW"]
            elif name in variables:
                value = variables[name]
            else:
                raise MongoError(f"Use of undefined variable: {name}", 17276)
            return get_path(value, path) if path else value
        if expr.startswith("$"):
            return get_path(variables.get("CURRENT", doc), expr[1:])
        return expr
    if isinstance(expr, list):
        return [
            None if value is MISSING else value
            for value in (_agg_evaluate(item, doc, variables) for item in expr)
        ]
    if not isinstance(expr, dict) or not expr:
        return expr

    op = next(iter(expr))
    if not op.startswith("$"):
        return {
            key: value
            for key, value in ((key, _agg_evaluate(raw, doc, variables)) for key, raw in expr.items())
            if value is not MISSING
        }
    if len(expr) != 1:
        raise MongoError("An expression object must contain exactly one operator", 15983)
    raw = expr[op]
    if op in ("$literal", "$const"):
        return raw
    args = _agg_parse(op, raw)
    return _AGG_EVALUATORS[op](op, args, doc, variables)


def _agg_scope(variables):
    if variables is not None and "NOW" in variables:
        return variables
    return {"NOW": DateTime.now(), **(variables or {})}


def evaluate(expr, doc, variables=None):
    try:
        return _agg_evaluate(expr, doc, _agg_scope(variables))
    except MongoError:
        raise
    except (ValueError, TypeError, OverflowError, ZeroDivisionError, DecimalException, IndexError) as exc:
        raise MongoError(f"Error evaluating aggregation expression: {exc}") from exc


def _agg_logic(op, args, doc, variables):
    def calc(expr):
        return _agg_evaluate(expr, doc, variables)

    if op in _AGG_COMPARISONS:
        return _agg_comparison(op, calc(args[0]), calc(args[1]))
    if op == "$not":
        return not agg_truth(calc(args[0]))
    if op == "$and":
        return all(agg_truth(calc(arg)) for arg in args)
    if op == "$or":
        return any(agg_truth(calc(arg)) for arg in args)
    if op == "$cond":
        condition, yes, no = (args[key] for key in ("if", "then", "else")) if isinstance(args, dict) else args
        return calc(yes if agg_truth(calc(condition)) else no)
    if op == "$switch":
        for branch in args["branches"]:
            if agg_truth(calc(branch["case"])):
                return calc(branch["then"])
        if "default" in args:
            return calc(args["default"])
        raise MongoError("$switch found no matching branch and no default", 40066)
    if op == "$ifNull":
        for arg in args[:-1]:
            value = calc(arg)
            if not _agg_null(value):
                return value
        return calc(args[-1])
    if op == "$let":
        bindings = {name: calc(raw) for name, raw in args["vars"].items()}
        return _agg_evaluate(args["in"], doc, {**variables, **bindings})
    if op == "$rand":
        return random.random()
    value = calc(args[0])
    if op == "$type":
        return "missing" if value is MISSING else bson_type(value)
    if op == "$isNumber":
        return numeric(value)
    return isinstance(value, list)


_AGG_MATH_UNARY = {
    "$abs": abs,
    "$sqrt": math.sqrt,
    "$exp": math.exp,
    "$ln": math.log,
    "$log10": math.log10,
    "$sin": math.sin,
    "$cos": math.cos,
    "$tan": math.tan,
    "$asin": math.asin,
    "$acos": math.acos,
    "$atan": math.atan,
    "$sinh": math.sinh,
    "$cosh": math.cosh,
    "$tanh": math.tanh,
    "$asinh": math.asinh,
    "$acosh": math.acosh,
    "$atanh": math.atanh,
    "$degreesToRadians": math.degrees,
    "$radiansToDegrees": math.radians,
}


def _agg_round_millis(value):
    number = _agg_number(value)
    base = int(number)
    if abs(number - base) >= 0.5:
        return base + (1 if number > 0 else -1)
    return base


def _agg_arithmetic(op, args, doc, variables):
    values = [_agg_evaluate(arg, doc, variables) for arg in args]
    if any(_agg_null(value) for value in values):
        return None
    if op == "$add":
        dates = [value for value in values if isinstance(value, DateTime)]
        if dates:
            if len(dates) != 1:
                raise MongoError("$add permits at most one date argument")
            return DateTime(
                dates[0].ms
                + sum(_agg_round_millis(value) for value in values if not isinstance(value, DateTime))
            )
    elif op == "$subtract" and isinstance(values[0], DateTime):
        left, right = values
        if isinstance(right, DateTime):
            return Int64(left.ms - right.ms)
        return DateTime(left.ms - _agg_round_millis(right))
    with localcontext(_AGG_DECIMAL):
        operands = _agg_numeric_operands(values)
        left = operands[0] if operands else 0
        if op == "$add":
            result = sum(operands)
        elif op == "$multiply":
            result = math.prod(operands)
        elif op == "$subtract":
            result = left - operands[1]
        elif op == "$divide":
            if operands[1] == 0:
                raise MongoError("Cannot divide by zero", 16608)
            result = left / operands[1]
        elif op == "$mod":
            right = operands[1]
            if right == 0:
                raise MongoError("Can't $mod by zero", 16610)
            if isinstance(left, Decimal):
                result = left % right
            elif isinstance(left, float) or isinstance(right, float):
                result = math.fmod(left, right)
            else:
                result = abs(left) % abs(right) * (-1 if left < 0 else 1)
        elif op == "$pow":
            right = operands[1]
            if isinstance(left, int) and isinstance(right, int) and not (0 <= right <= 63):
                result = math.pow(left, right)
            else:
                result = left**right
        elif op == "$log":
            result = (
                left.ln() / operands[1].ln() if isinstance(left, Decimal) else math.log(left, operands[1])
            )
        elif op == "$atan2":
            result = math.atan2(left, operands[1])
        elif op in ("$ceil", "$floor", "$round", "$trunc"):
            place = _agg_integer(values[1]) if len(values) == 2 else 0
            if not -20 < place < 100:
                raise MongoError(f"{op} precision must be between -19 and 99")
            if isinstance(left, (float, Decimal)) and not math.isfinite(left):
                return values[0]
            if op in ("$ceil", "$floor"):
                rounding = "ROUND_CEILING" if op == "$ceil" else "ROUND_FLOOR"
                if isinstance(left, Decimal):
                    result = left.to_integral_value(rounding=rounding)
                elif isinstance(left, float):
                    result = float(math.ceil(left) if op == "$ceil" else math.floor(left))
                else:
                    result = left
            elif isinstance(left, int) and place >= 0:
                result = left
            else:
                rounding = ROUND_HALF_EVEN if op == "$round" else ROUND_DOWN
                decimal = left if isinstance(left, Decimal) else Decimal(str(left))
                if decimal.is_zero() or decimal.adjusted() + place >= _AGG_DECIMAL.prec:
                    return values[0]
                result = decimal.quantize(Decimal(1).scaleb(-place), rounding=rounding)
                if not isinstance(left, Decimal):
                    result = type(left)(result)
        elif isinstance(left, Decimal):
            method = {
                "$sqrt": Decimal.sqrt,
                "$ln": Decimal.ln,
                "$log10": Decimal.log10,
                "$exp": Decimal.exp,
                "$abs": abs,
            }.get(op)
            result = method(left) if method else Decimal(str(_AGG_MATH_UNARY[op](left)))
        else:
            result = _AGG_MATH_UNARY[op](left)
        return _agg_numeric_result(result, values)


def _agg_bitwise(op, args, doc, variables):
    values = [_agg_evaluate(arg, doc, variables) for arg in args]
    if any(_agg_null(value) for value in values):
        return None
    if any(type(value) not in (int, Int64) for value in values):
        raise MongoError(f"{op} requires int32 or int64 arguments", 14)
    if op == "$bitNot":
        return _agg_int_result(~values[0], values)
    result = -1 if op == "$bitAnd" else 0
    for value in values:
        if op == "$bitAnd":
            result &= value
        elif op == "$bitOr":
            result |= value
        else:
            result ^= value
    return _agg_int_result(result, values)


def _agg_reduce_numeric(op, values):
    values = [value for value in values if numeric(value)]
    if not values:
        return 0 if op == "$sum" else None
    with localcontext(_AGG_DECIMAL):
        numbers = _agg_numeric_operands(values, exact=True)
        if op == "$sum":
            result = sum(numbers)
        elif op == "$avg":
            result = sum(numbers) / len(numbers)
        else:
            offset = 1 if op == "$stdDevSamp" else 0
            if len(numbers) <= offset:
                return None
            floats = [float(number) for number in numbers]
            if any(not math.isfinite(number) for number in floats):
                return float("nan")
            mean = math.fsum(floats) / len(floats)
            result = math.sqrt(math.fsum((number - mean) ** 2 for number in floats) / (len(floats) - offset))
        return _agg_numeric_result(result, values)


def _agg_extreme(op, values):
    values = [value for value in values if not _agg_null(value)]
    if not values:
        return None
    return (min if op == "$min" else max)(values, key=cmp_to_key(compare))


def _agg_reducer(op, args, doc, variables):
    values = [_agg_evaluate(arg, doc, variables) for arg in args]
    if len(values) == 1 and isinstance(values[0], list):
        values = values[0]
    if op in ("$min", "$max"):
        return _agg_extreme(op, values)
    return _agg_reduce_numeric(op, values)


def _agg_array_scope(op, args, doc, variables):
    def calc(value):
        return _agg_evaluate(value, doc, variables)

    if op in ("$sortArray", "$firstN", "$lastN", "$minN", "$maxN"):
        source = calc(args["input"])
        if _agg_null(source):
            return None
        source = _agg_array(source)
        if op == "$sortArray":
            ordering = args["sortBy"]
            if numeric(ordering):
                return sorted(source, key=cmp_to_key(compare), reverse=compare(ordering, 0) < 0)
            return sort_documents(source, ordering)
        count = _agg_pipeline_count(calc(args["n"]))
        if op == "$firstN":
            return source[:count]
        if op == "$lastN":
            return source[-count:]
        return sorted(
            (value for value in source if not _agg_null(value)),
            key=cmp_to_key(compare),
            reverse=op == "$maxN",
        )[:count]
    if op == "$zip":
        arrays = _agg_array(calc(args["inputs"]))
        if not arrays:
            raise MongoError("$zip requires at least one input array", 34465)
        longest = calc(args.get("useLongestLength", False))
        if not isinstance(longest, bool):
            raise MongoError("$zip useLongestLength must be a boolean")
        if any(_agg_null(array) for array in arrays):
            return None
        arrays = [_agg_array(array) for array in arrays]
        if not longest:
            if "defaults" in args:
                raise MongoError("$zip defaults require useLongestLength")
            return [list(row) for row in zip(*arrays)]
        defaults = _agg_array(calc(args.get("defaults", [None] * len(arrays))))
        if len(defaults) != len(arrays):
            raise MongoError("$zip defaults must match the number of inputs")
        return [
            [array[index] if index < len(array) else default for array, default in zip(arrays, defaults)]
            for index in range(max(map(len, arrays)))
        ]

    source = calc(args["input"])
    if _agg_null(source):
        return None
    source = _agg_array(source)
    if op == "$reduce":
        accumulator = calc(args["initialValue"])
        for value in source:
            accumulator = _agg_evaluate(args["in"], doc, {**variables, "value": accumulator, "this": value})
        return accumulator

    name = args.get("as", "this")
    _agg_variable_name(name)
    result = []
    limit = None
    if op == "$filter" and "limit" in args:
        limit = calc(args["limit"])
        if _agg_null(limit):
            limit = None
        else:
            limit = _agg_integer(limit)
            if limit < 1:
                raise MongoError("$filter limit must be greater than zero")
    for value in source:
        scoped = {**variables, name: value}
        if op == "$map":
            item = _agg_evaluate(args["in"], doc, scoped)
            result.append(None if item is MISSING else item)
        elif agg_truth(_agg_evaluate(args["cond"], doc, scoped)):
            result.append(value)
            if len(result) == limit:
                break
    return result


def _agg_array_ops(op, args, doc, variables):
    values = [_agg_evaluate(arg, doc, variables) for arg in args]
    if op == "$range":
        numbers = [_agg_integer(value) for value in values]
        if any(not _AGG_MIN_INT <= value <= _AGG_MAX_INT for value in numbers):
            raise MongoError("$range requires int32 arguments")
        interval = range(*numbers)
        if len(interval) > _AGG_MAX_ARRAY:
            raise MongoError("$range result exceeds the test server's memory limit", 146)
        return list(interval)
    if op == "$in":
        return any(bson_equal(values[0], item) for item in _agg_array(values[1]))
    if op == "$concatArrays":
        if any(_agg_null(value) for value in values):
            return None
        return [item for value in values for item in _agg_array(value)]
    if op.startswith("$set"):
        if op in ("$setIntersection", "$setUnion", "$setDifference") and any(
            _agg_null(value) for value in values
        ):
            return None
        return _agg_set_ops(op, [_agg_array(value) for value in values])

    source = values[0]
    if op == "$size":
        return len(_agg_array(source))
    if op in ("$anyElementTrue", "$allElementsTrue"):
        quantifier = any if op == "$anyElementTrue" else all
        return quantifier(agg_truth(item) for item in _agg_array(source))
    if _agg_null(source):
        return None
    source = _agg_array(source)
    if op == "$reverseArray":
        return source[::-1]
    if op in ("$arrayElemAt", "$first", "$last"):
        if op == "$arrayElemAt":
            if _agg_null(values[1]):
                return None
            index = _agg_integer(values[1])
        else:
            index = 0 if op == "$first" else -1
        return source[index] if -len(source) <= index < len(source) else MISSING
    if op == "$slice":
        if any(_agg_null(value) for value in values[1:]):
            return None
        start = _agg_integer(values[1])
        if len(values) == 2:
            return source[:start] if start >= 0 else source[start:]
        count = _agg_integer(values[2])
        if count <= 0:
            raise MongoError("$slice count must be positive")
        start = max(0, len(source) + start) if start < 0 else start
        return source[start : start + count]
    start, end = _agg_limits(values, len(source))
    return next(
        (index for index in range(start, min(end, len(source))) if bson_equal(source[index], values[1])), -1
    )


def _agg_limits(values, length):
    start = _agg_integer(values[2]) if len(values) > 2 else 0
    end = _agg_integer(values[3]) if len(values) > 3 else length
    if start < 0 or end < 0:
        raise MongoError("Start and end indices cannot be negative")
    return start, end


def _agg_set_ops(op, arrays):
    if op == "$setUnion":
        return _agg_unique(item for array in arrays for item in array)
    encoded = [{value_key(item) for item in array} for array in arrays]
    if op == "$setEquals":
        return all(group == encoded[0] for group in encoded[1:])
    if op == "$setIsSubset":
        return encoded[0] <= encoded[1]
    if not arrays:
        return []
    selected = encoded[0] - encoded[1] if op == "$setDifference" else set.intersection(*encoded)
    return _agg_unique(item for item in arrays[0] if value_key(item) in selected)


def _agg_object_ops(op, args, doc, variables):
    def calc(value):
        return _agg_evaluate(value, doc, variables)

    if op in ("$getField", "$setField", "$unsetField"):
        field = _agg_string(calc(args["field"]))
        if "\x00" in field:
            raise MongoError(f"{op} field may not contain a NUL byte")
        source = calc(args["input"]) if "input" in args else variables.get("CURRENT", doc)
        if _agg_null(source):
            return None
        source = _agg_object(source)
        if op == "$getField":
            return source.get(field, MISSING)
        result = dict(source)
        value = MISSING if op == "$unsetField" else calc(args["value"])
        if value is MISSING:
            result.pop(field, None)
        else:
            result[field] = value
        return result
    if op == "$mergeObjects":
        result = {}
        for arg in args:
            value = calc(arg)
            if not _agg_null(value):
                result.update(_agg_object(value))
        return result
    value = calc(args[0])
    if _agg_null(value):
        return None
    if op == "$objectToArray":
        return [{"k": key, "v": item} for key, item in _agg_object(value).items()]
    result = {}
    encoding = None
    for item in _agg_array(value):
        if isinstance(item, dict) and item.keys() == {"k", "v"}:
            key, array_value = item["k"], item["v"]
            item_encoding = dict
        elif isinstance(item, list) and len(item) == 2:
            key, array_value = item
            item_encoding = list
        else:
            raise MongoError("$arrayToObject requires [key, value] or {k, v} elements")
        if encoding is not None and item_encoding is not encoding:
            raise MongoError("$arrayToObject elements must have the same structure")
        encoding = item_encoding
        if "\x00" in _agg_string(key):
            raise MongoError("$arrayToObject keys cannot contain a NUL byte")
        result[key] = array_value
    return result


def _agg_compile_regex(pattern, options):
    options = None if _agg_null(options) else _agg_string(options)
    if isinstance(pattern, Regex):
        if options is not None and pattern.options:
            raise MongoError("Options must be specified in either the regex or options field")
        if options is None:
            options = pattern.options
        pattern = pattern.pattern
    return _q_regex(_agg_string(pattern), options or "")


def _agg_string_named(op, args, doc, variables):
    values = {key: _agg_evaluate(value, doc, variables) for key, value in args.items()}
    source = values["input"]
    if op.startswith("$regex"):
        if _agg_null(source) or _agg_null(values["regex"]):
            return {"$regexMatch": False, "$regexFind": None, "$regexFindAll": []}[op]
        regex = _agg_compile_regex(values["regex"], values.get("options"))
        source = _agg_string(source)

        def result(match):
            return {"match": match.group(), "idx": match.start(), "captures": list(match.groups())}

        if op == "$regexFindAll":
            return [result(match) for match in regex.finditer(source)]
        match = regex.search(source)
        if op == "$regexMatch":
            return match is not None
        return None if match is None else result(match)
    if any(_agg_null(value) for value in values.values()):
        return None
    source = _agg_string(source)
    if op in ("$replaceOne", "$replaceAll"):
        return source.replace(
            _agg_string(values["find"]), _agg_string(values["replacement"]), 1 if op == "$replaceOne" else -1
        )
    chars = _agg_string(
        values.get(
            "chars",
            "\x00\t\n\v\f\r \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a",
        )
    )
    return {"$trim": str.strip, "$ltrim": str.lstrip, "$rtrim": str.rstrip}[op](source, chars)


def _agg_as_string(value):
    if isinstance(value, str):
        return value
    if _agg_null(value):
        return ""
    return _agg_convert(value, "string")


def _agg_strings(op, args, doc, variables):
    values = [_agg_evaluate(arg, doc, variables) for arg in args]
    if op == "$concat":
        if any(_agg_null(value) for value in values):
            return None
        return "".join(map(_agg_string, values))
    source = values[0]
    if op in ("$toLower", "$toUpper"):
        source = _agg_as_string(source)
        return source.lower() if op == "$toLower" else source.upper()
    if op == "$strcasecmp":
        left, right = (_agg_as_string(value).upper() for value in values)
        return (left > right) - (left < right)
    if op in ("$substr", "$substrBytes", "$substrCP"):
        source = _agg_as_string(source)
        offset, count = (_agg_integer(value) for value in values[1:])
        if offset < 0 or (count < 0 and op == "$substrCP"):
            raise MongoError(f"{op} offset and length must be nonnegative")
        if op == "$substrCP":
            return source[offset : offset + count]
        raw = source.encode("utf-8")
        stop = len(raw) if count < 0 else offset + count
        if any(index < len(raw) and raw[index] & 0xC0 == 0x80 for index in (offset, stop)):
            raise MongoError(f"{op} offsets must fall on UTF-8 character boundaries")
        return raw[offset:stop].decode("utf-8")
    if op == "$strLenCP":
        return len(_agg_string(source))
    if op == "$strLenBytes":
        return len(_agg_string(source).encode("utf-8"))
    if op == "$split":
        if any(_agg_null(value) for value in values):
            return None
        source, separator = map(_agg_string, values)
        if not separator:
            raise MongoError("$split requires a nonempty separator")
        return source.split(separator)
    if _agg_null(source):
        return None
    source, search = map(_agg_string, values[:2])
    if op == "$indexOfBytes":
        source, search = source.encode("utf-8"), search.encode("utf-8")
    start, end = _agg_limits(values, len(source))
    return source.find(search, start, end)


def _agg_zone(value):
    name = _agg_string(value)
    if name in ("UTC", "GMT", "Z"):
        return _AGG_UTC
    offset = re.fullmatch(r"([+-])(\d\d)(?::?(\d\d))?", name)
    if offset:
        sign, hours, minutes = offset.groups()
        hours, minutes = int(hours), int(minutes or 0)
        if hours < 24 and minutes < 60:
            amount = timedelta(hours=hours, minutes=minutes)
            return timezone(amount if sign == "+" else -amount)
    try:
        return ZoneInfo(name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise MongoError(f"Unrecognized time zone: {name}") from exc


def _agg_as_date(value, zone=_AGG_UTC):
    if isinstance(value, ObjectId):
        value = DateTime(int.from_bytes(value.data[:4], "big") * 1000)
    elif isinstance(value, Timestamp):
        value = DateTime(value.time * 1000)
    if not isinstance(value, DateTime):
        raise MongoError("Expression requires a date, ObjectId, or timestamp", 14)
    return value.to_datetime().astimezone(zone)


def _agg_format_date(date, pattern):
    iso = date.isocalendar()
    offset = int(date.utcoffset().total_seconds() / 60)
    month = (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )[date.month - 1]
    substitutions = {
        "Y": f"{date.year:04d}",
        "m": f"{date.month:02d}",
        "d": f"{date.day:02d}",
        "H": f"{date.hour:02d}",
        "M": f"{date.minute:02d}",
        "S": f"{date.second:02d}",
        "L": f"{date.microsecond // 1000:03d}",
        "j": f"{date.timetuple().tm_yday:03d}",
        "z": date.strftime("%z"),
        "Z": str(offset),
        "U": date.strftime("%U"),
        "w": str((date.weekday() + 1) % 7 + 1),
        "G": f"{iso.year:04d}",
        "V": f"{iso.week:02d}",
        "u": str(iso.weekday),
        "b": month[:3],
        "B": month,
        "%": "%",
    }

    def replace(match):
        code = match[0][1:]
        if code not in substitutions:
            raise MongoError(f"Invalid format character '%{code}' in date format", 18536)
        return substitutions[code]

    return re.sub(r"%(.|$)", replace, pattern)


def _agg_parse_date(source, pattern=None, zone=None):
    source = _agg_string(source)
    if pattern is not None:
        pattern = _agg_string(pattern)
        pattern = re.sub(r"%%|%L", lambda match: "%f" if match[0] == "%L" else "%%", pattern)
        value = datetime.strptime(source, pattern)
    else:
        try:
            value = datetime.fromisoformat(source)
        except ValueError:
            for alternative in ("%Y-%m-%d %H:%M:%S.%f%z", "%b %d %Y", "%B %d %Y", "%Y/%m/%d"):
                try:
                    value = datetime.strptime(source, alternative)
                    break
                except ValueError:
                    continue
            else:
                raise MongoError(f"Could not parse date string {source!r}", 241)
    if zone is not None and value.tzinfo is not None:
        raise MongoError("A time zone was specified both in the date string and in the argument", 241)
    return DateTime.from_datetime(
        value if value.tzinfo is not None else value.replace(tzinfo=zone or _AGG_UTC)
    )


def _agg_date_parts(date):
    return {
        "year": date.year,
        "month": date.month,
        "day": date.day,
        "hour": date.hour,
        "minute": date.minute,
        "second": date.second,
        "millisecond": date.microsecond // 1000,
    }


def _agg_date_part(op, date):
    iso = date.isocalendar()
    if op == "$week":
        return int(date.strftime("%U"))
    part = {
        "$dayOfWeek": (date.weekday() + 1) % 7 + 1,
        "$dayOfYear": date.timetuple().tm_yday,
        "$dayOfMonth": date.day,
        "$isoWeek": iso.week,
        "$isoWeekYear": iso.year,
        "$isoDayOfWeek": iso.weekday,
    }.get(op, MISSING)
    return _agg_date_parts(date)[op[1:]] if part is MISSING else part


def _agg_date_from_parts(values, zone):
    has_iso = any(part in values for part in ("isoWeekYear", "isoWeek", "isoDayOfWeek"))
    has_calendar = any(part in values for part in ("year", "month", "day"))
    if has_iso and has_calendar:
        raise MongoError("$dateFromParts cannot mix calendar dates with ISO week dates")
    year_key = "isoWeekYear" if has_iso else "year"
    if year_key not in values:
        raise MongoError(f"$dateFromParts requires {year_key}")
    year = _agg_integer(values[year_key])
    if not 1 <= year <= 9999:
        raise MongoError("$dateFromParts requires a year between 1 and 9999")
    if has_iso:
        jan4 = datetime(year, 1, 4, tzinfo=zone)
        first = jan4 - timedelta(days=jan4.weekday())
        days = (
            7 * (_agg_integer(values.get("isoWeek", 1)) - 1) + _agg_integer(values.get("isoDayOfWeek", 1)) - 1
        )
    else:
        year, month = divmod(12 * year + _agg_integer(values.get("month", 1)) - 1, 12)
        first = datetime(year, month + 1, 1, tzinfo=zone)
        days = _agg_integer(values.get("day", 1)) - 1
    kwargs = {
        part + "s": _agg_integer(values.get(part, 0)) for part in ("hour", "minute", "second", "millisecond")
    }
    return DateTime.from_datetime(first + timedelta(days=days, **kwargs))


_AGG_MILLISECONDS = {"millisecond": 1, "second": 1000, "minute": 60_000, "hour": 3_600_000}
_AGG_MONTHS = {"month": 1, "quarter": 3, "year": 12}
_AGG_WEEKDAYS = {
    name: index
    for index, name in enumerate(
        ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    )
}


def _agg_add_date(date, unit, amount):
    if unit in _AGG_MONTHS:
        year, month = divmod(date.year * 12 + date.month - 1 + _AGG_MONTHS[unit] * amount, 12)
        month += 1
        return date.replace(year=year, month=month, day=min(date.day, calendar.monthrange(year, month)[1]))
    if unit in ("day", "week"):
        return date + timedelta(days=amount * (7 if unit == "week" else 1))
    return (date.astimezone(_AGG_UTC) + timedelta(milliseconds=_AGG_MILLISECONDS[unit] * amount)).astimezone(
        date.tzinfo
    )


def _agg_week_start(value):
    name = _agg_string(value).lower()
    day = next((index for weekday, index in _AGG_WEEKDAYS.items() if name in (weekday, weekday[:3])), None)
    if day is None:
        raise MongoError(f"Unrecognized startOfWeek: {value}")
    return day


def _agg_date_boundaries(date, unit, week_start):
    if unit in _AGG_MONTHS:
        return (date.year * 12 + date.month - 1) // _AGG_MONTHS[unit]
    if unit == "week":
        return (date.toordinal() - week_start - 1) // 7
    if unit == "day":
        return date.toordinal()
    # Sub-day boundaries need offsets as well as the calendar clock, particularly in half-hour zones.
    floored = date.replace(microsecond=date.microsecond // 1000 * 1000)
    if unit == "hour":
        floored = date.replace(minute=0, second=0, microsecond=0)
    elif unit == "minute":
        floored = date.replace(second=0, microsecond=0)
    elif unit == "second":
        floored = date.replace(microsecond=0)
    return DateTime.from_datetime(floored).ms // _AGG_MILLISECONDS[unit]


def _agg_date_arithmetic(op, values, zone):
    unit = _agg_string(values["unit"])
    if unit not in _AGG_MILLISECONDS and unit not in _AGG_MONTHS and unit not in ("day", "week"):
        raise MongoError(f"Unrecognized date unit: {unit}")
    date = _agg_as_date(values.get("startDate", values.get("date")), zone)
    if op in ("$dateAdd", "$dateSubtract"):
        amount = _agg_integer(values["amount"])
        return DateTime.from_datetime(_agg_add_date(date, unit, amount if op == "$dateAdd" else -amount))
    week_start = _agg_week_start(values.get("startOfWeek", "sunday")) if unit == "week" else 0
    position = _agg_date_boundaries(date, unit, week_start)
    if op == "$dateDiff":
        end = _agg_as_date(values["endDate"], zone)
        return Int64(_agg_date_boundaries(end, unit, week_start) - position)

    bin_size = _agg_integer(values.get("binSize", 1))
    if bin_size <= 0:
        raise MongoError("$dateTrunc binSize must be positive")
    reference = datetime(2000, 1, 1, tzinfo=zone)
    if unit == "week":
        reference += timedelta(days=(week_start - reference.weekday()) % 7)
    origin = _agg_date_boundaries(reference, unit, week_start)
    step = (position - origin) // bin_size * bin_size
    return DateTime.from_datetime(_agg_add_date(reference, unit, step))


def _agg_dates(op, args, doc, variables):
    def calc(value):
        return _agg_evaluate(value, doc, variables)

    if isinstance(args, list):
        source = calc(args[0])
        return None if _agg_null(source) else _agg_date_part(op, _agg_as_date(source))
    if op == "$dateFromString":
        source = calc(args["dateString"])
        if _agg_null(source):
            return calc(args.get("onNull"))
        try:
            zone = _agg_zone(calc(args["timezone"])) if "timezone" in args else None
            pattern = calc(args.get("format"))
            return _agg_parse_date(source, pattern, zone)
        except (MongoError, ValueError, OverflowError):
            if "onError" in args:
                return calc(args["onError"])
            raise

    values = {key: calc(value) for key, value in args.items() if key != "onNull"}
    if op == "$dateToString" and _agg_null(values["date"]):
        return calc(args.get("onNull"))
    if any(_agg_null(value) for value in values.values()):
        return None
    zone = _agg_zone(values.get("timezone", "UTC"))
    if op == "$dateFromParts":
        return _agg_date_from_parts(values, zone)
    if op in ("$dateAdd", "$dateSubtract", "$dateDiff", "$dateTrunc"):
        return _agg_date_arithmetic(op, values, zone)
    date = _agg_as_date(values["date"], zone)
    if op in _AGG_DATE_PARTS:
        return _agg_date_part(op, date)
    if op == "$dateToString":
        default = "%Y-%m-%dT%H:%M:%S.%LZ" if zone is _AGG_UTC else "%Y-%m-%dT%H:%M:%S.%L"
        return _agg_format_date(date, _agg_string(values.get("format", default)))
    parts = _agg_date_parts(date)
    iso = values.get("iso8601", False)
    if not isinstance(iso, bool):
        raise MongoError("$dateToParts iso8601 must be a boolean")
    if iso:
        iso_year, iso_week, iso_day = date.isocalendar()
        for name in ("year", "month", "day"):
            del parts[name]
        parts = {"isoWeekYear": iso_year, "isoWeek": iso_week, "isoDayOfWeek": iso_day, **parts}
    return parts


_AGG_TYPE_CODES = {
    1: "double",
    2: "string",
    3: "object",
    4: "array",
    5: "binData",
    7: "objectId",
    8: "bool",
    9: "date",
    16: "int",
    18: "long",
    19: "decimal",
}


def _agg_convert_binary(value, target, subtype, encoding):
    import base64
    from uuid import UUID

    if target == "binData" and isinstance(value, Binary):
        if subtype != value.subtype:
            raise MongoError("Changing the binary subtype is unsupported", 241)
        return value
    if encoding == "uuid":
        if subtype != 4:
            raise MongoError("UUID format requires binary subtype 4", 241)
        return Binary(UUID(value).bytes, 4) if target == "binData" else str(UUID(bytes=bytes(value)))
    if subtype == 4:
        raise MongoError("Binary subtype 4 requires UUID format", 241)
    if target == "string":
        functions = {
            "base64": lambda raw: base64.b64encode(raw).decode("ascii"),
            "base64url": lambda raw: base64.urlsafe_b64encode(raw).decode("ascii"),
            "hex": bytes.hex,
            "utf8": lambda raw: raw.decode("utf-8"),
        }
    else:
        functions = {
            "base64": lambda raw: base64.b64decode(raw, validate=True),
            "base64url": lambda raw: base64.b64decode(raw, altchars=b"-_", validate=True),
            "hex": bytes.fromhex,
            "utf8": lambda raw: raw.encode("utf-8"),
        }
    if encoding not in functions:
        raise MongoError(f"Unsupported binary encoding: {encoding!r}", 241)
    result = functions[encoding](value)
    return Binary(result, subtype) if target == "binData" else result


def _agg_convert(value, target, subtype=0, encoding=None):
    if type(target) in (int, Int64, float):
        target = _AGG_TYPE_CODES.get(target)
    if not isinstance(target, str):
        raise MongoError("Invalid conversion target", 241)
    source = bson_type(value)
    if target == "binData" and source in ("string", "binData"):
        return _agg_convert_binary(value, target, _agg_integer(subtype), encoding)
    if target == source:
        return value
    if target == "bool":
        return True if isinstance(value, str) else agg_truth(value)
    if target == "string":
        if isinstance(value, bool):
            return "true" if value else "false"
        if numeric(value):
            number = _agg_number(value)
            if isinstance(number, float):
                if math.isnan(number):
                    return "NaN"
                if math.isinf(number):
                    return "-Infinity" if number < 0 else "Infinity"
                if number.is_integer() and abs(number) < 1e16:
                    return str(int(number))
            return str(number)
        if isinstance(value, ObjectId):
            return value.data.hex()
        if isinstance(value, DateTime):
            return _agg_format_date(_agg_as_date(value), "%Y-%m-%dT%H:%M:%S.%LZ")
        if isinstance(value, Binary):
            return _agg_convert_binary(value, target, value.subtype, encoding)

    if target in ("int", "long", "double", "decimal"):
        if isinstance(value, DateTime) and target != "int":
            value = Int64(value.ms)
        if isinstance(value, bool):
            value = int(value)
        if isinstance(value, str):
            if target in ("int", "long"):
                if not re.fullmatch(r"[+-]?[0-9]+", value):
                    raise MongoError(f"Could not parse integer {value!r}", 241)
                number = int(value)
            elif re.fullmatch(
                r"[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|nan|inf(?:inity)?)",
                value,
                re.IGNORECASE,
            ):
                number = Decimal(value) if target == "decimal" else float(value)
            else:
                raise MongoError(f"Could not parse number {value!r}", 241)
        else:
            number = _agg_number(value)
        if target == "decimal":
            if isinstance(number, float):
                number = _agg_decimal_double(number)
            return Decimal128.from_decimal(Decimal(number))
        if target == "double":
            converted = float(number)
            if math.isinf(converted) and isinstance(number, Decimal) and number.is_finite():
                raise MongoError(f"Number out of range for double: {number}", 241)
            return converted
        integer = int(number)
        lower, upper = (_AGG_MIN_INT, _AGG_MAX_INT) if target == "int" else (_AGG_MIN_LONG, _AGG_MAX_LONG)
        if not lower <= integer <= upper:
            raise MongoError(f"Number out of range for {target}: {number}", 241)
        return integer if target == "int" else Int64(integer)

    if target == "objectId" and isinstance(value, str):
        if not re.fullmatch(r"[a-fA-F0-9]{24}", value):
            raise MongoError("ObjectId string must contain 24 hexadecimal characters", 241)
        return ObjectId(bytes.fromhex(value))
    if target == "date":
        if isinstance(value, str):
            return _agg_parse_date(value)
        if isinstance(value, (ObjectId, Timestamp)):
            return DateTime.from_datetime(_agg_as_date(value))
        if numeric(value) and source != "int":
            return DateTime(_agg_convert(value, "long"))
    raise MongoError(f"Unsupported conversion from {source} to {target}", 241)


def _agg_conversions(op, args, doc, variables):
    def calc(value):
        return _agg_evaluate(value, doc, variables)

    if isinstance(args, list):
        value = calc(args[0])
        return None if _agg_null(value) else _agg_convert(value, _AGG_CONVERSIONS[op])
    value = calc(args["input"])
    if _agg_null(value):
        return calc(args.get("onNull"))
    target = calc(args["to"])
    if _agg_null(target):
        return None
    subtype = 0
    if isinstance(target, dict):
        subtype = target.get("subtype", 0)
        target = target.get("type")
    encoding = calc(args.get("format"))
    try:
        return _agg_convert(value, target, subtype, encoding)
    except (MongoError, ValueError, TypeError, OverflowError, DecimalException) as exc:
        if "onError" in args:
            return calc(args["onError"])
        raise MongoError(str(exc), 241) from exc


def _agg_misc(op, args, doc, variables):
    value = _agg_evaluate(args[0], doc, variables)
    if _agg_null(value):
        return None
    if op == "$bsonSize":
        return len(encode_bson(_agg_object(value)))
    if op == "$binarySize":
        if isinstance(value, bytes):
            return len(value)
        return len(_agg_string(value).encode("utf-8"))
    if not isinstance(value, Timestamp):
        raise MongoError(f"{op} requires a BSON timestamp", 14)
    return Int64(value.time if op == "$tsSecond" else value.increment)


def _agg_evaluators():
    families = (
        (
            _agg_logic,
            _AGG_COMPARISONS
            | set("$not $and $or $cond $switch $ifNull $let $rand $type $isNumber $isArray".split()),
        ),
        (
            _agg_arithmetic,
            set(_AGG_MATH_UNARY)
            | set(
                "$add $subtract $multiply $divide $mod $pow $log $atan2 $ceil $floor $round $trunc".split()
            ),
        ),
        (_agg_bitwise, "$bitNot $bitAnd $bitOr $bitXor".split()),
        (_agg_reducer, "$sum $avg $min $max $stdDevPop $stdDevSamp".split()),
        (_agg_array_scope, "$map $filter $reduce $zip $sortArray $firstN $lastN $minN $maxN".split()),
        (
            _agg_array_ops,
            (
                "$range $in $concatArrays $setUnion $setIntersection $setEquals $setIsSubset $setDifference "
                "$size $anyElementTrue $allElementsTrue $reverseArray $arrayElemAt $first $last $slice $indexOfArray"
            ).split(),
        ),
        (
            _agg_object_ops,
            "$getField $setField $unsetField $mergeObjects $objectToArray $arrayToObject".split(),
        ),
        (
            _agg_string_named,
            "$regexMatch $regexFind $regexFindAll $replaceOne $replaceAll $trim $ltrim $rtrim".split(),
        ),
        (
            _agg_strings,
            (
                "$concat $toLower $toUpper $strcasecmp $substr $substrBytes $substrCP $strLenCP $strLenBytes "
                "$split $indexOfCP $indexOfBytes"
            ).split(),
        ),
        (
            _agg_dates,
            _AGG_DATE_PARTS
            | set(
                (
                    "$dateFromString $dateToString $dateFromParts $dateToParts "
                    "$dateAdd $dateSubtract $dateDiff $dateTrunc"
                ).split()
            ),
        ),
        (_agg_conversions, {*_AGG_CONVERSIONS, "$convert"}),
        (_agg_misc, "$tsSecond $tsIncrement $binarySize $bsonSize".split()),
    )
    return {op: evaluator for evaluator, names in families for op in names}


_AGG_EVALUATORS = _agg_evaluators()


# Aggregation pipelines

_AGG_UPDATE_STAGES = {"$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith"}
_AGG_WRITE_STAGES = {"$out", "$merge"}
_AGG_ACCUMULATORS = {
    "$sum",
    "$avg",
    "$min",
    "$max",
    "$stdDevPop",
    "$stdDevSamp",
    "$count",
    "$first",
    "$last",
    "$push",
    "$addToSet",
    "$mergeObjects",
    "$firstN",
    "$lastN",
    "$minN",
    "$maxN",
    "$top",
    "$bottom",
    "$topN",
    "$bottomN",
}


def _agg_single_key(spec, label):
    if not isinstance(spec, dict) or len(spec) != 1:
        raise MongoError(f"{label} requires exactly one field", 40323)
    return next(iter(spec.items()))


def _agg_check_paths(paths):
    prior = set()
    for path in sorted(paths):
        parts = path.split(".")
        if path in prior or any(".".join(parts[:end]) in prior for end in range(1, len(parts))):
            raise MongoError(f"Conflicting field path: {path}", 40176)
        prior.add(path)


def _agg_assignments(spec):
    def expand(document, prefix):
        for field, expression in document.items():
            path = prefix + _agg_field_name(field)
            if (
                isinstance(expression, dict)
                and expression
                and not any(key.startswith("$") for key in expression)
            ):
                yield from expand(expression, path + ".")
            else:
                yield path, expression

    result = list(expand(_agg_object(spec), ""))
    _agg_check_paths(path for path, _ in result)
    return result


def _agg_unset_fields(spec):
    fields = spec if isinstance(spec, list) else [spec]
    if not fields:
        raise MongoError("$unset requires at least one field", 31119)
    return [_agg_field_name(field) for field in fields]


def _agg_mutating_stage(doc, op, spec, variables):
    if op == "$project":
        return project(doc, spec, variables, aggregation=True)
    if op in ("$replaceRoot", "$replaceWith"):
        result = evaluate(spec["newRoot"] if op == "$replaceRoot" else spec, doc, variables)
        return deepcopy(_agg_object(result))
    if op == "$unset":
        result = deepcopy(doc)
        for field in _agg_unset_fields(spec):
            unset_path(result, field)
        return result
    assignments = [(path, evaluate(expr, doc, variables)) for path, expr in _agg_assignments(spec)]
    result = deepcopy(doc)
    for path, value in assignments:
        if value is MISSING:
            unset_path(result, path)
        else:
            set_path(result, path, value)
    return result


def _agg_pipeline_count(value, *, zero=False):
    if not numeric(value):
        raise MongoError("Pipeline stage requires an integer")
    try:
        count = _agg_integer(value)
    except (OverflowError, ValueError, DecimalException) as exc:
        raise MongoError("Pipeline stage requires an integer") from exc
    if count < int(not zero):
        raise MongoError(
            "Pipeline stage requires a nonnegative integer"
            if zero
            else "Pipeline stage requires a positive integer"
        )
    return count


def _agg_unwind_options(spec):
    if isinstance(spec, str):
        spec = {"path": spec}
    _agg_check_fields(spec, {"path"}, {"preserveNullAndEmptyArrays", "includeArrayIndex"}, "$unwind")
    path = spec["path"]
    if not isinstance(path, str) or not path.startswith("$") or path.startswith("$$"):
        raise MongoError("$unwind path must be a field reference", 28818)
    path = _agg_field_name(path[1:])
    preserve = spec.get("preserveNullAndEmptyArrays", False)
    if not isinstance(preserve, bool):
        raise MongoError("$unwind preserveNullAndEmptyArrays must be a boolean")
    index = spec.get("includeArrayIndex")
    if "includeArrayIndex" in spec:
        _agg_field_name(index)
    return path, preserve, index


def _agg_direct_path(doc, path):
    for part in path.split("."):
        if not isinstance(doc, dict):
            return MISSING
        doc = doc.get(part, MISSING)
    return doc


def _agg_unwind(documents, spec):
    path, preserve, index_name = _agg_unwind_options(spec)
    for doc in documents:
        value = _agg_direct_path(doc, path)
        if _agg_null(value) or (isinstance(value, list) and not value):
            if not preserve:
                continue
            row = deepcopy(doc)
            if isinstance(value, list):
                unset_path(row, path)
            if index_name is not None:
                set_path(row, index_name, None)
            yield row
        elif isinstance(value, list):
            for index, item in enumerate(value):
                row = deepcopy(doc)
                set_path(row, path, item)
                if index_name is not None:
                    set_path(row, index_name, Int64(index))
                yield row
        else:
            row = deepcopy(doc)
            if index_name is not None:
                set_path(row, index_name, None)
            yield row


def _agg_accumulator_specs(spec):
    for field, operand in _agg_object(spec).items():
        _agg_field_name(field, dotted=False)
        op, value = _agg_single_key(operand, "Accumulator")
        if op not in _AGG_ACCUMULATORS:
            raise MongoError(f"Unknown group accumulator: {op}", 15952)
        if op == "$count":
            if value != {}:
                raise MongoError("$count accumulator takes an empty document")
        elif op in ("$firstN", "$lastN", "$minN", "$maxN"):
            _agg_check_fields(value, {"input", "n"}, set(), op)
            validate_expression(value["input"])
            validate_expression(value["n"])
        elif op in ("$top", "$bottom", "$topN", "$bottomN"):
            required = {"sortBy", "output", "n"} if op.endswith("N") else {"sortBy", "output"}
            _agg_check_fields(value, required, set(), op)
            validate_sort(value["sortBy"])
            validate_expression(value["output"])
            if "n" in value:
                validate_expression(value["n"])
        else:
            validate_expression(value)
        yield field, op, value


def _agg_accumulate(op, expression, documents, variables):
    def calculation(doc):
        return evaluate(expression, doc, variables)

    if op == "$count":
        return _agg_int_result(len(documents), [])
    if op in ("$first", "$last"):
        if not documents:
            return None
        value = calculation(documents[0 if op == "$first" else -1])
        return None if value is MISSING else value
    if op in ("$top", "$bottom", "$topN", "$bottomN"):
        if not documents:
            return [] if op.endswith("N") else None
        ordered = sort_documents(documents, expression["sortBy"])
        if op in ("$top", "$bottom"):
            value = evaluate(expression["output"], ordered[0 if op == "$top" else -1], variables)
            return None if value is MISSING else value
        count = _agg_pipeline_count(evaluate(expression["n"], documents[0], variables))
        ordered = ordered[:count] if op == "$topN" else ordered[-count:]
        return [
            None if value is MISSING else value
            for value in (evaluate(expression["output"], doc, variables) for doc in ordered)
        ]
    if op in ("$firstN", "$lastN", "$minN", "$maxN"):
        if not documents:
            return []
        count = _agg_pipeline_count(evaluate(expression["n"], documents[0], variables))
        values = [evaluate(expression["input"], doc, variables) for doc in documents]
        if op in ("$firstN", "$lastN"):
            selected = values[:count] if op == "$firstN" else values[-count:]
            return [None if value is MISSING else value for value in selected]
        return sorted(
            (value for value in values if not _agg_null(value)),
            key=cmp_to_key(compare),
            reverse=(op == "$maxN"),
        )[:count]

    values = [calculation(doc) for doc in documents]
    if op == "$push":
        return [value for value in values if value is not MISSING]
    if op == "$addToSet":
        return _agg_unique(value for value in values if value is not MISSING)
    if op == "$mergeObjects":
        result = {}
        for value in values:
            if not _agg_null(value):
                result.update(_agg_object(value))
        return result
    if op in ("$min", "$max"):
        return _agg_extreme(op, values)
    return _agg_reduce_numeric(op, values)


def _agg_group_result(key, accumulators, documents, variables):
    return {
        "_id": key,
        **{field: _agg_accumulate(op, expr, documents, variables) for field, op, expr in accumulators},
    }


def _agg_group(documents, expression, accumulators, variables):
    groups = {}
    for doc in documents:
        key = evaluate(expression, doc, variables)
        if key is MISSING:
            key = None
        _, rows = groups.setdefault(value_key(key), (key, []))
        rows.append(doc)
    return [_agg_group_result(key, accumulators, rows, variables) for key, rows in groups.values()]


def _agg_bucket(documents, spec, variables, automatic=False):
    accumulators = list(_agg_accumulator_specs(spec.get("output", {"count": {"$sum": 1}})))
    if automatic:
        rows = []
        for doc in documents:
            key = evaluate(spec["groupBy"], doc, variables)
            rows.append((None if key is MISSING else key, doc))
        rows.sort(key=cmp_to_key(lambda left, right: compare(left[0], right[0])))
        count = _agg_pipeline_count(spec["buckets"])
        size = max(1, math.floor(len(rows) / count + 0.5))
        start = 0
        result = []
        while start < len(rows):
            end = min(len(rows), start + size)
            if len(result) + 1 == count:
                end = len(rows)
            while end < len(rows) and bson_equal(rows[end - 1][0], rows[end][0]):
                end += 1
            bounds = {"min": rows[start][0], "max": rows[min(end, len(rows) - 1)][0]}
            result.append(
                _agg_group_result(bounds, accumulators, [doc for _, doc in rows[start:end]], variables)
            )
            start = end
        return result

    boundaries = [evaluate(raw, {}, variables) for raw in spec["boundaries"]]
    first = boundaries[0]
    for left, right in zip(boundaries, boundaries[1:]):
        same_type = bson_type(first) == bson_type(right) or (numeric(first) and numeric(right))
        if not same_type or compare(left, right) >= 0:
            raise MongoError(
                "$bucket boundaries must have the same type and be sorted in ascending order", 40194
            )
    default = evaluate(spec["default"], {}, variables) if "default" in spec else MISSING
    if default is not MISSING and compare(first, default) <= 0 and compare(default, boundaries[-1]) < 0:
        raise MongoError("$bucket default must be outside the boundaries")
    groups = {}
    for doc in documents:
        key = evaluate(spec["groupBy"], doc, variables)
        bucket = next(
            (
                left
                for left, right in zip(boundaries, boundaries[1:])
                if compare(left, key) <= 0 and compare(key, right) < 0
            ),
            default,
        )
        if bucket is MISSING:
            raise MongoError("$bucket found no matching boundary and no default", 40066)
        _, rows = groups.setdefault(value_key(bucket), (bucket, []))
        rows.append(doc)
    group_values = sorted(groups.values(), key=cmp_to_key(lambda left, right: compare(left[0], right[0])))
    return [_agg_group_result(key, accumulators, rows, variables) for key, rows in group_values]


def _agg_join_values(doc, field):
    values = path_values(doc, field)
    flattened = [item for value in values for item in (value if isinstance(value, list) else [value])]
    return flattened or [None]


def _agg_join_index(documents, field):
    index = {}
    for offset, doc in enumerate(documents):
        for key in {value_key(value) for value in _agg_join_values(doc, field)}:
            index.setdefault(key, []).append(offset)
    return index


def _agg_lookup(documents, spec, variables, read_collection):
    source = read_collection(spec["from"]) if "from" in spec else []
    index = _agg_join_index(source, spec["foreignField"]) if "localField" in spec else None
    target = spec["as"]
    result = []
    for doc in documents:
        if index is None:
            candidates = source
        else:
            offsets = {
                offset
                for value in _agg_join_values(doc, spec["localField"])
                for offset in index.get(value_key(value), [])
            }
            candidates = [source[offset] for offset in sorted(offsets)]
        if "pipeline" in spec:
            bindings = {name: evaluate(expr, doc, variables) for name, expr in spec.get("let", {}).items()}
            candidates = _agg_run(
                candidates, spec["pipeline"], {**variables, **bindings}, read_collection, None
            )
        row = deepcopy(doc)
        set_path(row, target, candidates)
        result.append(row)
    return result


def _agg_graph_lookup(documents, spec, variables, read_collection):
    source = read_collection(spec["from"])
    if "restrictSearchWithMatch" in spec:
        source = [doc for doc in source if matches(doc, spec["restrictSearchWithMatch"], variables)]
    index = _agg_join_index(source, spec["connectToField"])
    max_depth = _agg_pipeline_count(spec.get("maxDepth", _AGG_MAX_LONG), zero=True)
    target = spec["as"]
    result = []
    for doc in documents:
        start = evaluate(spec["startWith"], doc, variables)
        frontier = deque((value, 0) for value in (start if isinstance(start, list) else [start]))
        visited = set()
        joined = []
        while frontier:
            value, depth = frontier.popleft()
            for offset in index.get(value_key(value), []):
                if offset in visited:
                    continue
                visited.add(offset)
                row = deepcopy(source[offset])
                if "depthField" in spec:
                    set_path(row, spec["depthField"], Int64(depth))
                joined.append(row)
                if depth < max_depth:
                    for next_value in _agg_join_values(source[offset], spec["connectFromField"]):
                        frontier.append((next_value, depth + 1))
        row = deepcopy(doc)
        set_path(row, target, joined)
        result.append(row)
    return result


def _agg_validate_write_stage(op, spec):
    if op == "$out":
        target = spec
    elif isinstance(spec, str):
        target = spec
    else:
        _agg_check_fields(spec, {"into"}, {"on", "let", "whenMatched", "whenNotMatched"}, "$merge")
        target = spec["into"]
        if "on" in spec:
            fields = spec["on"] if isinstance(spec["on"], list) else [spec["on"]]
            if not fields or len(fields) != len(set(map(_agg_field_name, fields))):
                raise MongoError("$merge on must specify distinct fields")
        method = spec.get("whenMatched", "merge")
        if isinstance(method, list):
            validate_update_pipeline(method)
        elif method not in ("replace", "keepExisting", "merge", "fail"):
            raise MongoError("Invalid $merge whenMatched option")
        if "let" in spec:
            if not isinstance(method, list):
                raise MongoError("$merge let variables require a whenMatched pipeline")
            for name, value in _agg_object(spec["let"]).items():
                _agg_variable_name(name)
                validate_expression(value)
        if spec.get("whenNotMatched", "insert") not in ("insert", "discard", "fail"):
            raise MongoError("Invalid $merge whenNotMatched option")
    if not isinstance(target, str):
        _agg_check_fields(target, {"db", "coll"} if op == "$out" else {"coll"}, {"db"}, op)
        if "db" in target and not _agg_string(target["db"]):
            raise MongoError(f"{op} database name cannot be empty")
        target = target["coll"]
    if not _agg_string(target):
        raise MongoError(f"{op} collection name cannot be empty")


def _agg_validate_stage(op, spec, *, facet=False):
    if op == "$match":
        validate_filter(spec)
    elif op == "$sort":
        validate_sort(spec)
    elif op in ("$skip", "$limit"):
        _agg_pipeline_count(spec, zero=(op == "$skip"))
    elif op == "$project":
        if not spec:
            raise MongoError("$project requires at least one field", 40177)
        try:
            validate_projection(spec, aggregation=True)
        except MongoError as exc:
            if exc.code == 168:
                raise MongoError(str(exc), 31325) from exc
            raise
    elif op in ("$set", "$addFields"):
        for _, expression in _agg_assignments(spec):
            validate_expression(expression)
    elif op == "$unset":
        _agg_unset_fields(spec)
    elif op == "$unwind":
        _agg_unwind_options(spec)
    elif op == "$group":
        if not isinstance(spec, dict) or "_id" not in spec:
            raise MongoError("$group requires an _id expression", 15955)
        validate_expression(spec["_id"])
        list(_agg_accumulator_specs({field: value for field, value in spec.items() if field != "_id"}))
    elif op in ("$bucket", "$bucketAuto"):
        if op == "$bucket":
            _agg_check_fields(spec, {"groupBy", "boundaries"}, {"default", "output"}, op)
            edges = spec["boundaries"]
            if not isinstance(edges, list) or len(edges) < 2:
                raise MongoError("$bucket requires at least two boundaries", 40192)
            for boundary in edges:
                validate_expression(boundary)
            if "default" in spec:
                validate_expression(spec["default"])
        else:
            _agg_check_fields(spec, {"groupBy", "buckets"}, {"output", "granularity"}, op)
            _agg_pipeline_count(spec["buckets"])
            if "granularity" in spec:
                raise MongoError("$bucketAuto granularity is unsupported", 115)
        validate_expression(spec["groupBy"])
        if "output" in spec:
            list(_agg_accumulator_specs(spec["output"]))
    elif op == "$count":
        _agg_field_name(spec, dotted=False)
    elif op == "$sample":
        _agg_check_fields(spec, {"size"}, set(), op)
        _agg_pipeline_count(spec["size"], zero=True)
    elif op == "$replaceRoot":
        _agg_check_fields(spec, {"newRoot"}, set(), op)
        validate_expression(spec["newRoot"])
    elif op in ("$sortByCount", "$replaceWith", "$documents"):
        validate_expression(spec)
    elif op == "$lookup":
        _agg_check_fields(spec, {"as"}, {"from", "pipeline", "localField", "foreignField", "let"}, op)
        _agg_field_name(spec["as"])
        if "from" not in spec and not spec.get("pipeline"):
            raise MongoError("$lookup requires from or a pipeline starting with $documents")
        if "from" in spec and not _agg_string(spec["from"]):
            raise MongoError("$lookup from cannot be empty")
        if ("localField" in spec) != ("foreignField" in spec):
            raise MongoError("$lookup requires localField and foreignField together")
        if "localField" in spec:
            if "from" not in spec:
                raise MongoError("$lookup localField/foreignField requires from")
            for name in ("localField", "foreignField"):
                _agg_field_name(spec[name])
        elif "pipeline" not in spec:
            raise MongoError("$lookup requires localField and foreignField or a pipeline")
        if "let" in spec and "pipeline" not in spec:
            raise MongoError("$lookup let requires a pipeline")
        for name, raw in _agg_object(spec.get("let", {})).items():
            _agg_variable_name(name)
            validate_expression(raw)
        if "pipeline" in spec:
            validate_pipeline(spec["pipeline"], allow_write=False)
            if "from" not in spec and "$documents" not in spec["pipeline"][0]:
                raise MongoError("$lookup without from must start with $documents")
    elif op == "$graphLookup":
        _agg_check_fields(
            spec,
            {"from", "startWith", "connectFromField", "connectToField", "as"},
            {"maxDepth", "depthField", "restrictSearchWithMatch"},
            op,
        )
        if not _agg_string(spec["from"]):
            raise MongoError("$graphLookup from cannot be empty")
        for name in ("connectFromField", "connectToField", "as", "depthField"):
            if name in spec:
                _agg_field_name(spec[name])
        validate_expression(spec["startWith"])
        if "maxDepth" in spec:
            _agg_pipeline_count(spec["maxDepth"], zero=True)
        if "restrictSearchWithMatch" in spec:
            validate_filter(spec["restrictSearchWithMatch"])
    elif op == "$facet":
        if facet:
            raise MongoError("Cannot nest $facet inside another $facet stage", 40600)
        for name, pipeline in _agg_object(spec).items():
            _agg_field_name(name, dotted=False)
            validate_pipeline(pipeline, allow_write=False, _facet=True)
    elif op == "$unionWith":
        if isinstance(spec, str):
            if not spec:
                raise MongoError("$unionWith collection cannot be empty")
        else:
            _agg_check_fields(spec, set(), {"coll", "pipeline"}, op)
            if "coll" in spec and not _agg_string(spec["coll"]):
                raise MongoError("$unionWith collection cannot be empty")
            if "pipeline" in spec:
                validate_pipeline(spec["pipeline"], allow_write=False)
            if "coll" not in spec and (not spec.get("pipeline") or "$documents" not in spec["pipeline"][0]):
                raise MongoError("$unionWith without coll must start with $documents")
    elif op == "$setWindowFields":
        _agg_window_specs(spec)
    elif op in _AGG_WRITE_STAGES:
        _agg_validate_write_stage(op, spec)
    else:
        raise MongoError(f"Unrecognized or unsupported pipeline stage name: '{op}'", 40324)


def validate_pipeline(pipeline, *, allow_write=True, _facet=False):
    if not isinstance(pipeline, list):
        raise MongoError("The pipeline must be an array", 14)
    if len(pipeline) > 1000:
        raise MongoError("The pipeline exceeds the limit of 1000 stages", 7749501)
    for index, stage in enumerate(pipeline):
        op, spec = _agg_single_key(stage, "A pipeline stage specification")
        if op in _AGG_WRITE_STAGES and (not allow_write or index != len(pipeline) - 1):
            raise MongoError(f"{op} must be the final stage of a writable top-level pipeline", 40601)
        if op == "$documents" and index:
            raise MongoError("$documents must be the first stage", 40602)
        _agg_validate_stage(op, spec, facet=_facet)


def validate_update_pipeline(pipeline):
    if not isinstance(pipeline, list):
        raise MongoError("The update pipeline must be an array", 14)
    for stage in pipeline:
        op, _ = _agg_single_key(stage, "An update pipeline stage specification")
        if op not in _AGG_UPDATE_STAGES:
            raise MongoError(f"{op} is not allowed in an update pipeline", 72)
    validate_pipeline(pipeline, allow_write=False)


def update_pipeline(document, pipeline, variables=None):
    validate_update_pipeline(pipeline)
    variables = _agg_scope(variables)
    for stage in pipeline:
        op, spec = next(iter(stage.items()))
        document = _agg_mutating_stage(document, op, spec, variables)
    return document


def _agg_run(documents, pipeline, variables, read_collection, write_collection):
    for stage in pipeline:
        op, spec = next(iter(stage.items()))
        if op == "$match":
            documents = [doc for doc in documents if matches(doc, spec, variables)]
        elif op == "$sort":
            documents = sort_documents(documents, spec)
        elif op == "$skip":
            documents = documents[int(_agg_number(spec)) :]
        elif op == "$limit":
            documents = documents[: int(_agg_number(spec))]
        elif op in _AGG_UPDATE_STAGES:
            documents = [_agg_mutating_stage(doc, op, spec, variables) for doc in documents]
        elif op == "$unwind":
            documents = list(_agg_unwind(documents, spec))
        elif op == "$group":
            accumulators = list(
                _agg_accumulator_specs({field: value for field, value in spec.items() if field != "_id"})
            )
            documents = _agg_group(documents, spec["_id"], accumulators, variables)
        elif op == "$count":
            documents = [{spec: _agg_int_result(len(documents), [])}] if documents else []
        elif op == "$sortByCount":
            documents = _agg_group(documents, spec, [("count", "$sum", 1)], variables)
            documents.sort(key=lambda doc: doc["count"], reverse=True)
        elif op in ("$bucket", "$bucketAuto"):
            documents = _agg_bucket(documents, spec, variables, automatic=(op == "$bucketAuto"))
        elif op == "$sample":
            documents = random.sample(documents, min(int(_agg_number(spec["size"])), len(documents)))
        elif op == "$facet":
            documents = [
                {
                    name: _agg_run(documents, branch, variables, read_collection, None)
                    for name, branch in spec.items()
                }
            ]
        elif op == "$lookup":
            documents = _agg_lookup(documents, spec, variables, read_collection)
        elif op == "$graphLookup":
            documents = _agg_graph_lookup(documents, spec, variables, read_collection)
        elif op == "$unionWith":
            if isinstance(spec, str):
                spec = {"coll": spec}
            other = list(read_collection(spec["coll"])) if "coll" in spec else []
            documents = documents + _agg_run(
                other, spec.get("pipeline", []), variables, read_collection, None
            )
        elif op == "$documents":
            documents = [_agg_object(doc) for doc in _agg_array(evaluate(spec, {}, variables))]
        elif op == "$setWindowFields":
            documents = _agg_windows(documents, spec, variables)
        elif op in _AGG_WRITE_STAGES:
            write_collection(op, spec, documents)
            return []
    return documents


def run_pipeline(documents, pipeline, *, variables=None, read_collection=None, write_collection=None):
    validate_pipeline(pipeline, allow_write=(write_collection is not None))
    if read_collection is None:

        def read_collection(name, database=None):
            raise MongoError(f"Cannot access collection {name!r} from this pipeline")

    try:
        return _agg_run(list(documents), pipeline, _agg_scope(variables), read_collection, write_collection)
    except MongoError:
        raise
    except (ValueError, OverflowError, ZeroDivisionError, DecimalException) as exc:
        raise MongoError(f"Error running aggregation pipeline: {exc}") from exc


_AGG_WINDOW_RANKS = {"$rank", "$denseRank", "$documentNumber"}


def _agg_window_specs(spec):
    _agg_check_fields(spec, {"output"}, {"partitionBy", "sortBy"}, "$setWindowFields")
    if "partitionBy" in spec:
        validate_expression(spec["partitionBy"])
    ordering = spec.get("sortBy", {})
    validate_sort(ordering)
    output = _agg_object(spec["output"])
    _agg_check_paths(_agg_field_name(field) for field in output)
    result = []
    for field, specification in output.items():
        options = _agg_object(specification)
        op, operand = _agg_single_key(
            {key: value for key, value in options.items() if key != "window"}, "A window output specification"
        )
        window = options.get("window")
        if op in _AGG_WINDOW_RANKS | {"$shift"}:
            if "window" in options:
                raise MongoError(f"{op} does not accept a window")
            if not ordering or (op in _AGG_WINDOW_RANKS and len(ordering) != 1):
                raise MongoError(f"{op} requires a valid sortBy specification")
            if op == "$shift":
                _agg_check_fields(operand, {"output", "by"}, {"default"}, op)
                _agg_integer(operand["by"])
                validate_expression(operand["output"])
                if "default" in operand:
                    validate_expression(operand["default"])
            elif operand != {}:
                raise MongoError(f"{op} takes an empty document")
        else:
            list(_agg_accumulator_specs({"value": {op: operand}}))
            if "window" in options:
                window = _agg_window_bounds(window, ordering)
        result.append((field, op, operand, window))
    return result


def _agg_window_bounds(window, ordering):
    _agg_check_fields(window, set(), {"documents", "range", "unit"}, "$setWindowFields window")
    kind = "range" if "range" in window else "documents"
    if "documents" in window and "range" in window:
        raise MongoError("A window may specify documents or range")
    unit = window.get("unit")
    if "unit" in window and (
        kind != "range" or unit not in {*_AGG_MILLISECONDS, *_AGG_MONTHS, "day", "week"}
    ):
        raise MongoError("Invalid time unit for range window")
    bounds = window.get(kind)
    if not isinstance(bounds, list) or len(bounds) != 2:
        raise MongoError("Window must specify two bounds")
    normalized = []
    for value in bounds:
        if value in ("current", "unbounded"):
            normalized.append(value)
        elif kind == "documents" or unit is not None:
            normalized.append(_agg_integer(value))
        else:
            normalized.append(_agg_number(value))
    lower, upper = normalized
    if (
        lower != "unbounded"
        and upper != "unbounded"
        and (0 if lower == "current" else lower) > (0 if upper == "current" else upper)
    ):
        raise MongoError("Lower window bound must not exceed upper bound")
    if kind == "range" and (len(ordering) != 1 or next(iter(ordering.values())) != 1):
        raise MongoError("Range windows require one ascending sortBy field")
    if not ordering and normalized != ["unbounded", "unbounded"]:
        raise MongoError("A bounded window requires sortBy")
    return kind, lower, upper, unit


def _agg_window_rows(rows, values, index, window):
    if window is None:
        return rows
    kind, lower, upper, unit = window
    if kind == "documents":
        first = 0 if lower == "unbounded" else index + (0 if lower == "current" else lower)
        last = len(rows) if upper == "unbounded" else index + (0 if upper == "current" else upper) + 1
        return rows[max(0, first) : max(0, last)]
    pivot = values[index]

    def boundary(value):
        if value == "current":
            return pivot
        if unit is not None:
            return DateTime.from_datetime(_agg_add_date(_agg_as_date(pivot), unit, value))
        number = _agg_number(pivot)
        if isinstance(value, Decimal) or isinstance(number, Decimal):
            with localcontext(_AGG_DECIMAL):
                return Decimal128.from_decimal(Decimal(value) + Decimal(number))
        return number + value

    first = MISSING if lower == "unbounded" else boundary(lower)
    last = MISSING if upper == "unbounded" else boundary(upper)
    return [
        row
        for value, row in zip(values, rows)
        if (first is MISSING or compare(value, first) >= 0) and (last is MISSING or compare(value, last) <= 0)
    ]


def _agg_windows(documents, spec, variables):
    partitions = {}
    outputs = _agg_window_specs(spec)
    for doc in documents:
        key = evaluate(spec.get("partitionBy"), doc, variables)
        if isinstance(key, list):
            raise MongoError("A window partition key cannot be an array", 14)
        partitions.setdefault(value_key(None if key is MISSING else key), []).append(doc)

    result = []
    for partition in partitions.values():
        ordering = spec.get("sortBy", {})
        rows = sort_documents(partition, ordering)
        sort_field = next(iter(ordering), None)
        rank, dense_rank = 1, 1
        values = [get_path(row, sort_field) for row in rows] if sort_field is not None else []
        values = [None if value is MISSING else value for value in values]
        for index, doc in enumerate(rows):
            if index and values and not bson_equal(values[index - 1], values[index]):
                rank = index + 1
                dense_rank += 1
            row = deepcopy(doc)
            for field, op, operand, window in outputs:
                if op in _AGG_WINDOW_RANKS:
                    value = {"$rank": rank, "$denseRank": dense_rank, "$documentNumber": index + 1}[op]
                    value = _agg_int_result(value, [])
                elif op == "$shift":
                    target = index + _agg_integer(operand["by"])
                    if 0 <= target < len(rows):
                        value = evaluate(operand["output"], rows[target], variables)
                    else:
                        value = evaluate(operand.get("default"), doc, variables)
                    if value is MISSING:
                        value = None
                else:
                    selected = _agg_window_rows(rows, values, index, window)
                    value = _agg_accumulate(op, operand, selected, variables)
                set_path(row, field, value)
            result.append(row)
    return result


# Catalog and write constraints


def require_document(value, name):
    if not isinstance(value, dict):
        raise MongoError(f"{name} must be an object", 14)
    return value


def require_list(value, name):
    if not isinstance(value, list):
        raise MongoError(f"{name} must be an array", 14)
    return value


def require_string(value, name):
    if not isinstance(value, str) or not value:
        raise MongoError(f"{name} must be a nonempty string", 14)
    return value


def require_bool(value, name):
    if not isinstance(value, bool):
        raise MongoError(f"{name} must be a boolean", 14)
    return value


def require_integer(value, name, minimum=None):
    if not isinstance(value, int) or isinstance(value, bool):
        raise MongoError(f"{name} must be an integer", 14)
    if minimum is not None and value < minimum:
        raise MongoError(f"{name} must be at least {minimum}")
    return value


def require_integral_number(value, name, minimum=-(1 << 63), maximum=(1 << 63) - 1):
    if not numeric(value):
        raise MongoError(f"{name} must be numeric", 14)
    try:
        number = int(value.to_decimal() if isinstance(value, Decimal128) else value)
    except (OverflowError, ValueError):
        raise MongoError(f"{name} must be finite") from None
    if not minimum <= number <= maximum:
        raise MongoError(f"{name} is outside the allowed range")
    return number


def database_name(value):
    require_string(value, "database name")
    if len(value.encode("utf-8")) >= 64 or any(char in value for char in '/\\. "$\0'):
        raise MongoError(f"invalid database name: {value!r}", 73)
    return value


def collection_name(value):
    require_string(value, "collection name")
    if "\0" in value or value.startswith(".") or value.startswith("$"):
        raise MongoError(f"invalid collection name: {value!r}", 73)
    return value


def namespace(value):
    require_string(value, "namespace")
    db, separator, name = value.partition(".")
    if not separator:
        raise MongoError(f"invalid namespace: {value!r}", 73)
    return database_name(db), collection_name(name)


def simple_collation(spec):
    if spec is not None and spec != {"locale": "simple"}:
        require_document(spec, "collation")
        raise MongoError("only the simple collation is supported", 72)


def document_size(document):
    encoded = encode_bson(document)
    if len(encoded) > MAX_BSON_SIZE:
        raise MongoError(f"BSON document is {len(encoded)} bytes and exceeds {MAX_BSON_SIZE}", 10334)
    return encoded


def with_id(document):
    require_document(document, "insert document")
    key = document["_id"] if "_id" in document else ObjectId.new()
    if isinstance(key, (list, Regex, Undefined)):
        raise MongoError("invalid _id field type")
    if isinstance(key, dict) and any(field.startswith("$") for field in key):
        raise MongoError("_id subfields cannot start with '$'")
    return {"_id": key, **document}


@dataclass
class Collection:
    database: str
    name: str
    uuid: bytes
    options: dict
    indexes: dict

    @property
    def is_view(self):
        return "viewOn" in self.options

    @property
    def capped(self):
        return self.options.get("capped", False)

    @property
    def ns(self):
        return f"{self.database}.{self.name}"


def id_index():
    return {"v": 2, "key": {"_id": 1}, "name": "_id_"}


class Catalog:
    def __init__(self, filename, *, readonly=False):
        if readonly:
            source = Path(filename).resolve(strict=True)
            if not source.is_file():
                raise ValueError(f"Expected a SQLite persistence file: {source}")
            self.sql = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, isolation_level=None)
            return
        self.sql = sqlite3.connect(filename or ":memory:", isolation_level=None, check_same_thread=False)
        try:
            self.sql.execute("PRAGMA foreign_keys = ON")
            self.sql.executescript("""
                CREATE TABLE IF NOT EXISTS mongo_collections (
                    db TEXT NOT NULL,
                    name TEXT NOT NULL,
                    uuid BLOB NOT NULL,
                    options BLOB NOT NULL,
                    PRIMARY KEY (db, name)
                );
                CREATE TABLE IF NOT EXISTS mongo_indexes (
                    db TEXT NOT NULL,
                    name TEXT NOT NULL,
                    idx TEXT NOT NULL,
                    spec BLOB NOT NULL,
                    PRIMARY KEY (db, name, idx),
                    FOREIGN KEY (db, name) REFERENCES mongo_collections (db, name)
                        ON UPDATE CASCADE ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS mongo_documents (
                    record INTEGER PRIMARY KEY AUTOINCREMENT,
                    db TEXT NOT NULL,
                    name TEXT NOT NULL,
                    data BLOB NOT NULL,
                    FOREIGN KEY (db, name) REFERENCES mongo_collections (db, name)
                        ON UPDATE CASCADE ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS mongo_collection_records ON mongo_documents (db, name, record);
                """)
        except BaseException:
            self.sql.close()
            raise

    def collection(self, db, name):
        row = self.sql.execute(
            "SELECT uuid, options FROM mongo_collections WHERE db=? AND name=?", (db, name)
        ).fetchone()
        if row is None:
            return None
        indexes = self.sql.execute(
            "SELECT idx, spec FROM mongo_indexes WHERE db=? AND name=? ORDER BY rowid", (db, name)
        )
        return Collection(
            db, name, row[0], decode_bson(row[1])[0], {key: decode_bson(spec)[0] for key, spec in indexes}
        )

    def create(self, db, name, options=None, indexes=None):
        options = {} if options is None else options
        identifier = uuid4().bytes
        self.sql.execute(
            "INSERT INTO mongo_collections (db, name, uuid, options) VALUES (?, ?, ?, ?)",
            (db, name, identifier, encode_bson(options)),
        )
        if indexes is None:
            indexes = {} if "viewOn" in options else {"_id_": id_index()}
        collection = Collection(db, name, identifier, options, {})
        for spec in indexes.values():
            self.set_index(collection, spec)
        return collection

    def set_options(self, collection, options):
        self.sql.execute(
            "UPDATE mongo_collections SET options=? WHERE db=? AND name=?",
            (encode_bson(options), collection.database, collection.name),
        )
        collection.options = options

    def set_index(self, collection, spec):
        self.sql.execute(
            """INSERT INTO mongo_indexes (db, name, idx, spec) VALUES (?, ?, ?, ?)
               ON CONFLICT(db, name, idx) DO UPDATE SET spec=excluded.spec""",
            (collection.database, collection.name, spec["name"], encode_bson(spec)),
        )
        collection.indexes[spec["name"]] = spec

    def drop_index(self, collection, name):
        self.sql.execute(
            "DELETE FROM mongo_indexes WHERE db=? AND name=? AND idx=?",
            (collection.database, collection.name, name),
        )
        del collection.indexes[name]

    def drop(self, db, name):
        return self.sql.execute("DELETE FROM mongo_collections WHERE db=? AND name=?", (db, name)).rowcount

    def record_rows(self, db, name):
        return self.sql.execute(
            "SELECT record, data FROM mongo_documents WHERE db=? AND name=? ORDER BY record",
            (db, name),
        )

    def records(self, db, name):
        return {record: (decode_bson(data)[0], len(data)) for record, data in self.record_rows(db, name)}

    def namespaces(self, db=None):
        if db is None:
            return self.sql.execute("SELECT db, name FROM mongo_collections ORDER BY db, name").fetchall()
        return self.sql.execute(
            "SELECT db, name FROM mongo_collections WHERE db=? ORDER BY name", (db,)
        ).fetchall()

    def data_stats(self, db, name=None):
        if name is None:
            return self.sql.execute(
                "SELECT count(*), coalesce(sum(length(data)), 0) FROM mongo_documents WHERE db=?", (db,)
            ).fetchone()
        return self.sql.execute(
            "SELECT count(*), coalesce(sum(length(data)), 0) FROM mongo_documents WHERE db=? AND name=?",
            (db, name),
        ).fetchone()


def index_candidates(value, parts, route=(), arrays=()):
    """Return values together with the array positions that produced them."""
    if isinstance(value, list):
        position = _q_index(parts[0]) if parts else None
        if position is not None and str(position) == parts[0]:
            item = value[position] if position < len(value) else MISSING
            return index_candidates(item, parts[1:], route + (parts[0],), arrays)
        if not value:
            return [(MISSING if parts else Undefined(), arrays + ((route, -1),))]
        result = []
        for position, item in enumerate(value):
            positions = arrays + ((route, position),)
            if parts:
                result.extend(index_candidates(item, parts, route + (position,), positions))
            else:
                result.append((item, positions))
        return result
    if not parts:
        return [(value, arrays)]
    if not isinstance(value, dict) or parts[0] not in value:
        return [(MISSING, arrays)]
    return index_candidates(value[parts[0]], parts[1:], route + (parts[0],), arrays)


def index_values(document, spec):
    if spec.get("partialFilterExpression") and not matches(document, spec["partialFilterExpression"]):
        return
    fields = [index_candidates(document, field.split(".")) for field in spec["key"]]
    if spec.get("sparse") and all(value is MISSING for candidates in fields for value, _ in candidates):
        return

    def combine(position, values, seen):
        if position == len(fields):
            yield values
            return
        for value, arrays in fields[position]:
            if any(path in seen and seen[path] != index for path, index in arrays):
                continue
            for path, _ in arrays:
                for other_path in seen:
                    length = min(len(path), len(other_path))
                    if path[:length] != other_path[:length]:
                        raise MongoError("cannot index parallel arrays", 171)
            entry = None if value is MISSING else value
            yield from combine(position + 1, (*values, entry), {**seen, **dict(arrays)})

    yield from combine(0, (), {})


def is_unique(spec):
    return spec["name"] == "_id_" or spec.get("unique", False)


def index_token(values):
    return tuple(map(value_key, values))


def check_index_spec(spec):
    require_document(spec, "index spec")
    key = require_document(spec.get("key"), "index key")
    if not key or len(key) > 32:
        raise MongoError("an index needs between 1 and 32 key fields", 67)
    for field, direction in key.items():
        if (
            any(not part or part.startswith("$") for part in field.split("."))
            or not numeric(direction)
            or direction not in (-1, 1)
        ):
            raise MongoError("only ascending and descending indexes are supported", 67)
    name = require_string(
        spec.get("name", "_".join(f"{field}_{int(order)}" for field, order in key.items())), "index name"
    )
    if name == "*" or "\0" in name:
        raise MongoError("invalid index name", 67)
    supported = {
        "v",
        "name",
        "key",
        "background",
        "unique",
        "sparse",
        "partialFilterExpression",
        "expireAfterSeconds",
        "hidden",
        "collation",
        "ns",
    }
    if unknown := spec.keys() - supported:
        raise MongoError(f"unsupported index option: {min(unknown)}", 197)
    simple_collation(spec.get("collation"))
    index_version = require_integer(spec.get("v", 2), "index version")
    if index_version not in (1, 2):
        raise MongoError("index version must be 1 or 2", 67)
    result = {"v": index_version, "key": {field: int(order) for field, order in key.items()}, "name": name}
    for option in ("unique", "sparse", "hidden"):
        if option in spec and require_bool(spec[option], option):
            result[option] = True
    if "partialFilterExpression" in spec:
        if result.get("sparse"):
            raise MongoError("partial and sparse options cannot be combined", 67)
        validate_filter(spec["partialFilterExpression"])
        result["partialFilterExpression"] = spec["partialFilterExpression"]
    if "expireAfterSeconds" in spec:
        seconds = require_integral_number(spec["expireAfterSeconds"], "expireAfterSeconds", 0, 2_147_483_647)
        if len(key) != 1 or "_id" in key:
            raise MongoError("a TTL index requires one non-_id field and valid expiry seconds", 67)
        result["expireAfterSeconds"] = seconds
    if name == "_id_" or result["key"] == {"_id": 1}:
        meaningful = result.keys() - {"v", "key", "name", "unique"}
        if result["key"] != {"_id": 1} or meaningful:
            raise MongoError("invalid options for the _id index", 197)
        if name != "_id_":
            raise MongoError("the _id index already exists", 85)
        return id_index()
    return result


class CollectionWriter:
    """A transaction-local snapshot with keys for enforcing unique indexes."""

    def __init__(self, catalog, collection, *, bypass=False):
        self.catalog = catalog
        self.collection = collection
        self.records = catalog.records(collection.database, collection.name)
        self.keys = {}
        self.bypass = bypass
        self.size = sum(size for _, size in self.records.values())
        for spec in collection.indexes.values():
            if is_unique(spec):
                self.keys[spec["name"]] = self.build_index(spec)

    def build_index(self, spec):
        keys = {}
        for record, (document, _) in self.records.items():
            for values in index_values(document, spec):
                if is_unique(spec):
                    keys[self.checked_key(keys, spec, values, record)] = record
        return keys

    def checked_key(self, keys, spec, values, record):
        key = index_token(values)
        if key in keys and keys[key] != record:
            key_value = dict(zip(spec["key"], values))
            raise MongoError(
                f"E11000 duplicate key error collection: {self.collection.ns} index: {spec['name']} dup key: {key_value!r}",
                11000,
                keyPattern=spec["key"],
                keyValue=key_value,
            )
        return key

    def prepare(self, document, record, previous):
        data = document_size(document)
        options = self.collection.options
        validator = options.get("validator")
        level = options.get("validationLevel", "strict")
        if (
            validator
            and not self.bypass
            and level != "off"
            and (level != "moderate" or previous is None or matches(previous, validator))
            and not matches(document, validator)
        ):
            if options.get("validationAction", "error") == "error":
                raise MongoError(
                    "Document failed validation", 121, errInfo={"failingDocumentId": document["_id"]}
                )
            LOGGER.warning("Document failed validation in %s", self.collection.ns)
        new_keys = {}
        for name, spec in self.collection.indexes.items():
            values = list(index_values(document, spec))
            if name in self.keys:
                new_keys[name] = [self.checked_key(self.keys[name], spec, value, record) for value in values]
        return data, new_keys

    def remove_keys(self, previous):
        for name, keys in self.keys.items():
            for values in index_values(previous, self.collection.indexes[name]):
                keys.pop(index_token(values), None)

    def set_record(self, record, document, data, keys):
        self.records[record] = document, len(data)
        self.size += len(data)
        for name, tokens in keys.items():
            for token in tokens:
                self.keys[name][token] = record

    def insert(self, document):
        document = with_id(document)
        data, keys = self.prepare(document, None, None)
        record = self.catalog.sql.execute(
            "INSERT INTO mongo_documents (db, name, data) VALUES (?, ?, ?)",
            (self.collection.database, self.collection.name, data),
        ).lastrowid
        self.set_record(record, document, data, keys)
        if self.collection.capped:
            self.enforce_cap()
        return document

    def enforce_cap(self):
        options = self.collection.options
        while (len(self.records) > 1 and self.size > options["size"]) or len(self.records) > options.get(
            "max", 2_147_483_647
        ):
            self.delete(next(iter(self.records)))

    def replace(self, record, document):
        previous, size = self.records[record]
        if "_id" not in document or encode_bson({"_id": document["_id"]}) != encode_bson(
            {"_id": previous["_id"]}
        ):
            raise MongoError(
                "Performing an update on the path '_id' would modify the immutable field '_id'", 66
            )
        document = with_id(document)
        data, keys = self.prepare(document, record, previous)
        if data == encode_bson(previous):
            return False
        self.catalog.sql.execute("UPDATE mongo_documents SET data=? WHERE record=?", (data, record))
        self.remove_keys(previous)
        self.size -= size
        self.set_record(record, document, data, keys)
        return True

    def delete(self, record):
        previous, size = self.records.pop(record)
        self.catalog.sql.execute("DELETE FROM mongo_documents WHERE record=?", (record,))
        self.remove_keys(previous)
        self.size -= size


# SCRAM authentication for the optional local test account


def saslprep(password):
    mapped = "".join(
        " " if stringprep.in_table_c12(char) else char
        for char in password
        if not stringprep.in_table_b1(char)
    )
    normalized = ucd_3_2_0.normalize("NFKC", mapped)
    prohibited = (
        stringprep.in_table_a1,
        stringprep.in_table_c12,
        stringprep.in_table_c21,
        stringprep.in_table_c22,
        stringprep.in_table_c3,
        stringprep.in_table_c4,
        stringprep.in_table_c5,
        stringprep.in_table_c6,
        stringprep.in_table_c7,
        stringprep.in_table_c8,
        stringprep.in_table_c9,
    )
    if any(check(char) for char in normalized for check in prohibited):
        raise ValueError("password contains a character prohibited by SASLprep")
    if any(map(stringprep.in_table_d1, normalized)) and (
        any(map(stringprep.in_table_d2, normalized))
        or not stringprep.in_table_d1(normalized[0])
        or not stringprep.in_table_d1(normalized[-1])
    ):
        raise ValueError("password fails SASLprep bidirectional text rules")
    return normalized


def sasl_attributes(payload):
    fields = {}
    for entry in payload.split(b","):
        key, separator, value = entry.partition(b"=")
        if not separator or len(key) != 1 or key in fields:
            raise MongoError("Authentication failed.", 18)
        fields[key] = value
    return fields


def sasl_payload(command):
    value = command.get("payload")
    if not isinstance(value, bytes) or len(value) > 8192:
        raise MongoError("Authentication failed.", 18)
    return bytes(value)


def scram_secret(mechanism, username, password):
    if mechanism == "SCRAM-SHA-256":
        return "sha256", saslprep(password).encode("utf-8")
    if mechanism == "SCRAM-SHA-1":
        digest = hashlib.md5(f"{username}:mongo:{password}".encode("utf-8"), usedforsecurity=False)
        return "sha1", digest.hexdigest().encode("ascii")
    raise MongoError(f"unsupported authentication mechanism {mechanism!r}", 18)


def scram_keys(algorithm, secret, salt, iterations):
    salted = hashlib.pbkdf2_hmac(algorithm, secret, salt, iterations)
    client_key = hmac.digest(salted, b"Client Key", algorithm)
    stored_key = hashlib.new(algorithm, client_key).digest()
    server_key = hmac.digest(salted, b"Server Key", algorithm)
    return client_key, stored_key, server_key


class TestAccount:
    def __init__(self, database, username, password):
        self.database = database_name(database)
        self.username = require_string(username, "auth user")
        if "\0" in self.username:
            raise ValueError("auth user cannot contain NUL")
        self.mechanisms = {}
        for mechanism, iterations in (("SCRAM-SHA-256", 15000), ("SCRAM-SHA-1", 10000)):
            algorithm, secret = scram_secret(mechanism, username, password)
            salt = secrets.token_bytes(16)
            _, stored_key, server_key = scram_keys(algorithm, secret, salt, iterations)
            self.mechanisms[mechanism] = (algorithm, salt, iterations, stored_key, server_key)

    def start(self, db, command, connection):
        connection.pop("scram", None)
        mechanism = command.get("mechanism")
        if not isinstance(mechanism, str) or db != self.database or mechanism not in self.mechanisms:
            raise MongoError("Authentication failed.", 18)
        payload = sasl_payload(command)
        header = payload[:3]
        if header not in (b"n,,", b"y,,"):
            raise MongoError("Authentication failed.", 18)
        first = payload[3:]
        fields = sasl_attributes(first)
        expected = self.username.encode("utf-8").replace(b"=", b"=3D").replace(b",", b"=2C")
        nonce = fields.get(b"r", b"")
        if (
            fields.get(b"n") != expected
            or b"m" in fields
            or not nonce
            or any(byte < 33 or byte > 126 or byte == 44 for byte in nonce)
        ):
            raise MongoError("Authentication failed.", 18)
        algorithm, salt, iterations, stored, server = self.mechanisms[mechanism]
        nonce += base64.b64encode(secrets.token_bytes(24))
        challenge = b"r=" + nonce + b",s=" + base64.b64encode(salt) + b",i=" + str(iterations).encode("ascii")
        options = command.get("options", {})
        skip_empty = isinstance(options, dict) and options.get("skipEmptyExchange") is True
        connection["scram"] = (header, first, challenge, nonce, algorithm, stored, server, skip_empty)
        return {"conversationId": 1, "done": False, "payload": Binary(challenge)}

    def continue_(self, db, command, connection):
        current = connection.pop("scram", None)
        payload = sasl_payload(command)
        if db != self.database or command.get("conversationId") != 1 or current is None:
            raise MongoError("Authentication failed.", 18)
        if current == "empty":
            if payload:
                raise MongoError("Authentication failed.", 18)
            connection["authenticated"] = True
            return {"conversationId": 1, "done": True, "payload": Binary(b"")}
        header, first, challenge, nonce, algorithm, stored, server, skip_empty = current
        attrs = sasl_attributes(payload)
        final, separator, proof_base64 = payload.rpartition(b",p=")
        if (
            not separator
            or attrs.get(b"p") != proof_base64
            or attrs.get(b"r") != nonce
            or attrs.get(b"c") != base64.b64encode(header)
        ):
            raise MongoError("Authentication failed.", 18)
        try:
            proof = base64.b64decode(proof_base64, validate=True)
        except ValueError:
            raise MongoError("Authentication failed.", 18) from None
        message = b",".join((first, challenge, final))
        signature = hmac.digest(stored, message, algorithm)
        if len(proof) != len(signature):
            raise MongoError("Authentication failed.", 18)
        client_key = bytes(a ^ b for a, b in zip(proof, signature))
        if not hmac.compare_digest(hashlib.new(algorithm, client_key).digest(), stored):
            raise MongoError("Authentication failed.", 18)
        server_signature = Binary(b"v=" + base64.b64encode(hmac.digest(server, message, algorithm)))
        if skip_empty:
            connection["authenticated"] = True
        else:
            connection["scram"] = "empty"
        return {"conversationId": 1, "done": skip_empty, "payload": server_signature}


# Commands

COMMANDS = {
    "aggregate": "aggregate",
    "buildInfo": "build_info",
    "bulkWrite": "bulk_write",
    "collMod": "coll_mod",
    "collStats": "coll_stats",
    "connectionStatus": "connection_status",
    "count": "count",
    "create": "create",
    "createIndexes": "create_indexes",
    "dataSize": "data_size",
    "dbStats": "db_stats",
    "delete": "delete",
    "distinct": "distinct",
    "drop": "drop",
    "dropDatabase": "drop_database",
    "dropIndexes": "drop_indexes",
    "endSessions": "end_sessions",
    "explain": "explain",
    "find": "find",
    "findAndModify": "find_one_write",
    "getCmdLineOpts": "cmd_line_opts",
    "getMore": "get_more",
    "getParameter": "get_parameter",
    "hello": "hello",
    "hostInfo": "host_info",
    "insert": "insert",
    "isMaster": "hello",
    "killCursors": "kill_cursors",
    "killSessions": "end_sessions",
    "listCollections": "list_collections",
    "listCommands": "list_commands",
    "listDatabases": "list_databases",
    "listIndexes": "list_indexes",
    "ping": "ping",
    "refreshSessions": "refresh_sessions",
    "renameCollection": "rename",
    "saslContinue": "sasl_continue",
    "saslStart": "sasl_start",
    "serverStatus": "server_status",
    "setParameter": "set_parameter",
    "update": "update",
    "validate": "validate",
    "whatsmyuri": "whatsmyuri",
}
WRITE_COMMANDS = {
    "aggregate",
    "bulkwrite",
    "collmod",
    "create",
    "createindexes",
    "delete",
    "drop",
    "dropdatabase",
    "dropindexes",
    "findandmodify",
    "insert",
    "renamecollection",
    "update",
}
ADMIN_COMMANDS = {
    "bulkwrite",
    "getcmdlineopts",
    "getparameter",
    "hostinfo",
    "listdatabases",
    "renamecollection",
    "serverstatus",
    "setparameter",
}
PRE_AUTH_COMMANDS = {"hello", "ismaster", "saslstart", "saslcontinue", "ping", "buildinfo"}
API_V1_COMMANDS = {
    "aggregate",
    "bulkwrite",
    "collmod",
    "count",
    "create",
    "createindexes",
    "delete",
    "drop",
    "dropdatabase",
    "dropindexes",
    "endsessions",
    "explain",
    "find",
    "findandmodify",
    "getmore",
    "hello",
    "insert",
    "killcursors",
    "listcollections",
    "listdatabases",
    "listindexes",
    "ping",
    "refreshsessions",
    "saslcontinue",
    "saslstart",
    "update",
}
COMMAND_METADATA = {
    "$db",
    "$clusterTime",
    "$readPreference",
    "lsid",
    "maxTimeMS",
    "comment",
    "writeConcern",
    "apiVersion",
    "apiStrict",
    "apiDeprecationErrors",
}
INDEX_PARAMETER_NAMES = {"expireAfterSeconds", "hidden"}
COLLECTION_VALIDATION_OPTIONS = {"validator", "validationLevel", "validationAction"}


def argument(command):
    return next(iter(command.values()))


def command_collection(command):
    return collection_name(argument(command))


def session_id(command):
    if "lsid" not in command:
        return None
    session = require_document(command["lsid"], "lsid")
    identifier = session.get("id")
    if not isinstance(identifier, Binary) or identifier.subtype != 4 or len(identifier) != 16:
        raise MongoError("lsid.id must be a UUID")
    uid = session.get("uid")
    return bytes(identifier), bytes(uid) if isinstance(uid, Binary) else None


def check_read_concern(command):
    if "readConcern" in command:
        concern = require_document(command["readConcern"], "readConcern")
        if concern.get("level", "local") not in ("local", "available", "majority", "linearizable"):
            raise MongoError("this readConcern level is unsupported on a standalone server", 72)
        if concern.keys() - {"level", "provenance"}:
            raise MongoError("timestamped readConcern is unsupported on a standalone server", 72)


def check_api_version(name, command):
    version = command.get("apiVersion")
    strict = command.get("apiStrict", False)
    flags = command.keys() & {"apiStrict", "apiDeprecationErrors"}
    for flag in flags:
        require_bool(command[flag], flag)
    if version is None:
        if flags:
            raise MongoError("apiStrict and apiDeprecationErrors require apiVersion", 4886600)
        return
    if require_string(version, "apiVersion") != "1":
        raise MongoError(f"Cannot accept API version {version}", 322)
    if strict and name not in API_V1_COMMANDS:
        raise MongoError(f"Command {next(iter(command))} is outside Stable API Version 1", 323)
    if strict and name == "aggregate" and "explain" in command:
        raise MongoError("aggregate.explain is unavailable in Stable API Version 1", 323)


def check_write_concern(command):
    if "writeConcern" not in command:
        return
    concern = require_document(command["writeConcern"], "writeConcern")
    if "j" in concern:
        require_bool(concern["j"], "writeConcern.j")
    if "wtimeout" in concern:
        require_integer(concern["wtimeout"], "writeConcern.wtimeout", 0)
    w = concern.get("w", 1)
    if w == "majority":
        return
    if isinstance(w, str):
        raise MongoError(f"cannot use non-majority 'w' mode {w!r} on a standalone server")
    if require_integer(w, "writeConcern.w", 0) > 1:
        raise MongoError("cannot use 'w' > 1 when a host is not replicated")


def command_variables(command):
    expressions = require_document(command.get("let", {}), "let")
    variables = {"NOW": DateTime.now()}
    for name, expression in expressions.items():
        if not name or not name[0].isalpha() or (name[0].isascii() and not name[0].islower()):
            raise MongoError(f"invalid variable name: {name!r}")
        variables[name] = evaluate(expression, {}, variables)
    return variables


def query_filter(spec):
    validate_filter(spec)
    return spec


def query_options(command):
    simple_collation(command.get("collation"))
    for option in ("tailable", "awaitData", "oplogReplay"):
        if command.get(option):
            raise MongoError(f"{option} is unsupported", 115)


def check_hint(collection, hint, *, allow_missing=False):
    if hint is None:
        return None
    if collection is None and allow_missing:
        return None
    if isinstance(hint, dict) and set(hint) == {"$natural"}:
        if hint["$natural"] not in (1, -1) or isinstance(hint["$natural"], bool):
            raise MongoError("$natural accepts only 1 or -1")
        return {"key": hint}
    indexes = {} if collection is None else collection.indexes
    if isinstance(hint, str):
        if hint == "$natural":
            return {"key": {"$natural": 1}}
        matching = [indexes[hint]] if hint in indexes else []
    elif isinstance(hint, dict):
        matching = [index for index in indexes.values() if list(index["key"].items()) == list(hint.items())]
    else:
        raise MongoError("hint must be a string or index key", 14)
    matching = [index for index in matching if not index.get("hidden")]
    if not matching:
        raise MongoError("hint provided does not correspond to an existing visible index")
    if len(matching) > 1:
        raise MongoError("hint is ambiguous; specify an index name")
    return matching[0]


def in_hinted_index(document, hint):
    if hint is None or not (hint.get("sparse") or hint.get("partialFilterExpression")):
        return True
    return next(index_values(document, hint), None) is not None


def check_validation_options(options):
    if "validator" in options:
        validate_filter(options["validator"])
    if options.get("validationLevel", "strict") not in ("off", "moderate", "strict"):
        raise MongoError("unsupported validationLevel")
    if options.get("validationAction", "error") not in ("warn", "error"):
        raise MongoError("unsupported validationAction")


@dataclass
class Tail:
    filter: dict
    variables: dict
    projection: dict | None
    skip: int
    remaining: int | None
    await_data: bool
    return_key: bool
    show_record_id: bool
    last_record: int = 0


@dataclass
class Cursor:
    database: str
    collection: str
    pending: deque
    session: tuple | None
    last_used: float
    no_timeout: bool = False
    tail: Tail | None = None
    in_use: bool = False

    @property
    def ns(self):
        return f"{self.database}.{self.collection}"


class MongoEngine:
    def __init__(self, filename=None, *, account=None, settings=None):
        self.catalog = Catalog(filename)
        self.account = account
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.cursors = {}
        self.started = time.monotonic()
        self.commands_run = 0
        self.settings = dict(settings or {})
        self.filename = filename
        self.closed = False
        ttl_interval = self.settings.get("ttl_interval", 60)
        self.ttl_enabled = bool(ttl_interval)
        self.ttl_interval = ttl_interval or 60
        self.ttl_changed = threading.Event()

    def close(self):
        with self.changed:
            self.closed = True
            self.changed.notify_all()
            self.ttl_changed.set()
            self.catalog.sql.close()

    def dispatch(self, db, command, connection):
        database_name(db)
        require_document(command, "command")
        if not command:
            raise MongoError("command is empty", 9)
        name = next(iter(command)).lower()
        methods = COMMAND_LOOKUP.get(name)
        if methods is None:
            raise MongoError(f"no such command: {next(iter(command))!r}", 59)
        if self.account and not connection.get("authenticated") and name not in PRE_AUTH_COMMANDS:
            raise MongoError(f"Command {next(iter(command))} requires authentication", 13)
        if name in ADMIN_COMMANDS and db != "admin":
            raise MongoError(f"{next(iter(command))} may only be run against the admin database", 13)
        if any(field in command for field in ("txnNumber", "startTransaction", "autocommit")):
            raise MongoError("Transaction numbers are only allowed on a replica set member or mongos", 20)
        check_api_version(name, command)
        session_id(command)
        check_read_concern(command)
        if name in WRITE_COMMANDS:
            check_write_concern(command)
        timeout = require_integer(command.get("maxTimeMS", 0), "maxTimeMS", 0)
        started = time.monotonic()
        with self.changed:
            self.reap_cursors()
            cursor = None
            if name == "getmore" and isinstance(argument(command), int):
                cursor = self.cursors.get(argument(command))
            execute = getattr(self, methods)
            if cursor is None or cursor.tail is None or not cursor.tail.await_data:
                return self.run_command(db, command, connection, name, execute, started, timeout)
            if cursor.in_use:
                raise MongoError("cursor is already in use", 237)
            cursor.in_use = True
            try:
                deadline = started + (timeout or 1000) / 1000
                response = self.run_command(db, command, connection, name, execute, started, 0)
                while response["cursor"]["id"] and not response["cursor"]["nextBatch"]:
                    delay = deadline - time.monotonic()
                    if delay <= 0 or self.closed:
                        break
                    self.changed.wait(min(delay, 0.25))
                    if self.closed:
                        break
                    response = self.run_command(db, command, connection, name, execute, started, 0)
                return response
            finally:
                cursor.in_use = False

    def run_command(self, db, command, connection, name, execute, started, timeout):
        self.catalog.sql.execute("BEGIN IMMEDIATE" if name in WRITE_COMMANDS else "BEGIN")
        try:
            result = execute(db, command, connection)
            if timeout and (time.monotonic() - started) * 1000 >= timeout:
                raise MongoError("operation exceeded time limit", 50)
            response = {"ok": 1.0, **result}
            if len(encode_bson(response)) + 32 > MAX_MESSAGE_SIZE:
                raise MongoError("reply exceeds maximum message size", 10334)
            self.catalog.sql.commit()
        except BaseException:
            self.catalog.sql.rollback()
            raise
        if name in WRITE_COMMANDS or name in {"killcursors", "endsessions", "killsessions"}:
            self.changed.notify_all()
        self.commands_run += 1
        return response

    def reap_cursors(self):
        cutoff = time.monotonic() - CURSOR_TIMEOUT
        for cursor_id, cursor in list(self.cursors.items()):
            if not cursor.no_timeout and not cursor.in_use and cursor.last_used < cutoff:
                del self.cursors[cursor_id]

    def forget_cursors(self, db, name=None):
        for cursor_id, cursor in list(self.cursors.items()):
            if cursor.database == db and (not name or cursor.collection == name):
                del self.cursors[cursor_id]

    @staticmethod
    def batch_entry_size(document, index):
        length = len(encode_bson(document))
        if length > MAX_BSON_SIZE:
            raise MongoError("result document exceeds the maximum BSON size", 10334)
        return length + len(str(index)) + 2

    def batch(self, cursor, size, field):
        if cursor.tail is not None:
            documents = self.tail_batch(cursor, size)
            cursor.last_used = time.monotonic()
            return {"ns": cursor.ns, field: documents}
        documents = []
        used = 0
        while cursor.pending and (size is None or len(documents) < size):
            length = self.batch_entry_size(cursor.pending[0], len(documents))
            if used + length > MAX_BSON_SIZE and documents:
                break
            documents.append(cursor.pending.popleft())
            used += length
        cursor.last_used = time.monotonic()
        return {"ns": cursor.ns, field: documents}

    def tail_batch(self, cursor, size):
        tail = cursor.tail
        params = cursor.database, cursor.collection, tail.last_record
        if size == 0 or tail.remaining == 0:
            return []
        if (
            tail.last_record
            and self.catalog.sql.execute(
                "SELECT 1 FROM mongo_documents WHERE db=? AND name=? AND record=?",
                params,
            ).fetchone()
            is None
        ):
            raise MongoError(f"cannot restore tailable cursor at record {tail.last_record}", 136)
        rows = self.catalog.sql.execute(
            "SELECT record, data FROM mongo_documents WHERE db=? AND name=? AND record>? ORDER BY record",
            params,
        )
        documents = []
        used = 0
        for record, raw in rows:
            if tail.remaining == 0 or (size is not None and len(documents) >= size):
                break
            doc = decode_bson(raw)[0]
            if not matches(doc, tail.filter, tail.variables):
                tail.last_record = record
                continue
            if tail.skip:
                tail.skip -= 1
                tail.last_record = record
                continue
            if tail.return_key:
                doc = {}
            elif tail.projection is not None:
                doc = project(doc, tail.projection, variables=tail.variables, query=tail.filter)
            if tail.show_record_id:
                doc = {**doc, "$recordId": Int64(record)}
            length = self.batch_entry_size(doc, len(documents))
            if used + length > MAX_BSON_SIZE and documents:
                break
            used += length
            documents.append(doc)
            tail.last_record = record
            if tail.remaining is not None:
                tail.remaining -= 1
        return documents

    def cursor_response(
        self, db, name, documents, command, size=101, *, single=False, keep_empty=False, tail=None
    ):
        cursor = Cursor(
            db,
            name,
            deque(documents),
            session_id(command),
            time.monotonic(),
            no_timeout=bool(command.get("noCursorTimeout")),
            tail=tail,
        )
        result = self.batch(cursor, size, "firstBatch")
        cursor_id = 0
        if (cursor.pending or keep_empty or tail is not None) and not single:
            cursor_id = secrets.randbits(63)
            while not cursor_id or cursor_id in self.cursors:
                cursor_id = secrets.randbits(63)
            self.cursors[cursor_id] = cursor
        return {"cursor": {"id": Int64(cursor_id), **result}}

    def cmd_get_more(self, db, command, _connection):
        cursor_id = require_integer(argument(command), "getMore")
        name = require_string(command.get("collection"), "collection")
        size = require_integer(command.get("batchSize", 0), "batchSize", 0)
        cursor = self.cursors.get(cursor_id)
        if cursor is None or (cursor.database, cursor.collection) != (db, name):
            raise MongoError(f"cursor id {cursor_id} not found", 43)
        if cursor.session != session_id(command):
            raise MongoError("cannot use cursor from a different session", 50738)
        try:
            result = self.batch(cursor, size or None, "nextBatch")
        except MongoError:
            del self.cursors[cursor_id]
            raise
        if not cursor.pending and (cursor.tail is None or cursor.tail.remaining == 0):
            del self.cursors[cursor_id]
            cursor_id = 0
        return {"cursor": {"id": Int64(cursor_id), **result}}

    def cmd_kill_cursors(self, db, command, _connection):
        name = require_string(argument(command), "collection")
        ids = require_list(command.get("cursors"), "cursors")
        for cursor_id in ids:
            require_integer(cursor_id, "cursor id")
        killed, missing = [], []
        for cursor_id in ids:
            cursor = self.cursors.get(cursor_id)
            if cursor is not None and (cursor.database, cursor.collection) == (db, name):
                del self.cursors[cursor_id]
                killed.append(Int64(cursor_id))
            else:
                missing.append(Int64(cursor_id))
        return {"cursorsKilled": killed, "cursorsNotFound": missing, "cursorsAlive": [], "cursorsUnknown": []}

    def cmd_end_sessions(self, _db, command, _connection):
        sessions = require_list(argument(command), "sessions")
        ids = {session_id({"lsid": session}) for session in sessions}
        for cursor_id, cursor in list(self.cursors.items()):
            if cursor.session in ids:
                del self.cursors[cursor_id]
        return {}

    def cmd_refresh_sessions(self, _db, command, _connection):
        for session in require_list(argument(command), "sessions"):
            session_id({"lsid": session})
        return {}

    def cmd_hello(self, _db, command, connection):
        writable_field = "isWritablePrimary" if next(iter(command)).lower() == "hello" else "ismaster"
        result = {
            writable_field: True,
            "helloOk": True,
            "maxBsonObjectSize": MAX_BSON_SIZE,
            "maxMessageSizeBytes": MAX_MESSAGE_SIZE,
            "maxWriteBatchSize": MAX_WRITE_BATCH,
            "localTime": DateTime.now(),
            "logicalSessionTimeoutMinutes": 30,
            "connectionId": Int64(connection.get("id", 1)),
            "minWireVersion": 0,
            "maxWireVersion": WIRE_VERSION,
            "readOnly": False,
        }
        account = self.account
        if "saslSupportedMechs" in command:
            result["saslSupportedMechs"] = (
                list(account.mechanisms)
                if (account and command["saslSupportedMechs"] == f"{account.database}.{account.username}")
                else []
            )
        if account and isinstance(spec := command.get("speculativeAuthenticate"), dict):
            try:
                result["speculativeAuthenticate"] = account.start(spec.get("db", "admin"), spec, connection)
            except MongoError:
                pass
        return result

    def cmd_sasl_start(self, db, command, connection):
        if self.account is None:
            raise MongoError("Authentication failed.", 18)
        return self.account.start(db, command, connection)

    def cmd_sasl_continue(self, db, command, connection):
        if self.account is None:
            raise MongoError("Authentication failed.", 18)
        return self.account.continue_(db, command, connection)

    def cmd_ping(self, _db, _command, _connection):
        return {}

    def cmd_build_info(self, _db, _command, _connection):
        return {
            "version": "8.0.0",
            "versionArray": [8, 0, 0, 0],
            "gitVersion": "mongo-test-server",
            "modules": [],
            "bits": 64,
            "debug": False,
            "maxBsonObjectSize": MAX_BSON_SIZE,
            "storageEngines": ["sqlite"],
        }

    def cmd_get_parameter(self, _db, command, _connection):
        parameters = {
            "featureCompatibilityVersion": {"version": "8.0"},
            "ttlMonitorEnabled": self.ttl_enabled,
            "ttlMonitorSleepSecs": self.ttl_interval,
            "logicalSessionRefreshMillis": 300_000,
            "authenticationMechanisms": list(self.account.mechanisms) if self.account else [],
        }
        if argument(command) == "*":
            return parameters
        selected = {key: value for key, value in parameters.items() if command.get(key)}
        if not selected:
            raise MongoError("no option found to get")
        return selected

    def cmd_set_parameter(self, _db, command, _connection):
        names = {"ttlMonitorEnabled": "ttl_enabled", "ttlMonitorSleepSecs": "ttl_interval"}
        enabled, interval = self.ttl_enabled, self.ttl_interval
        values = {}
        for key, value in list(command.items())[1:]:
            if key in COMMAND_METADATA:
                continue
            if key not in names:
                raise MongoError(f"unrecognized parameter: {key}")
            values[key] = getattr(self, names[key])
            if key == "ttlMonitorEnabled":
                enabled = require_bool(value, key)
            else:
                interval = require_integral_number(value, key, 1, 2_147_483_647)
        if not values:
            raise MongoError("no option found to set")
        self.ttl_enabled, self.ttl_interval = enabled, interval
        self.ttl_changed.set()
        return {"was": next(iter(values.values()))}

    def cmd_cmd_line_opts(self, _db, _command, _connection):
        return {
            "argv": ["mongo_test_server.py"],
            "parsed": {
                "net": {
                    "bindIp": self.settings.get("host", "127.0.0.1"),
                    "port": self.settings.get("port", 27017),
                },
                "storage": {"engine": "sqlite", **({"dbPath": str(self.filename)} if self.filename else {})},
                "security": {"authorization": "enabled" if self.account else "disabled"},
            },
        }

    def cmd_connection_status(self, _db, command, connection):
        account = self.account
        authenticated = account and connection.get("authenticated")
        info = {
            "authenticatedUsers": [{"user": account.username, "db": account.database}]
            if authenticated
            else [],
            "authenticatedUserRoles": [{"role": "root", "db": "admin"}] if authenticated else [],
        }
        if command.get("showPrivileges"):
            info["authenticatedUserPrivileges"] = (
                [{"resource": {"anyResource": True}, "actions": ["anyAction"]}] if authenticated else []
            )
        return {"authInfo": info}

    def cmd_host_info(self, _db, _command, _connection):
        return {
            "system": {
                "currentTime": DateTime.now(),
                "hostname": platform.node(),
                "cpuAddrSize": 64,
                "numCores": os.cpu_count() or 1,
                "cpuArch": platform.machine(),
            },
            "os": {"type": platform.system(), "name": platform.system(), "version": platform.release()},
            "extra": {},
        }

    def cmd_whatsmyuri(self, _db, _command, connection):
        address = connection.get("address", ("127.0.0.1", 0))
        host, port = address[:2]
        return {"you": f"[{host}]:{port}" if ":" in host else f"{host}:{port}"}

    def cmd_server_status(self, _db, _command, _connection):
        return {
            "host": platform.node(),
            "version": "8.0.0",
            "process": "mongo_test_server",
            "pid": Int64(os.getpid()),
            "uptime": time.monotonic() - self.started,
            "localTime": DateTime.now(),
            "storageEngine": {"name": "sqlite", "persistent": bool(self.filename)},
            "metrics": {
                "cursor": {"open": {"total": len(self.cursors)}},
                "commands": {"total": self.commands_run},
            },
        }

    def cmd_list_commands(self, _db, _command, _connection):
        return {
            "commands": {
                name: {"adminOnly": name.lower() in ADMIN_COMMANDS, "secondaryOk": True} for name in COMMANDS
            }
        }

    def collection(self, db, name, *, required=False, writable=False):
        collection = self.catalog.collection(db, name)
        if required and collection is None:
            raise MongoError(f"ns does not exist: {db}.{name}", 26)
        if writable and collection is not None and collection.is_view:
            raise MongoError(f"{db}.{name} is a view, not a collection", 166)
        return collection

    def check_view_cycle(self, db, name, source):
        seen = {name}
        while True:
            if source in seen:
                raise MongoError("view definition has a cycle", 17420)
            seen.add(source)
            collection = self.catalog.collection(db, source)
            if collection is None or not collection.is_view:
                return
            source = collection.options["viewOn"]

    def read_documents(self, db, name, seen=frozenset()):
        collection = self.catalog.collection(db, name)
        if collection is None:
            return []
        if collection.is_view:
            ns = db, name
            if ns in seen or len(seen) >= 20:
                raise MongoError("view dependency depth exceeded or cycle detected", 17420)
            visible = seen | {ns}
            return run_pipeline(
                self.read_documents(db, collection.options["viewOn"], visible),
                collection.options["pipeline"],
                variables={"NOW": DateTime.now()},
                read_collection=lambda coll, database=None: self.read_documents(
                    database or db, coll, visible
                ),
            )
        return [doc for doc, _ in self.catalog.records(db, name).values()]

    def cmd_create(self, db, command, _connection):
        name = command_collection(command)
        options = {}
        supported = COLLECTION_VALIDATION_OPTIONS | {
            "capped",
            "size",
            "max",
            "viewOn",
            "pipeline",
            "collation",
            "idIndex",
        }
        for option in supported & command.keys():
            options[option] = command[option]
        extras = command.keys() - supported - COMMAND_METADATA - {next(iter(command))}
        if extras:
            raise MongoError(f"unsupported create option: {min(extras)}", 72)
        simple_collation(options.get("collation"))
        options.pop("collation", None)
        if "viewOn" in options:
            source = collection_name(options["viewOn"])
            if options.keys() - {"viewOn", "pipeline"}:
                raise MongoError("view cannot have collection storage options", 72)
            options.setdefault("pipeline", [])
            validate_pipeline(options["pipeline"], allow_write=False)
            self.check_view_cycle(db, name, source)
        elif "pipeline" in options:
            raise MongoError("pipeline is only valid when creating a view", 72)
        else:
            check_validation_options(options)
            if "capped" in options:
                require_bool(options["capped"], "capped")
            if options.get("capped"):
                options["size"] = Int64(require_integral_number(options.get("size"), "size", 1))
                if "max" in options:
                    maximum = require_integral_number(options["max"], "max")
                    options["max"] = Int64(maximum if 0 < maximum < 2_147_483_647 else 2_147_483_647)
            elif "size" in options or "max" in options:
                raise MongoError("size and max require capped:true", 72)
        if "idIndex" in options:
            if check_index_spec(options.pop("idIndex")) != id_index():
                raise MongoError("create.idIndex must define the _id index", 72)
        existing = self.catalog.collection(db, name)
        if existing:
            if value_key(existing.options) == value_key(options) or existing.options == options:
                return {}
            raise MongoError(f"Collection {db}.{name} already exists", 48)
        self.catalog.create(db, name, options)
        return {}

    def cmd_drop(self, db, command, _connection):
        name = command_collection(command)
        collection = self.collection(db, name)
        if collection is None:
            return {}
        self.catalog.drop(db, name)
        self.forget_cursors(db, name)
        return {"ns": collection.ns, "nIndexesWas": len(collection.indexes)}

    def cmd_drop_database(self, db, _command, _connection):
        self.catalog.sql.execute("DELETE FROM mongo_collections WHERE db=?", (db,))
        self.forget_cursors(db)
        return {"dropped": db}

    def cmd_rename(self, _db, command, _connection):
        source_db, source = namespace(argument(command))
        target_db, target = namespace(command.get("to"))
        self.collection(source_db, source, required=True, writable=True)
        if (target_db, target) == (source_db, source):
            raise MongoError("source and target namespaces must differ")
        if self.catalog.collection(target_db, target):
            if not command.get("dropTarget"):
                raise MongoError(f"target namespace exists: {target_db}.{target}", 48)
            self.catalog.drop(target_db, target)
        self.catalog.sql.execute(
            "UPDATE mongo_collections SET db=?, name=? WHERE db=? AND name=?",
            (target_db, target, source_db, source),
        )
        self.forget_cursors(source_db, source)
        self.forget_cursors(target_db, target)
        return {}

    def cmd_list_databases(self, _db, command, _connection):
        databases = []
        total = 0
        filter_ = query_filter(command.get("filter", {}))
        name_only = command.get("nameOnly", False)
        for db in dict.fromkeys(db for db, _ in self.catalog.namespaces()):
            row = {"name": db}
            if not name_only:
                count, size = self.catalog.data_stats(db)
                row.update(sizeOnDisk=Int64(size), empty=count == 0)
            if matches(row, filter_):
                databases.append(row)
                total += row.get("sizeOnDisk", 0)
        response = {"databases": databases}
        if not name_only:
            response.update(totalSize=Int64(total), totalSizeMb=Int64(total // (1024 * 1024)))
        return response

    def cmd_list_collections(self, db, command, _connection):
        filter_ = query_filter(command.get("filter", {}))
        cursor = require_document(command.get("cursor", {}), "cursor")
        size = require_integer(cursor.get("batchSize", 101), "batchSize", 0)
        results = []
        for _, name in self.catalog.namespaces(db):
            collection = self.catalog.collection(db, name)
            row = {"name": name, "type": "view" if collection.is_view else "collection"}
            if not command.get("nameOnly"):
                row["options"] = collection.options
                row["info"] = {"readOnly": collection.is_view}
                if not collection.is_view:
                    row["info"]["uuid"] = Binary(collection.uuid, 4)
                    row["idIndex"] = collection.indexes["_id_"]
            if matches(row, filter_):
                results.append(row)
        return self.cursor_response(db, "$cmd.listCollections", results, command, size)

    def cmd_create_indexes(self, db, command, _connection):
        name = command_collection(command)
        specs = require_list(command.get("indexes"), "indexes")
        if not specs:
            raise MongoError("indexes cannot be empty")
        collection = self.collection(db, name, writable=True)
        created = collection is None
        if created:
            collection = self.catalog.create(db, name)
        writer = CollectionWriter(self.catalog, collection)
        before = len(collection.indexes)
        for spec in specs:
            index = check_index_spec(spec)
            existing = collection.indexes.get(index["name"])
            if existing:
                if list(existing["key"].items()) != list(index["key"].items()):
                    raise MongoError(f"index name {index['name']} already exists with different keys", 86)
                if existing != index:
                    raise MongoError(f"index {index['name']} already exists with different options", 85)
                continue
            if len(collection.indexes) >= 64:
                raise MongoError("a collection can have at most 64 indexes", 67)
            same_options = {
                key: value for key, value in index.items() if key not in {"key", "name", "v", "hidden"}
            }
            for other in collection.indexes.values():
                if list(other["key"].items()) == list(index["key"].items()) and same_options == {
                    key: value for key, value in other.items() if key not in {"key", "name", "v", "hidden"}
                }:
                    raise MongoError(f"equivalent index already exists as {other['name']}", 85)
            writer.build_index(index)
            self.catalog.set_index(collection, index)
        return {
            "numIndexesBefore": before,
            "numIndexesAfter": len(collection.indexes),
            "createdCollectionAutomatically": created,
        }

    def cmd_list_indexes(self, db, command, _connection):
        name = command_collection(command)
        collection = self.collection(db, name, required=True, writable=True)
        cursor = require_document(command.get("cursor", {}), "cursor")
        size = require_integer(cursor.get("batchSize", 101), "batchSize", 0)
        return self.cursor_response(db, name, collection.indexes.values(), command, size)

    def cmd_drop_indexes(self, db, command, _connection):
        collection = self.collection(db, command_collection(command), required=True, writable=True)
        selected = command.get("index")
        before = len(collection.indexes)
        if selected == "*":
            names = [name for name in collection.indexes if name != "_id_"]
        elif isinstance(selected, dict):
            names = [
                spec["name"]
                for spec in collection.indexes.values()
                if list(spec["key"].items()) == list(selected.items())
            ]
            if not names:
                raise MongoError("no index found for the key pattern", 27)
            if len(names) > 1:
                raise MongoError("more than one index matches the key pattern; specify an index name", 85)
        elif isinstance(selected, list):
            names = selected
        else:
            names = [selected]
        for name in names:
            if not isinstance(name, str) or name not in collection.indexes:
                raise MongoError(f"index not found: {name!r}", 27)
            if name == "_id_":
                raise MongoError("cannot drop _id index", 72)
        for name in dict.fromkeys(names):
            self.catalog.drop_index(collection, name)
        return {"nIndexesWas": before}

    def cmd_coll_mod(self, db, command, _connection):
        collection = self.collection(db, command_collection(command), required=True)
        allowed = (
            {"viewOn", "pipeline"}
            if collection.is_view
            else COLLECTION_VALIDATION_OPTIONS | {"index", "cappedSize", "cappedMax"}
        )
        extras = command.keys() - allowed - COMMAND_METADATA - {next(iter(command))}
        if extras:
            raise MongoError(f"unsupported collMod option: {min(extras)}", 72)
        options = dict(collection.options)
        for option in (COLLECTION_VALIDATION_OPTIONS | {"viewOn", "pipeline"}) & command.keys():
            options[option] = command[option]
        if collection.is_view:
            self.check_view_cycle(db, collection.name, collection_name(options["viewOn"]))
            validate_pipeline(options["pipeline"], allow_write=False)
        else:
            check_validation_options(options)
        result = {}
        if "index" in command:
            change = require_document(command["index"], "index")
            if change.keys() - (INDEX_PARAMETER_NAMES | {"name", "keyPattern"}):
                raise MongoError("unsupported collMod index option", 72)
            if "name" in change:
                selected = collection.indexes.get(require_string(change["name"], "index name"))
            else:
                key = require_document(change.get("keyPattern"), "keyPattern")
                available = [
                    index
                    for index in collection.indexes.values()
                    if list(index["key"].items()) == list(key.items())
                ]
                if len(available) > 1:
                    raise MongoError("ambiguous index key pattern", 85)
                selected = next(iter(available), None)
            if selected is None:
                raise MongoError("index not found", 27)
            for option in INDEX_PARAMETER_NAMES & change.keys():
                if option in selected:
                    result[option + "_old"] = selected[option]
                result[option + "_new"] = change[option]
            child = check_index_spec(
                {**selected, **{key: change[key] for key in INDEX_PARAMETER_NAMES & change.keys()}}
            )
            self.catalog.set_index(collection, child)
        for parameter, option in (("cappedSize", "size"), ("cappedMax", "max")):
            if parameter in command:
                if not collection.capped:
                    raise MongoError(f"{parameter} is only supported for capped collections", 72)
                options[option] = Int64(require_integer(command[parameter], parameter, 1))
        if options != collection.options:
            self.catalog.set_options(collection, options)
        if "cappedSize" in command or "cappedMax" in command:
            CollectionWriter(self.catalog, collection).enforce_cap()
        return result

    def writers(self, bypass=False):
        cached = {}

        def writer(db, name, *, create=False):
            if (db, name) in cached:
                return cached[(db, name)]
            collection = self.collection(db, name, writable=True)
            if collection is None:
                if not create:
                    return None
                collection = self.catalog.create(db, name)
            result = CollectionWriter(self.catalog, collection, bypass=bypass)
            cached[(db, name)] = result
            return result

        return writer

    @staticmethod
    def write_errors(error, index):
        fields = error.document()
        fields.pop("ok")
        return {"index": index, **fields}

    @staticmethod
    def write_operations(command, field):
        operations = require_list(command.get(field), field)
        if not operations or len(operations) > MAX_WRITE_BATCH:
            raise MongoError(f"{field} requires between 1 and {MAX_WRITE_BATCH} entries")
        return operations, require_bool(command.get("ordered", True), "ordered")

    def cmd_insert(self, db, command, _connection):
        name = command_collection(command)
        operations, ordered = self.write_operations(command, "documents")
        writer = self.writers(
            require_bool(command.get("bypassDocumentValidation", False), "bypassDocumentValidation")
        )
        inserted = 0
        errors = []
        for index, document in enumerate(operations):
            try:
                writer(db, name, create=True).insert(document)
                inserted += 1
            except MongoError as error:
                errors.append(self.write_errors(error, index))
                if ordered:
                    break
        return {"n": inserted, **({"writeErrors": errors} if errors else {})}

    @staticmethod
    def matching_records(writer, filter_, variables, sort=None, hint=None):
        if writer is None:
            return []
        records = [
            (rowid, doc)
            for rowid, (doc, _) in writer.records.items()
            if in_hinted_index(doc, hint) and matches(doc, filter_, variables)
        ]
        sort = sort or (hint["key"] if hint else None)
        if not sort:
            return records
        if "$natural" in sort:
            return MongoEngine.reverse_natural(records, sort)
        by_identity = {id(doc): rowid for rowid, doc in records}
        return [(by_identity[id(doc)], doc) for doc in sort_documents([doc for _, doc in records], sort)]

    def update_operation(self, writer, db, name, operation, variables):
        require_document(operation, "update operation")
        if "u" not in operation:
            raise MongoError("update operation requires 'u'", 9)
        simple_collation(operation.get("collation"))
        variables = {**variables, **require_document(operation.get("c", {}), "update constants")}
        filter_ = query_filter(operation.get("q", {}))
        update = operation["u"]
        many = require_bool(operation.get("multi", False), "multi")
        upsert = require_bool(operation.get("upsert", False), "upsert")
        array_filters = operation.get("arrayFilters")
        validate_update(update, array_filters)
        sort = operation.get("sort")
        if sort is not None:
            validate_sort(sort)
        if many and (
            sort is not None or (isinstance(update, dict) and not next(iter(update), "").startswith("$"))
        ):
            raise MongoError("multi update cannot use a sort or replacement document", 9)
        target = writer(db, name)
        hint = check_hint(
            target.collection if target else None, operation.get("hint"), allow_missing=not upsert
        )
        records = self.matching_records(target, filter_, variables, sort, hint)
        if not many:
            records = records[:1]
        modified = 0
        before = after = MISSING
        for record, before in records:
            after = modified_document(
                before, update, query=filter_, array_filters=array_filters, variables=variables
            )
            modified += target.replace(record, after)
        if upsert and not records:
            base = upsert_base(filter_)
            new = modified_document(
                base, update, query=filter_, array_filters=array_filters, inserting=True, variables=variables
            )
            if "_id" in base and (
                "_id" not in new or encode_bson({"_id": new["_id"]}) != encode_bson({"_id": base["_id"]})
            ):
                raise MongoError("upsert cannot change the immutable _id field from the query", 66)
            new = writer(db, name, create=True).insert(new)
            return 0, 0, new["_id"], MISSING, new
        return len(records), modified, MISSING, before, after

    def delete_operation(self, writer, db, name, operation, variables, sort=None):
        require_document(operation, "delete operation")
        simple_collation(operation.get("collation"))
        filter_ = query_filter(operation.get("q", {}))
        limit = require_integer(operation.get("limit"), "delete limit")
        if limit not in (1, 0):
            raise MongoError("delete limit must be 0 or 1")
        target = writer(db, name)
        hint = check_hint(target.collection if target else None, operation.get("hint"), allow_missing=True)
        records = self.matching_records(target, filter_, variables, sort, hint)
        if limit:
            records = records[:1]
        before = MISSING
        for record, before in records:
            target.delete(record)
        return len(records), before

    def cmd_update(self, db, command, _connection):
        name = command_collection(command)
        operations, ordered = self.write_operations(command, "updates")
        variables = command_variables(command)
        writer = self.writers(
            require_bool(command.get("bypassDocumentValidation", False), "bypassDocumentValidation")
        )
        matched = modified = 0
        upserts, errors = [], []
        for index, operation in enumerate(operations):
            try:
                count, changed, inserted, _, _ = self.update_operation(writer, db, name, operation, variables)
                matched += count
                modified += changed
                if inserted is not MISSING:
                    upserts.append({"index": index, "_id": inserted})
            except MongoError as error:
                errors.append(self.write_errors(error, index))
                if ordered:
                    break
        result = {"n": matched + len(upserts), "nModified": modified}
        if upserts:
            result["upserted"] = upserts
        if errors:
            result["writeErrors"] = errors
        return result

    def cmd_delete(self, db, command, _connection):
        name = command_collection(command)
        operations, ordered = self.write_operations(command, "deletes")
        variables = command_variables(command)
        writer = self.writers()
        removed = 0
        errors = []
        for index, operation in enumerate(operations):
            try:
                count, _ = self.delete_operation(writer, db, name, operation, variables)
                removed += count
            except MongoError as error:
                errors.append(self.write_errors(error, index))
                if ordered:
                    break
        return {"n": removed, **({"writeErrors": errors} if errors else {})}

    def cmd_find_one_write(self, db, command, _connection):
        name = command_collection(command)
        filter_ = query_filter(command.get("query", {}))
        sort = command.get("sort")
        projection = command.get("fields")
        if sort is not None:
            validate_sort(sort)
        if projection is not None:
            validate_projection(projection)
        variables = command_variables(command)
        writer = self.writers(
            require_bool(command.get("bypassDocumentValidation", False), "bypassDocumentValidation")
        )
        options = {
            key: command[key] for key in ("collation", "hint", "arrayFilters", "upsert") if key in command
        }
        if command.get("remove"):
            if "update" in command or command.get("upsert") or command.get("new"):
                raise MongoError("remove cannot be combined with update, new, or upsert", 9)
            count, value = self.delete_operation(
                writer, db, name, {"q": filter_, "limit": 1, **options}, variables, sort
            )
            last_error = {"n": count}
        elif "update" in command:
            operation = {"q": filter_, "u": command["update"], "sort": sort, **options}
            count, _, inserted, before, after = self.update_operation(writer, db, name, operation, variables)
            value = after if command.get("new") else before
            last_error = {"n": count + (inserted is not MISSING), "updatedExisting": count == 1}
            if inserted is not MISSING:
                last_error["upserted"] = inserted
        else:
            raise MongoError("findAndModify requires update or remove", 9)
        if value is MISSING:
            value = None
        elif projection is not None:
            value = project(value, projection, variables=variables, query=filter_)
        return {"lastErrorObject": last_error, "value": value}

    def cmd_bulk_write(self, db, command, _connection):
        operations, ordered = self.write_operations(command, "ops")
        ns_info = require_list(command.get("nsInfo"), "nsInfo")
        namespaces = []
        for info in ns_info:
            info = require_document(info, "namespace info")
            if info.get("isTimeseriesNamespace"):
                raise MongoError("time series collections are unsupported", 115)
            ns = namespace(info.get("ns"))
            if "collectionUUID" in info:
                collection = self.collection(*ns, required=True)
                if info["collectionUUID"] != Binary(collection.uuid, 4):
                    raise MongoError(f"UUID mismatch for {collection.ns}", 361)
            namespaces.append(ns)
        cursor = require_document(command.get("cursor", {}), "cursor")
        size = require_integer(cursor.get("batchSize", 101), "batchSize", 0)
        errors_only = require_bool(command.get("errorsOnly", False), "errorsOnly")
        writer = self.writers(
            require_bool(command.get("bypassDocumentValidation", False), "bypassDocumentValidation")
        )
        variables = command_variables(command)
        totals = {"nErrors": 0, "nInserted": 0, "nMatched": 0, "nModified": 0, "nUpserted": 0, "nDeleted": 0}
        results = []
        for index, operation in enumerate(operations):
            try:
                require_document(operation, "bulk write operation")
                kinds = operation.keys() & {"insert", "update", "delete"}
                if len(kinds) != 1:
                    raise MongoError("bulkWrite requires one operation type per operation", 9)
                kind = next(iter(kinds))
                ns_id = require_integer(operation[kind], "namespace index", 0)
                if ns_id >= len(namespaces):
                    raise MongoError("namespace index is out of bounds", 9)
                target_db, target = namespaces[ns_id]
                result = {"ok": 1.0, "idx": index}
                if kind == "insert":
                    writer(target_db, target, create=True).insert(operation.get("document"))
                    totals["nInserted"] += 1
                    result["n"] = 1
                elif kind == "update":
                    statement = {
                        "q": operation.get("filter", {}),
                        "u": operation.get("updateMods"),
                        **{
                            key: operation[key]
                            for key in ("multi", "upsert", "arrayFilters", "collation", "hint", "sort", "c")
                            if key in operation
                        },
                    }
                    n, modified, inserted, _, _ = self.update_operation(
                        writer, target_db, target, statement, variables
                    )
                    totals["nMatched"] += n
                    totals["nModified"] += modified
                    result.update(n=n, nModified=modified)
                    if inserted is not MISSING:
                        totals["nUpserted"] += 1
                        result.update(n=1, upserted={"_id": inserted})
                else:
                    many = require_bool(operation.get("multi", False), "multi")
                    statement = {
                        "q": operation.get("filter", {}),
                        "limit": 0 if many else 1,
                        **{key: operation[key] for key in ("collation", "hint") if key in operation},
                    }
                    n, _ = self.delete_operation(writer, target_db, target, statement, variables)
                    totals["nDeleted"] += n
                    result["n"] = n
                if not errors_only:
                    results.append(result)
            except MongoError as error:
                totals["nErrors"] += 1
                results.append({**error.document(), "idx": index, "n": 0})
                if ordered:
                    break
        return {**self.cursor_response(db, "$cmd.bulkWrite", results, command, size), **totals}

    @staticmethod
    def reverse_natural(documents, spec):
        direction = spec.get("$natural")
        if len(spec) != 1 or isinstance(direction, bool) or direction not in (1, -1):
            raise MongoError("invalid $natural sort")
        return documents[::-1] if direction == -1 else documents

    @staticmethod
    def find_bounds(doc, hint, minimum, maximum):
        def compare_bound(bound):
            for field, value in bound.items():
                comparison = compare(get_path(doc, field), value)
                if comparison:
                    return comparison * hint[field]
            return 0

        return (minimum is None or compare_bound(minimum) >= 0) and (
            maximum is None or compare_bound(maximum) < 0
        )

    def find_documents(self, db, command):
        query_options(command)
        name = command_collection(command)
        collection = self.collection(db, name)
        filter_ = query_filter(command.get("filter", {}))
        variables = command_variables(command)
        sort = require_document(command.get("sort", {}), "sort")
        if sort and "$natural" not in sort:
            validate_sort(sort)
        projection = command.get("projection")
        if projection is not None:
            validate_projection(projection)
        skip = require_integer(command.get("skip", 0), "skip", 0)
        limit = require_integer(command.get("limit", 0), "limit")
        index = check_hint(collection, command.get("hint"), allow_missing=True)
        hint = index["key"] if index else None
        minimum, maximum = command.get("min"), command.get("max")
        for label, bound in (("min", minimum), ("max", maximum)):
            if bound is not None and (
                not hint or "$natural" in hint or list(require_document(bound, label)) != list(hint)
            ):
                raise MongoError(f"{label} requires an explicit index hint with the same fields")
        show_record_ids = command.get("showRecordId", False)
        if show_record_ids and collection is not None and collection.is_view:
            raise MongoError("showRecordId is unavailable on a view", 166)
        if show_record_ids:
            records = self.catalog.records(db, name)
            data = [doc for doc, _ in records.values()]
            record_ids = {id(doc): record for record, (doc, _) in records.items()}
        else:
            data = self.read_documents(db, name)
        docs = [
            doc
            for doc in data
            if in_hinted_index(doc, index)
            and self.find_bounds(doc, hint, minimum, maximum)
            and matches(doc, filter_, variables)
        ]
        sort = sort or hint or {}
        if "$natural" in sort:
            docs = self.reverse_natural(docs, sort)
        elif sort:
            docs = sort_documents(docs, sort)
        docs = docs[skip:]
        if limit:
            docs = docs[: abs(limit)]
        if command.get("returnKey"):
            docs = [
                {
                    field: None if (value := get_path(doc, field)) is MISSING else value
                    for field in (hint or {})
                    if field != "$natural"
                }
                for doc in docs
            ]
        elif projection is not None or show_record_ids:
            result = []
            for doc in docs:
                selected = (
                    project(doc, projection, variables=variables, query=filter_)
                    if projection is not None
                    else doc
                )
                if show_record_ids:
                    selected = {**selected, "$recordId": Int64(record_ids[id(doc)])}
                result.append(selected)
            docs = result
        return docs, len(data)

    def cmd_find(self, db, command, _connection):
        size = require_integer(command.get("batchSize", 101), "batchSize", 0)
        if command.get("tailable"):
            return self.open_tail(db, command, size)
        if command.get("awaitData"):
            raise MongoError("awaitData requires tailable", 9)
        docs, _ = self.find_documents(db, command)
        single = bool(command.get("singleBatch")) or command.get("limit", 0) < 0
        name = command_collection(command)
        keep_empty = size == 0 and self.collection(db, name) is not None
        return self.cursor_response(db, name, docs, command, size, single=single, keep_empty=keep_empty)

    def open_tail(self, db, command, size):
        simple_collation(command.get("collation"))
        name = command_collection(command)
        coll = self.collection(db, name)
        filter_ = query_filter(command.get("filter", {}))
        projection = command.get("projection")
        if projection is not None:
            validate_projection(projection)
        sort = require_document(command.get("sort", {}), "sort")
        if sort and (sort != {"$natural": 1} or isinstance(sort["$natural"], bool)):
            raise MongoError("tailable cursors require natural ascending order")
        if "min" in command or "max" in command:
            raise MongoError("tailable cursors do not support index bounds")
        skip = require_integer(command.get("skip", 0), "skip", 0)
        limit = require_integer(command.get("limit", 0), "limit")
        if coll is not None and not coll.capped:
            raise MongoError("tailable cursor requested on non-capped collection")
        if (
            coll is None
            or self.catalog.sql.execute(
                "SELECT 1 FROM mongo_documents WHERE db=? AND name=? LIMIT 1",
                (db, name),
            ).fetchone()
            is None
        ):
            return self.cursor_response(db, name, [], command, size)
        tail = Tail(
            filter_,
            command_variables(command),
            projection,
            skip,
            abs(limit) or None,
            bool(command.get("awaitData")),
            bool(command.get("returnKey")),
            bool(command.get("showRecordId")),
        )
        return self.cursor_response(
            db, name, [], command, size, single=bool(command.get("singleBatch")) or limit < 0, tail=tail
        )

    def cmd_count(self, db, command, _connection):
        query_options(command)
        name = command_collection(command)
        filter_ = query_filter(command.get("query", {}))
        variables = command_variables(command)
        skip = require_integer(command.get("skip", 0), "skip", 0)
        limit = abs(require_integer(command.get("limit", 0), "limit"))
        hint = check_hint(self.collection(db, name), command.get("hint"), allow_missing=True)
        count = sum(
            matches(doc, filter_, variables)
            for doc in self.read_documents(db, name)
            if in_hinted_index(doc, hint)
        )
        count = max(count - skip, 0)
        return {"n": min(limit, count) if limit else count}

    def cmd_distinct(self, db, command, _connection):
        query_options(command)
        name = command_collection(command)
        field = require_string(command.get("key"), "key")
        filter_ = query_filter(command.get("query", {}))
        variables = command_variables(command)
        hint = check_hint(self.collection(db, name), command.get("hint"), allow_missing=True)
        distinct = {}
        for doc in self.read_documents(db, name):
            if in_hinted_index(doc, hint) and matches(doc, filter_, variables):
                for result in path_values(doc, field):
                    for value in result if isinstance(result, list) else [result]:
                        distinct.setdefault(value_key(value), value)
        response = {"values": list(distinct.values())}
        if len(encode_bson(response)) + 32 > MAX_BSON_SIZE:
            raise MongoError("distinct result exceeds the BSON size limit", 17217)
        return response

    def aggregate_write(self, db, stage, spec, documents, command, variables):
        bypass = require_bool(command.get("bypassDocumentValidation", False), "bypassDocumentValidation")
        if stage == "$out":
            if isinstance(spec, str):
                target_db, name = db, collection_name(spec)
            else:
                require_document(spec, "$out")
                if spec.keys() != {"db", "coll"}:
                    raise MongoError("$out requires db and coll fields")
                target_db, name = database_name(spec["db"]), collection_name(spec["coll"])
            old = self.collection(target_db, name, writable=True)
            if old is not None and old.capped:
                raise MongoError("$out cannot target a capped collection")
            if old is not None:
                self.catalog.drop(target_db, name)
            target = self.catalog.create(
                target_db, name, old.options if old else {}, old.indexes if old else None
            )
            writer = CollectionWriter(self.catalog, target, bypass=bypass)
            for doc in documents:
                writer.insert(doc)
            self.forget_cursors(target_db, name)
            return
        if isinstance(spec, str):
            spec = {"into": spec}
        require_document(spec, "$merge")
        if spec.keys() - {"into", "on", "let", "whenMatched", "whenNotMatched"}:
            raise MongoError("unsupported $merge option")
        into = spec.get("into")
        if isinstance(into, str):
            target_db, name = db, collection_name(into)
        else:
            into = require_document(into, "$merge.into")
            target_db, name = database_name(into.get("db", db)), collection_name(into.get("coll"))
        on = spec.get("on", "_id")
        fields = [on] if isinstance(on, str) else require_list(on, "$merge.on")
        for field in fields:
            require_string(field, "$merge.on field")
        if not fields or len(set(fields)) != len(fields):
            raise MongoError("$merge.on must contain distinct field names")
        when_matched = spec.get("whenMatched", "merge")
        when_not_matched = spec.get("whenNotMatched", "insert")
        if isinstance(when_matched, list):
            validate_update_pipeline(when_matched)
        elif when_matched not in ("replace", "keepExisting", "merge", "fail"):
            raise MongoError("unsupported $merge.whenMatched action")
        if when_not_matched not in ("insert", "discard", "fail"):
            raise MongoError("unsupported $merge.whenNotMatched action")
        merge_let = spec.get("let", None)
        if merge_let is not None:
            if not isinstance(when_matched, list):
                raise MongoError("$merge.let requires a pipeline for whenMatched")
            require_document(merge_let, "$merge.let")
        writer = self.writers(bypass)(target_db, name, create=True)
        index = next(
            (
                index
                for index in writer.collection.indexes.values()
                if set(index["key"]) == set(fields)
                and is_unique(index)
                and not index.get("partialFilterExpression")
            ),
            None,
        )
        if index is None:
            raise MongoError("Cannot find index to verify that join fields will be unique", 51183)
        for doc in documents:
            if "_id" in fields and "_id" not in doc:
                doc = {"_id": ObjectId.new(), **doc}
            values = tuple(get_path(doc, field) for field in index["key"])
            if any(value is MISSING or value is None or isinstance(value, list) for value in values):
                raise MongoError("$merge key cannot contain an array, missing value or null", 51132)
            record = writer.keys[index["name"]].get(index_token(values))
            if record is None:
                if when_not_matched == "insert":
                    writer.insert(doc)
                elif when_not_matched == "fail":
                    raise MongoError("$merge had no matching document", 13113)
                continue
            previous = writer.records[record][0]
            if when_matched == "fail":
                raise MongoError("$merge matched an existing document", 11000)
            if when_matched == "keepExisting":
                continue
            if when_matched == "merge":
                replacement = {**previous, **doc}
            elif when_matched == "replace":
                replacement = {"_id": previous["_id"], **doc}
            else:
                scope = (
                    {"new": doc}
                    if merge_let is None
                    else {key: evaluate(value, doc, variables) for key, value in merge_let.items()}
                )
                replacement = update_pipeline(previous, when_matched, {**variables, **scope})
            if any(
                not bson_equal(get_path(replacement, field), get_path(previous, field)) for field in fields
            ):
                raise MongoError("$merge cannot change fields in the 'on' key", 66)
            writer.replace(record, replacement)

    def aggregation_documents(self, db, command, *, allow_write):
        query_options(command)
        name = argument(command)
        collectionless = isinstance(name, int) and not isinstance(name, bool) and name == 1
        name = "$cmd.aggregate" if collectionless else collection_name(name)
        pipeline = require_list(command.get("pipeline"), "pipeline")
        if pipeline and isinstance(pipeline[0], dict) and "$changeStream" in pipeline[0]:
            raise MongoError("The $changeStream stage is only supported on replica sets", 40573)
        validate_pipeline(pipeline, allow_write=allow_write)
        variables = command_variables(command)
        if collectionless and (not pipeline or "$documents" not in pipeline[0]):
            raise MongoError("collectionless aggregation must start with $documents", 73)
        hint = check_hint(
            None if collectionless else self.collection(db, name), command.get("hint"), allow_missing=True
        )
        source = [] if collectionless else self.read_documents(db, name)
        if hint and "$natural" in hint["key"]:
            source = self.reverse_natural(source, hint["key"])
        elif hint:
            source = sort_documents([doc for doc in source if in_hinted_index(doc, hint)], hint["key"])

        def read_collection(coll, database=None):
            return self.read_documents(database_name(database or db), collection_name(coll))

        def write_collection(stage, spec, docs):
            self.aggregate_write(db, stage, spec, docs, command, variables)

        documents = run_pipeline(
            source,
            pipeline,
            variables=variables,
            read_collection=read_collection,
            write_collection=write_collection if allow_write else None,
        )
        return name, documents, len(source)

    def cmd_aggregate(self, db, command, _connection):
        if command.get("explain"):
            return self.cmd_explain(db, {"explain": command}, _connection)
        cursor = require_document(command.get("cursor", {}), "cursor")
        size = require_integer(cursor.get("batchSize", 101), "batchSize", 0)
        name, documents, _ = self.aggregation_documents(db, command, allow_write=True)
        return self.cursor_response(db, name, documents, command, size)

    def cmd_explain(self, db, command, connection):
        inner = require_document(argument(command), "explain")
        name = next(iter(inner), "").lower()
        verbosity = command.get("verbosity", "queryPlanner")
        if verbosity not in ("queryPlanner", "executionStats", "allPlansExecution"):
            raise MongoError("unrecognized explain verbosity")
        if name == "find":
            docs, examined = self.find_documents(db, inner)
            count, coll = len(docs), argument(inner)
        elif name == "aggregate":
            coll, docs, examined = self.aggregation_documents(db, inner, allow_write=False)
            count = len(docs)
        elif name in ("count", "distinct", "delete", "update", "findandmodify"):
            coll = command_collection(inner)
            examined = len(self.read_documents(db, coll))
            if name in ("count", "distinct"):
                result = getattr(self, COMMAND_LOOKUP[name])(db, inner, connection)
                count = result["n"] if name == "count" else len(result["values"])
            else:
                query = inner.get("query", {})
                if name in ("delete", "update"):
                    operations = require_list(inner.get("deletes" if name == "delete" else "updates"), name)
                    if len(operations) != 1:
                        raise MongoError("explain requires one write operation", 72)
                    query = require_document(operations[0], "operation").get("q", {})
                count = sum(
                    matches(doc, query_filter(query), command_variables(inner))
                    for doc in self.read_documents(db, coll)
                )
        else:
            raise MongoError(f"cannot explain {name or 'empty command'}", 115)
        plan = {"stage": "COLLSCAN", "direction": "forward"}
        result = {
            "explainVersion": "1",
            "queryPlanner": {"namespace": f"{db}.{coll}", "winningPlan": plan, "rejectedPlans": []},
            "command": inner,
        }
        if verbosity != "queryPlanner":
            result["executionStats"] = {
                "executionSuccess": True,
                "nReturned": count,
                "executionTimeMillis": 0,
                "totalKeysExamined": 0,
                "totalDocsExamined": examined,
                "executionStages": {**plan, "nReturned": count, "docsExamined": examined},
            }
            if verbosity == "allPlansExecution":
                result["executionStats"]["allPlansExecution"] = []
        return result

    def cmd_db_stats(self, db, command, _connection):
        scale = require_integer(command.get("scale", 1), "scale", 1)
        collections = [self.catalog.collection(db, name) for _, name in self.catalog.namespaces(db)]
        count, size = self.catalog.data_stats(db)
        return {
            "db": db,
            "collections": sum(not coll.is_view for coll in collections),
            "views": sum(coll.is_view for coll in collections),
            "objects": Int64(count),
            "avgObjSize": size / count if count else 0.0,
            "dataSize": size / scale,
            "storageSize": size / scale,
            "totalSize": size / scale,
            "indexes": sum(len(coll.indexes) for coll in collections),
            "indexSize": 0.0,
            "scaleFactor": scale,
        }

    def cmd_coll_stats(self, db, command, _connection):
        name = command_collection(command)
        collection = self.collection(db, name, writable=True)
        count, size = self.catalog.data_stats(db, name)
        scale = require_integer(command.get("scale", 1), "scale", 1)
        indexes = collection.indexes if collection else {}
        result = {
            "ns": f"{db}.{name}",
            "count": Int64(count),
            "size": Int64(size // scale),
            "storageSize": Int64(size // scale),
            "totalIndexSize": 0,
            "nindexes": len(indexes),
            "capped": bool(collection and collection.capped),
            "scaleFactor": scale,
            "indexSizes": dict.fromkeys(indexes, 0),
        }
        if count:
            result["avgObjSize"] = size / count
        if collection and collection.capped:
            result.update(maxSize=collection.options["size"], max=collection.options.get("max", -1))
        return result

    def cmd_data_size(self, _db, command, _connection):
        db, name = namespace(argument(command))
        collection = self.collection(db, name, writable=True)
        specs = (command.get("min"), command.get("max"))
        count, size = self.catalog.data_stats(db, name)
        if any(bound is not None for bound in specs):
            index = check_hint(collection, command.get("keyPattern"))
            if index is None or "$natural" in index["key"]:
                raise MongoError("range dataSize requires keyPattern for an existing ordered index")
            hint = index["key"]
            for bound in specs:
                if bound is not None and list(require_document(bound, "bound")) != list(hint):
                    raise MongoError("range fields must match keyPattern")
            sizes = [
                length
                for doc, length in self.catalog.records(db, name).values()
                if self.find_bounds(doc, hint, *specs)
            ]
            count, size = len(sizes), sum(sizes)
        return {"size": Int64(size), "numObjects": Int64(count), "millis": 0}

    def cmd_validate(self, db, command, _connection):
        collection = self.collection(db, command_collection(command), required=True, writable=True)
        writer = CollectionWriter(self.catalog, collection)
        keys = {
            index["name"]: sum(
                len({index_token(key) for key in index_values(doc, index)})
                for doc, _ in writer.records.values()
            )
            for index in collection.indexes.values()
        }
        return {
            "ns": collection.ns,
            "nInvalidDocuments": Int64(0),
            "nrecords": Int64(len(writer.records)),
            "nIndexes": len(collection.indexes),
            "keysPerIndex": keys,
            "valid": True,
            "warnings": [],
            "errors": [],
        }

    def expire_ttl(self):
        with self.lock:
            if self.closed or not self.ttl_enabled:
                return
            self.catalog.sql.execute("BEGIN IMMEDIATE")
            try:
                now = DateTime.now().ms
                for db, name in self.catalog.namespaces():
                    collection = self.catalog.collection(db, name)
                    indexes = [
                        index for index in collection.indexes.values() if "expireAfterSeconds" in index
                    ]
                    if not indexes or collection.capped:
                        continue
                    for record, (doc, _) in self.catalog.records(db, name).items():
                        expired = any(
                            isinstance(key[0], DateTime)
                            and key[0].ms <= now - index["expireAfterSeconds"] * 1000
                            for index in indexes
                            for key in index_values(doc, index)
                        )
                        if expired:
                            self.catalog.sql.execute("DELETE FROM mongo_documents WHERE record=?", (record,))
                self.catalog.sql.commit()
            except BaseException:
                self.catalog.sql.rollback()
                raise

    def monitor_ttl(self):
        while True:
            with self.lock:
                if self.closed:
                    return
                delay = self.ttl_interval if self.ttl_enabled else None
                self.ttl_changed.clear()
            if not self.ttl_changed.wait(delay):
                try:
                    self.expire_ttl()
                except sqlite3.Error:
                    LOGGER.exception("TTL pass failed")


COMMAND_LOOKUP = {name.lower(): f"cmd_{method}" for name, method in COMMANDS.items()}


# Extended JSON used by the command-line shell

_EJSON_TAGS = {
    "$binary",
    "$code",
    "$date",
    "$dbPointer",
    "$maxKey",
    "$minKey",
    "$numberDecimal",
    "$numberDouble",
    "$numberInt",
    "$numberLong",
    "$oid",
    "$regularExpression",
    "$scope",
    "$symbol",
    "$timestamp",
    "$undefined",
    "$uuid",
}
_EJSON_DOUBLE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?")
_EJSON_DATE = re.compile(
    r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,3}))?(Z|[+-][0-9]{2}:[0-9]{2})",
    re.IGNORECASE,
)
_EJSON_SPECIAL_DOUBLE = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}


def _ejson_text(value):
    if not isinstance(value, str):
        raise ValueError("expected a string")
    return value


def _ejson_integer(text, bits=64):
    if not re.fullmatch(r"[+-]?[0-9]+", _ejson_text(text)):
        raise ValueError("expected an integer string")
    value = int(text)
    if not -(1 << (bits - 1)) <= value < 1 << (bits - 1):
        raise ValueError(f"integer is outside the BSON int{bits} range")
    return value


def _ejson_double(text):
    if text in _EJSON_SPECIAL_DOUBLE:
        return _EJSON_SPECIAL_DOUBLE[text]
    if not _EJSON_DOUBLE.fullmatch(text):
        raise ValueError("expected a double string")
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("number is outside the BSON double range")
    return value


def _ejson_fields(value, *names):
    if not isinstance(value, dict) or value.keys() != set(names):
        raise ValueError(f"expected an object containing {', '.join(names)}")
    return (value[name] for name in names)


def _ejson_date(value):
    if isinstance(value, Int64):
        return DateTime(int(value))
    match = _EJSON_DATE.fullmatch(_ejson_text(value))
    if not match:
        raise ValueError("expected an ISO-8601 timestamp or a $numberLong object")
    date, fraction, offset = match.groups()
    if fraction is not None:
        date += "." + fraction.ljust(3, "0")
    return DateTime.from_datetime(
        datetime.fromisoformat(date + ("+00:00" if offset.upper() == "Z" else offset))
    )


def _ejson_decode(tag, value):
    if tag == "$numberInt":
        return _ejson_integer(value, 32)
    if tag == "$numberLong":
        return Int64(_ejson_integer(value))
    if tag == "$numberDouble":
        return _ejson_double(_ejson_text(value))
    if tag == "$numberDecimal":
        return Decimal128.from_decimal(_ejson_text(value))
    if tag == "$date":
        return _ejson_date(value)
    if tag == "$oid":
        if not re.fullmatch(r"[0-9a-fA-F]{24}", _ejson_text(value)):
            raise ValueError("expected 24 hexadecimal digits")
        return ObjectId(bytes.fromhex(value))
    if tag == "$uuid":
        return Binary(UUID(_ejson_text(value)).bytes, 4)
    if tag == "$binary":
        data, subtype = _ejson_fields(value, "base64", "subType")
        if not re.fullmatch(r"[0-9a-fA-F]{1,2}", _ejson_text(subtype)):
            raise ValueError("binary subtype must be one or two hexadecimal digits")
        return Binary(base64.b64decode(_ejson_text(data), validate=True), int(subtype, 16))
    if tag == "$regularExpression":
        pattern, options = _ejson_fields(value, "pattern", "options")
        if "\0" in _ejson_text(pattern) or set(_ejson_text(options)) - set("ilmsux"):
            raise ValueError("invalid regular expression pattern or options")
        return Regex(pattern, options)
    if tag == "$timestamp":
        seconds, increment = _ejson_fields(value, "t", "i")
        if any(type(part) is not int or not 0 <= part <= 0xFFFFFFFF for part in (seconds, increment)):
            raise ValueError("timestamp fields must be 32-bit unsigned integers")
        return Timestamp(seconds, increment)
    if tag == "$symbol":
        return Symbol(_ejson_text(value))
    if tag == "$dbPointer":
        namespace, identifier = _ejson_fields(value, "$ref", "$id")
        if not isinstance(identifier, ObjectId):
            raise ValueError("DBPointer $id must be an ObjectId")
        return DBPointer(_ejson_text(namespace), identifier)
    if tag == "$undefined":
        if value is not True:
            raise ValueError("expected true")
        return Undefined()
    if tag in ("$minKey", "$maxKey"):
        if type(value) is not int or value != 1:
            raise ValueError("expected 1")
        return MinKey() if tag == "$minKey" else MaxKey()
    raise ValueError("expected $code with an optional $scope object")


def _ejson_object(pairs):
    document = dict(pairs)
    if len(document) != len(pairs):
        raise ValueError("JSON objects cannot contain duplicate keys")
    tags = _EJSON_TAGS.intersection(document)
    if not tags:
        return document
    if "$code" in tags:
        try:
            if document.keys() == {"$code"}:
                return JavaScript(_ejson_text(document["$code"]))
            code, scope = _ejson_fields(document, "$code", "$scope")
            if not isinstance(scope, dict):
                raise ValueError("$scope must be an object")
            return JavaScript(_ejson_text(code), scope)
        except ValueError as exc:
            raise ValueError(f"invalid $code wrapper: {exc}") from None
    if len(document) != 1:
        raise ValueError("Extended JSON type wrappers cannot contain extra keys")
    tag, value = next(iter(document.items()))
    try:
        return _ejson_decode(tag, value)
    except (ValueError, DecimalException) as exc:
        reason = "invalid Decimal128 value" if isinstance(exc, DecimalException) else str(exc)
        raise ValueError(f"invalid {tag} wrapper: {reason}") from None


def _ejson_reject_constant(value):
    raise ValueError(f"use an Extended JSON $numberDouble wrapper for {value}")


def load_shell_json(text):
    """Parse standard JSON or MongoDB Extended JSON v2 for a shell command."""
    try:
        return json.loads(
            text,
            object_pairs_hook=_ejson_object,
            parse_int=_ejson_integer,
            parse_float=_ejson_double,
            parse_constant=_ejson_reject_constant,
        )
    except RecursionError:
        raise ValueError("JSON input is nested too deeply") from None


def _ejson_encode(value, canonical):
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if not canonical:
            return int(value)
        wide = isinstance(value, Int64) or not -(1 << 31) <= value < 1 << 31
        return {"$numberLong" if wide else "$numberInt": str(value)}
    if isinstance(value, float):
        if math.isfinite(value):
            return {"$numberDouble": repr(value)} if canonical else value
        text = "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")
        return {"$numberDouble": text}
    if isinstance(value, Mapping):
        return {name: _ejson_encode(item, canonical) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_ejson_encode(item, canonical) for item in value]
    if isinstance(value, ObjectId):
        return {"$oid": str(value)}
    if isinstance(value, Decimal128):
        number = value.to_decimal()
        return {"$numberDecimal": "NaN" if number.is_nan() else str(number)}
    if isinstance(value, datetime):
        value = DateTime.from_datetime(value)
    if isinstance(value, DateTime):
        if not canonical and 0 <= value.ms <= 253_402_300_799_999:
            precision = "milliseconds" if value.ms % 1000 else "seconds"
            return {"$date": value.to_datetime().isoformat(timespec=precision).replace("+00:00", "Z")}
        return {"$date": {"$numberLong": str(value.ms)}}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$binary": {
                "base64": base64.b64encode(value).decode("ascii"),
                "subType": format(getattr(value, "subtype", 0), "02x"),
            }
        }
    if isinstance(value, Regex):
        return {"$regularExpression": {"pattern": value.pattern, "options": value.options}}
    if isinstance(value, Timestamp):
        return {"$timestamp": {"t": value.time, "i": value.increment}}
    if isinstance(value, MinKey):
        return {"$minKey": 1}
    if isinstance(value, MaxKey):
        return {"$maxKey": 1}
    if isinstance(value, Undefined):
        return {"$undefined": True}
    if isinstance(value, JavaScript):
        document = {"$code": value.code}
        if value.scope is not None:
            document["$scope"] = _ejson_encode(value.scope, canonical)
        return document
    if isinstance(value, Symbol):
        return {"$symbol": value.value}
    if isinstance(value, DBPointer):
        return {"$dbPointer": {"$ref": value.namespace, "$id": {"$oid": str(value.object_id)}}}
    raise ValueError(f"cannot display {type(value).__name__} as Extended JSON")


def shell_json(value, *, pretty=False, canonical=False):
    """Render a BSON value as relaxed or canonical Extended JSON v2."""
    return json.dumps(
        _ejson_encode(value, canonical),
        ensure_ascii=False,
        allow_nan=False,
        indent=2 if pretty else None,
    )


# MongoDB shell connection


class ShellConnection:
    """A synchronous MongoDB connection for the command-line shell."""

    def __init__(self, host, port, *, timeout=5.0, tls_context=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.tls_context = tls_context
        self._socket = None
        self._request_ids = itertools.count(1)
        self._message_limit = MAX_MESSAGE_SIZE

    def __enter__(self):
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._socket = sock
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.tls_context is not None:
                self._socket = self.tls_context.wrap_socket(sock, server_hostname=self.host)
            self._message_limit = MAX_MESSAGE_SIZE
            hello = self.command("admin", {"hello": 1})
            limit = hello.get("maxMessageSizeBytes")
            if isinstance(limit, int) and limit >= _WIRE_HEADER.size + 5:
                self._message_limit = min(self._message_limit, limit)
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_exc):
        self.close()

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def command(self, db, command):
        database_name(db)
        command = dict(require_document(command, "command"))
        command.pop("$db", None)
        if not command:
            raise MongoError("command is empty", 9)
        name = next(iter(command)).lower()
        if self._socket is None:
            raise OSError("MongoDB connection is closed")

        # Standard write commands carry their large arrays as document sequences.
        fields = {
            "insert": ("documents",),
            "update": ("updates",),
            "delete": ("deletes",),
            "bulkwrite": ("ops", "nsInfo"),
        }.get(name, ())
        concern = command.get("writeConcern")
        unacknowledged = (
            bool(fields)
            and isinstance(concern, dict)
            and isinstance(w := concern.get("w"), int)
            and not isinstance(w, bool)
            and w == 0
        )
        sequences = []
        for field in fields:
            documents = command.get(field)
            if isinstance(documents, list) and documents and all(isinstance(doc, dict) for doc in documents):
                if len(documents) > MAX_WRITE_BATCH:
                    raise MongoError("write batch exceeds maximum number of documents")
                del command[field]
                identifier = field.encode("utf-8") + b"\0"
                encoded = b"".join(encode_bson(doc) for doc in documents)
                sequences.append(
                    b"\1" + _BSON_I32.pack(4 + len(identifier) + len(encoded)) + identifier + encoded
                )
        flags = 2 if unacknowledged else 0
        body = b"".join((_WIRE_U32.pack(flags), b"\0", encode_bson({**command, "$db": db}), *sequences))
        size = _WIRE_HEADER.size + len(body)
        if size > self._message_limit:
            raise MongoError("command exceeds maximum message size", 10334)
        request_id = next(self._request_ids) & 0x7FFFFFFF
        request = _WIRE_HEADER.pack(size, request_id, 0, _OP_MSG) + body

        try:
            self._socket.sendall(request)
            if unacknowledged:
                return {"ok": 1.0, "acknowledged": False}
            header = _wire_receive(self._socket, _WIRE_HEADER.size)
            if header is None:
                raise _WireError("MongoDB closed the connection before sending a reply")
            size, _, response_to, opcode = _WIRE_HEADER.unpack(header)
            if (
                opcode != _OP_MSG
                or response_to != request_id
                or not _WIRE_HEADER.size < size <= self._message_limit
            ):
                raise _WireError("Invalid MongoDB reply header")
            body = _wire_receive(self._socket, size - _WIRE_HEADER.size)
            if body is None:
                raise _WireError("MongoDB closed the connection in the middle of a reply")
            _, response, more_to_come = _read_op_msg(header, body, reply=True)
            if more_to_come:
                raise _WireError("Unexpected streaming MongoDB reply")
        except (OSError, BSONError, _WireError):
            self.close()
            raise

        if response.get("ok") != 1:
            details = {key: value for key, value in response.items() if key not in {"code", "errmsg", "ok"}}
            raise MongoError(
                response.get("errmsg", "MongoDB command failed"), response.get("code", 1), **details
            )
        return response

    def authenticate(self, db, username, password, mechanism=None):
        database_name(db)
        require_string(username, "username")
        if "\0" in username:
            raise MongoError("username cannot contain NUL")
        if not isinstance(password, str):
            raise MongoError("password must be a string", 14)
        if mechanism is None:
            hello = self.command("admin", {"hello": 1, "saslSupportedMechs": f"{db}.{username}"})
            mechanisms = hello.get("saslSupportedMechs", [])
            mechanism = "SCRAM-SHA-256" if "SCRAM-SHA-256" in mechanisms else "SCRAM-SHA-1"
        algorithm, secret = scram_secret(mechanism, username, password)

        user = username.encode("utf-8").replace(b"=", b"=3D").replace(b",", b"=2C")
        nonce = base64.b64encode(secrets.token_bytes(24))
        header = b"n,,"
        first = b"n=" + user + b",r=" + nonce
        result = self.command(
            db,
            {
                "saslStart": 1,
                "mechanism": mechanism,
                "payload": Binary(header + first),
                "options": {"skipEmptyExchange": True},
            },
        )
        conversation_id = result.get("conversationId")
        if (
            result.get("done") is not False
            or not isinstance(conversation_id, int)
            or isinstance(conversation_id, bool)
        ):
            raise MongoError("Invalid SCRAM challenge", 18)
        challenge = sasl_payload(result)
        attributes = sasl_attributes(challenge)
        server_nonce = attributes.get(b"r", b"")
        if not server_nonce.startswith(nonce) or server_nonce == nonce or b"m" in attributes:
            raise MongoError("Invalid SCRAM server nonce", 18)
        try:
            salt = base64.b64decode(attributes[b"s"], validate=True)
            count = attributes[b"i"]
            if not salt or not 1 <= len(count) <= 7 or not count.isdigit():
                raise ValueError
            iterations = int(count)
        except (KeyError, ValueError):
            raise MongoError("Invalid SCRAM salt or iteration count", 18) from None
        if not 4096 <= iterations <= 1_000_000:
            raise MongoError("SCRAM iteration count is outside the supported range", 18)

        final = b"c=" + base64.b64encode(header) + b",r=" + server_nonce
        message = b",".join((first, challenge, final))
        client_key, stored_key, server_key = scram_keys(algorithm, secret, salt, iterations)
        client_signature = hmac.digest(stored_key, message, algorithm)
        proof = bytes(a ^ b for a, b in zip(client_key, client_signature))
        reply = self.command(
            db,
            {
                "saslContinue": 1,
                "conversationId": conversation_id,
                "payload": Binary(final + b",p=" + base64.b64encode(proof)),
            },
        )
        signature = base64.b64encode(hmac.digest(server_key, message, algorithm))
        server_proof = sasl_attributes(sasl_payload(reply)).get(b"v", b"")
        if reply.get("conversationId") != conversation_id or not hmac.compare_digest(signature, server_proof):
            raise MongoError("SCRAM server proof verification failed", 18)
        if reply.get("done") is False:
            reply = self.command(
                db, {"saslContinue": 1, "conversationId": conversation_id, "payload": Binary(b"")}
            )
            if reply.get("conversationId") != conversation_id or sasl_payload(reply):
                raise MongoError("Invalid SCRAM final response", 18)
        if reply.get("done") is not True:
            raise MongoError("SCRAM authentication did not complete", 18)


# Command-line entry point


SHELL_HELP = """
  show dbs                         List databases
  show collections                 List collections in the current database
  use <database>                   Switch the current database
  db                               Print the current database
  ping                             Test the connection
  find <collection> [filter]       Print matching documents
  count <collection> [filter]      Count matching documents
  insert <collection> <document>   Insert a document or an array of documents
  update <collection> <statement>  Run an update such as {"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}
  delete <collection> <filter>     Delete one matching document
  delete-many <collection> <filter> Delete all matching documents
  run <command>                    Send a raw JSON command. A bare JSON object also works.
  help                             Show these commands
  exit                             Close the shell""".lstrip("\n")


class CommandShell:
    def __init__(self, connection, database):
        self.connection = connection
        self.database = database

    def run(self, command):
        require_document(command, "command")
        if not command:
            raise ValueError("command cannot be empty")
        reply = self.connection.command(self.database, command)
        error = next(iter(reply.get("writeErrors", [])), reply.get("writeConcernError"))
        if error:
            raise MongoError(error.get("errmsg", "write failed"), error.get("code", 2))
        if reply.get("nErrors"):
            batch = reply.get("cursor", {}).get("firstBatch", [])
            error = next((item for item in batch if not item.get("ok")), {})
            raise MongoError(error.get("errmsg", "bulk write failed"), error.get("code", 2))
        return reply

    def read_cursor(self, command):
        cursor = self.run(command)["cursor"]
        db, _, collection = cursor["ns"].partition(".")
        documents = list(cursor["firstBatch"])
        while cursor["id"]:
            reply = self.connection.command(db, {"getMore": cursor["id"], "collection": collection})
            cursor = reply["cursor"]
            documents.extend(cursor["nextBatch"])
        return documents

    def collection_command(self, action, argument):
        parts = argument.split(maxsplit=1)
        if not parts:
            raise ValueError(f"{action} requires a collection name")
        name = collection_name(parts[0])
        if len(parts) == 2:
            value = load_shell_json(parts[1])
        elif action in ("find", "count"):
            value = {}
        else:
            raise ValueError(f"{action} requires a JSON argument")
        if action == "insert":
            return self.run({"insert": name, "documents": value if isinstance(value, list) else [value]})
        require_document(value, "filter or update statement")
        if action == "find":
            return self.read_cursor({"find": name, "filter": value})
        if action == "count":
            return self.run({"count": name, "query": value})["n"]
        if action == "update":
            if not {"q", "u"} <= value.keys():
                raise ValueError("update expects a statement with 'q' (filter) and 'u' (update)")
            return self.run({"update": name, "updates": [value]})
        limit = 1 if action == "delete" else 0
        return self.run({"delete": name, "deletes": [{"q": value, "limit": limit}]})

    def execute(self, line):
        if line.startswith("{"):
            return self.run(load_shell_json(line))
        action, *rest = line.split(maxsplit=1)
        action = action.lower()
        argument = rest[0] if rest else ""
        if action == "run":
            return self.run(load_shell_json(argument))
        if action == "use":
            self.database = database_name(argument)
            return self.database
        if action == "db" and not argument:
            return self.database
        if action == "ping" and not argument:
            return self.run({"ping": 1})
        if action == "show":
            if argument.lower() in ("dbs", "databases"):
                reply = self.connection.command("admin", {"listDatabases": 1, "nameOnly": True})
                return sorted(row["name"] for row in reply["databases"])
            if argument.lower() == "collections":
                rows = self.read_cursor({"listCollections": 1, "nameOnly": True})
                return sorted(row["name"] for row in rows)
            raise ValueError("use 'show dbs' or 'show collections'")
        if action in ("find", "count", "insert", "update", "delete", "delete-many"):
            return self.collection_command(action, argument)
        raise ValueError(f"unknown shell command {action!r}; enter 'help' for available commands")


_ASK_PASSWORD = object()


def add_credential_arguments(parser):
    parser.add_argument("-u", "--username", "--auth-user", help="username (admin by default with a password)")
    parser.add_argument("--auth-db", help="authentication database (default: admin or the URI auth database)")
    password = parser.add_mutually_exclusive_group()
    password.add_argument(
        "-p",
        "--password",
        nargs="?",
        const=_ASK_PASSWORD,
        metavar="PASSWORD",
        help="password, or prompt when this option has no value",
    )
    password.add_argument("--password-file", type=Path, help="read the password from a UTF-8 file")


def resolve_password(args, fallback=None, *, required=False):
    if args.password is _ASK_PASSWORD:
        password = getpass.getpass("Password: ")
    elif args.password is not None:
        password = args.password
    elif args.password_file is not None:
        password = args.password_file.read_text(encoding="utf-8").removesuffix("\n")
    elif fallback is not None:
        password = fallback
    else:
        password = os.environ.get("MONGO_TEST_PASSWORD") or None
        if password is None and required:
            if not sys.stdin.isatty():
                raise ValueError("supply a password with --password, --password-file, or MONGO_TEST_PASSWORD")
            password = getpass.getpass("Password: ")
    if password == "":
        raise ValueError("password cannot be empty")
    return password


def parse_shell_uri(uri):
    if uri is None:
        return {}
    try:
        parsed = urlsplit(uri)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ValueError("invalid MongoDB URI or port") from None
    if (
        parsed.scheme != "mongodb"
        or not hostname
        or "," in parsed.netloc.rsplit("@", 1)[-1]
        or parsed.netloc.count("@") > 1
        or parsed.fragment
    ):
        raise ValueError("expected a mongodb:// URI with one host")
    database = (
        unquote(parsed.path.removeprefix("/"), errors="strict") if parsed.path not in ("", "/") else None
    )
    options = {}
    for name, value in parse_qsl(parsed.query, keep_blank_values=True, max_num_fields=16):
        key = name.lower()
        if key in options:
            raise ValueError(f"URI option appears more than once: {name}")
        if key in ("tls", "ssl", "directconnection", "retryreads", "retrywrites"):
            if value.lower() not in ("true", "false"):
                raise ValueError(f"URI option {name} expects true or false")
            value = value.lower() == "true"
        elif key not in ("authsource", "authmechanism"):
            raise ValueError(f"unsupported MongoDB URI option: {name}")
        options[key] = value
    if "tls" in options and "ssl" in options and options["tls"] != options["ssl"]:
        raise ValueError("tls and ssl URI options disagree")
    username = None if parsed.username is None else unquote(parsed.username, errors="strict")
    password = None if parsed.password is None else unquote(parsed.password, errors="strict")
    return {
        "host": hostname,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "auth_db": options.get("authsource", database),
        "mechanism": options.get("authmechanism"),
        "tls": options.get("tls", options.get("ssl", False)),
    }


def shell_input(expressions, shell, interactive):
    if expressions:
        yield from expressions
    elif interactive:
        while True:
            try:
                yield input(f"{shell.database}> ")
            except EOFError:
                print()
                return
    else:
        yield from sys.stdin


def print_shell_error(error):
    if isinstance(error, MongoError):
        name = error.details.get("codeName", ERROR_NAMES.get(error.code, "MongoError"))
        print(f"{name} ({error.code}): {error}", file=sys.stderr)
    else:
        print(f"Error: {error}", file=sys.stderr)


def shell_main(argv):
    parser = argparse.ArgumentParser(
        prog=f"{Path(sys.argv[0]).name} shell",
        description="Connect using the built-in MongoDB shell.",
        epilog=SHELL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    uri = parser.add_mutually_exclusive_group()
    uri.add_argument("uri", nargs="?", help="optional mongodb:// connection URI")
    uri.add_argument(
        "--uri", dest="uri_option", metavar="URI", help="use the supplied mongodb:// connection URI"
    )
    parser.add_argument("--host", help="server hostname (default: 127.0.0.1)")
    parser.add_argument("--port", type=port_number, help="server port (default: 27017)")
    parser.add_argument("-d", "--database", help="initial database (default: test)")
    add_credential_arguments(parser)
    parser.add_argument("--auth-mechanism", choices=("SCRAM-SHA-256", "SCRAM-SHA-1"))
    parser.add_argument("--tls", action="store_true", help="connect using TLS")
    parser.add_argument(
        "--tls-ca-file", type=Path, help="trust this PEM certificate authority and enable TLS"
    )
    parser.add_argument(
        "-e", "--eval", action="append", help="run a shell command or JSON command (repeatable)"
    )
    parser.add_argument("--pretty", action="store_true", help="indent JSON output")
    parser.add_argument(
        "--canonical", action="store_true", help="preserve every BSON type in Extended JSON output"
    )
    args = parser.parse_args(argv)
    try:
        target = parse_shell_uri(args.uri if args.uri is not None else args.uri_option)
        host = args.host if args.host is not None else target.get("host") or "127.0.0.1"
        port = args.port if args.port is not None else target.get("port")
        port = 27017 if port is None else port
        if not host or not port:
            raise ValueError("shell requires a hostname and a nonzero port")
        database = args.database if args.database is not None else target.get("database")
        database = database_name("test" if database is None else database)
        username = args.username if args.username is not None else target.get("username")
        if username is not None:
            require_string(username, "username")
        mechanism = args.auth_mechanism or target.get("mechanism")
        if mechanism is not None and mechanism not in ("SCRAM-SHA-256", "SCRAM-SHA-1"):
            raise ValueError("supported mechanisms are SCRAM-SHA-256 and SCRAM-SHA-1")
        auth_db = args.auth_db if args.auth_db is not None else target.get("auth_db")
        auth_db = database_name("admin" if auth_db is None else auth_db)
        password = None
        if username is not None or args.password is not None or args.password_file is not None:
            password = resolve_password(args, target.get("password"), required=True)
        elif mechanism:
            raise ValueError("authMechanism requires a username or password")
        context = None
        if args.tls or args.tls_ca_file or target.get("tls"):
            context = ssl.create_default_context(cafile=args.tls_ca_file)
    except (ValueError, OSError, MongoError, EOFError) as exc:
        parser.error(str(exc) or "password input ended")

    interactive = not args.eval and sys.stdin.isatty()
    try:
        with ShellConnection(host, port, tls_context=context) as connection:
            if password is not None:
                connection.authenticate(auth_db, username or "admin", password, mechanism)
            shell = CommandShell(connection, database)
            if interactive:
                print("MongoDB test shell. Enter 'help' for commands.")
            for line in shell_input(args.eval, shell, interactive):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower() in ("exit", "quit"):
                    break
                if line.lower() == "help":
                    print(SHELL_HELP)
                    continue
                try:
                    reply = shell.execute(line)
                    print(shell_json(reply, pretty=args.pretty or interactive, canonical=args.canonical))
                except (MongoError, ValueError) as exc:
                    print_shell_error(exc)
                    if not interactive:
                        return 1
    except (MongoError, OSError, ValueError) as exc:
        print_shell_error(exc)
        return 1
    return 0


def port_number(value):
    number = int(value)
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError("port must be in the range 0–65535")
    return number


def ttl_seconds(value):
    seconds = float(value)
    if not 0 <= seconds <= threading.TIMEOUT_MAX:
        raise argparse.ArgumentTypeError("TTL interval is outside the allowed range")
    return seconds


def serve_main(argv):
    script = Path(sys.argv[0]).name
    parser = argparse.ArgumentParser(
        description="Run a MongoDB-compatible test server (standard library only).",
        epilog=f"The default URI is mongodb://127.0.0.1:27017. "
               f"See '{script} shell --help' for the client or '{script} export --help' for snapshot export.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", type=port_number, default=27017, help="listen port (0 selects a free port)")
    parser.add_argument("--db-file", type=Path, help="SQLite persistence file (default: in-memory)")
    parser.add_argument(
        "--ttl-interval",
        type=ttl_seconds,
        default=60.0,
        metavar="SECONDS",
        help="seconds between TTL checks (default: %(default)s, 0 disables)",
    )
    add_credential_arguments(parser)
    parser.add_argument("--tls-cert", type=Path, help="PEM server certificate, optionally with private key")
    parser.add_argument("--tls-key", type=Path, help="separate PEM private key for --tls-cert")
    parser.add_argument("--verbose", action="store_true", help="show connection and protocol diagnostics")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.tls_key and not args.tls_cert:
        parser.error("--tls-key requires --tls-cert")
    try:
        account = context = None
        password = resolve_password(args, required=args.username is not None)
        if password is not None:
            default_user = "admin" if args.username is None else args.username
            account = TestAccount("admin" if args.auth_db is None else args.auth_db, default_user, password)
        if args.tls_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(args.tls_cert, args.tls_key)
        settings = {"host": args.host, "port": args.port, "ttl_interval": args.ttl_interval}
        engine = MongoEngine(args.db_file, account=account, settings=settings)
    except (OSError, ValueError, sqlite3.Error, MongoError, EOFError) as exc:
        parser.error(str(exc) or "password input ended")

    monitor = None

    def request_shutdown(_signal, _frame):
        raise KeyboardInterrupt

    try:
        with MongoTCPServer((args.host, args.port), engine.dispatch, context) as server:
            host, port = server.server_address[:2]
            engine.settings["port"] = port
            monitor = threading.Thread(target=engine.monitor_ttl, name="ttl-monitor", daemon=True)
            monitor.start()
            signal.signal(signal.SIGTERM, request_shutdown)
            uri_host = f"[{host}]" if ":" in host else host
            print(f"MongoDB test server listening on mongodb://{uri_host}:{port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
    except OSError as exc:
        LOGGER.error("Could not start server: %s", exc)
        return 1
    finally:
        engine.close()
        if monitor is not None:
            monitor.join()
    return 0


# MongoDB dump and native dbPath export


def _wait_native_mongod(process, log_path, socket_path):
    deadline = time.monotonic() + 45
    last_error = ""
    with log_path.open("rb") as log:
        while time.monotonic() < deadline:
            offset = log.tell()
            line = log.readline()
            if line.endswith(b"\n"):
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeError):
                    continue
                if event.get("s") in {"E", "F"}:
                    last_error = str(event.get("msg", ""))[:200]
                if (
                    event.get("id") == 23016
                    and event.get("attr", {}).get("port") == 0
                    and socket_path.exists()
                    and process.poll() is None
                ):
                    return
                continue
            log.seek(offset)
            status = process.poll()
            if status is not None:
                detail = f": {last_error}" if last_error else ""
                raise ValueError(f"mongod exited during export startup with status {status}{detail}")
            time.sleep(0.05)
    raise ValueError("mongod did not open its export socket within 45 seconds")


def _stop_native_mongod(process):
    if process.poll() is not None:
        return False
    process.terminate()
    try:
        return process.wait(timeout=45) == 0
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return False


def export_native_dbpath(dump_directory: Path, dbpath_directory: Path, mongod: str, mongorestore: str) -> None:
    """Restore a dump into an empty staging directory and close WiredTiger before publishing it."""
    if os.name != "posix":
        raise ValueError("dbPath export requires mongod support for Unix domain sockets")
    if not dump_directory.is_dir():
        raise ValueError("The MongoDB dump directory is missing")
    if not dbpath_directory.is_dir() or any(dbpath_directory.iterdir()):
        raise ValueError("Native dbPath staging directory must be empty")

    # Binding just this socket keeps the unauthenticated, temporary MongoDB
    # process reachable only inside the owner-only directory.
    with tempfile.TemporaryDirectory(prefix="jjap-mongo-", dir="/tmp") as directory:
        private = Path(directory)
        socket_path = private / "mongo.sock"
        startup_log = private / "mongod.log"
        with startup_log.open("wb") as log:
            process = subprocess.Popen([
                mongod, "--bind_ip", str(socket_path), "--port", "0", "--nounixsocket",
                "--dbpath", str(dbpath_directory.resolve()), "--storageEngine", "wiredTiger",
                "--setParameter", "diagnosticDataCollectionEnabled=false",
                "--setParameter", "ttlMonitorEnabled=false",
                "--setParameter", "disableLogicalSessionCacheRefresh=true",
            ], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        restored = False
        try:
            _wait_native_mongod(process, startup_log, socket_path)
            uri = f"mongodb://{quote(str(socket_path), safe='')}/?directConnection=true&serverSelectionTimeoutMS=10000"
            with (private / "mongorestore.log").open("wb") as log:
                restore = subprocess.run([
                    mongorestore, "--uri", uri, "--dir", str(dump_directory.resolve()),
                    "--drop", "--preserveUUID", "--stopOnError", "--maintainInsertionOrder",
                    "--bypassDocumentValidation", "--keepIndexVersion",
                ], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, check=False)
            if restore.returncode:
                raise ValueError(f"mongorestore failed during dbPath export with status {restore.returncode}")
            restored = True
        finally:
            if not _stop_native_mongod(process) and restored:
                raise ValueError("mongod did not shut down cleanly after export; dbPath was not published")


def _dump_collection_basename(name):
    escaped = quote_plus(name, safe="")
    if len(escaped) <= 238:
        return escaped
    # mongorestore uses the original collectionName in metadata for truncated paths.
    digest = hashlib.sha1(name.encode("utf-8"), usedforsecurity=False).digest()
    suffix = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"{escaped[:208]}%24{suffix}"


def _dump_collection(catalog, source, destination):
    stem = _dump_collection_basename(source.name)
    metadata = {
        "options": source.options,
        "indexes": list(source.indexes.values()),
        "collectionName": source.name,
        "type": "view" if source.is_view else "collection",
    }
    if not source.is_view:
        if not isinstance(source.uuid, bytes) or len(source.uuid) != 16:
            raise ValueError(f"Invalid collection UUID for {source.ns!r}")
        # Older server builds wrote arbitrary 16 bytes. MongoDB requires an RFC 4122 v4 UUID on restore.
        metadata["uuid"] = UUID(bytes=source.uuid, version=4).hex
    with (destination / f"{stem}.metadata.json").open("x", encoding="utf-8") as file:
        file.write(shell_json(metadata, canonical=True) + "\n")
    count = 0
    if not source.is_view:
        with (destination / f"{stem}.bson").open("xb") as file:
            for record, data in catalog.record_rows(source.database, source.name):
                if not isinstance(data, bytes) or decode_bson(data)[1] != len(data):
                    raise ValueError(f"Invalid BSON record {record} in {source.ns!r}")
                file.write(data)
                count += 1
    return count


def export_mongo_dump(filename, destination, only_database=None):
    """Write a mongodump directory from one committed SQLite snapshot."""
    if only_database is not None:
        database_name(only_database)
    catalog = Catalog(filename, readonly=True)
    try:
        catalog.sql.execute("BEGIN")
        # Validate all three tables, including for an empty catalog.
        for table in ("mongo_collections", "mongo_indexes", "mongo_documents"):
            if catalog.sql.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
                raise ValueError("Expected a MongoDB test server SQLite persistence file")
        namespaces = catalog.namespaces(only_database)
        if only_database is not None and not namespaces:
            raise ValueError(f"Database {only_database!r} was not found")
        destination.mkdir(mode=0o700)
        databases = {}
        document_count = 0
        for db, name in namespaces:
            database_name(db)
            collection_name(name)
            if "$" in name or name.startswith("system.") or ".system." in name or len(f"{db}.{name}".encode()) > 255:
                raise ValueError(f"MongoDB cannot restore {db}.{name} as an ordinary collection or view")
            if db not in databases:
                directory = destination / db
                directory.mkdir()
                databases[db] = directory
            source = catalog.collection(db, name)
            document_count += _dump_collection(catalog, source, databases[db])
        return len(databases), len(namespaces), document_count
    finally:
        catalog.sql.close()


def _publish_mongo_export(staged, destination):
    if os.name == "nt":
        # Windows rename refuses an existing destination.
        staged.rename(destination)
        return
    destination.mkdir(mode=0o700)
    try:
        staged.replace(destination)
    except BaseException:
        destination.rmdir()
        raise


def export_main(argv):
    parser = argparse.ArgumentParser(description="Export a MongoDB test server SQLite snapshot.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--db", help="export just one database (default: all databases)")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--format", choices=("dump", "dbpath"), default="dump",
                        help="mongorestore directory or native WiredTiger dbPath (default: %(default)s)")
    parser.add_argument("--mongod", default="mongod", help="mongod executable for dbpath export")
    parser.add_argument("--mongorestore", default="mongorestore", help="mongorestore executable for dbpath export")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        if args.format == "dbpath":
            if os.name != "posix":
                raise ValueError("dbPath export requires mongod support for Unix domain sockets")
            mongod = shutil.which(args.mongod)
            mongorestore = shutil.which(args.mongorestore)
            if mongod is None or mongorestore is None:
                missing = args.mongod if mongod is None else args.mongorestore
                raise FileNotFoundError(f"MongoDB executable not found: {missing}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".mongo-export-", dir=destination.parent) as temporary:
            stage = Path(temporary)
            dump = stage / "dump"
            databases, collections, documents = export_mongo_dump(args.db_file.expanduser(), dump, args.db)
            result = dump
            if args.format == "dbpath":
                result = stage / "dbpath"
                result.mkdir(mode=0o700)
                export_native_dbpath(dump, result, mongod, mongorestore)
            _publish_mongo_export(result, destination)
    except (OSError, ValueError, sqlite3.Error, MongoError) as exc:
        parser.exit(1, f"MongoDB export failed: {exc}\n")
    print(f"Exported {documents} documents across {collections} collections/views in {databases} databases to {destination}")
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        if argv[:1] == ["shell"]:
            return shell_main(argv[1:])
        if argv[:1] == ["export"]:
            return export_main(argv[1:])
        return serve_main(argv[1:] if argv[:1] == ["serve"] else argv)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
