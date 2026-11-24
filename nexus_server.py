#!/usr/bin/env python3
"""Single-file Nexus Repository-compatible test server for Python 3.10+.

Run: python3 nexus_server.py --port 8081
REST: http://127.0.0.1:8081/service/rest/v1/repositories
Raw upload: curl -T example.bin http://127.0.0.1:8081/repository/raw-hosted/example.bin
OCI registry: 127.0.0.1:8081/docker-hosted/example/image:tag
Use --db-file test.sqlite3 to persist repository configuration and artifacts.
Set NEXUS_TEST_PASSWORD or --password-file to require a single test account.
Export: python3 nexus_server.py export --db-file test.sqlite3 --out nexus-export
Migrate: python3 nexus_server.py migrate --db-file test.sqlite3 --target http://nexus:8081 --password-file pw
Import: python3 nexus_server.py import --db-file new.sqlite3 --export-dir nexus-export (or --oci-layout, --maven-dir,
--source URL); POST /_jjap/reset restores the startup repositories.

Protocol references:
https://help.sonatype.com/en/api-reference.html
https://help.sonatype.com/en/components-api.html
https://packaging.python.org/en/latest/specifications/simple-repository-api/
https://docs.pypi.org/api/upload/
https://docs.npmjs.com/cli/commands/npm-publish/
https://github.com/opencontainers/distribution-spec/blob/main/spec.md
https://github.com/opencontainers/image-spec/blob/main/image-layout.md
"""

from __future__ import annotations

import argparse
import base64
import email.policy
import fnmatch
import getpass
import hashlib
import hmac
import html
import html.parser
import http.client
import io
import json
import logging
import mimetypes
import os
import re
import secrets
import sqlite3
import ssl
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from email.parser import BytesParser
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


LOG = logging.getLogger("nexus_test_server")
REST = "/service/rest/v1/"
FORMATS = {"raw", "maven2", "pypi", "npm", "docker"}
PAGE_SIZE = 50
METADATA_LIMIT = 2 * 1024 * 1024
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
NPM_NAME = re.compile(r"(?:@[a-z0-9_.-]+/)?[a-z0-9][a-z0-9_.-]*")
COORDINATE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.+-]*")
HASHES = ("md5", "sha1", "sha256", "sha512")


class RequestError(Exception):
    def __init__(self, status, message, field=None):
        super().__init__(message)
        self.status = status
        self.field = field


def iso_date():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_object(value):
    if not isinstance(value, dict):
        raise RequestError(400, "Expected a JSON object")
    return value


def normalize_project(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def asset_path(path):
    if not path or len(path) > 4096 or any(
        part in {"", ".", ".."} for part in path.split("/")
    ) or "\\" in path or re.search(r"[\x00-\x1f\x7f]", path):
        raise RequestError(400, "Invalid asset path")
    return path


def encoded_path(path):
    return urllib.parse.quote(path, safe="/")


def mime_type(path):
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def encode_id(repository, identifier):
    return base64.urlsafe_b64encode(f"{repository}:{identifier:032x}".encode()).decode().rstrip("=")


def decode_id(token):
    try:
        raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
        repository, identifier = raw.decode("ascii").rsplit(":", 1)
        number = int(identifier, 16)
        if not NAME.fullmatch(repository) or not 0 < number < 2**63:
            raise ValueError
        return repository, number
    except (ValueError, UnicodeError) as exc:
        raise RequestError(404, "Item not found") from exc


def page(items, token):
    start = 0
    if token:
        try:
            start = int(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode())
            if start < 0:
                raise ValueError
        except (ValueError, UnicodeError) as exc:
            raise RequestError(400, "Invalid continuationToken", "continuationToken") from exc
    end = start + PAGE_SIZE
    following = base64.urlsafe_b64encode(str(end).encode()).decode().rstrip("=") if end < len(items) else None
    return {"items": items[start:end], "continuationToken": following}


def version_key(version):
    return tuple((1, int(part)) if part.isdecimal() else (0, part.lower())
                 for part in re.split(r"([0-9]+)", version))


def maven_coordinates(path):
    parts = path.split("/")
    if len(parts) < 4:
        return "", path, ""
    group, name, version, filename = ".".join(parts[:-3]), *parts[-3:]
    if not filename.startswith(name + "-") or filename.startswith("maven-metadata"):
        return "", path, ""
    return group, name, version


def maven_metadata(group, name, versions, modified):
    root = ET.Element("metadata", modelVersion="1.1.0")
    ET.SubElement(root, "groupId").text = group
    ET.SubElement(root, "artifactId").text = name
    versioning = ET.SubElement(root, "versioning")
    ET.SubElement(versioning, "latest").text = versions[-1]
    release = next((version for version in reversed(versions) if not version.endswith("-SNAPSHOT")), None)
    if release:
        ET.SubElement(versioning, "release").text = release
    nodes = ET.SubElement(versioning, "versions")
    for version in versions:
        ET.SubElement(nodes, "version").text = version
    timestamp = datetime.fromisoformat(modified.replace("Z", "+00:00"))
    ET.SubElement(versioning, "lastUpdated").text = timestamp.strftime("%Y%m%d%H%M%S")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def parse_pom(data):
    if len(data) > METADATA_LIMIT:
        raise RequestError(400, "POM metadata is too large")
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise RequestError(400, "Invalid Maven POM") from exc
    group = root.findtext("{*}groupId") or root.findtext("{*}parent/{*}groupId") or ""
    name = root.findtext("{*}artifactId") or ""
    version = root.findtext("{*}version") or root.findtext("{*}parent/{*}version") or ""
    return group.strip(), name.strip(), version.strip()


def package_metadata(filename, data):
    try:
        if filename.endswith((".whl", ".zip")):
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                member = next((entry for entry in archive.infolist()
                               if entry.filename.endswith((".dist-info/METADATA", "/PKG-INFO"))), None)
                if member is None or member.file_size > METADATA_LIMIT:
                    raise ValueError
                with archive.open(member) as file:
                    return BytesParser(policy=email.policy.default).parse(file, headersonly=True)
        if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
            with tarfile.open(fileobj=io.BytesIO(data), mode="r|gz") as archive:
                for member in archive:
                    if (member.name.endswith("/PKG-INFO") and member.name.count("/") == 1
                            and member.isfile() and member.size <= METADATA_LIMIT):
                        with archive.extractfile(member) as file:
                            # Streamed tar members are not seekable, which the text parser requires.
                            return BytesParser(policy=email.policy.default).parsebytes(file.read(), headersonly=True)
                    if member.offset_data + member.size > 128 * 1024 * 1024:
                        break
    except (ValueError, EOFError, OSError, zipfile.BadZipFile, tarfile.TarError) as exc:
        raise RequestError(400, "Invalid PyPI distribution") from exc
    raise RequestError(400, "PyPI distribution is missing package metadata")


def npm_manifest(data):
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r|gz") as archive:
            for member in archive:
                if member.name in {"package/package.json", "./package/package.json"}:
                    if not member.isfile() or member.size > METADATA_LIMIT:
                        break
                    with archive.extractfile(member) as file:
                        return json_object(json.load(file))
                if member.offset_data + member.size > 128 * 1024 * 1024:
                    break
    except (ValueError, EOFError, OSError, tarfile.TarError) as exc:
        raise RequestError(400, "Invalid npm tarball") from exc
    raise RequestError(400, "npm tarball is missing package/package.json")


DEFAULT_REPOSITORIES = (
    ("raw-hosted", "raw", "hosted", "{}"),
    ("maven-releases", "maven2", "hosted", '{"maven":{"versionPolicy":"RELEASE"}}'),
    ("maven-snapshots", "maven2", "hosted", '{"maven":{"versionPolicy":"SNAPSHOT"}}'),
    ("maven-public", "maven2", "group", '{"group":{"memberNames":["maven-releases","maven-snapshots"]}}'),
    ("pypi-hosted", "pypi", "hosted", "{}"),
    ("npm-hosted", "npm", "hosted", "{}"),
    ("docker-hosted", "docker", "hosted", "{}"),
)


class Store:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.registry_uploads = {}
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS repositories (
                name TEXT PRIMARY KEY, format TEXT NOT NULL, kind TEXT NOT NULL, config TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS components (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repository TEXT NOT NULL REFERENCES repositories(name) ON DELETE CASCADE,
                namespace TEXT NOT NULL, name TEXT NOT NULL, version TEXT NOT NULL,
                UNIQUE(repository, namespace, name, version)
            );
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repository TEXT NOT NULL REFERENCES repositories(name) ON DELETE CASCADE,
                component_id INTEGER NOT NULL REFERENCES components(id) ON DELETE CASCADE,
                path TEXT NOT NULL, content BLOB NOT NULL, media_type TEXT NOT NULL,
                size INTEGER NOT NULL, checksum TEXT NOT NULL, modified TEXT NOT NULL,
                metadata TEXT NOT NULL,
                UNIQUE(repository, path)
            );
            CREATE INDEX IF NOT EXISTS assets_component ON assets(component_id);
            CREATE TABLE IF NOT EXISTS npm_tags (
                repository TEXT NOT NULL REFERENCES repositories(name) ON DELETE CASCADE,
                name TEXT NOT NULL, tag TEXT NOT NULL, version TEXT NOT NULL,
                PRIMARY KEY(repository, name, tag)
            );
            CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        with self.db:
            initialized = self.db.execute("SELECT 1 FROM settings WHERE name = 'initialized'").fetchone()
            if initialized is None:
                self.db.executemany("INSERT INTO repositories VALUES (?, ?, ?, ?)", DEFAULT_REPOSITORIES)
                self.db.execute("INSERT INTO settings VALUES ('initialized', '1')")

    def reset(self, defaults=True):
        """Remove every repository and asset. Caller holds the store lock and a write transaction."""
        for table in ("npm_tags", "assets", "components", "repositories"):
            self.db.execute(f"DELETE FROM {table}")
        self.db.execute("DELETE FROM sqlite_sequence WHERE name IN ('components', 'assets')")
        self.registry_uploads.clear()
        if defaults:
            self.db.executemany("INSERT INTO repositories VALUES (?, ?, ?, ?)", DEFAULT_REPOSITORIES)

    def repository(self, name):
        with self.lock:
            result = self.db.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()
        if result is None:
            raise RequestError(404, f"Repository not found: {name}")
        return result

    def source_repositories(self, repository):
        with self.lock:
            return list(self._source_repositories(repository, set()))

    def _source_repositories(self, repository, seen):
        config = json.loads(repository["config"])
        if repository["name"] in seen or not config.get("online", True):
            return
        seen.add(repository["name"])
        if repository["kind"] != "group":
            yield repository
            return
        for name in config["group"]["memberNames"]:
            child = self.db.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()
            if child is not None:
                yield from self._source_repositories(child, seen)

    def save_repository(self, name, format_name, kind, values, update=False):
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise RequestError(400, "Invalid repository name", "name")
        if not isinstance(values.get("online", True), bool):
            raise RequestError(400, "online must be a boolean", "online")
        storage = json_object(values.get("storage", {}))
        if storage.get("writePolicy", "ALLOW") not in ("ALLOW", "ALLOW_ONCE", "DENY"):
            raise RequestError(400, "Unsupported storage.writePolicy", "storage.writePolicy")
        if format_name == "maven2":
            maven = json_object(values.get("maven", {}))
            if maven.get("versionPolicy", "MIXED") not in ("RELEASE", "SNAPSHOT", "MIXED"):
                raise RequestError(400, "Unsupported maven.versionPolicy", "maven.versionPolicy")
        if kind == "proxy":
            if format_name not in {"raw", "maven2"}:
                raise RequestError(400, "HTTP proxying is supported for raw and Maven repositories")
            proxy = json_object(values.get("proxy", {}))
            remote = proxy.get("remoteUrl", "")
            try:
                location = urllib.parse.urlsplit(remote)
                valid = (location.scheme in {"http", "https"} and location.hostname and not location.username
                         and not location.password and not location.query and not location.fragment)
                location.port
            except (ValueError, AttributeError, TypeError):
                valid = False
            if not valid:
                raise RequestError(400, "Invalid HTTP(S) upstream URL", "proxy.remoteUrl")
        values = {key: values[key] for key in ("online", "storage", "maven", "proxy", "group") if key in values}
        with self.lock, self.db:
            row = self.db.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()
            if update and (row is None or (row["format"], row["kind"]) != (format_name, kind)):
                raise RequestError(404, f"Repository not found: {name}")
            if not update and row is not None:
                raise RequestError(409, f"Repository already exists: {name}")
            if kind == "group":
                members = json_object(values.get("group", {})).get("memberNames", [])
                if not isinstance(members, list) or any(not isinstance(member, str) for member in members):
                    raise RequestError(400, "group.memberNames must be an array of names")
                pending = list(members)
                visited = set()
                while pending:
                    member = pending.pop()
                    if member == name:
                        raise RequestError(400, "Group membership contains a cycle")
                    if member in visited:
                        continue
                    visited.add(member)
                    other = self.repository(member)
                    if other["format"] != format_name:
                        raise RequestError(400, "Group members must use the same format")
                    if other["kind"] == "group":
                        pending.extend(json.loads(other["config"])["group"]["memberNames"])
                values["group"] = {"memberNames": list(dict.fromkeys(members))}
            if update:
                self.db.execute("UPDATE repositories SET config = ? WHERE name = ?", (json.dumps(values), name))
            else:
                self.db.execute("INSERT INTO repositories VALUES (?, ?, ?, ?)",
                                (name, format_name, kind, json.dumps(values)))

    def put(self, repository, path, content, media_type, coordinates=None, metadata=None, proxy=False, modified=None):
        """Insert one asset. Caller holds the store lock and a write transaction."""
        path = asset_path(path)
        config = json.loads(repository["config"])
        if not proxy:
            check_upload(repository)
        existing = self.db.execute("SELECT id FROM assets WHERE repository = ? AND path = ?",
                                   (repository["name"], path)).fetchone()
        policy = config.get("storage", {}).get("writePolicy", "ALLOW")
        if existing is not None and not proxy and (
                policy == "ALLOW_ONCE" or repository["format"] in {"npm", "pypi"}):
            raise RequestError(409, f"Asset already exists: {path}")

        group, name, version = coordinates or (
            maven_coordinates(path) if repository["format"] == "maven2" else ("", path, "")
        )
        if repository["format"] == "maven2" and version and not proxy:
            mode = config.get("maven", {}).get("versionPolicy", "MIXED")
            snapshot = version.endswith("-SNAPSHOT")
            if (mode == "RELEASE" and snapshot) or (mode == "SNAPSHOT" and not snapshot):
                raise RequestError(400, f"Repository version policy is {mode}")

        keys = repository["name"], group, name, version
        components = self.db.execute("SELECT id FROM components WHERE repository = ? "
                                     "AND namespace = ? AND name = ? AND version = ?", keys).fetchone()
        identifier = (components["id"] if components else self.db.execute(
            "INSERT INTO components(repository, namespace, name, version) VALUES (?, ?, ?, ?)", keys
        ).lastrowid)
        digests = {method: hashlib.new(method, content, usedforsecurity=False).hexdigest() for method in HASHES}
        self.db.execute("""
            INSERT INTO assets(repository, component_id, path, content, media_type, size, checksum, modified, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repository, path) DO UPDATE SET component_id=excluded.component_id,
                content=excluded.content, media_type=excluded.media_type, size=excluded.size,
                checksum=excluded.checksum, modified=excluded.modified, metadata=excluded.metadata
        """, (repository["name"], identifier, path, content, media_type, len(content),
              json.dumps(digests), modified or iso_date(), json.dumps(metadata or {})))

    def delete_asset(self, repository, path):
        self.db.execute("DELETE FROM assets WHERE repository = ? AND path = ?", (repository, path))
        self.prune_components(repository)

    def prune_components(self, repository):
        self.db.execute("DELETE FROM components WHERE repository = ? AND NOT EXISTS "
                        "(SELECT 1 FROM assets WHERE component_id = components.id)", (repository,))

    def listing_rows(self, names=None):
        query = """
            SELECT assets.id AS id, assets.path, assets.size, assets.checksum, assets.modified, assets.metadata,
                   assets.media_type, components.id AS component_id, components.namespace, components.name,
                   components.version, repositories.name AS repository, repositories.format
            FROM assets JOIN components ON assets.component_id = components.id
            JOIN repositories ON assets.repository = repositories.name
        """
        parameters = ()
        if names is not None:
            if not names:
                return []
            query += " WHERE assets.repository IN (" + ",".join("?" for _ in names) + ")"
            parameters = tuple(names)
        query += " ORDER BY assets.repository, assets.path"
        return self.db.execute(query, parameters).fetchall()


def check_upload(repository):
    config = json.loads(repository["config"])
    if repository["kind"] != "hosted":
        raise RequestError(405, "Uploads require a hosted repository")
    if not config.get("online", True):
        raise RequestError(503, "Repository is offline")
    if config.get("storage", {}).get("writePolicy") == "DENY":
        raise RequestError(403, "Repository is read-only")


OCI_DIGEST = re.compile(r"sha256:[a-f0-9]{64}")
OCI_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
OCI_SEGMENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
OCI_IMAGE = re.compile(rf"{OCI_SEGMENT}(?:/{OCI_SEGMENT})*")
OCI_PATH = re.compile(
    r"(?P<image>.+)/(?:(?P<tags>tags/list)|(?P<upload>blobs/uploads(?:/(?P<session>[^/]+))?/?)"
    r"|(?P<kind>blobs|manifests)/(?P<reference>[^/]+))"
)
OCI_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}
OCI_HEADERS = {"Docker-Distribution-Api-Version": "registry/2.0"}


class RegistryError(RequestError):
    def __init__(self, status, code, message):
        super().__init__(status, message)
        self.code = code


class RegistryHandler:
    def registry(self, path, params, repository_name="docker-hosted", base="/v2"):
        try:
            self.authorize()
            if not path:
                if self.is_read():
                    return self.json(200, {}, OCI_HEADERS)
                raise RegistryError(405, "UNSUPPORTED", "Method not supported")
            repository = self.store.repository(repository_name)
            if repository["format"] != "docker":
                raise RegistryError(404, "NAME_UNKNOWN", "Docker repository not found")
            if not json.loads(repository["config"]).get("online", True):
                raise RequestError(503, "Repository is offline")
            if path == "_catalog" and self.is_read():
                images = {asset[len("v2/"):].rsplit("/manifests/", 1)[0]
                          for asset in self.registry_paths(repository, "v2/") if "/manifests/" in asset}
                return self.registry_page("repositories", images, params, base + "/_catalog")
            match = OCI_PATH.fullmatch(path)
            if not match:
                raise RegistryError(404, "UNSUPPORTED", "Registry route not found")
            image = match["image"]
            if len(image) > 255 or not OCI_IMAGE.fullmatch(image):
                raise RegistryError(400, "NAME_INVALID", "Invalid image name")
            asset_base = f"v2/{image}"
            location = f"{base}/{image}"
            if match["tags"] and self.is_read():
                prefix = asset_base + "/manifests/"
                assets = self.registry_paths(repository, asset_base + "/")
                if not assets:
                    raise RegistryError(404, "NAME_UNKNOWN", "Image repository not found")
                tags = {asset[len(prefix):] for asset in assets
                        if asset.startswith(prefix) and OCI_TAG.fullmatch(asset[len(prefix):])}
                return self.registry_page("tags", tags, params, location + "/tags/list", name=image)
            if match["upload"]:
                return self.registry_upload(repository, asset_base, location, match["session"], params)
            if match["kind"]:
                return self.registry_content(repository, asset_base, location, match["kind"], match["reference"])
            raise RegistryError(405, "UNSUPPORTED", "Method not supported")
        except RequestError as exc:
            self.close_connection = True
            code = getattr(exc, "code", {
                400: "UNSUPPORTED", 401: "UNAUTHORIZED", 403: "DENIED", 404: "NAME_UNKNOWN",
                405: "UNSUPPORTED", 409: "DENIED", 413: "SIZE_INVALID",
            }.get(exc.status, "UNKNOWN"))
            headers = dict(OCI_HEADERS)
            if exc.status == 401:
                headers["WWW-Authenticate"] = 'Basic realm="Nexus Registry"'
            self.json(exc.status, {"errors": [{"code": code, "message": str(exc)}]}, headers)

    def registry_paths(self, repository, prefix):
        with self.store.lock:
            names = [source["name"] for source in self.store.source_repositories(repository)]
            return [row["path"] for row in self.store.listing_rows(names) if row["path"].startswith(prefix)]

    def registry_page(self, key, values, params, location, **extra):
        last = params.get("last", "")
        values = sorted(value for value in values if value > last)
        headers = dict(OCI_HEADERS)
        if "n" in params:
            if not re.fullmatch(r"[0-9]{1,6}", params["n"]) or int(params["n"]) < 1:
                raise RegistryError(400, "PAGINATION_NUMBER_INVALID", "n must be a positive integer")
            count = int(params["n"])
            if len(values) > count:
                values = values[:count]
                headers["Link"] = f'<{location}?{urllib.parse.urlencode({"n": count, "last": values[-1]})}>; rel="next"'
        return self.json(200, {key: values, **extra}, headers)

    def registry_asset(self, repository, path):
        for source in self.store.source_repositories(repository):
            asset = self.store.db.execute("SELECT * FROM assets WHERE repository = ? AND path = ?",
                                          (source["name"], path)).fetchone()
            if asset is not None:
                return asset
        return None

    def registry_upload(self, repository, asset_base, location, identifier, params):
        method = self.command
        if (identifier is None and method != "POST") or (
            identifier is not None and method not in {"GET", "HEAD", "PATCH", "PUT", "DELETE"}
        ):
            raise RegistryError(405, "UNSUPPORTED", "Method not supported")
        body = self.read_body() if method in {"POST", "PATCH", "PUT"} else b""
        with self.store.lock:
            uploads = self.store.registry_uploads
            now = time.monotonic()
            for expired in [key for key, value in uploads.items() if value[3] <= now]:
                del uploads[expired]
            if identifier is None:
                check_upload(repository)
                mount = params.get("mount", "")
                source = params.get("from", "").removeprefix(repository["name"] + "/")
                if OCI_DIGEST.fullmatch(mount) and OCI_IMAGE.fullmatch(source):
                    asset = self.registry_asset(repository, f"v2/{source}/blobs/{mount}")
                    if asset is not None:
                        return self.registry_save_blob(repository, asset_base, location, mount, asset["content"])
                if "digest" in params:
                    return self.registry_save_blob(repository, asset_base, location, params["digest"], body)
                if len(uploads) >= 128 or sum(len(entry[2]) for entry in uploads.values()) + len(body) > self.server.max_body:
                    raise RegistryError(429, "TOOMANYREQUESTS", "Too many in-progress uploads")
                identifier = secrets.token_hex(16)
                uploads[identifier] = (repository["name"], asset_base, bytearray(body), now + 3600)
            entry = uploads.get(identifier)
            if entry is None or entry[:2] != (repository["name"], asset_base):
                raise RegistryError(404, "BLOB_UPLOAD_UNKNOWN", "Upload session not found")
            buffered = entry[2]
            if method in {"PATCH", "PUT"}:
                part = self.headers.get("Content-Range")
                if part:
                    span = re.fullmatch(r"(?:bytes )?([0-9]{1,20})-([0-9]{1,20})(?:/\*)?", part)
                    if not span or (int(span[1]), int(span[2])) != (len(buffered), len(buffered) + len(body) - 1):
                        raise RegistryError(416, "RANGE_INVALID", "Unexpected upload offset")
                if len(buffered) + len(body) > self.server.max_body:
                    raise RegistryError(413, "SIZE_INVALID", "Blob is too large")
            if method == "DELETE":
                del uploads[identifier]
                return self.respond(204, headers=OCI_HEADERS)
            if method == "PUT":
                response = self.registry_save_blob(repository, asset_base, location, params.get("digest", ""),
                                                   bytes(buffered) + body)
                del uploads[identifier]
                return response
            if method == "PATCH":
                if sum(len(entry[2]) for entry in uploads.values()) + len(body) > self.server.max_body:
                    raise RegistryError(429, "TOOMANYREQUESTS", "Too many in-progress uploads")
                buffered.extend(body)
                uploads[identifier] = (*entry[:3], now + 3600)
            headers = dict(OCI_HEADERS, Location=f"{location}/blobs/uploads/{identifier}",
                           Range=f"0-{max(len(buffered) - 1, 0)}")
            headers["Docker-Upload-UUID"] = identifier
            self.respond(204 if self.is_read() else 202, headers=headers)

    def registry_save_blob(self, repository, asset_base, location, digest, body):
        if not OCI_DIGEST.fullmatch(digest) or digest != "sha256:" + hashlib.sha256(body).hexdigest():
            raise RegistryError(400, "DIGEST_INVALID", "Blob content does not match digest")
        path = f"{asset_base}/blobs/{digest}"
        with self.store.db:
            check_upload(repository)
            if self.registry_asset(repository, path) is None:
                self.store.put(repository, path, body, "application/octet-stream", ("", asset_base[3:], digest))
        headers = dict(OCI_HEADERS, Location=f"{location}/blobs/{digest}")
        headers["Docker-Content-Digest"] = digest
        self.respond(201, headers=headers)

    def registry_content(self, repository, asset_base, location, kind, reference):
        valid = OCI_DIGEST.fullmatch(reference) or (kind == "manifests" and OCI_TAG.fullmatch(reference))
        if not valid:
            raise RegistryError(400, "DIGEST_INVALID", "Invalid digest or tag")
        path = f"{asset_base}/{kind}/{reference}"
        if self.command == "PUT" and kind == "manifests":
            return self.registry_save_manifest(repository, asset_base, location, path, reference)
        if not self.is_read() and self.command != "DELETE":
            raise RegistryError(405, "UNSUPPORTED", "Method not supported")
        with self.store.lock:
            row = self.registry_asset(repository, path)
            if row is None:
                code = "BLOB_UNKNOWN" if kind == "blobs" else "MANIFEST_UNKNOWN"
                raise RegistryError(404, code, "Registry content not found")
            digest = "sha256:" + json.loads(row["checksum"])["sha256"]
            if self.command == "DELETE":
                check_upload(repository)
                with self.store.db:
                    if kind == "manifests" and OCI_DIGEST.fullmatch(reference):
                        prefix = f"{asset_base}/manifests/"
                        for asset in self.store.listing_rows([repository["name"]]):
                            if asset["path"].startswith(prefix) and asset["checksum"] == row["checksum"]:
                                self.store.delete_asset(repository["name"], asset["path"])
                    else:
                        self.store.delete_asset(repository["name"], path)
                return self.respond(202, headers=OCI_HEADERS)
        headers = dict(OCI_HEADERS)
        headers["Docker-Content-Digest"] = digest
        self.send_content(row["content"], row["media_type"], json.loads(row["checksum"]), row["modified"], headers)

    def registry_save_manifest(self, repository, asset_base, location, path, reference):
        body = self.read_body(4 * 1024 * 1024)
        try:
            manifest = json_object(json.loads(body))
            if manifest.get("schemaVersion") != 2:
                raise ValueError("Invalid schema version")
            media_type = self.headers.get("Content-Type", "").split(";", 1)[0]
            if media_type not in OCI_MEDIA_TYPES:
                media_type = manifest.get("mediaType")
            if not isinstance(media_type, str) or media_type not in OCI_MEDIA_TYPES:
                raise ValueError("Invalid media type")
            if manifest.get("mediaType", media_type) != media_type:
                raise ValueError("Manifest media type mismatch")
            if media_type.endswith((".index.v1+json", ".list.v2+json")):
                kind, descriptors = "manifests", manifest["manifests"]
            else:
                if not isinstance(manifest["layers"], list):
                    raise ValueError("Layers must be a list")
                kind, descriptors = "blobs", [manifest["config"], *manifest["layers"]]
            if not isinstance(descriptors, list) or not all(
                isinstance(item, dict) and isinstance(item.get("digest"), str)
                and OCI_DIGEST.fullmatch(item["digest"]) and type(item.get("size")) is int and item["size"] >= 0
                and isinstance(item.get("mediaType"), str) and item["mediaType"] for item in descriptors
            ):
                raise ValueError("Invalid manifest descriptors")
        except (ValueError, UnicodeError, TypeError, KeyError, RequestError) as exc:
            raise RegistryError(400, "MANIFEST_INVALID", "Invalid image manifest") from exc
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if OCI_DIGEST.fullmatch(reference) and reference != digest:
            raise RegistryError(400, "DIGEST_INVALID", "Manifest content does not match digest")
        canonical = f"{asset_base}/manifests/{digest}"
        with self.store.lock, self.store.db:
            check_upload(repository)
            for item in descriptors:
                asset = self.registry_asset(repository, f"{asset_base}/{kind}/{item['digest']}")
                if asset is None:
                    raise RegistryError(400, "MANIFEST_BLOB_UNKNOWN", "Referenced content not found")
                if asset["size"] != item["size"]:
                    raise RegistryError(400, "MANIFEST_INVALID", "Referenced content does not match descriptor size")
            if self.registry_asset(repository, canonical) is None:
                self.store.put(repository, canonical, body, media_type, ("", asset_base[3:], digest))
            if path != canonical:
                self.store.put(repository, path, body, media_type, ("", asset_base[3:], reference))
        headers = dict(OCI_HEADERS, Location=f"{location}/manifests/{digest}")
        headers["Docker-Content-Digest"] = digest
        self.respond(201, headers=headers)


class Handler(RegistryHandler, BaseHTTPRequestHandler):
    server_version = "NexusTest"
    protocol_version = "HTTP/1.1"

    @property
    def store(self):
        return self.server.store

    @property
    def origin(self):
        host = self.headers.get("Host", "")
        if not re.fullmatch(r"[A-Za-z0-9.\-\[\]:_]+", host):
            return self.server.origin
        return self.server.scheme + "://" + host

    def repository_url(self, name, path=""):
        return f"{self.origin}/repository/{name}/" + encoded_path(path)

    def log_message(self, pattern, *args):
        LOG.info("%s %s", self.client_address[0], pattern % args)

    def authorize(self):
        if self.server.password is None:
            return
        authorization = self.headers.get("Authorization", "")
        scheme, _, token = authorization.partition(" ")
        try:
            if scheme.lower() == "basic":
                value = base64.b64decode(token, validate=True)
                expected = f"{self.server.username}:{self.server.password}".encode()
                if hmac.compare_digest(value, expected):
                    return
            if scheme.lower() == "bearer" and hmac.compare_digest(
                    token.encode(), self.server.password.encode()):
                return
        except (ValueError, UnicodeError):
            pass
        raise RequestError(401, "Authentication required")

    def read_body(self, limit=None):
        limit = self.server.max_body if limit is None else min(limit, self.server.max_body)
        transfer = self.headers.get("Transfer-Encoding", "").lower()
        if transfer:
            if transfer != "chunked" or self.headers.get("Content-Length") is not None:
                raise RequestError(400, "Unsupported Transfer-Encoding")
            result = bytearray()
            while True:
                line = self.rfile.readline(1024)
                size_text = line.split(b";", 1)[0].strip()
                if not line.endswith(b"\r\n") or not re.fullmatch(rb"[0-9A-Fa-f]+", size_text):
                    raise RequestError(400, "Invalid chunk size")
                length = int(size_text, 16)
                if length == 0:
                    for _ in range(128):
                        trailer = self.rfile.readline(8192)
                        if trailer == b"\r\n":
                            return bytes(result)
                        if not trailer.endswith(b"\r\n"):
                            break
                    raise RequestError(400, "Invalid chunked footer")
                if len(result) + length > limit:
                    raise RequestError(413, "Request body too large")
                chunk = self.rfile.read(length)
                if len(chunk) != length or self.rfile.read(2) != b"\r\n":
                    raise RequestError(400, "Invalid chunked body")
                result.extend(chunk)

        lengths = self.headers.get_all("Content-Length") or ["0"]
        if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,20}", lengths[0]):
            raise RequestError(400, "Invalid Content-Length")
        length = int(lengths[0])
        if length > limit:
            raise RequestError(413, "Request body too large")
        body = self.rfile.read(length)
        if len(body) != length:
            raise RequestError(400, "Incomplete request body")
        return body

    def read_json(self, limit=METADATA_LIMIT):
        try:
            return json_object(json.loads(self.read_body(limit)))
        except (ValueError, UnicodeError) as exc:
            raise RequestError(400, "Invalid JSON request body") from exc

    def multipart(self):
        content_type = self.headers.get("Content-Type", "")
        if not content_type.lower().startswith("multipart/form-data;"):
            raise RequestError(415, "Expected multipart/form-data")
        synthetic = b"Content-Type: " + content_type.encode("latin-1") + b"\r\n\r\n" + self.read_body()
        message = BytesParser(policy=email.policy.default).parsebytes(synthetic)
        if not message.is_multipart() or message.defects:
            raise RequestError(400, "Invalid multipart body")
        fields, uploads = {}, {}
        for part in message.iter_parts():
            field = part.get_param("name", header="content-disposition")
            if not field or part.is_multipart() or field in uploads:
                raise RequestError(400, "Invalid or duplicate multipart file field")
            filename, content = part.get_filename(), part.get_payload(decode=True)
            if filename is None:
                try:
                    fields.setdefault(field, content.decode(part.get_content_charset() or "utf-8"))
                except (LookupError, UnicodeError) as exc:
                    raise RequestError(400, f"Invalid text field: {field}") from exc
            else:
                if field in fields or "/" in filename or "\\" in filename:
                    raise RequestError(400, "Multipart filename must not include directories", field)
                uploads[field] = (asset_path(filename), content, part.get_content_type())
        return fields, uploads

    def respond(self, status, body=b"", media_type=None, headers=None):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        if media_type:
            self.send_header("Content-Type", media_type)
        for name, value in (headers or {}).items():
            self.send_header(name, str(value))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD" and body:
            self.wfile.write(body)

    def json(self, status, value, headers=None):
        self.respond(status, json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode(),
                     "application/json", headers)

    def send_content(self, body, media_type, checksum=None, modified=None, extra_headers=None):
        digest = checksum["sha256"] if checksum else hashlib.sha256(body).hexdigest()
        headers = {"ETag": f'"{digest}"', "Accept-Ranges": "bytes", **(extra_headers or {})}
        if modified:
            stamp = datetime.fromisoformat(modified.replace("Z", "+00:00")).timestamp()
            headers["Last-Modified"] = formatdate(stamp, usegmt=True)
        match = self.headers.get("If-None-Match", "")
        if any(value.strip().removeprefix("W/") in {headers["ETag"], "*"} for value in match.split(",")):
            return self.respond(304, headers=headers)
        request_range = self.headers.get("Range", "")
        if self.command == "GET" and request_range and self.headers.get("If-Range", headers["ETag"]) == headers["ETag"]:
            match = re.fullmatch(r"bytes=([0-9]{0,20})-([0-9]{0,20})", request_range)
            if match:
                first, last = match.groups()
                if first:
                    start = int(first)
                    end = min(int(last), len(body) - 1) if last else len(body) - 1
                elif last:
                    start, end = max(len(body) - int(last), 0), len(body) - 1
                else:
                    start, end = 1, 0
                if start <= end:
                    headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
                    return self.respond(206, body[start:end + 1], media_type, headers)
            headers["Content-Range"] = f"bytes */{len(body)}"
            return self.respond(416, headers=headers)
        self.respond(200, body, media_type, headers)

    def handle_http(self):
        try:
            self.dispatch()
        except RequestError as exc:
            self.close_connection = True
            headers = {"WWW-Authenticate": 'Basic realm="Nexus Repository Manager"'} if exc.status == 401 else None
            if self.path.startswith(REST):
                self.json(exc.status, [{"id": exc.field or "*", "message": str(exc)}], headers)
            else:
                self.json(exc.status, {"error": str(exc)}, headers)
        except (ConnectionError, TimeoutError):
            self.close_connection = True
        except Exception:
            self.close_connection = True
            LOG.exception("Unhandled request error")
            self.json(500, {"error": "Internal server error"})

    do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = handle_http

    def dispatch(self):
        try:
            url = urllib.parse.urlsplit(self.path)
            path = urllib.parse.unquote(url.path, errors="strict")
            params = dict(urllib.parse.parse_qsl(url.query, keep_blank_values=True, max_num_fields=100))
        except (ValueError, UnicodeError) as exc:
            raise RequestError(400, "Invalid request URL") from exc

        if path in {"/service/rest/v1/status", "/service/rest/v1/status/writable"} and self.is_read():
            return self.respond(200)
        if path == "/_jjap/reset":
            if self.command != "POST":
                raise RequestError(405, "Use POST to reset the test server")
            self.authorize()
            self.read_body(METADATA_LIMIT)
            with self.store.lock, self.store.db:
                self.store.reset()
            return self.json(200, {"reset": True})
        if path.startswith(REST):
            self.authorize()
            return self.rest(path[len(REST):].strip("/").split("/"), params)
        if path == "/v2" or path.startswith("/v2/"):
            tail = path[4:]
            name, _, scoped = tail.partition("/")
            if scoped and NAME.fullmatch(name):
                try:
                    repository = self.store.repository(name)
                except RequestError:
                    pass
                else:
                    if repository["format"] == "docker":
                        return self.registry(scoped, params, name, f"/v2/{name}")
            return self.registry(tail, params)
        if path.startswith("/repository/"):
            _, _, name, tail = (path + "/").split("/", 3)
            tail = tail[:-1]
            repository = self.store.repository(name)
            if repository["format"] == "docker" and (tail == "v2" or tail.startswith("v2/")):
                return self.registry(tail[3:], params, name, f"/repository/{name}/v2")
            login = repository["format"] == "npm" and self.command == "PUT" and tail.startswith("-/user/org.couchdb.user:")
            if not login:
                self.authorize()
            if not json.loads(repository["config"]).get("online", True):
                raise RequestError(503, "Repository is offline")
            return self.content(repository, tail)
        if path == "/" and self.is_read():
            return self.json(200, {"name": "Nexus Repository test server", "api": REST})
        raise RequestError(404, "Endpoint not found")

    def is_read(self):
        return self.command in {"GET", "HEAD"}

    def repository_payload(self, row, detail=False):
        config = json.loads(row["config"])
        result = {"name": row["name"], "format": row["format"], "type": row["kind"],
                  "url": self.repository_url(row["name"]).rstrip("/"), "online": config.get("online", True),
                  "attributes": {key: value for key, value in config.items() if key != "online"}}
        if detail:
            result.update(config)
            result.setdefault("storage", {"blobStoreName": "default", "strictContentTypeValidation": False,
                                           "writePolicy": "ALLOW"})
        return result

    def asset_payload(self, row):
        return {"id": encode_id(row["repository"], row["id"]), "repository": row["repository"],
                "format": row["format"], "path": row["path"], "downloadUrl": self.repository_url(
                    row["repository"], row["path"]), "checksum": json.loads(row["checksum"]),
                "contentType": row["media_type"], "fileSize": row["size"], "lastModified": row["modified"],
                "blobCreated": row["modified"], "blobUpdated": row["modified"], "blobStoreName": "default"}

    def items(self, rows, assets):
        if assets:
            return [self.asset_payload(row) for row in rows]
        components = {}
        for row in rows:
            item = components.setdefault(row["component_id"], {
                "id": encode_id(row["repository"], row["component_id"]), "repository": row["repository"],
                "format": row["format"], "group": row["namespace"] or None, "name": row["name"],
                "version": row["version"] or None, "assets": [],
            })
            item["assets"].append(self.asset_payload(row))
        return list(components.values())

    def rest(self, segments, params):
        first, *tail = segments
        if [first, *tail] == ["status", "check"] and self.is_read():
            return self.json(200, {"database": {"healthy": True}, "readOnly": {"healthy": True}})
        if first == "repositories":
            return self.rest_repositories(tail)
        if first in {"assets", "components"}:
            return self.rest_items(first == "assets", tail, params)
        if first == "search" and self.is_read() and tail in ([], ["assets"], ["assets", "download"]):
            rows = self.search(params)
            if tail == ["assets", "download"]:
                if not rows:
                    raise RequestError(404, "Asset search returned no results")
                if len(rows) > 1 and not params.get("sort"):
                    raise RequestError(400, "Search returned multiple assets. Refine the query or use sort.")
                return self.respond(302, headers={"Location": self.repository_url(rows[0]["repository"],
                                                                                rows[0]["path"])})
            return self.json(200, page(self.items(rows, bool(tail)), params.get("continuationToken")))
        raise RequestError(404, "REST endpoint not found")

    def rest_repositories(self, segments):
        if not segments and self.is_read():
            with self.store.lock:
                rows = self.store.db.execute("SELECT * FROM repositories ORDER BY name").fetchall()
            return self.json(200, [self.repository_payload(row) for row in rows])
        if len(segments) == 1:
            repository = self.store.repository(segments[0])
            if self.is_read():
                return self.json(200, self.repository_payload(repository))
            if self.command == "DELETE":
                with self.store.lock, self.store.db:
                    self.store.db.execute("DELETE FROM repositories WHERE name = ?", (repository["name"],))
                return self.respond(204)
        if len(segments) in {2, 3}:
            format_name = "maven2" if segments[0] == "maven" else segments[0]
            kind = segments[1]
            if format_name not in FORMATS or kind not in {"hosted", "group", "proxy"}:
                raise RequestError(404, "Repository type endpoint not found")
            if len(segments) == 2 and self.command == "POST":
                values = self.read_json()
                self.store.save_repository(values.get("name"), format_name, kind, values)
                return self.respond(201)
            if len(segments) == 3:
                repository = self.store.repository(segments[2])
                if (repository["format"], repository["kind"]) != (format_name, kind):
                    raise RequestError(404, "Repository type does not match")
                if self.is_read():
                    return self.json(200, self.repository_payload(repository, detail=True))
                if self.command == "PUT":
                    values = self.read_json()
                    if values.get("name", segments[2]) != segments[2]:
                        raise RequestError(400, "Repository cannot be renamed", "name")
                    self.store.save_repository(segments[2], format_name, kind, values, update=True)
                    return self.respond(204)
        raise RequestError(405, "Unsupported repository operation")

    def rest_items(self, assets, segments, params):
        if not segments:
            name = params.get("repository")
            if not name:
                raise RequestError(422, "Parameter 'repository' is required", "repository")
            repository = self.store.repository(name)
            if not assets and self.command == "POST":
                return self.upload_component(repository)
            if self.is_read():
                with self.store.lock:
                    rows = self.store.listing_rows([name])
                return self.json(200, page(self.items(rows, assets), params.get("continuationToken")))
        elif len(segments) == 1:
            name, identifier = decode_id(segments[0])
            field = "id" if assets else "component_id"
            with self.store.lock, self.store.db:
                rows = [row for row in self.store.listing_rows([name]) if row[field] == identifier]
                if not rows:
                    raise RequestError(404, "Item not found")
                if self.command == "DELETE":
                    if assets:
                        self.store.delete_asset(name, rows[0]["path"])
                    else:
                        self.store.db.execute("DELETE FROM components WHERE id = ?", (identifier,))
                    return self.respond(204)
            if self.is_read():
                return self.json(200, self.items(rows, assets)[0])
        raise RequestError(405, "Unsupported item operation")

    def search(self, params):
        namespace_alias = params.get("maven.groupId", params.get("group"))
        name_alias = params.get("maven.artifactId", params.get("raw.name", params.get("name")))
        version_alias = params.get("maven.baseVersion", params.get("version"))
        repository = params.get("repository", params.get("repositoryName"))
        scope = params.get("npm.scope")
        criteria = {"repository": repository, "format": params.get("format"), "namespace": namespace_alias,
                    "name": name_alias, "version": version_alias}
        with self.store.lock:
            rows = self.store.listing_rows()
        extension, classifier = params.get("maven.extension"), params.get("maven.classifier")
        keyword = params.get("q", "").lower()

        def matches(row):
            if any(value is not None and not fnmatch.fnmatchcase(row[field], value)
                   for field, value in criteria.items()):
                return False
            if keyword and not any(keyword in row[key].lower() for key in ("name", "namespace", "path", "version")):
                return False
            if scope is not None and row["namespace"].lstrip("@") != scope.lstrip("@"):
                return False
            checksum = json.loads(row["checksum"])
            if any(key in params and checksum[key] != params[key].lower() for key in HASHES):
                return False
            if extension is not None or classifier is not None:
                filename = row["path"].rsplit("/", 1)[-1]
                suffix = filename.removeprefix(f'{row["name"]}-{row["version"]}')
                if row["format"] != "maven2" or suffix == filename:
                    return False
                if extension is not None and not suffix.endswith("." + extension):
                    return False
                part = suffix.split(".", 1)[0].removeprefix("-")
                if classifier is not None and not fnmatch.fnmatchcase(part, classifier):
                    return False
            return True

        results = [row for row in rows if matches(row)]
        sort = params.get("sort")
        if sort == "version":
            key = lambda row: version_key(row["version"])
        elif sort == "group":
            key = lambda row: row["namespace"]
        elif sort in {"name", "repository", "format", "path"}:
            key = lambda row: row[sort]
        else:
            return results
        return sorted(results, key=key, reverse=params.get("direction", "desc" if sort == "version" else "asc").lower() == "desc")

    def upload_component(self, repository):
        check_upload(repository)
        fields, uploads = self.multipart()
        format_name = repository["format"]
        pending = []

        if format_name == "raw":
            directory = fields.get("raw.directory", "").strip("/")
            for field, (_, content, media) in uploads.items():
                if not re.fullmatch(r"raw\.asset[1-9][0-9]*", field):
                    continue
                filename = fields.get(field + ".filename")
                if not filename:
                    raise RequestError(400, "Missing filename", field + ".filename")
                if "/" in filename:
                    raise RequestError(400, "Raw filename must not include directories", field + ".filename")
                pending.append(("/".join(filter(None, (directory, filename))), content, media, None, None))
        elif format_name == "maven2":
            assets = [(field, upload) for field, upload in uploads.items()
                      if re.fullmatch(r"maven2\.asset[1-9][0-9]*", field)]
            pom = next((upload[1] for field, upload in assets
                        if fields.get(field + ".extension") == "pom"), None)
            parsed = parse_pom(pom) if pom is not None else ("", "", "")
            group, name, version = (
                fields.get("maven2.groupId") or parsed[0],
                fields.get("maven2.artifactId") or parsed[1],
                fields.get("maven2.version") or fields.get("version") or parsed[2],
            )
            if not all(COORDINATE.fullmatch(value) for value in (group, name, version)) or ".." in group:
                raise RequestError(400, "Valid Maven groupId, artifactId, and version are required")
            coordinates = group, name, version
            prefix = f"{group.replace('.', '/')}/{name}/{version}/{name}-{version}"
            for field, (_, bytes_value, media) in assets:
                extension = fields.get(field + ".extension", "")
                classifier = fields.get(field + ".classifier", "")
                if not COORDINATE.fullmatch(extension) or classifier and not COORDINATE.fullmatch(classifier):
                    raise RequestError(400, "Invalid Maven extension or classifier", field)
                suffix = "-" + classifier if classifier else ""
                pending.append((prefix + suffix + "." + extension, bytes_value, media, coordinates, None))
            if pending and pom is None and fields.get("maven2.generate-pom", "").lower() == "true":
                project = ET.Element("project", xmlns="http://maven.apache.org/POM/4.0.0")
                for node, value in (("modelVersion", "4.0.0"), ("groupId", group), ("artifactId", name),
                                    ("version", version), ("packaging", fields.get("maven2.packaging", "jar"))):
                    ET.SubElement(project, node).text = value
                pending.append((prefix + ".pom", ET.tostring(project, encoding="utf-8", xml_declaration=True),
                                "application/xml", coordinates, None))
        elif format_name == "pypi" and "pypi.asset" in uploads:
            upload = uploads["pypi.asset"]
            metadata = package_metadata(upload[0], upload[1])
            pending.append(self.pypi_upload(upload, metadata.get("Name", ""), metadata.get("Version", ""),
                                            metadata.get("Requires-Python")))
        elif format_name == "npm" and "npm.asset" in uploads:
            _, content, _ = uploads["npm.asset"]
            metadata = npm_manifest(content)
            name, version = metadata.get("name", ""), metadata.get("version", "")
            pending.append(self.npm_upload(name, version, content, metadata))

        if not pending:
            raise RequestError(400, "No files supplied for this repository format")
        if len({record[0] for record in pending}) != len(pending):
            raise RequestError(400, "Multiple uploads target the same path")
        with self.store.lock, self.store.db:
            repository = self.store.repository(repository["name"])
            for record in pending:
                self.store.put(repository, *record)
        self.respond(204)

    def content(self, repository, path):
        format_name = repository["format"]
        if format_name == "pypi":
            return self.pypi(repository, path)
        if format_name == "npm":
            return self.npm(repository, path)
        path = asset_path(path)
        if self.is_read():
            return self.download(repository, path)
        if self.command == "PUT":
            check_upload(repository)
            body = self.read_body()
            supplied = self.headers.get("Content-Type") or mime_type(path)
            with self.store.lock, self.store.db:
                repository = self.store.repository(repository["name"])
                self.store.put(repository, path, body, supplied)
            return self.respond(201, headers={"Location": self.repository_url(repository["name"], path)})
        if self.command == "DELETE":
            check_upload(repository)
            with self.store.lock, self.store.db:
                if self.get_asset(repository["name"], path) is None:
                    raise RequestError(404, "Asset not found")
                self.store.delete_asset(repository["name"], path)
            return self.respond(204)
        raise RequestError(405, "Unsupported repository content operation")

    def get_asset(self, name, path):
        with self.store.lock:
            return self.store.db.execute("SELECT content, media_type, checksum, modified FROM assets "
                                         "WHERE repository = ? AND path = ?", (name, path)).fetchone()

    def proxy_asset(self, repository, path):
        name = repository["name"]
        cached = self.get_asset(name, path)
        config = json.loads(repository["config"])["proxy"]
        max_age = config.get("contentMaxAge", 1440)
        if not isinstance(max_age, (int, float)):
            max_age = 1440
        if cached is not None:
            then = datetime.fromisoformat(cached["modified"].replace("Z", "+00:00"))
            if max_age < 0 or (datetime.now(timezone.utc) - then).total_seconds() < max_age * 60:
                return cached
        remote = config["remoteUrl"].rstrip("/") + "/" + encoded_path(path)
        request = urllib.request.Request(remote, headers={"User-Agent": self.server_version})
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(request, timeout=self.server.upstream_timeout) as response:
                content = response.read(self.server.max_body + 1)
                media = response.headers.get_content_type()
                if len(content) > self.server.max_body:
                    raise RequestError(502, "Upstream asset exceeds the configured size limit")
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                return None
            if cached is not None:
                return cached
            raise RequestError(502, f"Upstream returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, ValueError, TimeoutError, OSError) as exc:
            if cached is not None:
                return cached
            raise RequestError(502, "Upstream is unavailable") from exc
        with self.store.lock, self.store.db:
            current = self.store.repository(name)
            if current["config"] != repository["config"]:
                raise RequestError(409, "Repository configuration changed during the request")
            self.store.put(current, path, content, media, proxy=True)
        return self.get_asset(name, path)

    def download(self, repository, path):
        sources = self.store.source_repositories(repository)
        for source in sources:
            asset = (self.proxy_asset(source, path) if source["kind"] == "proxy"
                     else self.get_asset(source["name"], path))
            if asset is not None:
                return self.send_content(asset["content"], asset["media_type"], json.loads(asset["checksum"]),
                                         asset["modified"])
        if repository["format"] == "maven2":
            generated = self.generated_maven(sources, path)
            if generated is not None:
                return self.send_content(*generated)
        raise RequestError(404, "Asset not found")

    def generated_maven(self, repositories, path):
        base, _, extension = path.rpartition(".")
        method = extension if extension in HASHES else None
        original = base if method else path
        if method:
            for repo in repositories:
                stored = self.get_asset(repo["name"], original)
                if stored is not None:
                    return json.loads(stored["checksum"])[method].encode(), "text/plain"
        parent, _, filename = original.rpartition("/")
        namespace, _, name = parent.rpartition("/")
        if filename != "maven-metadata.xml" or not namespace:
            return None
        group = namespace.replace("/", ".")
        with self.store.lock:
            rows = self.store.listing_rows([repo["name"] for repo in repositories])
        matches = [row for row in rows if row["namespace"] == group and row["name"] == name and row["version"]]
        if not matches:
            return None
        versions = sorted({row["version"] for row in matches}, key=version_key)
        data = maven_metadata(group, name, versions, max(row["modified"] for row in matches))
        if method:
            return hashlib.new(method, data, usedforsecurity=False).hexdigest().encode(), "text/plain"
        return data, "application/xml"

    @staticmethod
    def pypi_upload(upload, name, version, requires):
        if not NAME.fullmatch(name) or not COORDINATE.fullmatch(version):
            raise RequestError(400, "Invalid PyPI project name or version")
        filename, data, media = upload
        if not filename.endswith((".whl", ".tar.gz", ".zip")):
            raise RequestError(400, "Unsupported PyPI distribution filename")
        project = normalize_project(name)
        return f"packages/{project}/{filename}", data, media, ("", project, version), {"requires-python": requires}

    def pypi(self, repository, path):
        if self.command == "POST" and path in {"", "legacy", "legacy/"}:
            check_upload(repository)
            fields, files = self.multipart()
            if fields.get(":action", "file_upload") != "file_upload" or "content" not in files:
                raise RequestError(400, "Expected a PyPI file_upload request")
            upload = files["content"]
            digest = fields.get("sha256_digest")
            if digest and digest.lower() != hashlib.sha256(upload[1]).hexdigest():
                raise RequestError(400, "PyPI upload SHA-256 mismatch")
            record = self.pypi_upload(upload, fields.get("name", ""), fields.get("version", ""),
                                      fields.get("requires_python") or None)
            with self.store.lock, self.store.db:
                self.store.put(self.store.repository(repository["name"]), *record)
            return self.respond(200)
        if not self.is_read():
            raise RequestError(405, "Unsupported PyPI operation")
        if path == "simple" or path.startswith("simple/"):
            return self.pypi_index(repository, path)
        if path.startswith("packages/"):
            return self.download(repository, asset_path(path))
        raise RequestError(404, "PyPI endpoint not found")

    def pypi_index(self, repository, path):
        if not path.endswith("/"):
            return self.respond(301, headers={"Location": self.repository_url(repository["name"], path + "/")})
        key = path[len("simple/"):-1]
        if key and not NAME.fullmatch(key):
            raise RequestError(404, "PyPI project not found")
        project = normalize_project(key)
        if key != project:
            return self.respond(301, headers={"Location": self.repository_url(repository["name"], f"simple/{project}/")})
        sources = self.store.source_repositories(repository)
        with self.store.lock:
            rows = self.store.listing_rows([source["name"] for source in sources])
        media_json = "application/vnd.pypi.simple.v1+json"
        media_html = "application/vnd.pypi.simple.v1+html"
        accepted = self.headers.get("Accept", "")
        prefer_json = media_json in accepted and accepted.index(media_json) < min(
            (accepted.index(media) for media in (media_html, "text/html") if media in accepted),
            default=len(accepted),
        )
        if not project:
            projects = sorted({row["name"] for row in rows})
            if prefer_json:
                data = {"meta": {"api-version": "1.0"}, "projects": [{"name": name} for name in projects]}
            else:
                data = [f'<a href="{name}/">{name}</a>' for name in projects]
        else:
            files = dict()
            source_order = {source["name"]: index for index, source in enumerate(sources)}
            for row in sorted(rows, key=lambda row: source_order[row["repository"]]):
                if row["name"] == project:
                    filename = row["path"].rsplit("/", 1)[-1]
                    files.setdefault(filename, {"filename": filename, "url": self.repository_url(
                        repository["name"], row["path"]), "hashes": json.loads(row["checksum"]),
                        "requires-python": json.loads(row["metadata"]).get("requires-python"), "yanked": False})
            if not files:
                raise RequestError(404, "PyPI project not found")
            if prefer_json:
                data = {"meta": {"api-version": "1.0"}, "name": project, "files": list(files.values())}
            else:
                data = []
                for entry in files.values():
                    url = html.escape(entry["url"] + "#sha256=" + entry["hashes"]["sha256"], quote=True)
                    python = entry["requires-python"]
                    requires = f' data-requires-python="{html.escape(python, quote=True)}"' if python else ""
                    data.append(f'<a href="{url}"{requires}>{html.escape(entry["filename"])}</a>')
        if prefer_json:
            return self.respond(200, json.dumps(data).encode(), media_json)
        body = '<!doctype html><meta name="pypi:repository-version" content="1.0"><body>\n' + "\n".join(data)
        self.respond(200, body.encode(), media_html if media_html in accepted else "text/html; charset=utf-8")

    @staticmethod
    def npm_upload(name, version, content, metadata):
        if not isinstance(name, str) or not NPM_NAME.fullmatch(name) or len(name) > 214:
            raise RequestError(400, "Invalid npm package name")
        if not isinstance(version, str) or not COORDINATE.fullmatch(version):
            raise RequestError(400, "Invalid npm package version")
        unscoped = name.rsplit("/", 1)[-1]
        group = name.split("/", 1)[0] if name.startswith("@") else ""
        return (f"{name}/-/{unscoped}-{version}.tgz", content, "application/octet-stream",
                (group, name, version), metadata)

    def npm(self, repository, path):
        method = self.command
        if path == "-/ping" and self.is_read():
            return self.json(200, {})
        if path == "-/whoami" and self.is_read():
            return self.json(200, {"username": self.server.username})
        if path.startswith("-/user/org.couchdb.user:") and method == "PUT":
            check_upload(repository)
            body = self.read_json()
            user, password = body.get("name"), body.get("password")
            expected = self.server.password
            if (not isinstance(user, str) or not isinstance(password, str) or user != self.server.username
                    or (expected is not None and not hmac.compare_digest(expected.encode(), password.encode()))):
                raise RequestError(401, "Invalid npm username or password")
            return self.json(201, {"ok": True, "id": "org.couchdb.user:" + user, "token": expected or "nexus-test"})

        if path.startswith("-/package/") and "/dist-tags" in path:
            package, _, tag = path[len("-/package/"):].partition("/dist-tags")
            if NPM_NAME.fullmatch(package) and (not tag or tag.startswith("/")):
                return self.npm_dist_tags(repository, package, tag.lstrip("/"))
        if self.is_read() and "/-/" in path:
            return self.download(repository, asset_path(path))
        if not NPM_NAME.fullmatch(path):
            raise RequestError(404, "npm package not found")
        if self.is_read():
            return self.npm_document(repository, path)
        if method == "PUT":
            return self.npm_publish(repository, path)
        raise RequestError(405, "Unsupported npm operation")

    def npm_listing(self, repository, package):
        sources = self.store.source_repositories(repository)
        source_order = {source["name"]: index for index, source in enumerate(sources)}
        with self.store.lock:
            rows = [row for row in self.store.listing_rows(list(source_order)) if row["name"] == package]
            tags = {}
            for source in sources:
                candidates = self.store.db.execute(
                    "SELECT tag, version FROM npm_tags WHERE repository = ? AND name = ? ORDER BY tag",
                    (source["name"], package),
                )
                for record in candidates:
                    tags.setdefault(record["tag"], record["version"])
        versions = {}
        for row in sorted(rows, key=lambda row: source_order[row["repository"]]):
            versions.setdefault(row["version"], row)
        tags = {tag: version for tag, version in tags.items() if version in versions}
        if versions:
            tags.setdefault("latest", max(versions, key=version_key))
        return versions, tags

    def npm_document(self, repository, package):
        versions, tags = self.npm_listing(repository, package)
        if not versions:
            raise RequestError(404, "npm package not found")
        documents = {}
        for version, row in versions.items():
            metadata = json.loads(row["metadata"])
            checksum = json.loads(row["checksum"])
            integrity = base64.b64encode(bytes.fromhex(checksum["sha512"])).decode()
            metadata.update({
                "name": package, "version": version, "_id": f"{package}@{version}",
                "dist": {"tarball": self.repository_url(repository["name"], row["path"]),
                         "shasum": checksum["sha1"], "integrity": "sha512-" + integrity},
            })
            documents[version] = metadata
        document = {"_id": package, "name": package, "versions": documents, "dist-tags": tags,
                    "time": {version: row["modified"] for version, row in versions.items()}}
        self.json(200, document)

    def npm_dist_tags(self, repository, package, tag):
        if self.is_read():
            _, tags = self.npm_listing(repository, package)
            return self.json(200, tags)
        check_upload(repository)
        if not NAME.fullmatch(tag):
            raise RequestError(400, "Invalid npm dist-tag")
        if self.command == "PUT":
            try:
                version = json.loads(self.read_body(METADATA_LIMIT))
            except (ValueError, UnicodeError) as exc:
                raise RequestError(400, "Expected an npm version string") from exc
            with self.store.lock, self.store.db:
                versions, _ = self.npm_listing(repository, package)
                if not isinstance(version, str) or version not in versions:
                    raise RequestError(404, "npm package version not found")
                self.store.db.execute("INSERT OR REPLACE INTO npm_tags VALUES (?, ?, ?, ?)",
                                      (repository["name"], package, tag, version))
            return self.json(201, {"ok": True})
        if self.command == "DELETE":
            with self.store.lock, self.store.db:
                self.store.db.execute("DELETE FROM npm_tags WHERE repository = ? AND name = ? AND tag = ?",
                                      (repository["name"], package, tag))
            return self.json(200, {"ok": True})
        raise RequestError(405, "Unsupported npm dist-tag operation")

    def npm_publish(self, repository, package):
        check_upload(repository)
        body = self.read_json(self.server.max_body)
        attachments = json_object(body.get("_attachments", {}))
        versions = json_object(body.get("versions", {}))
        tags = json_object(body.get("dist-tags", {}))
        if not versions or not attachments or body.get("name", package) != package:
            raise RequestError(400, "Invalid npm publish document")
        pending = []
        for version, metadata in versions.items():
            metadata = json_object(metadata)
            if metadata.get("name") != package or metadata.get("version") != version:
                raise RequestError(400, "Inconsistent npm package metadata")
            tarball = json_object(metadata.get("dist", {})).get("tarball", "")
            if not isinstance(tarball, str):
                raise RequestError(400, "Missing npm tarball URL")
            try:
                tarball_path = urllib.parse.unquote(urllib.parse.urlsplit(tarball).path)
            except ValueError as exc:
                raise RequestError(400, "Invalid npm tarball URL") from exc
            qualified_filename = tarball_path.partition("/-/")[2]
            filename = tarball_path.rsplit("/", 1)[-1]
            attachment = json_object(attachments.get(qualified_filename, attachments.get(filename)))
            try:
                content = base64.b64decode(attachment["data"], validate=True)
            except (KeyError, ValueError, TypeError) as exc:
                raise RequestError(400, "Invalid npm tarball attachment") from exc
            if attachment.get("length", len(content)) != len(content):
                raise RequestError(400, "npm tarball length mismatch")
            pending.append(self.npm_upload(package, version, content, metadata))
        with self.store.lock, self.store.db:
            repository = self.store.repository(repository["name"])
            for record in pending:
                self.store.put(repository, *record)
            present, _ = self.npm_listing(repository, package)
            for tag, version in tags.items():
                if not NAME.fullmatch(tag) or not isinstance(version, str) or version not in present:
                    raise RequestError(400, "Invalid npm version or dist-tag")
                self.store.db.execute("INSERT OR REPLACE INTO npm_tags VALUES (?, ?, ?, ?)",
                                      (repository["name"], package, tag, version))
        self.json(201, {"ok": True})


class Server(ThreadingHTTPServer):
    def get_request(self):
        request, address = super().get_request()
        request.settimeout(30)
        return request, address


# Export to native layouts and migration into Nexus Repository


SNAPSHOT_TABLES = {"repositories", "components", "assets", "npm_tags", "settings"}
API_FORMATS = {"maven2": "maven"}
CHECKSUM_SUFFIXES = tuple("." + method for method in HASHES)
OCI_RESERVED = {"blobs", "index.json", "oci-layout"}
OCI_REF_NAME = "org.opencontainers.image.ref.name"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
PYPI_FIELDS = (("Summary", "summary"), ("Home-page", "home_page"), ("Author", "author"),
               ("Author-email", "author_email"), ("License", "license"), ("Keywords", "keywords"))
SIMPLE_HEAD = '<!doctype html><meta name="pypi:repository-version" content="1.0"><body>\n'


class MigrationError(Exception):
    pass


def warn(message):
    print(f"warning: {message}", file=sys.stderr)


def json_bytes(value):
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


class Snapshot:
    """A read-only view of a persistence file inside one read transaction."""

    def __init__(self, path):
        source = Path(path).expanduser().resolve(strict=True)
        if not source.is_file():
            raise ValueError(f"Expected a SQLite persistence file: {source}")
        self.db = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute("BEGIN")
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            valid = SNAPSHOT_TABLES <= tables and self.db.execute(
                "SELECT 1 FROM settings WHERE name = 'initialized'").fetchone() is not None
            if valid:
                self.repositories = {row["name"]: {**dict(row), "config": json.loads(row["config"])}
                                     for row in self.db.execute("SELECT * FROM repositories ORDER BY name")}
                self.tags = {}
                for row in self.db.execute("SELECT * FROM npm_tags ORDER BY repository, name, tag"):
                    self.tags.setdefault((row["repository"], row["name"]), {})[row["tag"]] = row["version"]
        except (sqlite3.DatabaseError, ValueError):
            valid = False
        if not valid:
            self.db.close()
            raise ValueError(f"Not a Nexus test server SQLite persistence file: {source}")

    def close(self):
        self.db.close()

    def count(self, repository):
        return self.db.execute("SELECT COUNT(*) FROM assets WHERE repository = ?", (repository,)).fetchone()[0]

    def listing(self, repository):
        rows = self.db.execute("""
            SELECT assets.id, assets.path, assets.media_type, assets.size, assets.checksum, assets.modified,
                   assets.metadata, components.name AS component, components.version
            FROM assets JOIN components ON components.id = assets.component_id
            WHERE assets.repository = ? ORDER BY assets.path
        """, (repository,))
        return [{**dict(row), "repository": repository, "checksum": json.loads(row["checksum"]),
                 "metadata": json.loads(row["metadata"])} for row in rows]

    def read(self, asset):
        content = self.db.execute("SELECT content FROM assets WHERE id = ?", (asset["id"],)).fetchone()[0]
        recorded = asset["checksum"]
        if not isinstance(content, bytes) or len(content) != asset["size"] or any(
                hashlib.new(method, content, usedforsecurity=False).hexdigest() != recorded.get(method)
                for method in ("sha1", "sha256")):
            raise ValueError(f"{asset['repository']}/{asset['path']} does not match its recorded size and checksums")
        return content


def repository_request(repository, known):
    """The Nexus REST create request (path and JSON body) for one repository."""
    name, format_name, kind, config = (repository[key] for key in ("name", "format", "kind", "config"))
    storage = {"blobStoreName": "default", "strictContentTypeValidation": False, "writePolicy": "ALLOW",
               **config.get("storage", {})}
    if kind != "hosted":
        storage.pop("writePolicy", None)
    body = {"name": name, "online": config.get("online", True), "storage": storage}
    if format_name == "raw":
        body["raw"] = {"contentDisposition": "INLINE"}
    if format_name == "maven2" and kind != "group":
        body["maven"] = {"versionPolicy": "MIXED", "layoutPolicy": "PERMISSIVE", "contentDisposition": "INLINE",
                         **config.get("maven", {})}
    if format_name == "docker":
        body["docker"] = {"v1Enabled": False, "forceBasicAuth": True}
    if kind == "proxy":
        proxy = config.get("proxy", {})
        body["proxy"] = {"contentMaxAge": 1440, "metadataMaxAge": proxy.get("contentMaxAge", 1440), **proxy}
        # Mirrors the test server, which keeps no negative cache and never blocks an upstream.
        body["negativeCache"] = {"enabled": False, "timeToLive": 1440}
        body["httpClient"] = {"blocked": False, "autoBlock": False}
    if kind == "group":
        members = config.get("group", {}).get("memberNames", [])
        missing = [member for member in members if member not in known]
        if missing:
            warn(f"{name}: dropping group members that do not exist: {', '.join(missing)}")
        body["group"] = {"memberNames": [member for member in members if member in known]}
    return {"name": name, "format": format_name, "type": kind,
            "path": f"{REST}repositories/{API_FORMATS.get(format_name, format_name)}/{kind}", "body": body}


def creation_order(repositories):
    order = [name for kind in ("hosted", "proxy") for name, repository in repositories.items()
             if repository["kind"] == kind]
    placed = set(order)

    def place(name, trail):
        repository = repositories.get(name)
        if repository is None or name in placed or name in trail:
            return
        for member in repository["config"].get("group", {}).get("memberNames", []):
            place(member, trail | {name})
        placed.add(name)
        order.append(name)

    for name in repositories:
        place(name, frozenset())
    return order


def npm_dist_tags(snapshot, repository, package, versions):
    tags = {tag: version for tag, version in snapshot.tags.get((repository, package), {}).items()
            if version in versions}
    if versions:
        tags.setdefault("latest", max(versions, key=version_key))
    return tags


def npm_dist(asset, tarball):
    checksum = asset["checksum"]
    return {"tarball": tarball, "shasum": checksum["sha1"],
            "integrity": "sha512-" + base64.b64encode(bytes.fromhex(checksum["sha512"])).decode()}


def docker_images(snapshot, repository, assets):
    """Group registry assets by image: blobs and manifests by digest, tags, push order and problems."""
    images = {}
    for asset in assets:
        parts = asset["path"].rsplit("/", 2)
        image = parts[0].removeprefix("v2/") if len(parts) == 3 and parts[0].startswith("v2/") else ""
        kind, reference = parts[1:] if len(parts) == 3 else ("", "")
        digest = "sha256:" + asset["checksum"]["sha256"]
        tag = kind == "manifests" and OCI_TAG.fullmatch(reference) and not OCI_DIGEST.fullmatch(reference)
        if kind not in {"blobs", "manifests"} or not OCI_IMAGE.fullmatch(image) or not (
                tag or OCI_DIGEST.fullmatch(reference)):
            warn(f"{repository}: skipping {asset['path']}: not a Docker registry path")
            continue
        entry = images.setdefault(image, {"blobs": {}, "manifests": {}, "tags": {}, "problems": []})
        if tag:
            entry["tags"][reference] = digest
            entry.setdefault("tagged", {})[digest] = asset
            continue
        if reference != digest:
            entry["problems"].append(f"{repository}/{asset['path']} does not match its digest")
        entry[kind][reference] = asset
    for image, entry in images.items():
        for digest, asset in entry.pop("tagged", {}).items():
            entry["manifests"].setdefault(digest, asset)
        children = {}
        for digest, asset in entry["manifests"].items():
            index = asset["media_type"].endswith((".index.v1+json", ".list.v2+json"))
            pool = entry["manifests"] if index else entry["blobs"]
            try:
                manifest = json_object(json.loads(snapshot.read(asset)))
                descriptors = manifest["manifests"] if index else [manifest["config"], *manifest["layers"]]
                references = [(item["digest"], item["size"]) for item in descriptors]
            except (ValueError, KeyError, TypeError, RequestError) as exc:
                entry["problems"].append(f"{repository}/{image}@{digest}: unreadable manifest ({exc})")
                continue
            for reference, size in references:
                if reference not in pool or pool[reference]["size"] != size:
                    entry["problems"].append(f"{repository}/{image}@{digest} references missing content {reference}")
            children[digest] = [reference for reference, _ in references if index and reference in pool]
        order = []

        def visit(digest):
            if digest not in order:
                for child in children.get(digest, ()):
                    visit(child)
                order.append(digest)

        for digest in sorted(entry["manifests"]):
            visit(digest)
        nested = {child for values in children.values() for child in values}
        entry["order"] = order
        entry["untagged"] = [digest for digest in order
                             if digest not in nested and digest not in entry["tags"].values()]
    return images


def write_export_file(folder, relative, content, modified=None):
    try:
        asset_path(relative)
    except RequestError as exc:
        raise ValueError(f"Invalid asset path: {relative!r}") from exc
    target = folder.joinpath(*relative.split("/"))
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "xb") as file:
            file.write(content)
    except (FileExistsError, NotADirectoryError) as exc:
        raise ValueError(f"{folder.name}/{relative} collides with another exported path") from exc
    if modified:
        stamp = datetime.fromisoformat(modified.replace("Z", "+00:00")).timestamp()
        os.utime(target, (stamp, stamp))


def default_media_type(format_name, path):
    """The media type import assigns when an export records none."""
    return mime_type(path) if format_name in {"raw", "maven2"} else "application/octet-stream"


def manifest_media_type(content):
    try:
        media_type = json.loads(content).get("mediaType")
    except (ValueError, AttributeError):
        return None
    return media_type if media_type in OCI_MEDIA_TYPES else None


def export_files(snapshot, repository, folder, overrides):
    name, format_name = repository["name"], repository["format"]
    packages, projects = {}, {}
    assets = snapshot.listing(name)
    for asset in assets:
        write_export_file(folder, asset["path"], snapshot.read(asset), asset["modified"])
        if asset["media_type"] != default_media_type(format_name, asset["path"]):
            overrides[asset["path"]] = {"mediaType": asset["media_type"]}
        if format_name == "npm":
            packages.setdefault(asset["component"], {})[asset["version"]] = asset
        elif format_name == "pypi":
            projects.setdefault(asset["component"], []).append(asset)
    for package, versions in packages.items():
        ordered = sorted(versions, key=version_key)
        document = {"_id": package, "name": package, "dist-tags": npm_dist_tags(snapshot, name, package, versions),
                    "versions": {version: {**versions[version]["metadata"], "name": package, "version": version,
                                           "_id": f"{package}@{version}",
                                           "dist": npm_dist(versions[version], versions[version]["path"])}
                                 for version in ordered},
                    "time": {version: versions[version]["modified"] for version in ordered}}
        write_export_file(folder, f"{package}/packument.json", json_bytes(document))
    if projects:
        links = [f'<a href="{html.escape(project)}/">{html.escape(project)}</a>' for project in sorted(projects)]
        write_export_file(folder, "simple/index.html", (SIMPLE_HEAD + "\n".join(links)).encode())
    for project, files in projects.items():
        links = []
        for asset in files:
            url = html.escape(f"../../{encoded_path(asset['path'])}#sha256={asset['checksum']['sha256']}", quote=True)
            python = asset["metadata"].get("requires-python")
            requires = f' data-requires-python="{html.escape(python, quote=True)}"' if python else ""
            links.append(f'<a href="{url}"{requires}>{html.escape(asset["path"].rsplit("/", 1)[-1])}</a>')
        write_export_file(folder, f"simple/{project}/index.html", (SIMPLE_HEAD + "\n".join(links)).encode())
    return len(assets)


def export_docker(snapshot, repository, folder, overrides):
    name = repository["name"]
    assets = snapshot.listing(name)
    images = docker_images(snapshot, name, assets)
    for image, entry in images.items():
        if entry["problems"]:
            raise ValueError(entry["problems"][0])
        segments = image.split("/")
        for index in range(1, len(segments)):
            if "/".join(segments[:index]) in images and segments[index] in OCI_RESERVED:
                raise ValueError(f"{name}/{image} would be written inside the OCI layout of another image")
    count = 0
    for image, entry in images.items():
        target = folder.joinpath(*image.split("/"))
        write_export_file(target, "oci-layout", b'{"imageLayoutVersion": "1.0.0"}\n')
        for digest, asset in sorted({**entry["blobs"], **entry["manifests"]}.items()):
            write_export_file(target, "blobs/sha256/" + digest.removeprefix("sha256:"), snapshot.read(asset),
                              asset["modified"])
        manifests = entry["manifests"]
        descriptors = [{"mediaType": manifests[digest]["media_type"], "digest": digest,
                        "size": manifests[digest]["size"], "annotations": {OCI_REF_NAME: tag}}
                       for tag, digest in sorted(entry["tags"].items())]
        descriptors += [{"mediaType": manifests[digest]["media_type"], "digest": digest,
                         "size": manifests[digest]["size"]} for digest in entry["untagged"]]
        write_export_file(target, "index.json", json_bytes(
            {"schemaVersion": 2, "mediaType": OCI_INDEX, "manifests": descriptors}))
        count += len(entry["blobs"]) + len(manifests) + len(entry["tags"])
    # The OCI layout keeps no tag timestamps, and manifest documents may omit their media type.
    for asset in assets:
        kind = asset["path"].rsplit("/", 2)[-2:-1]
        if kind == ["manifests"]:
            record = {}
            if asset["media_type"] != manifest_media_type(snapshot.read(asset)):
                record["mediaType"] = asset["media_type"]
            if not OCI_DIGEST.fullmatch(asset["path"].rsplit("/", 1)[-1]):
                record["modified"] = asset["modified"]
            if record:
                overrides[asset["path"]] = record
    return len(images), count


def export_nexus(source, destination, include_proxy_cache=False):
    snapshot = Snapshot(source)
    try:
        repositories = snapshot.repositories
        requests = [repository_request(repositories[name], repositories) for name in creation_order(repositories)]
        destination.mkdir()
        counts = {"repositories": len(requests), "assets": 0, "images": 0, "cached": 0}
        for request in requests:
            repository = repositories[request["name"]]
            # The test server's own settings and asset attributes let import restore the snapshot exactly.
            request["config"] = repository["config"]
            if repository["kind"] == "group" or (repository["kind"] == "proxy" and not include_proxy_cache):
                counts["cached"] += snapshot.count(repository["name"])
                continue
            folder = destination / "content" / repository["name"]
            try:
                folder.mkdir(parents=True)
            except FileExistsError as exc:
                raise ValueError(f"Repository names collide on this file system: {repository['name']}") from exc
            overrides = {}
            if repository["format"] == "docker":
                images, assets = export_docker(snapshot, repository, folder, overrides)
                counts["images"] += images
            else:
                assets = export_files(snapshot, repository, folder, overrides)
            if overrides:
                request["assets"] = overrides
            counts["assets"] += assets
        write_export_file(destination, "repositories.json", json_bytes(requests))
        return counts
    finally:
        snapshot.close()


def publish_export(staged, destination):
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
    parser = argparse.ArgumentParser(
        prog=f"{Path(sys.argv[0]).name} export",
        description="Export a Nexus test server SQLite snapshot as Nexus REST repository requests and native layouts.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--include-proxy-cache", action="store_true",
                        help="also export the content cached by proxy repositories")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".nexus-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "export"
            counts = export_nexus(args.db_file, staged, args.include_proxy_cache)
            publish_export(staged, destination)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"Nexus export failed: {exc}\n")
    skipped = f"; {counts['cached']} cached proxy assets not exported" if counts["cached"] else ""
    print(f"Exported {counts['repositories']} repositories, {counts['assets']} assets and {counts['images']} "
          f"Docker images to {destination}{skipped}")
    return 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def http_failure(method, url, status, data):
    detail = " ".join(data[:300].decode("utf-8", "replace").split())
    return f"{method} {url} returned HTTP {status}" + (f": {detail}" if detail else "")


def registry_api(endpoint):
    """Registry API root for an endpoint; a URL without a /v2 segment gets one appended."""
    endpoint = endpoint.rstrip("/")
    return endpoint if "v2" in urllib.parse.urlsplit(endpoint).path.split("/") else endpoint + "/v2"


def registry_root(api):
    """The /v2/ base URL of a registry API URL and the path in front of it."""
    parts = urllib.parse.urlsplit(api)
    segments = parts.path.split("/")
    prefix = "/".join(segments[:len(segments) - 1 - segments[::-1].index("v2")])
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, prefix + "/v2/", "", "")), prefix


def registry_location(api, location):
    prefix = registry_root(api)[1]
    # Nexus answers uploads under /repository/<name>/v2 with a Location relative to its connector root.
    if prefix and location.startswith("/v2/") and not location.startswith(prefix + "/v2/"):
        location = prefix + location
    return urllib.parse.urljoin(api + "/", location)


def multipart_body(fields, field, filename, content):
    boundary = secrets.token_hex(16)
    while boundary.encode() in content or any(boundary in value for _, value in fields):
        boundary = secrets.token_hex(16)
    parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
             for key, value in fields]
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'
                 f'Content-Type: application/octet-stream\r\n\r\n'.encode() + content)
    return b"".join(parts) + f"\r\n--{boundary}--\r\n".encode(), f"multipart/form-data; boundary={boundary}"


def pypi_fields(asset, content):
    filename = asset["path"].rsplit("/", 1)[-1]
    try:
        metadata = package_metadata(filename, content)
    except Exception:  # Best effort: streamed sdists can fail in package_metadata.
        metadata = {}
    project = str(metadata.get("Name") or "")
    if normalize_project(project) != asset["component"]:
        project = asset["component"]
    wheel = filename.endswith(".whl")
    fields = [(":action", "file_upload"), ("protocol_version", "1"),
              ("metadata_version", str(metadata.get("Metadata-Version") or "2.1")), ("name", project),
              ("version", asset["version"]), ("filetype", "bdist_wheel" if wheel else "sdist"),
              ("pyversion", filename[:-4].split("-")[-3] if wheel and filename.count("-") >= 4 else "source"),
              ("md5_digest", asset["checksum"]["md5"]), ("sha256_digest", asset["checksum"]["sha256"])]
    fields += [(field, str(metadata[header])) for header, field in PYPI_FIELDS if metadata.get(header)]
    if asset["metadata"].get("requires-python"):
        fields.append(("requires_python", asset["metadata"]["requires-python"]))
    return fields


def upload_rank(asset):
    path = asset["path"]
    if path.endswith(CHECKSUM_SUFFIXES):
        return 2, path
    return int(path.rsplit("/", 1)[-1] == "maven-metadata.xml"), path


class NexusClient:
    def __init__(self, target, username, password, timeout):
        self.target = target
        self.basic = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        self.timeout = timeout
        self.opener = urllib.request.build_opener(NoRedirect)
        self.tokens = {}

    def request(self, method, url, body=None, headers=None, authorization=None):
        headers = {"User-Agent": "nexus-test-server-migrate", **(headers or {}),
                   "Authorization": authorization or self.basic}
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as exc:
            try:
                return exc.code, exc.headers, exc.read()
            finally:
                exc.close()
        except (OSError, ValueError, http.client.HTTPException) as exc:
            raise MigrationError(f"{method} {url} failed: {getattr(exc, 'reason', exc)}") from exc

    def call(self, method, url, body=None, headers=None, expect=(200, 201, 204)):
        status, response_headers, data = self.request(method, url, body, headers)
        if status not in expect:
            raise MigrationError(http_failure(method, url, status, data))
        return response_headers, data

    def registry(self, method, url, body=None, headers=None):
        host = urllib.parse.urlsplit(url).netloc
        status, response_headers, data = self.request(method, url, body, headers, self.tokens.get(host))
        if status == 401 and self.tokens.pop(host, None):
            status, response_headers, data = self.request(method, url, body, headers)
        challenge = response_headers.get("WWW-Authenticate", "") if status == 401 else ""
        if challenge[:7].lower() == "bearer ":
            self.tokens[host] = "Bearer " + self.bearer_token(url, challenge)
            status, response_headers, data = self.request(method, url, body, headers, self.tokens[host])
        return status, response_headers, data

    def bearer_token(self, url, challenge):
        params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        realm = params.pop("realm", "")
        allowed = {urllib.parse.urlsplit(address).hostname for address in (url, self.target)}
        if urllib.parse.urlsplit(realm).scheme not in {"http", "https"} or (
                urllib.parse.urlsplit(realm).hostname not in allowed):
            raise MigrationError(f"Refusing to send credentials to registry token realm {realm!r}")
        _, data = self.call("GET", realm + ("&" if "?" in realm else "?") + urllib.parse.urlencode(params),
                            expect=(200,))
        try:
            document = json_object(json.loads(data))
        except (ValueError, RequestError) as exc:
            raise MigrationError("Registry token endpoint returned invalid JSON") from exc
        token = document.get("token") or document.get("access_token")
        if not isinstance(token, str) or not token:
            raise MigrationError("Registry token endpoint returned no token")
        return token


def list_assets(client, name):
    """Every item of a repository's REST asset listing, following continuation tokens."""
    items, token = [], None
    while True:
        query = urllib.parse.urlencode({"repository": name, **({"continuationToken": token} if token else {})})
        _, data = client.call("GET", f"{client.target}{REST}assets?{query}", expect=(200,))
        try:
            listing = json_object(json.loads(data))
            for item in listing["items"]:
                if not isinstance(item["path"], str):
                    raise TypeError
                items.append(item)
        except (ValueError, KeyError, TypeError, AttributeError, RequestError) as exc:
            raise MigrationError(f"{name}: {client.target} returned an invalid asset listing") from exc
        token = listing.get("continuationToken")
        if not token:
            return items


class Migration:
    def __init__(self, snapshot, client, endpoints, dry_run=False):
        self.snapshot, self.client, self.endpoints = snapshot, client, endpoints
        self.dry_run = dry_run
        self.planned = set()
        self.failures = []
        self.counts = {"created": 0, "existing": 0, "uploaded": 0, "skipped": 0}

    def plan(self, message, count=True):
        """In a dry run, report a write instead of sending it."""
        print(f"would {message}")
        if count:
            self.counts["uploaded"] += 1

    def fail(self, message):
        self.failures.append(message)
        print(f"error: {message}", file=sys.stderr)

    def attempt(self, label, action, *args):
        try:
            action(*args)
        except (MigrationError, ValueError, RequestError) as exc:
            self.fail(f"{label}: {exc}")

    def run(self, names):
        target = self.client.target
        _, data = self.client.call("GET", f"{target}{REST}repositories", expect=(200,))
        try:
            remote = {item["name"]: (item["format"], item["type"]) for item in json.loads(data)}
        except (ValueError, KeyError, TypeError) as exc:
            raise MigrationError(f"{target} did not return a Nexus repository list") from exc
        ready, final = [], []
        for name in names:
            repository = self.snapshot.repositories[name]
            request = repository_request(repository, self.snapshot.repositories)
            if name in remote:
                if remote[name] != (repository["format"], repository["kind"]):
                    self.fail(f"{name}: conflict: the target repository is {'/'.join(remote[name])}, "
                              f"the source is {repository['format']}/{repository['kind']}")
                    continue
                self.counts["existing"] += 1
            elif not self.create(request, final):
                continue
            ready.append(name)
            if repository["kind"] == "proxy" and self.snapshot.count(name):
                warn(f"{name}: {self.snapshot.count(name)} cached proxy assets are not migrated; "
                     "the target proxy fetches them from its remote")
        for name in ready:
            if self.snapshot.repositories[name]["kind"] == "hosted":
                self.upload(name)
        for request in final:
            if self.dry_run:
                self.plan(f"apply the read-only or offline settings of {request['name']}", count=False)
                continue
            self.attempt(request["name"], self.client.call, "PUT", f"{target}{request['path']}/{request['name']}",
                         json_bytes(request["body"]), {"Content-Type": "application/json"})
        return len(ready)

    def create(self, request, final):
        body = request["body"]
        endpoint = urllib.parse.urlsplit(self.endpoints.get(request["name"], ""))
        target = urllib.parse.urlsplit(self.client.target)
        if endpoint.port and endpoint.path.strip("/") == "" and endpoint.netloc != target.netloc:
            body["docker"]["httpsPort" if endpoint.scheme == "https" else "httpPort"] = endpoint.port
        staged = body
        if request["type"] == "hosted" and (not body["online"] or body["storage"].get("writePolicy") == "DENY"):
            # Upload into a writable repository, then apply the read-only or offline state.
            staged = {**body, "online": True, "storage": {**body["storage"], "writePolicy": "ALLOW"}}
            final.append(request)
        if self.dry_run:
            self.plan(f"create repository {request['name']} ({request['format']}/{request['type']})", count=False)
            self.planned.add(request["name"])
            self.counts["created"] += 1
            return True
        try:
            self.client.call("POST", self.client.target + request["path"], json_bytes(staged),
                             {"Content-Type": "application/json"})
        except MigrationError as exc:
            self.fail(f"{request['name']}: {exc}")
            return False
        self.counts["created"] += 1
        return True

    def remote_assets(self, name):
        return {item["path"].lstrip("/"): item.get("checksum") or {} for item in list_assets(self.client, name)}

    def compare(self, label, asset, checksum):
        """Return "missing", "same" or "conflict" (reported) for an asset already looked up on the target."""
        if checksum is None:
            return "missing"
        methods = [method for method in ("sha256", "sha1") if checksum.get(method)]
        if methods and all(checksum[method] == asset["checksum"][method] for method in methods):
            self.counts["skipped"] += 1
            return "same"
        self.fail(f"{label}: conflict: the target already holds different content")
        return "conflict"

    def upload(self, name):
        repository = self.snapshot.repositories[name]
        assets = self.snapshot.listing(name)
        if repository["format"] == "docker":
            return self.upload_docker(name, assets)
        try:
            remote = {} if name in self.planned else self.remote_assets(name)
        except MigrationError as exc:
            return self.fail(str(exc))
        if repository["format"] == "npm":
            return self.upload_npm(name, assets, remote)
        pypi = repository["format"] == "pypi"
        if pypi:
            remote = {path.rsplit("/", 1)[-1]: checksum for path, checksum in remote.items()
                      if path.startswith("packages/")}
        for asset in sorted(assets, key=upload_rank):
            label = f"{name}/{asset['path']}"
            key = asset["path"].rsplit("/", 1)[-1] if pypi else asset["path"]
            if self.compare(label, asset, remote.get(key)) == "missing":
                self.attempt(label, self.upload_pypi if pypi else self.upload_file, name, asset)

    def upload_file(self, name, asset):
        content = self.snapshot.read(asset)
        if self.dry_run:
            return self.plan(f"upload {name}/{asset['path']}")
        self.client.call("PUT", f"{self.client.target}/repository/{name}/{encoded_path(asset['path'])}",
                         content, {"Content-Type": asset["media_type"]})
        self.counts["uploaded"] += 1

    def upload_pypi(self, name, asset):
        content = self.snapshot.read(asset)
        if self.dry_run:
            return self.plan(f"upload {name}/{asset['path']} through the PyPI upload API")
        body, media_type = multipart_body(pypi_fields(asset, content), "content",
                                          asset["path"].rsplit("/", 1)[-1], content)
        self.client.call("POST", f"{self.client.target}/repository/{name}/", body, {"Content-Type": media_type})
        self.counts["uploaded"] += 1

    def upload_npm(self, name, assets, remote):
        packages = {}
        for asset in assets:
            packages.setdefault(asset["component"], {})[asset["version"]] = asset
        for package, versions in sorted(packages.items()):
            tags = npm_dist_tags(self.snapshot, name, package, versions)
            present = set()
            for version in sorted(versions, key=version_key):
                label = f"{name}/{package}@{version}"
                state = self.compare(label, versions[version], remote.get(versions[version]["path"]))
                if state == "missing":
                    try:
                        self.publish_npm(name, package, version, versions[version], tags)
                    except (MigrationError, ValueError) as exc:
                        self.fail(f"{label}: {exc}")
                        continue
                if state != "conflict":
                    present.add(version)
            tags = {tag: version for tag, version in tags.items() if version in present}
            if tags:
                self.attempt(f"{name}/{package}", self.sync_npm_tags, name, package, tags)

    def publish_npm(self, name, package, version, asset, tags):
        content = self.snapshot.read(asset)
        if self.dry_run:
            return self.plan(f"publish {name}/{package}@{version}")
        base = f"{self.client.target}/repository/{name}"
        attachment = f"{package}-{version}.tgz"
        manifest = {**asset["metadata"], "name": package, "version": version, "_id": f"{package}@{version}",
                    "dist": npm_dist(asset, f"{base}/{package}/-/{attachment}")}
        # Nexus rejects a publish without dist-tags; the final tag sync restores the source tags.
        document = {"_id": package, "name": package, "versions": {version: manifest},
                    "dist-tags": {tag: value for tag, value in tags.items() if value == version} or {"latest": version},
                    "_attachments": {attachment: {"content_type": "application/octet-stream", "length": len(content),
                                                  "data": base64.b64encode(content).decode()}}}
        for key in ("description", "readme"):
            if isinstance(manifest.get(key), str):
                document[key] = manifest[key]
        self.client.call("PUT", f"{base}/{urllib.parse.quote(package, safe='@')}", json_bytes(document),
                         {"Content-Type": "application/json"})
        self.counts["uploaded"] += 1

    def sync_npm_tags(self, name, package, tags):
        url = f"{self.client.target}/repository/{name}/-/package/{urllib.parse.quote(package, safe='@')}/dist-tags"
        if name in self.planned:
            current = {}
        else:
            status, _, data = self.client.request("GET", url)
            if status == 404 and self.dry_run:
                current = {}
            elif status != 200:
                raise MigrationError(http_failure("GET", url, status, data))
            else:
                current = json_object(json.loads(data))
        for tag, version in sorted(tags.items()):
            if current.get(tag) != version:
                if self.dry_run:
                    self.plan(f"set npm dist-tag {name}/{package}:{tag} to {version}", count=False)
                    continue
                self.client.call("PUT", f"{url}/{urllib.parse.quote(tag, safe='')}", json.dumps(version).encode(),
                                 {"Content-Type": "application/json"})

    def upload_docker(self, name, assets):
        api = registry_api(self.endpoints[name]) if name in self.endpoints else (
            f"{self.client.target}/repository/{name}/v2")
        images = docker_images(self.snapshot, name, assets)
        if images and name not in self.planned:
            ping = registry_root(api)[0]
            deadline = time.monotonic() + 30
            while True:
                try:
                    status, _, data = self.client.registry("GET", ping)
                    break
                except MigrationError as exc:
                    # A newly created Docker connector opens its port asynchronously.
                    if time.monotonic() > deadline:
                        return self.fail(f"{name}: {exc}")
                    time.sleep(0.5)
            if status != 200:
                return self.fail(f"{name}: {http_failure('GET', ping, status, data)}")
        for image, entry in sorted(images.items()):
            for problem in entry["problems"]:
                self.fail(problem)
            if entry["problems"]:
                continue
            for digest, asset in sorted(entry["blobs"].items()):
                self.attempt(f"{name}/{image}@{digest}", self.push_blob, api, image, digest, asset)
            references = [(digest, digest) for digest in entry["order"]] + sorted(entry["tags"].items())
            for reference, digest in references:
                label = f"{name}/{image}{'@' if reference == digest else ':'}{reference}"
                self.attempt(label, self.push_manifest, api, image, reference, entry["manifests"][digest])

    def push_blob(self, api, image, digest, asset):
        url = f"{api}/{image}/blobs/{digest}"
        status, headers, data = self.planned_head(api, url)
        if status == 200:
            if headers.get("Content-Length") != str(asset["size"]):
                raise MigrationError("conflict: the target holds this digest with a different size")
            self.counts["skipped"] += 1
            return
        if status != 404:
            raise MigrationError(http_failure("HEAD", url, status, data))
        content = self.snapshot.read(asset)
        if self.dry_run:
            return self.plan(f"push blob {image}@{digest} to {api}")
        status, headers, data = self.client.registry("POST", f"{api}/{image}/blobs/uploads/", b"")
        if status != 202 or not headers.get("Location"):
            raise MigrationError(http_failure("POST", f"{api}/{image}/blobs/uploads/", status, data))
        location = registry_location(api, headers["Location"])
        upload = location + ("&" if "?" in location else "?") + urllib.parse.urlencode({"digest": digest})
        status, headers, data = self.client.registry("PUT", upload, content,
                                                     {"Content-Type": "application/octet-stream"})
        if status != 201:
            raise MigrationError(http_failure("PUT", upload, status, data))
        status, headers, _ = self.client.registry("HEAD", url)
        if status != 200 or headers.get("Docker-Content-Digest", digest) != digest or (
                headers.get("Content-Length") != str(asset["size"])):
            raise MigrationError(f"uploaded blob did not verify (HEAD returned HTTP {status})")
        self.counts["uploaded"] += 1

    def push_manifest(self, api, image, reference, asset):
        url = f"{api}/{image}/manifests/{reference}"
        digest = "sha256:" + asset["checksum"]["sha256"]
        accept = {"Accept": ", ".join(sorted(OCI_MEDIA_TYPES))}
        status, headers, data = self.planned_head(api, url, accept)
        if status == 200:
            current = headers.get("Docker-Content-Digest", reference if OCI_DIGEST.fullmatch(reference) else None)
            if current != digest:
                raise MigrationError(f"conflict: the target tag points to {current or 'other content'}")
            self.counts["skipped"] += 1
            return
        if status != 404:
            raise MigrationError(http_failure("HEAD", url, status, data))
        content = self.snapshot.read(asset)
        if self.dry_run:
            return self.plan(f"push manifest {image}{'@' if reference == digest else ':'}{reference} to {api}")
        status, headers, data = self.client.registry("PUT", url, content, {"Content-Type": asset["media_type"]})
        if status != 201:
            raise MigrationError(http_failure("PUT", url, status, data))
        if headers.get("Docker-Content-Digest", digest) != digest:
            raise MigrationError(f"the target stored the manifest as {headers['Docker-Content-Digest']}")
        self.counts["uploaded"] += 1

    def planned_head(self, api, url, headers=None):
        # A repository a dry run would create cannot be inspected yet, so everything in it is missing.
        if self.dry_run and any(api == (registry_api(self.endpoints[name]) if name in self.endpoints else
                                        f"{self.client.target}/repository/{name}/v2") for name in self.planned):
            return 404, {}, b""
        return self.client.registry("HEAD", url, headers=headers)


def http_url(value, option):
    parts = urllib.parse.urlsplit(value)
    try:
        parts.port
    except ValueError as exc:
        raise ValueError(f"{option} has an invalid port: {value}") from exc
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.query or parts.fragment:
        raise ValueError(f"{option} must be an http(s) URL without credentials or query: {value}")
    return value.rstrip("/")


def migrate_main(argv):
    parser = argparse.ArgumentParser(
        prog=f"{Path(sys.argv[0]).name} migrate",
        description="Create the repositories of a Nexus test server SQLite snapshot in a running Nexus Repository "
                    "and upload their content through the REST, format and Docker registry APIs.")
    parser.add_argument("--db-file", type=Path, required=True, help="existing SQLite persistence file")
    parser.add_argument("--target", required=True, help="Nexus base URL, for example http://nexus:8081")
    parser.add_argument("--username", default="admin", help="Nexus user (default: %(default)s)")
    parser.add_argument("--password-file", type=Path, required=True, help="file holding the Nexus password")
    parser.add_argument("--repository", action="extend", nargs="+", default=[], metavar="NAME",
                        help="migrate only these repositories (default: all)")
    parser.add_argument("--docker-endpoint", action="extend", nargs="+", default=[], metavar="REPO=URL",
                        help="registry URL of a Docker repository: a connector such as http://nexus:8082 (set as "
                             "its HTTP port when the repository is created) or a URL containing /v2 "
                             "(default: TARGET/repository/REPO)")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="read the source and the target and report what would change, without writing")
    args = parser.parse_args(argv)
    if not args.username or ":" in args.username or not 0 < args.timeout <= 3600:
        parser.error("--username must be non-empty without a colon and --timeout must be in (0, 3600]")
    try:
        target = http_url(args.target, "--target")
        endpoints = {}
        for item in args.docker_endpoint:
            name, separator, url = item.partition("=")
            if not separator:
                raise ValueError(f"--docker-endpoint expects REPO=URL: {item}")
            endpoints[name] = http_url(url, "--docker-endpoint")
        password = args.password_file.read_text(encoding="utf-8").rstrip("\r\n")
        if not password:
            raise ValueError(f"The password file is empty: {args.password_file}")
        snapshot = Snapshot(args.db_file)
        try:
            for name in [*args.repository, *endpoints]:
                if name not in snapshot.repositories:
                    raise ValueError(f"Repository not found in the source: {name}")
                if name in endpoints and snapshot.repositories[name]["format"] != "docker":
                    raise ValueError(f"--docker-endpoint names a non-Docker repository: {name}")
            names = [name for name in creation_order(snapshot.repositories)
                     if not args.repository or name in args.repository]
            client = NexusClient(target, args.username, password, args.timeout)
            migration = Migration(snapshot, client, endpoints, args.dry_run)
            repositories = migration.run(names)
        finally:
            snapshot.close()
    except (OSError, ValueError, sqlite3.Error, MigrationError) as exc:
        parser.exit(1, f"Nexus migrate failed: {exc}\n")
    counts, failures = migration.counts, migration.failures
    verb, suffix = ("Would migrate", " (dry run, nothing was written)") if args.dry_run else ("Migrated", "")
    print(f"{verb} {repositories} repositories ({counts['created']} created, {counts['existing']} already present) "
          f"and {counts['uploaded']} assets ({counts['skipped']} already present) to {target}{suffix}"
          + (f"; {len(failures)} failures" if failures else ""))
    return 1 if failures else 0


# Import into a new persistence file


MAVEN_LOCAL_FILES = re.compile(r"_remote\.repositories|_maven\.repositories|resolver-status\.properties"
                               r"|maven-metadata-.+\.xml|.+\.lastUpdated")
OCI_IMAGE_NAME = "io.containerd.image.name"
CONFIG_KEYS = ("online", "storage", "maven", "proxy", "group")


def file_modified(path):
    milliseconds = round(path.stat().st_mtime_ns / 1_000_000)
    stamp = datetime.fromtimestamp(milliseconds // 1000, timezone.utc).replace(microsecond=milliseconds % 1000 * 1000)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_timestamp(value):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None
    stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def content_files(folder):
    """Regular files below a folder as (relative POSIX path, Path), in path order."""
    found = []
    for directory, subdirectories, filenames in os.walk(folder):
        subdirectories.sort()
        for filename in filenames:
            path = Path(directory, filename)
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"{path} is not a regular file")
            found.append((path.relative_to(folder).as_posix(), path))
    return sorted(found)


def pypi_version(filename, content):
    try:
        version = str(package_metadata(filename, content).get("Version") or "")
    except RequestError:
        version = ""
    if not version:
        stem = filename.removesuffix(".tar.gz").removesuffix(".whl").removesuffix(".zip")
        version = stem.split("-")[1] if filename.endswith(".whl") else stem.rpartition("-")[2]
    return version


class SimpleLinks(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.requires = {}

    def handle_starttag(self, tag, attributes):
        attributes = dict(attributes)
        if tag == "a" and attributes.get("href"):
            filename = urllib.parse.unquote(urllib.parse.urlsplit(attributes["href"]).path.rsplit("/", 1)[-1])
            self.requires[filename] = attributes.get("data-requires-python")


def oci_tag(reference):
    """The tag part of an org.opencontainers.image.ref.name annotation."""
    tail = reference.rsplit("/", 1)[-1]
    tag = tail.rsplit(":", 1)[-1] if ":" in tail else reference
    if not OCI_TAG.fullmatch(tag):
        raise ValueError(f"Invalid image tag in the OCI layout: {reference!r}")
    return tag


def oci_image_name(reference):
    """The repository path of a full image reference such as docker.io/library/app:1.0."""
    name = reference.split("@", 1)[0]
    head, _, tail = name.rpartition("/")
    name = (head + "/" if head else "") + tail.split(":", 1)[0]
    first, _, rest = name.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        name = rest
    return name.lower()


class OciLayout:
    """The manifests, tags and blobs of an OCI image layout, checked against their digests and sizes."""

    def __init__(self, folder):
        self.folder = folder
        try:
            layout = json_object(json.loads((folder / "oci-layout").read_bytes()))
            index = json_object(json.loads((folder / "index.json").read_bytes()))
            descriptors = index["manifests"]
            if not isinstance(layout.get("imageLayoutVersion"), str) or not isinstance(descriptors, list):
                raise ValueError("missing imageLayoutVersion or manifests")
        except (OSError, ValueError, KeyError, RequestError) as exc:
            raise ValueError(f"{folder} is not an OCI image layout: {exc}") from exc
        self.blobs = {}
        for relative, path in content_files(folder / "blobs") if (folder / "blobs").is_dir() else ():
            algorithm, _, encoded = relative.partition("/")
            if algorithm != "sha256" or not OCI_DIGEST.fullmatch("sha256:" + encoded):
                raise ValueError(f"{path}: only sha256 blobs are supported")
            self.blobs["sha256:" + encoded] = path
        self.manifests, self.tags, self.names, self.untagged = {}, {}, set(), []
        for descriptor in descriptors:
            digest = self.visit(descriptor)
            annotations = descriptor.get("annotations") or {}
            if OCI_REF_NAME not in annotations:
                self.untagged.append(digest)
            if isinstance(annotations.get(OCI_IMAGE_NAME), str):
                self.names.add(oci_image_name(annotations[OCI_IMAGE_NAME]))
            reference = annotations.get(OCI_REF_NAME)
            if isinstance(reference, str):
                tag = oci_tag(reference)
                if self.tags.get(tag, digest) != digest:
                    raise ValueError(f"{folder}: tag {tag} names two different manifests")
                self.tags[tag] = digest

    def digests(self):
        return list(self.blobs)

    def modified(self, digest):
        return file_modified(self.blobs[digest])

    def check(self, descriptor):
        digest, size = descriptor.get("digest"), descriptor.get("size")
        if not isinstance(digest, str) or digest not in self.blobs or type(size) is not int:
            raise ValueError(f"{self.folder}: missing or invalid blob for descriptor {digest!r}")
        if self.blobs[digest].stat().st_size != size:
            raise ValueError(f"{self.folder}: {digest} does not match its descriptor size")
        return digest

    def read(self, digest):
        content = self.blobs[digest].read_bytes()
        if "sha256:" + hashlib.sha256(content).hexdigest() != digest:
            raise ValueError(f"{self.blobs[digest]} does not match its digest")
        return content

    def visit(self, descriptor, depth=0):
        if not isinstance(descriptor, dict) or depth > 8:
            raise ValueError(f"{self.folder}: invalid or deeply nested manifest descriptor")
        digest = self.check(descriptor)
        if digest in self.manifests:
            return digest
        content = self.read(digest)
        media_type = manifest_media_type(content) or descriptor.get("mediaType")
        if media_type not in OCI_MEDIA_TYPES:
            raise ValueError(f"{self.folder}: {digest} has unsupported media type {media_type!r}")
        try:
            document = json_object(json.loads(content))
            index = media_type.endswith((".index.v1+json", ".list.v2+json"))
            children = document["manifests"] if index else [document["config"], *document["layers"]]
        except (ValueError, KeyError, TypeError, RequestError) as exc:
            raise ValueError(f"{self.folder}: {digest} is not a valid image manifest") from exc
        self.manifests[digest] = (content, media_type)
        for child in children:
            self.visit(child, depth + 1) if index else self.check(child)
        return digest


class Importer:
    def __init__(self, store):
        self.store = store
        self.counts = {"repositories": 0, "assets": 0, "images": 0, "skipped": 0}

    def create(self, repositories, name):
        repository = repositories[name]
        config = dict(repository["config"])
        if "group" in config:
            members = config["group"].get("memberNames", [])
            missing = [member for member in members if member not in repositories]
            if missing:
                warn(f"{name}: dropping group members that are not imported: {', '.join(missing)}")
            config["group"] = {**config["group"], "memberNames": [member for member in members if member not in missing]}
        try:
            self.store.save_repository(name, repository["format"], repository["kind"], config)
        except RequestError as exc:
            raise ValueError(f"{name}: {exc}") from exc
        self.counts["repositories"] += 1
        return self.store.repository(name)

    def put(self, repository, path, content, media_type, coordinates=None, metadata=None, modified=None, checked=False):
        try:
            # Imported content was accepted by its source, so only plain directory imports apply upload rules.
            self.store.put(repository, path, content, media_type, coordinates, metadata, not checked, modified)
        except RequestError as exc:
            raise ValueError(f"{repository['name']}/{path}: {exc}") from exc
        self.counts["assets"] += 1

    def npm_tags(self, repository, package, tags, versions):
        for tag, version in sorted(tags.items()):
            if NAME.fullmatch(tag) and version in versions:
                self.store.db.execute("INSERT OR REPLACE INTO npm_tags VALUES (?, ?, ?, ?)",
                                      (repository["name"], package, tag, version))
            else:
                warn(f"{repository['name']}/{package}: skipping dist-tag {tag!r} for a version that is not imported")

    def image(self, repository, image, layout, overrides=None, checked=False):
        if not OCI_IMAGE.fullmatch(image) or len(image) > 255:
            raise ValueError(f"Invalid image name: {image!r}")
        overrides, base = overrides or {}, f"v2/{image}"
        for digest in sorted(layout.digests()):
            if digest not in layout.manifests:
                self.put(repository, f"{base}/blobs/{digest}", layout.read(digest), "application/octet-stream",
                         ("", image, digest), modified=layout.modified(digest), checked=checked)
        references = [(digest, digest) for digest in sorted(layout.manifests)] + sorted(layout.tags.items())
        for reference, digest in references:
            content, media_type = layout.manifests[digest]
            path = f"{base}/manifests/{reference}"
            record = overrides.get(path, {})
            self.put(repository, path, content, record.get("mediaType", media_type), ("", image, reference),
                     modified=record.get("modified") or layout.modified(digest), checked=checked)
        self.counts["images"] += 1


def import_export(importer, folder, names):
    try:
        requests = json.loads((folder / "repositories.json").read_bytes())
        repositories = {}
        for request in requests:
            body = json_object(request["body"])
            config = request.get("config")
            if config is None:
                # Exports written before the settings were recorded carry only the REST create request.
                config = {key: body[key] for key in CONFIG_KEYS if key in body}
            repositories[request["name"]] = {"name": request["name"], "format": request["format"],
                                             "kind": request["type"], "config": json_object(config),
                                             "assets": json_object(request.get("assets", {}))}
    except (OSError, ValueError, KeyError, TypeError, RequestError) as exc:
        raise ValueError(f"{folder} is not a Nexus test server export: {exc}") from exc
    unknown = [name for name in names if name not in repositories]
    if unknown:
        raise ValueError(f"Repository not found in the export: {', '.join(unknown)}")
    if names:
        repositories = {name: repositories[name] for name in repositories if name in names}
    for name in creation_order(repositories):
        repository = importer.create(repositories, name)
        content = folder / "content" / name
        if repositories[name]["kind"] == "group" or not content.is_dir():
            continue
        overrides = repositories[name]["assets"]
        if repository["format"] == "docker":
            for relative, path in content_files(content):
                if relative == "oci-layout" or relative.endswith("/oci-layout"):
                    image = relative.removesuffix("oci-layout").rstrip("/")
                    importer.image(repository, image, OciLayout(path.parent), overrides)
        elif repository["format"] == "npm":
            import_npm_export(importer, repository, content, overrides)
        else:
            import_files(importer, repository, content, overrides)


def import_files(importer, repository, folder, overrides):
    format_name = repository["format"]
    files = content_files(folder)
    requires = {}
    if format_name == "pypi":
        for relative, path in files:
            if relative.startswith("simple/") and relative.count("/") == 2:
                parser = SimpleLinks()
                parser.feed(path.read_text(encoding="utf-8"))
                requires.update(parser.requires)
    for relative, path in files:
        if format_name == "pypi" and relative.startswith("simple/"):
            continue
        content, record = path.read_bytes(), overrides.get(relative, {})
        coordinates = metadata = None
        parts = relative.split("/")
        if format_name == "pypi" and len(parts) == 3 and parts[0] == "packages":
            coordinates = ("", parts[1], pypi_version(parts[2], content))
            metadata = {"requires-python": requires.get(parts[2])}
        importer.put(repository, relative, content, record.get("mediaType", default_media_type(format_name, relative)),
                     coordinates, metadata, record.get("modified") or file_modified(path))


def import_npm_export(importer, repository, folder, overrides):
    files = dict(content_files(folder))
    tarballs = {}
    for relative, path in files.items():
        package = relative.removesuffix("/packument.json")
        if package == relative or "/-/" in relative or not NPM_NAME.fullmatch(package):
            continue
        try:
            document = json_object(json.loads(path.read_bytes()))
            versions = {version: json_object(item) for version, item in json_object(document["versions"]).items()}
            tags = json_object(document.get("dist-tags", {}))
            for version, item in versions.items():
                tarball = json_object(item["dist"])["tarball"]
                if tarball not in files:
                    raise ValueError(f"missing tarball {tarball}")
                metadata = {key: value for key, value in item.items() if key not in ("_id", "dist")}
                tarballs[tarball] = (package, version, metadata)
        except (OSError, ValueError, KeyError, TypeError, RequestError) as exc:
            raise ValueError(f"{repository['name']}/{relative}: invalid packument: {exc}") from exc
        tarballs[relative] = (package, None, tags)
    for relative, path in files.items():
        entry = tarballs.get(relative)
        if entry is not None and entry[1] is None:
            continue
        content, record = path.read_bytes(), overrides.get(relative, {})
        try:
            if entry is None:
                manifest = npm_manifest(content)
                entry = (manifest.get("name"), manifest.get("version"), manifest)
            package, version, metadata = entry
            path_name, _, media, coordinates, _ = Handler.npm_upload(package, version, content, metadata)
        except RequestError as exc:
            raise ValueError(f"{repository['name']}/{relative}: {exc}") from exc
        if path_name != relative:
            raise ValueError(f"{repository['name']}/{relative}: expected the npm tarball at {path_name}")
        importer.put(repository, relative, content, record.get("mediaType", media), coordinates, metadata,
                     record.get("modified") or file_modified(path))
    for relative, (package, version, tags) in tarballs.items():
        if version is None:
            versions = {entry[1] for entry in tarballs.values() if entry[0] == package and entry[1] is not None}
            # Export adds the latest tag the server derives when none is stored.
            if versions and tags.get("latest") == max(versions, key=version_key):
                tags = {tag: value for tag, value in tags.items() if tag != "latest"}
            importer.npm_tags(repository, package, tags, versions)


def plain_repository(importer, name, format_name):
    """An existing default repository, or a new hosted one."""
    try:
        repository = importer.store.repository(name)
    except RequestError:
        importer.store.save_repository(name, format_name, "hosted", {})
        importer.counts["repositories"] += 1
        return importer.store.repository(name)
    if repository["format"] != format_name or repository["kind"] != "hosted":
        raise ValueError(f"{name} is a {repository['format']}/{repository['kind']} repository, "
                         f"not a hosted {format_name} repository")
    return repository


def import_maven_directory(importer, folder, name):
    repository = plain_repository(importer, name, "maven2")
    for relative, path in content_files(folder):
        if MAVEN_LOCAL_FILES.fullmatch(relative.rsplit("/", 1)[-1]):
            importer.counts["skipped"] += 1
            continue
        importer.put(repository, relative, path.read_bytes(), mime_type(relative), modified=file_modified(path),
                     checked=True)


def import_oci_directory(importer, folder, name, image):
    repository = plain_repository(importer, name, "docker")
    layout = OciLayout(folder)
    tag = None
    if image is not None and ":" in image.rsplit("/", 1)[-1]:
        image, tag = image.rsplit(":", 1)
        if not OCI_TAG.fullmatch(tag) or len(set(layout.untagged)) != 1:
            raise ValueError("--image NAME:TAG needs a valid tag and an OCI layout with exactly one untagged image")
        layout.tags[tag] = layout.untagged[0]
    if image is None:
        if len(layout.names) > 1:
            raise ValueError(f"The OCI layout names several images ({', '.join(sorted(layout.names))}); pass --image")
        image = next(iter(layout.names), folder.resolve().name.lower())
    importer.image(repository, image, layout, checked=True)


class RemoteNexus:
    """Read-only access to a running Nexus Repository for import."""

    def __init__(self, client, endpoints):
        self.client, self.endpoints = client, endpoints

    def get(self, url, headers=None, registry=False):
        status, response_headers, data = (self.client.registry if registry else self.client.request)("GET", url,
                                                                                                    headers=headers)
        if status != 200:
            raise MigrationError(http_failure("GET", url, status, data))
        return response_headers, data

    def json(self, url):
        try:
            return json.loads(self.get(url)[1])
        except ValueError as exc:
            raise MigrationError(f"GET {url} returned invalid JSON") from exc

    def repositories(self, names):
        target = self.client.target
        listing = self.json(f"{target}{REST}repositories")
        if not isinstance(listing, list) or not all(isinstance(item, dict) for item in listing):
            raise MigrationError(f"{target} did not return a Nexus repository list")
        available = {item.get("name"): item for item in listing}
        unknown = [name for name in names if name not in available]
        if unknown:
            raise ValueError(f"Repository not found in the source: {', '.join(unknown)}")
        repositories, skipped = {}, 0
        for name, item in available.items():
            format_name, kind = item.get("format"), item.get("type")
            if names and name not in names:
                continue
            problem = None
            if format_name not in FORMATS:
                problem = f"the {format_name} format is not supported by the test server"
            elif kind not in {"hosted", "proxy", "group"}:
                problem = f"the {kind} repository type is not supported"
            elif kind == "proxy" and format_name not in {"raw", "maven2"}:
                problem = "the test server proxies only raw and Maven repositories"
            if problem:
                if names:
                    raise ValueError(f"{name}: {problem}")
                warn(f"skipping repository {name}: {problem}")
                skipped += 1
                continue
            url = f"{target}{REST}repositories/{API_FORMATS.get(format_name, format_name)}/{kind}/{name}"
            status, _, data = self.client.request("GET", url)
            details = json.loads(data) if status == 200 else {"online": item.get("online", True), **(
                item.get("attributes") or {})}
            config = {key: details[key] for key in CONFIG_KEYS
                      if key in details and (key == "online" or isinstance(details[key], dict))}
            if format_name != "maven2":
                config.pop("maven", None)
            repositories[name] = {"name": name, "format": format_name, "kind": kind, "config": config}
        return repositories, skipped

    def download(self, name, item):
        path = item["path"].lstrip("/")
        url = f"{self.client.target}/repository/{name}/{encoded_path(path)}"
        headers, content = self.get(url)
        checksum = item.get("checksum") or {}
        checked = [method for method in HASHES if isinstance(checksum.get(method), str)]
        if not checked:
            raise ValueError(f"{name}/{path}: the source lists no checksum for this asset")
        for method in checked:
            if hashlib.new(method, content, usedforsecurity=False).hexdigest() != checksum[method].lower():
                raise ValueError(f"{name}/{path}: the downloaded content does not match its {method} checksum")
        media_type = item.get("contentType") or headers.get_content_type()
        return path, content, media_type, iso_timestamp(item.get("lastModified") or item.get("blobCreated"))

    def import_files(self, importer, repository, items):
        name, format_name = repository["name"], repository["format"]
        packages = {}
        for item in items:
            path = item["path"].lstrip("/")
            if format_name == "npm" and "/-/" not in path or format_name == "pypi" and not path.startswith("packages/"):
                importer.counts["skipped"] += 1  # Package metadata and indexes the server generates itself.
                continue
            path, content, media_type, modified = self.download(name, item)
            if format_name == "pypi":
                filename = path.rsplit("/", 1)[-1]
                try:
                    metadata = package_metadata(filename, content)
                    record = Handler.pypi_upload((filename, content, media_type), metadata.get("Name", ""),
                                                 metadata.get("Version", ""), metadata.get("Requires-Python"))
                except RequestError as exc:
                    raise ValueError(f"{name}/{path}: {exc}") from exc
                importer.put(repository, *record[:2], media_type, *record[3:], modified)
            elif format_name == "npm":
                try:
                    manifest = npm_manifest(content)
                except RequestError as exc:
                    raise ValueError(f"{name}/{path}: {exc}") from exc
                package = manifest.get("name")
                if package not in packages:
                    url = f"{self.client.target}/repository/{name}/{urllib.parse.quote(str(package), safe='@')}"
                    status, _, data = self.client.request("GET", url)
                    packages[package] = json_object(json.loads(data)) if status == 200 else {}
                document = packages[package]
                version_document = (document.get("versions") or {}).get(manifest.get("version"))
                metadata = {key: value for key, value in (version_document or manifest).items()
                            if key not in ("_id", "dist")}
                try:
                    record = Handler.npm_upload(package, manifest.get("version"), content, metadata)
                except RequestError as exc:
                    raise ValueError(f"{name}/{path}: {exc}") from exc
                importer.put(repository, *record[:2], media_type, *record[3:], modified)
            else:
                importer.put(repository, path, content, media_type, modified=modified)
        for package, document in packages.items():
            versions = {row["version"] for row in importer.store.listing_rows([name]) if row["name"] == package}
            importer.npm_tags(repository, package, document.get("dist-tags") or {}, versions)

    def import_docker(self, importer, repository, items):
        name = repository["name"]
        api = registry_api(self.endpoints[name]) if name in self.endpoints else (
            f"{self.client.target}/repository/{name}/v2")
        self.get(registry_root(api)[0], registry=True)
        references, modified, stamps = {}, {}, {}
        for item in items:
            parts = item["path"].lstrip("/").removeprefix("v2/").rsplit("/", 2)
            stamp = iso_timestamp(item.get("lastModified"))
            if len(parts) == 3 and parts[1] == "blobs" and OCI_DIGEST.fullmatch(parts[2]) and stamp:
                stamps[parts[2]] = stamp
            if len(parts) == 3 and parts[1] == "manifests" and OCI_IMAGE.fullmatch(parts[0]):
                references.setdefault(parts[0], set())
                if OCI_DIGEST.fullmatch(parts[2]):
                    references[parts[0]].add(parts[2])
                if stamp:
                    modified[f"v2/{parts[0]}/manifests/{parts[2]}"] = {"modified": stamp}
        status, headers, data = self.client.registry("GET", f"{api}/_catalog")
        if status == 200:
            for image in self.paged(api, f"{api}/_catalog", "repositories", status, headers, data):
                references.setdefault(image, set())
        for image in sorted(references):
            url = f"{api}/{image}/tags/list"
            status, headers, data = self.client.registry("GET", url)
            if status == 404:
                continue
            if status != 200:
                raise MigrationError(http_failure("GET", url, status, data))
            layout = RemoteImage(self, api, image, stamps)
            for tag in self.paged(api, url, "tags", status, headers, data):
                layout.tags[tag] = layout.manifest(tag)
            for digest in sorted(references[image]):
                layout.manifest(digest)
            if layout.manifests:
                importer.image(repository, image, layout, modified)

    def paged(self, api, url, key, status, headers, data):
        values = []
        for _ in range(10_000):
            try:
                values += [value for value in json_object(json.loads(data)).get(key) or [] if isinstance(value, str)]
            except (ValueError, RequestError) as exc:
                raise MigrationError(f"GET {url} returned invalid JSON") from exc
            link = re.match(r'\s*<([^>]+)>\s*;\s*rel="?next"?', headers.get("Link", ""))
            if not link:
                return values
            url = registry_location(api, link[1])
            status, headers, data = self.client.registry("GET", url)
            if status != 200:
                raise MigrationError(http_failure("GET", url, status, data))
        raise MigrationError(f"{url}: too many result pages")


class RemoteImage:
    """An image read through the registry API, shaped like an OciLayout for Importer.image."""

    def __init__(self, remote, api, image, stamps):
        self.remote, self.api, self.image, self.stamps = remote, api, image, stamps
        self.manifests, self.tags, self.contents = {}, {}, {}

    def manifest(self, reference, depth=0):
        url = f"{self.api}/{self.image}/manifests/{reference}"
        headers, content = self.remote.get(url, {"Accept": ", ".join(sorted(OCI_MEDIA_TYPES))}, registry=True)
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        if headers.get("Docker-Content-Digest", digest) != digest or OCI_DIGEST.fullmatch(reference) and (
                reference != digest):
            raise ValueError(f"{url} does not match its digest")
        media_type = headers.get_content_type()
        media_type = media_type if media_type in OCI_MEDIA_TYPES else manifest_media_type(content)
        if media_type is None or depth > 8:
            raise ValueError(f"{url}: unsupported manifest media type {headers.get_content_type()}")
        if digest not in self.manifests:
            try:
                document = json_object(json.loads(content))
                index = media_type.endswith((".index.v1+json", ".list.v2+json"))
                children = document["manifests"] if index else [document["config"], *document["layers"]]
                children = [(item["digest"], item["size"]) for item in children]
            except (ValueError, KeyError, TypeError, RequestError) as exc:
                raise ValueError(f"{url} is not a valid image manifest") from exc
            self.manifests[digest] = (content, media_type)
            self.contents[digest] = content
            for child, size in children:
                if index:
                    self.manifest(child, depth + 1)
                elif child not in self.contents:
                    _, blob = self.remote.get(f"{self.api}/{self.image}/blobs/{child}", registry=True)
                    if "sha256:" + hashlib.sha256(blob).hexdigest() != child or len(blob) != size:
                        raise ValueError(f"{self.image}@{child}: the downloaded blob does not match its descriptor")
                    self.contents[child] = blob
        return digest

    def digests(self):
        return list(self.contents)

    def modified(self, digest):
        return self.stamps.get(digest)

    def read(self, digest):
        return self.contents[digest]


def import_main(argv):
    parser = argparse.ArgumentParser(
        prog=f"{Path(sys.argv[0]).name} import",
        description="Build a new Nexus test server SQLite file from an export directory, an OCI image layout, "
                    "a local Maven repository, or a running Nexus Repository.")
    parser.add_argument("--db-file", type=Path, required=True, help="new SQLite persistence file to create")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--export-dir", type=Path, help="directory written by the export command")
    source.add_argument("--oci-layout", type=Path, help="OCI image layout directory, for example from skopeo or buildx")
    source.add_argument("--maven-dir", type=Path, help="local Maven repository, for example ~/.m2/repository")
    source.add_argument("--source", help="Nexus base URL to read, for example http://nexus:8081")
    parser.add_argument("--repository", action="extend", nargs="+", default=[], metavar="NAME",
                        help="repositories to import (default: all); the target for --oci-layout and --maven-dir")
    parser.add_argument("--image", metavar="NAME[:TAG]",
                        help="image name for --oci-layout (default: from the layout or its directory); a tag "
                             "applies to the layout's one untagged image")
    parser.add_argument("--username", default="admin", help="Nexus user for --source (default: %(default)s)")
    parser.add_argument("--password-file", type=Path, help="file holding the Nexus password for --source")
    parser.add_argument("--docker-endpoint", action="extend", nargs="+", default=[], metavar="REPO=URL",
                        help="registry URL of a Docker repository of --source (default: SOURCE/repository/REPO)")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds (default: %(default)s)")
    args = parser.parse_args(argv)
    plain = args.oci_layout or args.maven_dir
    if plain and len(args.repository) != 1:
        parser.error("--oci-layout and --maven-dir need exactly one --repository")
    if args.image and not args.oci_layout:
        parser.error("--image applies only to --oci-layout")
    if (args.docker_endpoint or args.password_file) and not args.source:
        parser.error("--password-file and --docker-endpoint apply only to --source")
    if args.source and not args.password_file:
        parser.error("--source needs --password-file")
    if not args.username or ":" in args.username or not 0 < args.timeout <= 3600:
        parser.error("--username must be non-empty without a colon and --timeout must be in (0, 3600]")
    destination = args.db_file.expanduser().absolute()
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"The persistence file already exists: {destination}")
        if plain and not plain.is_dir():
            raise ValueError(f"Not a directory: {plain}")
        if args.export_dir and not args.export_dir.is_dir():
            raise ValueError(f"Not a directory: {args.export_dir}")
        if args.source:
            source_url = http_url(args.source, "--source")
            endpoints = {}
            for item in args.docker_endpoint:
                name, separator, url = item.partition("=")
                if not separator:
                    raise ValueError(f"--docker-endpoint expects REPO=URL: {item}")
                endpoints[name] = http_url(url, "--docker-endpoint")
            password = args.password_file.read_text(encoding="utf-8").rstrip("\r\n")
            if not password:
                raise ValueError(f"The password file is empty: {args.password_file}")
            remote = RemoteNexus(NexusClient(source_url, args.username, password, args.timeout), endpoints)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".nexus-import-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "import.sqlite3"
            store = Store(staged)
            try:
                importer = Importer(store)
                with store.lock, store.db:
                    if args.export_dir or args.source:
                        store.reset(defaults=False)
                    if args.export_dir:
                        import_export(importer, args.export_dir, args.repository)
                    elif args.maven_dir:
                        import_maven_directory(importer, args.maven_dir, args.repository[0])
                    elif args.oci_layout:
                        import_oci_directory(importer, args.oci_layout, args.repository[0], args.image)
                    else:
                        repositories, importer.counts["skipped"] = remote.repositories(args.repository)
                        for name in creation_order(repositories):
                            repository = importer.create(repositories, name)
                            if repository["kind"] != "hosted":
                                continue
                            items = list_assets(remote.client, name)
                            if items and not repositories[name]["config"].get("online", True):
                                warn(f"{name}: skipping {len(items)} assets of an offline repository")
                                importer.counts["skipped"] += len(items)
                                continue
                            if repository["format"] == "docker":
                                remote.import_docker(importer, repository, items)
                            else:
                                remote.import_files(importer, repository, items)
                store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                store.db.close()
            publish_file(staged, destination)
    except (OSError, ValueError, sqlite3.Error, MigrationError, RequestError) as exc:
        parser.exit(1, f"Nexus import failed: {exc}\n")
    counts = importer.counts
    skipped = f"; {counts['skipped']} items skipped" if counts["skipped"] else ""
    print(f"Imported {counts['repositories']} repositories, {counts['assets']} assets and {counts['images']} "
          f"Docker images into {destination}{skipped}")
    return 0


def publish_file(staged, destination):
    """Publish a complete file without replacing an existing one."""
    try:
        os.link(staged, destination)
    except FileExistsError:
        raise
    except OSError:
        # File systems without hard links: a checked rename.
        if os.path.lexists(destination):
            raise FileExistsError(f"The persistence file already exists: {destination}") from None
        os.rename(staged, destination)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["export"]:
        return export_main(argv[1:])
    if argv[:1] == ["migrate"]:
        return migrate_main(argv[1:])
    if argv[:1] == ["import"]:
        return import_main(argv[1:])
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=8081, help="listen port, 0 chooses a free port (default: %(default)s)")
    parser.add_argument("--db-file", type=Path, help="SQLite file for repository configuration and artifacts")
    parser.add_argument("--username", default="admin", help="test account username (default: %(default)s)")
    credentials = parser.add_mutually_exclusive_group()
    credentials.add_argument("--password", action="store_true", help="prompt for the test account password")
    credentials.add_argument("--password-file", type=Path, help="read the test account password from a file")
    parser.add_argument("--tls-cert", type=Path, help="PEM TLS certificate")
    parser.add_argument("--tls-key", type=Path, help="PEM TLS private key")
    parser.add_argument("--max-body", type=int, default=64 * 1024 * 1024, help="maximum request/asset bytes")
    parser.add_argument("--upstream-timeout", type=float, default=10.0, help="proxy connection timeout, in seconds")
    parser.add_argument("--verbose", action="store_true", help="log individual HTTP requests")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535 or args.max_body <= 0 or not 0 < args.upstream_timeout <= 300:
        parser.error("--port, --max-body or --upstream-timeout is out of range")
    if not args.username or ":" in args.username:
        parser.error("--username must be non-empty and must not contain a colon")
    if args.tls_key and not args.tls_cert:
        parser.error("--tls-key requires --tls-cert")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    try:
        password = os.environ.get("NEXUS_TEST_PASSWORD")
        if args.password:
            password = getpass.getpass("Nexus test password: ")
        elif args.password_file:
            password = args.password_file.read_text(encoding="utf-8").rstrip("\r\n")
        if password == "":
            parser.error("The test account password must not be empty")
        store = Store(args.db_file or ":memory:")
        try:
            with Server((args.host, args.port), Handler) as server:
                server.store = store
                server.username, server.password = args.username, password
                server.max_body, server.upstream_timeout = args.max_body, args.upstream_timeout
                server.scheme = "https" if args.tls_cert else "http"
                if args.tls_cert:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    context.load_cert_chain(args.tls_cert, args.tls_key)
                    server.socket = context.wrap_socket(server.socket, server_side=True)
                server.origin = f"{server.scheme}://{args.host}:{server.server_port}"
                print(f"Nexus test server listening on {server.origin}", flush=True)
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
        finally:
            with store.lock:
                store.db.close()
    except (OSError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
