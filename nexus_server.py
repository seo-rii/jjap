#!/usr/bin/env python3
"""Single-file Nexus Repository-compatible test server for Python 3.10+.

Run: python3 nexus_server.py --port 8081
REST: http://127.0.0.1:8081/service/rest/v1/repositories
Raw upload: curl -T example.bin http://127.0.0.1:8081/repository/raw-hosted/example.bin
OCI registry: 127.0.0.1:8081/docker-hosted/example/image:tag
Use --db-file test.sqlite3 to persist repository configuration and artifacts.
Set NEXUS_TEST_PASSWORD or --password-file to require a single test account.

Protocol references:
https://help.sonatype.com/en/api-reference.html
https://help.sonatype.com/en/components-api.html
https://packaging.python.org/en/latest/specifications/simple-repository-api/
https://docs.pypi.org/api/upload/
https://docs.npmjs.com/cli/commands/npm-publish/
https://github.com/opencontainers/distribution-spec/blob/main/spec.md
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
import io
import json
import logging
import mimetypes
import os
import re
import secrets
import sqlite3
import ssl
import tarfile
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
                            return BytesParser(policy=email.policy.default).parse(file, headersonly=True)
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
                self.db.executemany("INSERT INTO repositories VALUES (?, ?, ?, ?)", [
                    ("raw-hosted", "raw", "hosted", "{}"),
                    ("maven-releases", "maven2", "hosted", '{"maven":{"versionPolicy":"RELEASE"}}'),
                    ("maven-snapshots", "maven2", "hosted", '{"maven":{"versionPolicy":"SNAPSHOT"}}'),
                    ("maven-public", "maven2", "group",
                     '{"group":{"memberNames":["maven-releases","maven-snapshots"]}}'),
                    ("pypi-hosted", "pypi", "hosted", "{}"),
                    ("npm-hosted", "npm", "hosted", "{}"),
                    ("docker-hosted", "docker", "hosted", "{}"),
                ])
                self.db.execute("INSERT INTO settings VALUES ('initialized', '1')")

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

    def put(self, repository, path, content, media_type, coordinates=None, metadata=None, proxy=False):
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
              json.dumps(digests), iso_date(), json.dumps(metadata or {})))

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

    def pypi_upload(self, upload, name, version, requires):
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

    def npm_upload(self, name, version, content, metadata):
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


def main(argv=None):
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
