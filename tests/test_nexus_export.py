"""Verify Nexus test server exports and migrations through copied server files."""

import base64
import contextlib
import hashlib
import http.client
import io
import json
import os
import pathlib
import queue
import re
import runpy
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
import urllib.parse
import zipfile
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "nexus_server.py"
PASSWORD = "migrate secret"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"


def environment():
    return {key: value for key, value in os.environ.items()
            if not key.endswith("_TEST_PASSWORD") and not key.lower().endswith("_proxy")}


@contextlib.contextmanager
def running_server(script, *args):
    command = [sys.executable, "-I", "-S", "-B", "-u", str(script), "--port", "0", *args]
    with subprocess.Popen(command, cwd=script.parent, env=environment(), stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True) as process:
        lines = queue.Queue()
        reader = threading.Thread(target=lambda: [lines.put(line) for line in process.stdout] + [lines.put(None)],
                                  daemon=True)
        reader.start()
        try:
            line = lines.get(timeout=10)
            match = re.search(r"127\.0\.0\.1:(\d+)", line or "")
            if not match:
                raise AssertionError(f"Server did not start: {line}")
            yield int(match[1])
        finally:
            process.terminate()
            process.wait(timeout=10)
            reader.join(timeout=5)


class Client:
    def __init__(self, port, password=None):
        self.port = port
        self.authorization = "Basic " + base64.b64encode(f"admin:{password}".encode()).decode() if password else None

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = dict(headers or {})
        if self.authorization:
            headers["Authorization"] = self.authorization
        if isinstance(body, (dict, list)):
            body, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        try:
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            return response.status, {key.lower(): value for key, value in response.getheaders()}, response.read()
        finally:
            connection.close()

    def ok(self, method, path, body=None, headers=None):
        status, response_headers, data = self.request(method, path, body, headers)
        if status >= 300:
            raise AssertionError(f"{method} {path} returned {status}: {data[:200]!r}")
        return response_headers, data


def multipart(fields, files):
    boundary = "jjap-" + os.urandom(8).hex()
    parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
             for key, value in fields.items()]
    parts += [f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"; filename="{filename}"\r\n'
              f"Content-Type: application/octet-stream\r\n\r\n".encode() + content + b"\r\n"
              for key, (filename, content) in files.items()]
    return b"".join(parts) + f"--{boundary}--\r\n".encode(), {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def tar_gz(members):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            info.size, info.mtime = len(content), 1700000000
            archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def wheel(name, version, requires):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{name}-{version}.dist-info/METADATA",
                         f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\nSummary: Demo\n"
                         f"Requires-Python: {requires}\n")
        archive.writestr("demo_pkg/__init__.py", "VALUE = 1\n")
    return buffer.getvalue()


def digest(content):
    return "sha256:" + hashlib.sha256(content).hexdigest()


def descriptor(media_type, content, **extra):
    return {"mediaType": media_type, "digest": digest(content), "size": len(content), **extra}


def build_fixture(port):
    """Populate a running test server through its HTTP APIs and return what the migration must preserve."""
    client = Client(port)
    rest = "/service/rest/v1/repositories"
    raw = {"docs/readme.txt": b"hello nexus\n", "bin/data.bin": bytes(range(256)) * 4,
           "space dir/ümlaut #1.txt": "ü\n".encode()}
    for path, content in raw.items():
        client.ok("PUT", "/repository/raw-hosted/" + urllib.parse.quote(path), content)
    for name, settings in (("raw-locked", {"writePolicy": "DENY"}), ("raw-offline", {"writePolicy": "ALLOW_ONCE"})):
        client.ok("POST", rest + "/raw/hosted", {"name": name, "online": True, "storage": {"writePolicy": "ALLOW"}})
        client.ok("PUT", f"/repository/{name}/kept.txt", name.encode())
        update = {"name": name, "online": name != "raw-offline", "storage": settings}
        client.ok("PUT", f"{rest}/raw/hosted/{name}", update)
    upstream = f"http://127.0.0.1:{port}/repository/raw-hosted/"
    client.ok("POST", rest + "/raw/proxy", {"name": "raw-proxy", "online": True,
                                            "proxy": {"remoteUrl": upstream, "contentMaxAge": -1}})
    client.ok("GET", "/repository/raw-proxy/docs/readme.txt")
    client.ok("POST", rest + "/raw/group", {"name": "raw-all", "group": {
        "memberNames": ["raw-proxy", "raw-hosted", "raw-locked"]}})
    client.ok("POST", rest + "/raw/group", {"name": "raw-top", "group": {"memberNames": ["raw-offline", "raw-all"]}})

    pom = (b'<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
           b"<groupId>org.jjap</groupId><artifactId>demo</artifactId><version>1.0</version></project>")
    jar = b"PK\x03\x04 demo jar"
    metadata = (b'<?xml version="1.0" encoding="UTF-8"?><metadata><groupId>org.jjap</groupId>'
                b"<artifactId>demo</artifactId><versioning><release>1.0</release><versions><version>1.0</version>"
                b"</versions><lastUpdated>20200101000000</lastUpdated></versioning></metadata>")
    maven = {"maven-releases": {"org/jjap/demo/1.0/demo-1.0.pom": pom, "org/jjap/demo/1.0/demo-1.0.jar": jar,
                                "org/jjap/demo/1.0/demo-1.0.jar.sha1": hashlib.sha1(jar).hexdigest().encode(),
                                "org/jjap/demo/maven-metadata.xml": metadata},
             "maven-snapshots": {"org/jjap/demo/1.1-SNAPSHOT/demo-1.1-20240101.000000-1.jar": b"snapshot jar"}}
    for repository, files in maven.items():
        for path, content in files.items():
            client.ok("PUT", f"/repository/{repository}/{path}", content)
    body, headers = multipart({"maven2.groupId": "org.jjap", "maven2.artifactId": "lib", "maven2.version": "2.0",
                               "maven2.asset1.extension": "jar", "maven2.generate-pom": "true"},
                              {"maven2.asset1": ("lib.jar", b"library jar")})
    client.ok("POST", "/service/rest/v1/components?repository=maven-releases", body, headers)
    maven["maven-releases"]["org/jjap/lib/2.0/lib-2.0.jar"] = b"library jar"

    built = wheel("Demo_Pkg", "1.0", ">=3.8")
    body, headers = multipart({":action": "file_upload", "name": "Demo_Pkg", "version": "1.0",
                               "requires_python": ">=3.8", "sha256_digest": hashlib.sha256(built).hexdigest()},
                              {"content": ("Demo_Pkg-1.0-py3-none-any.whl", built)})
    client.ok("POST", "/repository/pypi-hosted/", body, headers)
    source = tar_gz({"demo_pkg-1.1/PKG-INFO": b"Metadata-Version: 2.1\nName: demo-pkg\nVersion: 1.1\n"})
    body, headers = multipart({":action": "file_upload", "name": "demo-pkg", "version": "1.1"},
                              {"content": ("demo_pkg-1.1.tar.gz", source)})
    client.ok("POST", "/repository/pypi-hosted/", body, headers)
    later = wheel("demo_pkg", "1.2", ">=3.10")
    body, headers = multipart({}, {"pypi.asset": ("demo_pkg-1.2-py3-none-any.whl", later)})
    client.ok("POST", "/service/rest/v1/components?repository=pypi-hosted", body, headers)
    pypi = {"Demo_Pkg-1.0-py3-none-any.whl": (built, ">=3.8"), "demo_pkg-1.1.tar.gz": (source, None),
            "demo_pkg-1.2-py3-none-any.whl": (later, ">=3.10")}

    npm = {}
    for version, tag in (("1.0.0", "latest"), ("2.0.0-beta.1", "beta")):
        manifest = {"name": "@jjap/widget", "version": version, "description": "Widget", "custom": {"keep": [1, 2]}}
        tarball = tar_gz({"package/package.json": json.dumps(manifest).encode()})
        npm[("@jjap/widget", version)] = tarball
        client.ok("PUT", "/repository/npm-hosted/@jjap%2fwidget", {
            "_id": "@jjap/widget", "name": "@jjap/widget", "dist-tags": {tag: version},
            "versions": {version: {**manifest, "dist": {"tarball": "http://elsewhere/@jjap/widget/-/@jjap/widget-"
                                                                    f"{version}.tgz"}}},
            "_attachments": {f"@jjap/widget-{version}.tgz": {"data": base64.b64encode(tarball).decode(),
                                                            "length": len(tarball)}}})
    client.ok("PUT", "/repository/npm-hosted/-/package/@jjap%2fwidget/dist-tags/stable", b'"1.0.0"',
              {"Content-Type": "application/json"})
    plain = tar_gz({"package/package.json": b'{"name":"plain","version":"0.1.0","main":"index.js"}'})
    npm[("plain", "0.1.0")] = plain
    body, headers = multipart({}, {"npm.asset": ("plain-0.1.0.tgz", plain)})
    client.ok("POST", "/service/rest/v1/components?repository=npm-hosted", body, headers)

    image = "/repository/docker-hosted/v2/jjap/app"
    blobs = {}

    def push(content):
        headers, _ = client.ok("POST", image + "/blobs/uploads/")
        client.ok("PUT", f"{headers['location']}?digest={digest(content)}", content)
        blobs[digest(content)] = content
        return content

    layer = push(tar_gz({"app/run.sh": b"#!/bin/sh\necho hi\n"}))
    manifests, tags = {}, {}

    def manifest(platform, media_type=OCI_MANIFEST):
        config = push(json.dumps({"architecture": platform, "os": "linux",
                                  "rootfs": {"type": "layers", "diff_ids": [digest(layer)]}}).encode())
        layer_type = "application/vnd.oci.image.layer.v1.tar+gzip"
        content = json.dumps({"schemaVersion": 2, "mediaType": media_type,
                              "config": descriptor("application/vnd.oci.image.config.v1+json", config),
                              "layers": [descriptor(layer_type, layer)]}).encode()
        client.ok("PUT", f"{image}/manifests/{digest(content)}", content, {"Content-Type": media_type})
        manifests[digest(content)] = (content, media_type)
        return content

    amd64, arm64, untagged = manifest("amd64"), manifest("arm64"), manifest("s390x", DOCKER_MANIFEST)
    index = json.dumps({"schemaVersion": 2, "mediaType": OCI_INDEX, "manifests": [
        descriptor(OCI_MANIFEST, amd64, platform={"architecture": "amd64", "os": "linux"}),
        descriptor(OCI_MANIFEST, arm64, platform={"architecture": "arm64", "os": "linux"})]}).encode()
    manifests[digest(index)] = (index, OCI_INDEX)
    for tag, content, media_type in (("1.0", amd64, OCI_MANIFEST), ("latest", amd64, OCI_MANIFEST),
                                     ("multi", index, OCI_INDEX)):
        client.ok("PUT", f"{image}/manifests/{tag}", content, {"Content-Type": media_type})
        tags[tag] = digest(content)
    client.ok("POST", rest + "/docker/group", {"name": "docker-all", "group": {"memberNames": ["docker-hosted"]}})
    return {"raw": raw, "maven": maven, "pypi": pypi, "npm": npm, "blobs": blobs, "manifests": manifests,
            "tags": tags, "untagged": digest(untagged), "child": digest(arm64), "index": digest(index)}


class NexusExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-nexus-fixture-")
        cls.addClassCleanup(temporary.cleanup)
        cls.fixture_directory = pathlib.Path(temporary.name)
        cls.fixture_script = cls.fixture_directory / SCRIPT.name
        shutil.copyfile(SCRIPT, cls.fixture_script)
        cls.fixture_source = cls.fixture_directory / "fixture.sqlite3"
        with running_server(cls.fixture_script, "--db-file", str(cls.fixture_source)) as port:
            cls.fixture = build_fixture(port)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-nexus-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = pathlib.Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.source = self.directory / "origin #?.sqlite3"
        for suffix in ("", "-wal", "-shm"):
            if pathlib.Path(f"{self.fixture_source}{suffix}").exists():
                shutil.copyfile(f"{self.fixture_source}{suffix}", f"{self.source}{suffix}")

    def run_script(self, *args, check=True):
        result = subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)],
                                cwd=self.directory, env=environment(), text=True, capture_output=True, timeout=60)
        if check and result.returncode:
            raise AssertionError(f"{args[0]} failed: {result.stdout}{result.stderr}")
        return result

    def source_digest(self):
        return [hashlib.sha256(pathlib.Path(f"{self.source}{suffix}").read_bytes()).hexdigest()
                for suffix in ("", "-wal") if pathlib.Path(f"{self.source}{suffix}").exists()]

    def test_export_writes_rest_requests_and_native_layouts(self):
        before = self.source_digest()
        output = self.directory / "export"
        result = self.run_script("export", "--db-file", self.source, "--out", output, "--include-proxy-cache")
        self.assertIn("Docker images", result.stdout)
        self.assertEqual(self.source_digest(), before)
        requests = json.loads((output / "repositories.json").read_text(encoding="utf-8"))
        by_name = {request["name"]: request for request in requests}
        kinds = [request["type"] for request in requests]
        self.assertEqual(kinds, sorted(kinds, key=["hosted", "proxy", "group"].index))
        names = [request["name"] for request in requests]
        self.assertLess(names.index("raw-all"), names.index("raw-top"))
        self.assertEqual(by_name["raw-top"]["body"]["group"]["memberNames"], ["raw-offline", "raw-all"])
        self.assertEqual(by_name["raw-all"]["body"]["group"]["memberNames"], ["raw-proxy", "raw-hosted", "raw-locked"])
        self.assertEqual(by_name["maven-releases"]["path"], "/service/rest/v1/repositories/maven/hosted")
        self.assertEqual(by_name["maven-releases"]["body"]["maven"]["versionPolicy"], "RELEASE")
        self.assertEqual(by_name["raw-locked"]["body"]["storage"]["writePolicy"], "DENY")
        self.assertIs(by_name["raw-offline"]["body"]["online"], False)
        self.assertEqual(by_name["raw-proxy"]["body"]["proxy"]["contentMaxAge"], -1)
        self.assertIn("negativeCache", by_name["raw-proxy"]["body"])
        self.assertEqual(by_name["docker-all"]["body"]["docker"], {"v1Enabled": False, "forceBasicAuth": True})
        self.assertNotIn("writePolicy", by_name["raw-all"]["body"]["storage"])

        content = output / "content"
        for path, data in self.fixture["raw"].items():
            self.assertEqual((content / "raw-hosted" / path).read_bytes(), data)
        self.assertEqual((content / "raw-proxy/docs/readme.txt").read_bytes(), self.fixture["raw"]["docs/readme.txt"])
        for repository, files in self.fixture["maven"].items():
            for path, data in files.items():
                self.assertEqual((content / repository / path).read_bytes(), data)
        self.assertTrue((content / "maven-releases/org/jjap/lib/2.0/lib-2.0.pom").is_file())
        for filename, (data, _) in self.fixture["pypi"].items():
            self.assertEqual((content / "pypi-hosted/packages/demo-pkg" / filename).read_bytes(), data)
        index = (content / "pypi-hosted/simple/demo-pkg/index.html").read_text(encoding="utf-8")
        wheel_hash = hashlib.sha256(self.fixture["pypi"]["Demo_Pkg-1.0-py3-none-any.whl"][0]).hexdigest()
        self.assertIn(f'href="../../packages/demo-pkg/Demo_Pkg-1.0-py3-none-any.whl#sha256={wheel_hash}"', index)
        self.assertIn('data-requires-python="&gt;=3.8"', index)
        for (package, version), data in self.fixture["npm"].items():
            unscoped = package.rsplit("/", 1)[-1]
            self.assertEqual((content / "npm-hosted" / package / "-" / f"{unscoped}-{version}.tgz").read_bytes(), data)
        packument = json.loads((content / "npm-hosted/@jjap/widget/packument.json").read_text(encoding="utf-8"))
        self.assertEqual(packument["dist-tags"], {"beta": "2.0.0-beta.1", "latest": "1.0.0", "stable": "1.0.0"})
        self.assertEqual(packument["versions"]["1.0.0"]["custom"], {"keep": [1, 2]})
        dist = packument["versions"]["1.0.0"]["dist"]
        data = self.fixture["npm"][("@jjap/widget", "1.0.0")]
        self.assertEqual(dist["shasum"], hashlib.sha1(data).hexdigest())
        self.assertEqual(dist["integrity"], "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode())
        self.assertEqual((content / "npm-hosted" / dist["tarball"]).read_bytes(), data)
        plain = json.loads((content / "npm-hosted/plain/packument.json").read_text(encoding="utf-8"))
        self.assertEqual((plain["dist-tags"], plain["versions"]["0.1.0"]["main"]), ({"latest": "0.1.0"}, "index.js"))
        self.assertFalse((content / "raw-all").exists())

        layout = content / "docker-hosted/jjap/app"
        self.assertEqual(json.loads((layout / "oci-layout").read_text(encoding="utf-8")), {"imageLayoutVersion": "1.0.0"})
        index = json.loads((layout / "index.json").read_text(encoding="utf-8"))
        self.assertEqual((index["schemaVersion"], index["mediaType"]), (2, OCI_INDEX))
        blobs = {path.name: path.read_bytes() for path in (layout / "blobs/sha256").iterdir()}
        for name, data in blobs.items():
            self.assertEqual(hashlib.sha256(data).hexdigest(), name)
        expected = {**self.fixture["blobs"], **{key: value[0] for key, value in self.fixture["manifests"].items()}}
        self.assertEqual({"sha256:" + name for name in blobs}, set(expected))

        def check(item):
            self.assertEqual(len(blobs[item["digest"].removeprefix("sha256:")]), item["size"])

        refs = {}
        for item in index["manifests"]:
            check(item)
            refs.setdefault(item.get("annotations", {}).get("org.opencontainers.image.ref.name"), []).append(item)
            document = json.loads(blobs[item["digest"].removeprefix("sha256:")])
            self.assertEqual(item["mediaType"], self.fixture["manifests"][item["digest"]][1])
            for child in document.get("manifests", []):
                check(child)
                for layer in [json.loads(blobs[child["digest"][7:]])["config"], *json.loads(blobs[child["digest"][7:]])["layers"]]:
                    check(layer)
            for layer in [document["config"], *document["layers"]] if "config" in document else []:
                check(layer)
        self.assertEqual({tag: items[0]["digest"] for tag, items in refs.items() if tag}, self.fixture["tags"])
        self.assertEqual([item["digest"] for item in refs[None]], [self.fixture["untagged"]])
        self.assertNotIn(self.fixture["child"], [item["digest"] for item in index["manifests"]])
        self.assertEqual(list(self.directory.glob(".nexus-export-*")), [])

        default = self.directory / "default"
        result = self.run_script("export", "--db-file", self.source, "--out", default)
        self.assertIn("1 cached proxy assets not exported", result.stdout)
        self.assertFalse((default / "content/raw-proxy").exists())

    def test_migrate_round_trip_into_second_server(self):
        before = self.source_digest()
        password_file = self.directory / "password.txt"
        password_file.write_text(PASSWORD + "\n", encoding="utf-8")
        target_db = self.directory / "target.sqlite3"
        with running_server(self.script, "--db-file", target_db, "--password-file", password_file) as port:
            target = f"http://127.0.0.1:{port}"
            client = Client(port, PASSWORD)
            result = self.run_script("migrate", "--db-file", self.source, "--target", target,
                                     "--password-file", password_file, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("1 cached proxy assets are not migrated", result.stderr)
            self.assertRegex(result.stdout, r"Migrated 13 repositories \(6 created, 7 already present\)")
            self.assertEqual(self.source_digest(), before)
            self.check_target(client)

            result = self.run_script("migrate", "--db-file", self.source, "--target", target + "/",
                                     "--password-file", password_file, "--docker-endpoint",
                                     f"docker-hosted={target}/v2/docker-hosted", check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertRegex(result.stdout, r"and 0 assets \(\d+ already present\)")

            client.ok("PUT", "/repository/raw-hosted/docs/readme.txt", b"changed on the target")
            client.ok("PUT", "/repository/docker-hosted/v2/jjap/app/manifests/latest",
                      self.fixture["manifests"][self.fixture["untagged"]][0], {"Content-Type": DOCKER_MANIFEST})
            result = self.run_script("migrate", "--db-file", self.source, "--target", target, "--password-file",
                                     password_file, "--repository", "raw-hosted", "docker-hosted", check=False)
            self.assertEqual(result.returncode, 1)
            self.assertIn("raw-hosted/docs/readme.txt: conflict", result.stderr)
            self.assertIn("docker-hosted/jjap/app:latest: conflict", result.stderr)
            self.assertIn("2 failures", result.stdout)
            self.assertEqual(client.ok("GET", "/repository/raw-hosted/docs/readme.txt")[1], b"changed on the target")

            password_file.write_text("wrong", encoding="utf-8")
            result = self.run_script("migrate", "--db-file", self.source, "--target", target,
                                     "--password-file", password_file, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertIn("Nexus migrate failed:", result.stderr)
            self.assertIn("HTTP 401", result.stderr)

    def check_target(self, client):
        fixture = self.fixture
        for path, data in fixture["raw"].items():
            self.assertEqual(client.ok("GET", "/repository/raw-hosted/" + urllib.parse.quote(path))[1], data)
        self.assertEqual(client.ok("GET", "/repository/raw-locked/kept.txt")[1], b"raw-locked")
        self.assertEqual(client.request("GET", "/repository/raw-offline/kept.txt")[0], 503)
        offline_assets = json.loads(client.ok("GET", "/service/rest/v1/assets?repository=raw-offline")[1])["items"]
        self.assertEqual([(item["path"], item["checksum"]["sha1"]) for item in offline_assets],
                         [("kept.txt", hashlib.sha1(b"raw-offline").hexdigest())])
        for repository, files in fixture["maven"].items():
            for path, data in files.items():
                self.assertEqual(client.ok("GET", f"/repository/{repository}/{path}")[1], data)
        rest = "/service/rest/v1/repositories"
        locked = json.loads(client.ok("GET", f"{rest}/raw/hosted/raw-locked")[1])
        offline = json.loads(client.ok("GET", f"{rest}/raw/hosted/raw-offline")[1])
        self.assertEqual((locked["storage"]["writePolicy"], offline["online"]), ("DENY", False))
        self.assertEqual(offline["storage"]["writePolicy"], "ALLOW_ONCE")
        self.assertEqual(client.request("PUT", "/repository/raw-locked/new.txt", b"x")[0], 403)
        top = json.loads(client.ok("GET", f"{rest}/raw/group/raw-top")[1])
        self.assertEqual(top["group"]["memberNames"], ["raw-offline", "raw-all"])
        group = json.loads(client.ok("GET", f"{rest}/raw/group/raw-all")[1])
        self.assertEqual(group["group"]["memberNames"], ["raw-proxy", "raw-hosted", "raw-locked"])
        proxy = json.loads(client.ok("GET", f"{rest}/raw/proxy/raw-proxy")[1])
        self.assertEqual(proxy["proxy"]["contentMaxAge"], -1)
        listing = json.loads(client.ok("GET", "/service/rest/v1/assets?repository=raw-proxy")[1])
        self.assertEqual(listing["items"], [])
        json.loads(client.ok("GET", f"{rest}/docker/group/docker-all")[1])

        _, data = client.ok("GET", "/repository/pypi-hosted/simple/demo-pkg/",
                            headers={"Accept": "application/vnd.pypi.simple.v1+json"})
        files = {item["filename"]: item for item in json.loads(data)["files"]}
        self.assertEqual(set(files), set(fixture["pypi"]))
        for filename, (content, requires) in fixture["pypi"].items():
            self.assertEqual(files[filename]["requires-python"], requires)
            self.assertEqual(files[filename]["hashes"]["sha256"], hashlib.sha256(content).hexdigest())
            self.assertEqual(client.ok("GET", urllib.parse.urlsplit(files[filename]["url"]).path)[1], content)

        widget = json.loads(client.ok("GET", "/repository/npm-hosted/@jjap%2fwidget")[1])
        self.assertEqual(widget["dist-tags"], {"beta": "2.0.0-beta.1", "latest": "1.0.0", "stable": "1.0.0"})
        self.assertEqual(widget["versions"]["1.0.0"]["custom"], {"keep": [1, 2]})
        plain = json.loads(client.ok("GET", "/repository/npm-hosted/plain")[1])
        self.assertEqual((plain["dist-tags"], plain["versions"]["0.1.0"]["main"]), ({"latest": "0.1.0"}, "index.js"))
        for (package, version), content in fixture["npm"].items():
            document = widget if package == "@jjap/widget" else plain
            tarball = urllib.parse.urlsplit(document["versions"][version]["dist"]["tarball"]).path
            self.assertEqual(client.ok("GET", tarball)[1], content)

        image = "/repository/docker-hosted/v2/jjap/app"
        for reference, (content, media_type) in fixture["manifests"].items():
            headers, data = client.ok("GET", f"{image}/manifests/{reference}")
            self.assertEqual((data, headers["content-type"], headers["docker-content-digest"]),
                             (content, media_type, reference))
        for tag, reference in fixture["tags"].items():
            headers, data = client.ok("GET", f"{image}/manifests/{tag}")
            self.assertEqual((data, headers["docker-content-digest"]), (fixture["manifests"][reference][0], reference))
        for reference, content in fixture["blobs"].items():
            self.assertEqual(client.ok("GET", f"{image}/blobs/{reference}")[1], content)
        tags = json.loads(client.ok("GET", f"{image}/tags/list")[1])["tags"]
        self.assertEqual(tags, sorted(fixture["tags"]))

    def test_export_reads_one_snapshot_while_the_server_writes(self):
        exporter = runpy.run_path(str(self.script))["export_nexus"]
        original = exporter.__globals__["export_files"]
        writer = sqlite3.connect(self.source)
        self.addCleanup(writer.close)
        replacement = b"written during the export"
        checksum = json.dumps({method: hashlib.new(method, replacement).hexdigest()
                               for method in ("md5", "sha1", "sha256", "sha512")})

        def write_concurrently(snapshot, repository, folder):
            if repository["name"] == "maven-releases":
                with writer:
                    writer.execute("UPDATE assets SET content = ?, size = ?, checksum = ? WHERE path = ?",
                                   (replacement, len(replacement), checksum, "docs/readme.txt"))
                    writer.execute("DELETE FROM repositories WHERE name = 'raw-locked'")
            return original(snapshot, repository, folder)

        output = self.directory / "snapshot"
        with patch.dict(exporter.__globals__, {"export_files": write_concurrently}):
            counts = exporter(self.source, output)
        self.assertEqual((output / "content/raw-hosted/docs/readme.txt").read_bytes(), b"hello nexus\n")
        self.assertEqual((output / "content/raw-locked/kept.txt").read_bytes(), b"raw-locked")
        self.assertEqual(counts["repositories"], 13)
        self.assertIsNone(writer.execute("SELECT 1 FROM repositories WHERE name = 'raw-locked'").fetchone())

    def test_corrupted_content_fails_export_and_is_reported_by_migrate(self):
        with contextlib.closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute("UPDATE assets SET content = ? WHERE path = ?", (b"tampered\n", "bin/data.bin"))
            layer = next(key for key in self.fixture["blobs"] if len(self.fixture["blobs"][key]) > 100)
            connection.execute("UPDATE assets SET content = zeroblob(length(content)) WHERE path LIKE ?",
                               (f"%/blobs/{layer}",))
        output = self.directory / "broken"
        result = self.run_script("export", "--db-file", self.source, "--out", output, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertRegex(result.stderr, r"^Nexus export failed: .* does not match its recorded size and checksums\n$")
        self.assertFalse(output.exists())
        self.assertEqual(list(self.directory.glob(".nexus-export-*")), [])

        password_file = self.directory / "password.txt"
        password_file.write_text(PASSWORD, encoding="utf-8")
        with running_server(self.script, "--password-file", password_file) as port:
            result = self.run_script("migrate", "--db-file", self.source, "--target", f"http://127.0.0.1:{port}",
                                     "--password-file", password_file, "--repository", "raw-hosted",
                                     "docker-hosted", check=False)
            client = Client(port, PASSWORD)
            self.assertEqual(client.ok("GET", "/repository/raw-hosted/docs/readme.txt")[1], b"hello nexus\n")
            self.assertEqual(client.request("GET", "/repository/raw-hosted/bin/data.bin")[0], 404)
        self.assertEqual(result.returncode, 1)
        self.assertIn("error: raw-hosted/bin/data.bin: raw-hosted/bin/data.bin does not match", result.stderr)
        self.assertIn(f"/blobs/{layer} does not match", result.stderr)

    def test_rejects_existing_destination_and_invalid_sources(self):
        output = self.directory / "used"
        output.mkdir()
        (output / "marker.txt").write_text("preserve", encoding="utf-8")
        result = self.run_script("export", "--db-file", self.source, "--out", output, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((output / "marker.txt").read_text(encoding="utf-8"), "preserve")

        other = self.directory / "other.sqlite3"
        with contextlib.closing(sqlite3.connect(other)) as connection, connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        garbage = self.directory / "garbage.sqlite3"
        garbage.write_bytes(b"not a database at all" * 10)
        for source in (other, garbage, self.directory / "missing.sqlite3"):
            target = self.directory / "never"
            result = self.run_script("export", "--db-file", source, "--out", target, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("Nexus export failed: "), result.stderr)
            self.assertFalse(target.exists())
            password = self.directory / "password.txt"
            password.write_text("x", encoding="utf-8")
            result = self.run_script("migrate", "--db-file", source, "--target", "http://127.0.0.1:9",
                                     "--password-file", password, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertTrue(result.stderr.startswith("Nexus migrate failed: "), result.stderr)
        result = self.run_script("export", "--db-file", other, "--out", target, check=False)
        self.assertIn("Not a Nexus test server SQLite persistence file", result.stderr)
        self.assertFalse((self.directory / "missing.sqlite3").exists())
        self.assertEqual(list(self.directory.glob(".nexus-export-*")), [])
        result = self.run_script("migrate", "--db-file", self.source, "--target", "http://127.0.0.1:9",
                                 "--password-file", self.directory / "password.txt", "--repository", "nope",
                                 check=False)
        self.assertIn("Repository not found in the source: nope", result.stderr)


if __name__ == "__main__":
    unittest.main()
