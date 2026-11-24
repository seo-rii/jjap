"""Verify Nexus test server import, migrate --dry-run and reset through copied server files."""

import base64
import contextlib
import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import urllib.parse

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_nexus_export as fixtures  # noqa: E402


PASSWORD = fixtures.PASSWORD
REST = "/service/rest/v1"
API_FORMATS = {"maven2": "maven"}


def state(port, password=None, assets=True, aliases=()):
    """Everything a client can observe about the server, with its address (and aliases) replaced by HOST."""
    client = fixtures.Client(port, password)
    host = f"127.0.0.1:{port}"

    def get(path, headers=None):
        status, _, data = client.request("GET", path, headers=headers)
        text = data.decode()
        for other in (host, *(f"127.0.0.1:{alias}" for alias in aliases)):
            text = text.replace(other, "HOST")
        return json.loads(text) if status == 200 else status

    result = {"repositories": get(f"{REST}/repositories")}
    for repository in result["repositories"]:
        name, kind = repository["name"], repository["type"]
        api = API_FORMATS.get(repository["format"], repository["format"])
        result[name] = {"settings": get(f"{REST}/repositories/{api}/{kind}/{name}")}
        if not assets:
            continue
        items, token = [], None
        while True:
            query = urllib.parse.urlencode({"repository": name, **({"continuationToken": token} if token else {})})
            listing = get(f"{REST}/assets?{query}")
            items += listing["items"]
            token = listing["continuationToken"]
            if not token:
                break
        for item in items:
            del item["id"]
            path = urllib.parse.urlsplit(item["downloadUrl"].replace("HOST", host)).path
            status, _, content = client.request("GET", path)
            item["content"] = hashlib.sha256(content).hexdigest() if status == 200 else status
        result[name]["assets"] = sorted(items, key=lambda item: item["path"])
        components = get(f"{REST}/components?repository={name}")["items"]
        result[name]["components"] = sorted(([item["group"], item["name"], item["version"],
                                              sorted(asset["path"] for asset in item["assets"])]
                                             for item in components), key=json.dumps)
    if "npm-hosted" in result:
        for package in ("@jjap%2fwidget", "plain"):
            result["npm-hosted"][package] = get(f"/repository/npm-hosted/{package}")
    if "pypi-hosted" in result:
        result["pypi-hosted"]["simple"] = get("/repository/pypi-hosted/simple/demo-pkg/",
                                              {"Accept": "application/vnd.pypi.simple.v1+json"})
    if "docker-hosted" in result:
        result["docker-hosted"]["tags"] = get("/repository/docker-hosted/v2/jjap/app/tags/list")
    return result


class NexusImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-nexus-import-fixture-")
        cls.addClassCleanup(temporary.cleanup)
        directory = pathlib.Path(temporary.name)
        script = directory / fixtures.SCRIPT.name
        shutil.copyfile(fixtures.SCRIPT, script)
        cls.fixture_source = directory / "fixture.sqlite3"
        with fixtures.running_server(script, "--db-file", str(cls.fixture_source)) as port:
            cls.fixture = fixtures.build_fixture(port)
            cls.expected = state(port)
            cls.fixture_port = port

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-nexus-import-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = pathlib.Path(temporary.name)
        self.script = self.directory / fixtures.SCRIPT.name
        shutil.copyfile(fixtures.SCRIPT, self.script)
        self.source = self.directory / "origin #?.sqlite3"
        for suffix in ("", "-wal", "-shm"):
            if pathlib.Path(f"{self.fixture_source}{suffix}").exists():
                shutil.copyfile(f"{self.fixture_source}{suffix}", f"{self.source}{suffix}")
        self.password_file = self.directory / "password.txt"
        self.password_file.write_text(PASSWORD + "\n", encoding="utf-8")

    def run_script(self, *args, check=True):
        result = subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), *map(str, args)],
                                cwd=self.directory, env=fixtures.environment(), text=True, capture_output=True,
                                timeout=120)
        if check and result.returncode:
            raise AssertionError(f"{args[0]} failed: {result.stdout}{result.stderr}")
        return result

    def serve(self, db_file, password=False):
        options = ["--password-file", self.password_file] if password else []
        return fixtures.running_server(self.script, "--db-file", str(db_file), *map(str, options))

    def assertNoStaging(self):
        self.assertEqual(list(self.directory.glob(".nexus-*")), [])

    def test_export_import_round_trip_is_exact(self):
        export = self.directory / "export"
        self.run_script("export", "--db-file", self.source, "--out", export, "--include-proxy-cache")
        imported = self.directory / "imported" / "nexus.sqlite3"
        result = self.run_script("import", "--db-file", imported, "--export-dir", export)
        self.assertRegex(result.stdout, r"^Imported 13 repositories, 30 assets and 1 Docker images into .*nexus\.sqlite3\n$")
        self.assertEqual(list(imported.parent.iterdir()), [imported])
        with self.serve(imported) as port:
            self.assertEqual(state(port, aliases=[self.fixture_port]), self.expected)
            # The imported file is a normal persistence file that accepts new content.
            fixtures.Client(port).ok("PUT", "/repository/raw-hosted/new.txt", b"new")

        # Exports written before settings and attributes were recorded still import, with Nexus defaults.
        requests = json.loads((export / "repositories.json").read_text(encoding="utf-8"))
        for request in requests:
            request.pop("config"), request.pop("assets", None)
        (export / "repositories.json").write_text(json.dumps(requests), encoding="utf-8")
        older = self.directory / "older.sqlite3"
        self.run_script("import", "--db-file", older, "--export-dir", export, "--repository", "maven-releases",
                        "raw-hosted", "raw-locked", "docker-hosted")
        with self.serve(older) as port:
            observed = state(port)
        self.assertEqual([item["name"] for item in observed["repositories"]],
                         ["docker-hosted", "maven-releases", "raw-hosted", "raw-locked"])
        self.assertEqual(observed["maven-releases"]["settings"]["maven"]["versionPolicy"], "RELEASE")
        self.assertEqual(observed["raw-locked"]["settings"]["storage"]["writePolicy"], "DENY")
        for name in ("raw-hosted", "maven-releases", "docker-hosted"):
            self.assertEqual([(item["path"], item["content"]) for item in observed[name]["assets"]],
                             [(item["path"], item["content"]) for item in self.expected[name]["assets"]])
        self.assertNoStaging()

    def test_import_from_a_running_server(self):
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        with self.serve(self.source, password=True) as port:
            source = f"http://127.0.0.1:{port}"
            imported = self.directory / "live.sqlite3"
            result = self.run_script("import", "--db-file", imported, "--source", source,
                                     "--password-file", self.password_file)
            self.assertIn("Imported 13 repositories", result.stdout)
            self.assertIn("warning: raw-offline: skipping 1 assets of an offline repository", result.stderr)
            subset = self.directory / "subset.sqlite3"
            self.run_script("import", "--db-file", subset, "--source", source, "--password-file", self.password_file,
                            "--repository", "docker-hosted", "raw-all", "--docker-endpoint",
                            f"docker-hosted={source}/v2/docker-hosted")
            self.password_file.write_text("wrong", encoding="utf-8")
            refused = self.directory / "refused.sqlite3"
            result = self.run_script("import", "--db-file", refused, "--source", source,
                                     "--password-file", self.password_file, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertRegex(result.stderr, r"^Nexus import failed: GET .* returned HTTP 401")
            self.assertNotIn("wrong", result.stdout + result.stderr)
            self.assertFalse(refused.exists())
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), before)
        expected = json.loads(json.dumps(self.expected))
        # Proxy caches are not imported, and offline repositories cannot be read.
        for name in ("raw-proxy", "raw-offline"):
            expected[name]["assets"] = expected[name]["components"] = []

        def effective(snapshot):
            # The REST detail view fills in Nexus defaults, which the import then stores explicitly.
            for item in snapshot["repositories"]:
                item.pop("attributes")
                snapshot[item["name"]]["settings"].pop("attributes")
            return snapshot

        with self.serve(imported) as port:
            self.assertEqual(effective(state(port, aliases=[self.fixture_port])), effective(expected))
        with self.serve(subset) as port:
            observed = state(port)
        self.assertEqual([item["name"] for item in observed["repositories"]], ["docker-hosted", "raw-all"])
        self.assertEqual(observed["raw-all"]["settings"]["group"]["memberNames"], [])
        self.assertEqual(effective(observed)["docker-hosted"], expected["docker-hosted"])
        self.assertNoStaging()

    def oci_layout(self, folder, annotations):
        layer = fixtures.tar_gz({"hello.txt": b"hello\n"})
        config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        manifest = json.dumps({"schemaVersion": 2, "config": fixtures.descriptor(
            "application/vnd.oci.image.config.v1+json", config), "layers": [fixtures.descriptor(
                "application/vnd.oci.image.layer.v1.tar+gzip", layer)]}).encode()
        for content in (layer, config, manifest):
            target = folder / "blobs" / "sha256" / hashlib.sha256(content).hexdigest()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            os.utime(target, (1700000000.25, 1700000000.25))
        (folder / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}', encoding="utf-8")
        (folder / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [fixtures.descriptor(
            fixtures.OCI_MANIFEST, manifest, annotations=annotations)]}), encoding="utf-8")
        return manifest, layer

    def test_import_plain_oci_layout_and_maven_directory(self):
        layout = self.directory / "layout"
        manifest, layer = self.oci_layout(layout, {"org.opencontainers.image.ref.name": "1.0",
                                                   "io.containerd.image.name": "docker.io/library/demo:1.0"})
        maven = self.directory / "m2"
        files = {"org/jjap/tool/1.0/tool-1.0.jar": b"jar", "org/jjap/tool/1.0/tool-1.0.pom": b"<project/>",
                 "org/jjap/tool/1.0/tool-1.0.jar.sha1": hashlib.sha1(b"jar").hexdigest().encode(),
                 "org/jjap/tool/1.1-SNAPSHOT/tool-1.1-SNAPSHOT.jar": b"snapshot",
                 "org/jjap/tool/1.0/_remote.repositories": b"local", "org/jjap/tool/1.0/tool-1.0.jar.lastUpdated": b"",
                 "org/jjap/tool/maven-metadata-central.xml": b"<metadata/>",
                 "org/jjap/tool/resolver-status.properties": b""}
        for path, content in files.items():
            target = maven.joinpath(*path.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)

        docker = self.directory / "docker.sqlite3"
        result = self.run_script("import", "--db-file", docker, "--oci-layout", layout, "--repository", "docker-hosted")
        self.assertIn("Imported 0 repositories, 4 assets and 1 Docker images", result.stdout)
        renamed = self.directory / "renamed.sqlite3"
        untagged = self.directory / "untagged"
        untagged_manifest, _ = self.oci_layout(untagged, {})
        self.run_script("import", "--db-file", renamed, "--oci-layout", untagged, "--repository", "containers",
                        "--image", "team/tool:2.0")
        refused = self.directory / "refused.sqlite3"
        result = self.run_script("import", "--db-file", refused, "--maven-dir", maven, "--repository",
                                 "maven-releases", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Nexus import failed: maven-releases/org/jjap/tool/1.1-SNAPSHOT/tool-1.1-SNAPSHOT.jar: "
                      "Repository version policy is RELEASE", result.stderr)
        self.assertFalse(refused.exists())
        local = self.directory / "maven.sqlite3"
        result = self.run_script("import", "--db-file", local, "--maven-dir", maven, "--repository", "maven-local")
        self.assertIn("Imported 1 repositories, 4 assets and 0 Docker images", result.stdout)
        self.assertIn("4 items skipped", result.stdout)

        with self.serve(docker) as port:
            client = fixtures.Client(port)
            image = "/repository/docker-hosted/v2/library/demo"
            headers, data = client.ok("GET", f"{image}/manifests/1.0")
            self.assertEqual((data, headers["content-type"]), (manifest, fixtures.OCI_MANIFEST))
            self.assertEqual(client.ok("GET", f"{image}/blobs/{fixtures.digest(layer)}")[1], layer)
            self.assertEqual(json.loads(client.ok("GET", f"{image}/tags/list")[1])["tags"], ["1.0"])
            self.assertEqual(headers["last-modified"], "Tue, 14 Nov 2023 22:13:20 GMT")
            names = [item["name"] for item in json.loads(client.ok("GET", f"{REST}/repositories")[1])]
            self.assertIn("raw-hosted", names)
        with self.serve(renamed) as port:
            client = fixtures.Client(port)
            settings = json.loads(client.ok("GET", f"{REST}/repositories/docker/hosted/containers")[1])
            self.assertEqual(settings["format"], "docker")
            self.assertEqual(client.ok("GET", "/repository/containers/v2/team/tool/manifests/2.0")[1],
                             untagged_manifest)
            self.assertEqual(json.loads(client.ok("GET", "/repository/containers/v2/team/tool/tags/list")[1])["tags"],
                             ["2.0"])
        with self.serve(local) as port:
            client = fixtures.Client(port)
            for path, content in list(files.items())[:4]:
                self.assertEqual(client.ok("GET", f"/repository/maven-local/{path}")[1], content)
            self.assertEqual(client.request("GET", "/repository/maven-local/org/jjap/tool/1.0/_remote.repositories")[0],
                             404)
            metadata = client.ok("GET", "/repository/maven-local/org/jjap/tool/maven-metadata.xml")[1]
            self.assertIn(b"<version>1.1-SNAPSHOT</version>", metadata)

        (layout / "blobs" / "sha256" / hashlib.sha256(layer).hexdigest()).write_bytes(b"x" * len(layer))
        result = self.run_script("import", "--db-file", refused, "--oci-layout", layout, "--repository",
                                 "docker-hosted", check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not match its digest", result.stderr)
        self.assertFalse(refused.exists())
        self.assertNoStaging()

    def test_import_refuses_existing_files_and_invalid_sources(self):
        existing = self.directory / "existing.sqlite3"
        existing.write_bytes(b"keep")
        export = self.directory / "export"
        self.run_script("export", "--db-file", self.source, "--out", export)
        result = self.run_script("import", "--db-file", existing, "--export-dir", export, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Nexus import failed: The persistence file already exists", result.stderr)
        self.assertEqual(existing.read_bytes(), b"keep")
        target = self.directory / "never.sqlite3"
        for args, message in (
            (["--export-dir", self.directory], "is not a Nexus test server export"),
            (["--export-dir", self.directory / "missing"], "Not a directory"),
            (["--export-dir", export, "--repository", "nope"], "Repository not found in the export: nope"),
            (["--oci-layout", export, "--repository", "docker-hosted"], "is not an OCI image layout"),
            (["--oci-layout", export / "content/docker-hosted/jjap/app", "--repository", "raw-hosted"],
             "raw-hosted is a raw/hosted repository, not a hosted docker repository"),
            (["--source", "http://127.0.0.1:9", "--password-file", self.password_file], "Nexus import failed: GET"),
        ):
            with self.subTest(args=args):
                result = self.run_script("import", "--db-file", target, *args, check=False)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertTrue(result.stderr.startswith("Nexus import failed: "), result.stderr)
                self.assertIn(message, result.stderr)
                self.assertFalse(target.exists())
        result = self.run_script("import", "--db-file", target, "--maven-dir", export, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("need exactly one --repository", result.stderr)
        layout = export / "content/docker-hosted/jjap/app"
        next((layout / "blobs/sha256").iterdir()).write_bytes(b"corrupt")
        result = self.run_script("import", "--db-file", target, "--export-dir", export, check=False)
        self.assertIn("does not match", result.stderr)
        self.assertFalse(target.exists())
        self.assertNoStaging()

    def test_migrate_dry_run_reports_without_writing(self):
        target_db = self.directory / "target.sqlite3"
        with self.serve(target_db, password=True) as port:
            target = f"http://127.0.0.1:{port}"
            migrate = ["migrate", "--db-file", self.source, "--target", target, "--password-file", self.password_file]
            before = state(port, PASSWORD)
            result = self.run_script(*migrate, "--dry-run")
            self.assertEqual(state(port, PASSWORD), before)
            self.assertIn("would create repository raw-locked (raw/hosted)\n", result.stdout)
            self.assertIn("would apply the read-only or offline settings of raw-offline\n", result.stdout)
            self.assertIn("would upload raw-hosted/docs/readme.txt\n", result.stdout)
            self.assertIn("would publish npm-hosted/@jjap/widget@2.0.0-beta.1\n", result.stdout)
            self.assertIn("would set npm dist-tag npm-hosted/@jjap/widget:stable to 1.0.0\n", result.stdout)
            self.assertIn("would push manifest jjap/app:multi to", result.stdout)
            planned = int(re.search(
                r"^Would migrate 13 repositories \(6 created, 7 already present\) and (\d+) assets \(0 already "
                r"present\) to http://127\.0\.0\.1:\d+ \(dry run, nothing was written\)$", result.stdout, re.M)[1])
            result = self.run_script(*migrate)
            self.assertIn(f"and {planned} assets (0 already present)", result.stdout)

            result = self.run_script(*migrate, "--dry-run")
            self.assertRegex(result.stdout, r"Would migrate 13 repositories \(0 created, 13 already present\) "
                                            r"and 0 assets \(\d+ already present\)")
            self.assertNotIn("would ", result.stdout)
            client = fixtures.Client(port, PASSWORD)
            client.ok("PUT", "/repository/raw-hosted/docs/readme.txt", b"changed on the target")
            client.ok("DELETE", "/repository/raw-hosted/bin/data.bin")
            before = state(port, PASSWORD)
            result = self.run_script(*migrate, "--dry-run", "--repository", "raw-hosted", check=False)
            self.assertEqual(result.returncode, 1)
            self.assertIn("error: raw-hosted/docs/readme.txt: conflict", result.stderr)
            self.assertIn("would upload raw-hosted/bin/data.bin\n", result.stdout)
            self.assertRegex(result.stdout, r"and 1 assets .*; 1 failures\n$")
            self.assertEqual(state(port, PASSWORD), before)

    def test_reset_restores_the_startup_repositories(self):
        database = self.directory / "reset.sqlite3"
        with self.serve(database, password=True) as port:
            client = fixtures.Client(port, PASSWORD)
            startup = state(port, PASSWORD)
            client.ok("POST", f"{REST}/repositories/raw/hosted", {"name": "extra", "online": True})
            client.ok("PUT", "/repository/extra/file.txt", b"extra")
            client.ok("PUT", "/repository/raw-hosted/file.txt", b"raw")
            client.ok("DELETE", f"{REST}/repositories/maven-public")
            tarball = fixtures.tar_gz({"package/package.json": b'{"name":"plain","version":"0.1.0"}'})
            body, headers = fixtures.multipart({}, {"npm.asset": ("plain-0.1.0.tgz", tarball)})
            client.ok("POST", f"{REST}/components?repository=npm-hosted", body, headers)
            client.ok("PUT", "/repository/npm-hosted/-/package/plain/dist-tags/next", b'"0.1.0"')
            self.assertNotEqual(state(port, PASSWORD), startup)
            anonymous = fixtures.Client(port)
            self.assertEqual(anonymous.request("POST", "/_jjap/reset")[0], 401)
            for method in ("GET", "PUT", "DELETE"):
                self.assertEqual(client.request(method, "/_jjap/reset")[0], 405)
            _, data = client.ok("POST", "/_jjap/reset", b"{}", {"Content-Type": "application/json"})
            self.assertEqual(json.loads(data), {"reset": True})
            self.assertEqual(state(port, PASSWORD), startup)
            client.ok("PUT", "/repository/raw-hosted/after.txt", b"after reset")
            identifier = json.loads(client.ok("GET", f"{REST}/assets?repository=raw-hosted")[1])["items"][0]["id"]
            self.assertEqual(base64.urlsafe_b64decode(identifier + "==").decode()[-2:], "01")
            client.ok("POST", "/_jjap/reset")
        with self.serve(database, password=True) as port:
            self.assertEqual(state(port, PASSWORD), startup)
        with contextlib.closing(sqlite3.connect(database)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM assets").fetchone()[0], 0)
        with fixtures.running_server(self.script) as port:
            client = fixtures.Client(port)
            client.ok("PUT", "/repository/raw-hosted/memory.txt", b"x")
            client.ok("POST", "/_jjap/reset")
            self.assertEqual(client.request("GET", "/repository/raw-hosted/memory.txt")[0], 404)


if __name__ == "__main__":
    unittest.main()
