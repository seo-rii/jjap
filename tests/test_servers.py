"""Exercise copied server files through their network protocols."""

import contextlib
import hashlib
import http.client
import json
import os
import pathlib
import queue
import re
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"


@contextlib.contextmanager
def running_server(script, directory, *args):
    standalone = directory / script
    shutil.copyfile(ROOT / script, standalone)
    env = {key: value for key, value in os.environ.items() if not key.endswith("_TEST_PASSWORD")}
    command = [sys.executable, "-I", "-S", "-B", "-u", str(standalone), "--port", "0", *args]
    with subprocess.Popen(
        command, cwd=directory, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    ) as process:
        lines = queue.Queue()

        def collect_output():
            for line in process.stdout:
                lines.put(line)
            lines.put(None)

        reader = threading.Thread(target=collect_output, daemon=True)
        reader.start()
        try:
            output = []
            while True:
                try:
                    line = lines.get(timeout=10)
                except queue.Empty:
                    raise AssertionError(f"{script} did not announce its port. {''.join(output)}") from None
                if line is None:
                    raise AssertionError(f"{script} exited during startup. {''.join(output)}")
                if match := re.search(r"127\.0\.0\.1:(\d+)", line):
                    yield int(match[1])
                    break
                output.append(line)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            reader.join(timeout=5)


def redis_command(*parts):
    fields = [part.encode() if isinstance(part, str) else part for part in parts]
    return f"*{len(fields)}\r\n".encode() + b"".join(
        f"${len(field)}\r\n".encode() + field + b"\r\n" for field in fields
    )


def bolt_string(value):
    data = value.encode()
    if len(data) < 16:
        return bytes([0x80 | len(data)]) + data
    return bytes([0xD0, len(data)]) + data


class Bolt:
    def __init__(self, sock):
        self.sock = sock
        self.reader = sock.makefile("rb")

    def close(self):
        self.reader.close()

    def receive(self):
        data = bytearray()
        while length := struct.unpack(">H", self.reader.read(2))[0]:
            data.extend(self.reader.read(length))
        return bytes(data)

    def send(self, payload):
        self.sock.sendall(struct.pack(">H", len(payload)) + payload + b"\x00\x00")

    def execute(self, statement):
        self.send(b"\xb3\x10" + bolt_string(statement) + b"\xa0\xa0")
        self.require_success()
        self.send(b"\xb1\x3f\xa1\x81n\xff")
        records = []
        while (response := self.receive()).startswith(b"\xb1\x71"):
            records.append(response[2:])
        self.check_success(response)
        return records

    def require_success(self):
        self.check_success(self.receive())

    @staticmethod
    def check_success(response):
        if not response.startswith(b"\xb1\x70"):
            raise AssertionError(f"Expected Bolt SUCCESS, got {response!r}")


@contextlib.contextmanager
def bolt_session(port):
    with socket.create_connection((HOST, port), timeout=5) as sock:
        with contextlib.closing(Bolt(sock)) as bolt:
            version = b"\x00\x00\x04\x04"
            sock.sendall(b"\x60\x60\xb0\x17" + version + b"\x00" * 12)
            if bolt.reader.read(4) != version:
                raise AssertionError("Bolt 4.4 negotiation failed")
            bolt.send(b"\xb1\x01\xa1\x8auser_agent\x84jjap")
            bolt.require_success()
            yield bolt


def http_request(port, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection(HOST, port, timeout=5)
    with contextlib.closing(connection):
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        return response.status, response.headers, response.read()


class StandaloneServerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="jjap-test-server-")
        self.addCleanup(directory.cleanup)
        self.directory = pathlib.Path(directory.name)
        self.db_file = str(self.directory / "test-db")

    def server(self, script):
        return running_server(script, self.directory, "--db-file", self.db_file)

    def test_redis_pipeline_binary_watch_and_restart(self):
        script = "redis_server.py"
        with self.server(script) as port:
            with (
                socket.create_connection((HOST, port), timeout=5) as first,
                socket.create_connection((HOST, port), timeout=5) as second,
                first.makefile("rb") as reply,
                second.makefile("rb") as competing_reply,
            ):
                value = b"\x00\xff\r\n"
                first.sendall(redis_command("SET", "binary", value) + redis_command("GET", "binary"))
                self.assertEqual(reply.readline(), b"+OK\r\n")
                self.assertEqual(reply.readline(), b"$4\r\n")
                self.assertEqual(reply.read(6), value + b"\r\n")

                first.sendall(redis_command("WATCH", "counter") + redis_command("MULTI"))
                self.assertEqual(reply.readline(), b"+OK\r\n")
                self.assertEqual(reply.readline(), b"+OK\r\n")
                first.sendall(redis_command("INCR", "counter"))
                self.assertEqual(reply.readline(), b"+QUEUED\r\n")
                second.sendall(redis_command("SET", "counter", "10"))
                self.assertEqual(competing_reply.readline(), b"+OK\r\n")
                first.sendall(redis_command("EXEC"))
                self.assertEqual(reply.readline(), b"*-1\r\n")

        with self.server(script) as port, socket.create_connection((HOST, port), timeout=5) as sock:
            sock.sendall(redis_command("GET", "counter"))
            with sock.makefile("rb") as reply:
                self.assertEqual(reply.read(8), b"$2\r\n10\r\n")

    def test_redis_single_node_cluster_enforces_hash_slots(self):
        script = "redis_server.py"
        with running_server(script, self.directory, "--cluster") as port:
            with socket.create_connection((HOST, port), timeout=5) as sock, sock.makefile("rb") as reply:
                sock.sendall(
                    redis_command("CLUSTER", "KEYSLOT", "{team}member")
                    + redis_command("CLUSTER", "KEYSLOT", "{team}score")
                    + redis_command("MSET", "{team}member", "jjap", "{team}score", "1")
                    + redis_command("MGET", "{team}member", "{other}score")
                )
                slot = reply.readline()
                self.assertRegex(slot, rb"^:[0-9]+\r\n$")
                self.assertEqual(reply.readline(), slot)
                self.assertEqual(reply.readline(), b"+OK\r\n")
                self.assertTrue(reply.readline().startswith(b"-CROSSSLOT "))

    def test_neo4j_graph_rollback_and_restart(self):
        script = "neo4j_server.py"
        with self.server(script) as port, bolt_session(port) as bolt:
            records = bolt.execute("CREATE (n:Example {name: 'kept'}) RETURN n.name AS name")
            self.assertEqual(records, [b"\x91\x84kept"])
            bolt.send(b"\xb1\x11\xa0")
            bolt.require_success()
            bolt.execute("CREATE (n:Example {name: 'rolled back'}) RETURN n.name AS name")
            bolt.send(b"\xb0\x13")
            bolt.require_success()
            bolt.send(b"\xb3\x10" + bolt_string("INVALID QUERY") + b"\xa0\xa0")
            self.assertTrue(bolt.receive().startswith(b"\xb1\x7f"))
            bolt.send(b"\xb0\x0f")
            bolt.require_success()
            self.assertEqual(bolt.execute("MATCH (n:Example) RETURN n.name AS name"), records)

        with self.server(script) as port, bolt_session(port) as bolt:
            records = bolt.execute("MATCH (n:Example) RETURN n.name AS name")
            self.assertEqual(records, [b"\x91\x84kept"])

    def test_nexus_raw_content_search_and_restart(self):
        script = "nexus_server.py"
        path = "/repository/raw-hosted/jjap/fixture.bin"
        content = b"\x00nexus\r\n\xff"
        with self.server(script) as port:
            status, _, _ = http_request(port, "PUT", path, content)
            self.assertIn(status, (200, 201, 204))
            status, _, body = http_request(port, "GET", "/service/rest/v1/search/assets?repository=raw-hosted")
            self.assertEqual(status, 200)
            assets = json.loads(body)["items"]
            self.assertEqual([item["path"] for item in assets], ["jjap/fixture.bin"])

        with self.server(script) as port:
            status, _, body = http_request(port, "GET", path)
            self.assertEqual((status, body), (200, content))

    def test_nexus_oci_digest_chunked_upload_and_pull(self):
        script = "nexus_server.py"
        image = "/v2/docker-hosted/jjap/example"
        content = b"{}"
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        media_type = "application/vnd.oci.image.manifest.v1+json"
        manifest_document = {
            "schemaVersion": 2, "mediaType": media_type, "layers": [],
            "config": {"mediaType": "application/vnd.oci.empty.v1+json", "digest": digest, "size": len(content)},
        }
        manifest = json.dumps(manifest_document).encode()
        with self.server(script) as port:
            status, headers, _ = http_request(port, "POST", image + "/blobs/uploads/")
            self.assertEqual(status, 202)
            upload = headers["Location"]
            status, _, _ = http_request(port, "PATCH", upload, content, {"Content-Range": "1-2"})
            self.assertEqual(status, 416)
            status, _, _ = http_request(port, "PATCH", upload, content, {"Content-Range": "0-1"})
            self.assertEqual(status, 202)
            status, headers, _ = http_request(port, "PUT", f"{upload}?digest={digest}")
            self.assertEqual((status, headers["Docker-Content-Digest"]), (201, digest))
            target = "/v2/docker-hosted/jjap/mounted/blobs/uploads/"
            status, _, _ = http_request(port, "POST", f"{target}?mount={digest}&from=docker-hosted/jjap/example")
            self.assertEqual(status, 201)
            manifest_document["config"]["size"] += 1
            status, _, _ = http_request(port, "PUT", image + "/manifests/latest", json.dumps(manifest_document),
                                        {"Content-Type": media_type})
            self.assertEqual(status, 400)
            status, _, _ = http_request(port, "PUT", image + "/manifests/latest", manifest,
                                        {"Content-Type": media_type})
            self.assertEqual(status, 201)

        with self.server(script) as port:
            status, headers, body = http_request(port, "GET", image + "/manifests/latest")
            self.assertEqual((status, headers["Content-Type"], body), (200, media_type, manifest))
            status, _, body = http_request(port, "GET", f"{image}/blobs/{digest}", headers={"Range": "bytes=1-1"})
            self.assertEqual((status, body), (206, content[1:]))


if __name__ == "__main__":
    unittest.main()
