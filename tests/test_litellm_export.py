"""Verify LiteLLM proxy configuration and virtual key exports without LiteLLM installed."""

import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.request import Request, urlopen


SCRIPT = Path(__file__).resolve().parents[1] / "litellm_server.py"
# Columns of model LiteLLM_VerificationToken in litellm/proxy/schema.prisma that the export writes.
TOKEN_COLUMNS = {"token", "key_alias", "models", "expires"}


def read_config(path):
    text = path.read_text(encoding="utf-8")
    comment, _, body = text.partition("\n")
    assert comment.startswith("# "), comment
    # Characters PyYAML refuses in a stream, or folds as line breaks inside a string, must be escaped.
    assert not re.search("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u2028\u2029\ufeff\ufffe\uffff]", body)
    return json.loads(body)


class LiteLLMExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.litellm = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-litellm-export-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    def command(self, *args, check=True, env=None):
        return subprocess.run([sys.executable, "-I", "-S", "-B", str(self.script), "export", *args],
                              cwd=self.directory, check=check, text=True, capture_output=True, timeout=15, env=env)

    def write_json(self, value, name="config.json"):
        path = self.directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_config_keeps_routing_and_references_the_master_key(self):
        config = self.write_json({
            "model_list": [
                {"model_name": "chat", "litellm_params": {"model": "provider/id", "api_base": "http://127.0.0.1:9/v1",
                                                          "mock_response": "ignored", "temperature": 1e20}},
                {"model_name": "chat", "litellm_params": {"model": "openai/id", "api_base": "https://example.test/v1",
                                                          "api_key": "os.environ/UPSTREAM_KEY"},
                 "model_info": {"id": "second"}},
                {"model_name": "fixed \u2615\x85\u2028\ufeff", "litellm_params": {
                    "model": "mock/chat", "mock_response": "answer\x7f", "mock_tool_arguments": {"f": {"x": 1}}},
                 "model_info": {"supported_endpoints": ["/v1/chat/completions"]}},
                {"model_name": "echo", "litellm_params": {"model": "mock/chat"}},
                {"model_name": "vectors", "litellm_params": {"model": "mock/embeddings"},
                 "model_info": {"supported_endpoints": ["/v1/embeddings"]}},
            ],
            "router_settings": {"routing_strategy": "simple-shuffle", "num_retries": 2},
            "litellm_settings": {"drop_params": True},
            "general_settings": {"master_key": "sk-literal", "alerting": []},
        })
        output = self.directory / "proxy"
        env = {key: value for key, value in os.environ.items() if key != "UPSTREAM_KEY"}
        result = self.command("--config", str(config), "--out", str(output), env=env)
        self.assertEqual(result.stdout.strip(), f"Exported 5 deployments to config.yaml in {output}")
        self.assertEqual(sorted(path.name for path in output.iterdir()), ["config.yaml"])
        for message in ("removed mock_response from upstream deployment 'chat'", "dropped mock_tool_arguments",
                        "'echo' echoes input", "'vectors' returns one constant 16-dimension embedding",
                        "replaced general_settings.master_key"):
            self.assertIn(message, result.stderr)

        text = (output / "config.yaml").read_text(encoding="utf-8")
        self.assertIn('"temperature": 1.0e+20', text)
        self.assertIn("\\u0085\\u2028\\ufeff", text)
        self.assertNotIn("sk-literal", text)
        exported = read_config(output / "config.yaml")
        upstream, second, fixed, echo, vectors = exported["model_list"]
        self.assertEqual(upstream["litellm_params"], {"model": "openai/provider/id", "api_base": "http://127.0.0.1:9/v1",
                                                      "temperature": 1e20, "api_key": "none"})
        self.assertEqual(second, {"model_name": "chat", "model_info": {"id": "second"}, "litellm_params": {
            "model": "openai/id", "api_base": "https://example.test/v1", "api_key": "os.environ/UPSTREAM_KEY"}})
        self.assertEqual(fixed["model_name"], "fixed \u2615\x85\u2028\ufeff")
        self.assertEqual(fixed["litellm_params"], {"model": "openai/mock/chat", "mock_response": "answer\x7f"})
        self.assertEqual(echo["litellm_params"]["mock_response"], "This is a mock request")
        self.assertEqual(vectors["litellm_params"]["mock_response"], [0.0] * 16)
        self.assertEqual(exported["router_settings"], {"routing_strategy": "simple-shuffle", "num_retries": 2})
        self.assertEqual(exported["litellm_settings"], {"drop_params": True})
        self.assertEqual(exported["general_settings"], {"master_key": "os.environ/LITELLM_MASTER_KEY", "alerting": []})

    def test_virtual_keys_keep_litellm_token_hashes(self):
        source = self.directory / "gateway #?.sqlite3"
        gateway = self.litellm["Gateway"](self.litellm["DEFAULT_MODELS"], source, "sk-master")
        try:
            scoped = gateway.create_key({"models": ["mock-chat"], "key_alias": "o'brien \u00fcn\u00ef"})
            expiring = gateway.create_key({"duration": "2h"})
            removed = gateway.create_key({})
            gateway.delete_keys({"keys": [removed["key"]]})
            gateway.record("req_1", "mock-chat", hashlib.sha256(scoped["key"].encode()).hexdigest(),
                           {"prompt_tokens": 2, "completion_tokens": 3})
            expires = gateway.db.execute("SELECT expires FROM gateway_keys WHERE alias IS NULL").fetchone()[0]
        finally:
            gateway.db.close()
        snapshot = hashlib.sha256(source.read_bytes()).hexdigest()
        output = self.directory / "proxy"
        result = self.command("--db-file", str(source), "--out", str(output))
        self.assertIn("and 2 virtual keys to keys.sql", result.stdout)
        self.assertIn("skipped 1 usage records and 0 stored responses", result.stderr)
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), snapshot)

        sql = (output / "keys.sql").read_text(encoding="utf-8")
        self.assertTrue(sql.splitlines()[2] == "BEGIN;" and sql.endswith("COMMIT;\n"))
        statements = re.findall(r'INSERT INTO "LiteLLM_VerificationToken" \(([^)]*)\)\nVALUES \((.*)\)\n'
                                r'ON CONFLICT \("token"\) DO NOTHING;', sql)
        self.assertEqual(len(statements), 2)
        for columns, _ in statements:
            self.assertEqual({column.strip('"') for column in columns.split(", ")}, TOKEN_COLUMNS)
        # LiteLLM's hash_token is hashlib.sha256(key.encode()).hexdigest().
        first, second = (values for _, values in statements)
        self.assertEqual(first, f"'{hashlib.sha256(scoped['key'].encode()).hexdigest()}', 'o''brien \u00fcn\u00ef', "
                                "ARRAY['mock-chat']::TEXT[], NULL")
        moment = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(expires)) + f".{int(expires % 1 * 1000):03d}"
        self.assertEqual(second, f"'{hashlib.sha256(expiring['key'].encode()).hexdigest()}', NULL, '{{}}'::TEXT[], "
                                 f"TIMESTAMP '{moment}'")
        self.assertNotIn(hashlib.sha256(removed["key"].encode()).hexdigest(), sql)
        self.assertNotIn(scoped["key"], sql)

    def test_rejects_existing_destination_and_invalid_sources(self):
        used = self.directory / "used"
        used.mkdir()
        (used / "keep.txt").write_text("preserve", encoding="utf-8")
        result = self.command("--out", str(used), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)
        self.assertEqual((used / "keep.txt").read_text(encoding="utf-8"), "preserve")

        other = self.directory / "other.sqlite3"
        with sqlite3.connect(other) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
        connection.close()
        missing = self.directory / "missing.sqlite3"
        nan = self.directory / "nan.json"
        nan.write_text('{"model_list": [{"model_name": "m", "litellm_params": {"model": "mock/chat", "x": NaN}}]}',
                       encoding="utf-8")
        cases = [
            (["--db-file", str(other)], "not a LiteLLM test server SQLite persistence file"),
            (["--db-file", str(missing)], "database not found"),
            (["--config", str(nan)], "Out of range float values are not JSON compliant"),
            (["--config", str(self.write_json({"model_list": []}, "empty.json"))], "nonempty model_list"),
        ]
        for arguments, message in cases:
            target = self.directory / "out"
            result = self.command(*arguments, "--out", str(target), check=False)
            self.assertEqual(result.returncode, 1, arguments)
            self.assertTrue(result.stderr.startswith("LiteLLM export failed: "), result.stderr)
            self.assertIn(message, result.stderr)
            self.assertFalse(target.exists())
        self.assertFalse(missing.exists())
        self.assertEqual(list(self.directory.glob(".litellm-export-*")), [])

    @unittest.skipUnless(shutil.which("litellm"), "the LiteLLM proxy is not installed")
    def test_litellm_proxy_serves_exported_config(self):
        config = self.write_json({"model_list": [
            {"model_name": "fixed", "litellm_params": {"model": "mock/chat", "mock_response": "fixed answer"}}]})
        output = self.directory / "proxy"
        self.command("--config", str(config), "--out", str(output))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        master = "sk-" + secrets.token_hex(32)
        process = subprocess.Popen(["litellm", "--config", str(output / "config.yaml"), "--host", "127.0.0.1",
                                    "--port", str(port)], env={**os.environ, "LITELLM_MASTER_KEY": master},
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            headers = {"Authorization": "Bearer " + master, "Content-Type": "application/json"}
            deadline = time.monotonic() + 120
            while True:
                try:
                    with urlopen(Request(f"http://127.0.0.1:{port}/v1/models", headers=headers), timeout=2) as response:
                        models = json.load(response)
                    break
                except OSError:
                    if time.monotonic() > deadline or process.poll() is not None:
                        raise
                    time.sleep(1)
            self.assertEqual([model["id"] for model in models["data"]], ["fixed"])
            body = json.dumps({"model": "fixed", "messages": [{"role": "user", "content": "hi"}]}).encode()
            with urlopen(Request(f"http://127.0.0.1:{port}/v1/chat/completions", data=body, headers=headers),
                         timeout=30) as response:
                self.assertEqual(json.load(response)["choices"][0]["message"]["content"], "fixed answer")
        finally:
            process.terminate()
            process.wait(timeout=30)


if __name__ == "__main__":
    unittest.main()
