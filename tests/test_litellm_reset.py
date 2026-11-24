"""Verify POST /_jjap/reset on the LiteLLM test server, which runs as a separate process."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


SCRIPT = Path(__file__).resolve().parents[1] / "litellm_server.py"
MASTER = "sk-master-reset"


class LiteLLMResetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-litellm-reset-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        (self.directory / "master").write_text(MASTER + "\n", encoding="utf-8")
        config = {"model_list": [
            {"model_name": "chat", "litellm_params": {"model": "mock/chat"}},
            {"model_name": "chat", "litellm_params": {"model": "mock/chat", "mock_response": "second"}},
        ]}
        (self.directory / "config.json").write_text(json.dumps(config), encoding="utf-8")

    def start(self, *options):
        process = subprocess.Popen(
            [sys.executable, "-I", "-S", "-B", str(self.script), "--port", "0", "--config", "config.json", *options],
            cwd=self.directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        line = process.stdout.readline()
        self.assertIn("listening on http://", line, process.stderr.read() if not line else line)
        self.root = line.split("listening on ", 1)[1].strip()
        return process

    def call(self, path, method="GET", body=None, key=MASTER):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = "Bearer " + key
        data = json.dumps(body).encode() if body is not None else None
        try:
            with urlopen(Request(self.root + path, data=data, method=method, headers=headers), timeout=10) as response:
                return response.status, json.load(response), response.headers
        except HTTPError as error:
            with error:
                return error.code, json.load(error) if error.headers.get("Content-Type") == "application/json" else None, error.headers

    def chat(self, key):
        return self.call("/v1/chat/completions", "POST", {"model": "chat", "messages": [{"role": "user", "content": "hi"}]}, key)

    def test_reset_forgets_keys_usage_and_responses_but_keeps_models_and_master_key(self):
        process = self.start("--db-file", "gateway.sqlite3", "--password-file", "master")
        key = self.call("/key/generate", "POST", {"models": ["chat"], "key_alias": "tests"})[1]["key"]
        self.assertEqual(self.chat(key)[0], 200)
        stored = self.call("/v1/responses", "POST", {"model": "chat", "input": "remember"}, key)[1]["id"]
        self.assertEqual(self.call(f"/v1/responses/{stored}", key=key)[0], 200)
        self.assertEqual(len(self.call("/spend/logs")[1]), 2)

        self.assertEqual(self.call("/_jjap/reset", "POST", key=None)[0], 401)
        self.assertEqual(self.call("/_jjap/reset", "POST", key=key)[0], 401)
        for method in ("GET", "PUT", "DELETE", "PATCH"):
            status, body, headers = self.call("/_jjap/reset", method)
            self.assertEqual((method, status, headers.get("Allow")), (method, 405, "POST"))
            self.assertEqual(body["error"]["code"], "method_not_allowed")
        self.assertEqual(self.call("/v1/models", "PUT")[0], 501)
        self.assertEqual(self.call("/_jjap/reset", "POST")[:2], (200, {"reset": True}))

        self.assertEqual(self.chat(key)[0], 401)
        self.assertEqual(self.call("/spend/logs")[1], [])
        self.assertEqual(self.call(f"/v1/responses/{stored}")[0], 404)
        self.assertEqual([model["id"] for model in self.call("/v1/models")[1]["data"]], ["chat"])
        contents = [self.chat(MASTER)[1]["choices"][0]["message"]["content"] for _ in range(2)]
        self.assertEqual(contents[1], "second")  # The round-robin position starts over.
        fresh = self.call("/key/generate", "POST", {})[1]["key"]
        self.assertEqual(self.chat(fresh)[0], 200)
        process.terminate()
        process.wait(10)
        self.start("--db-file", "gateway.sqlite3", "--password-file", "master")
        self.assertEqual(self.chat(key)[0], 401)
        self.assertEqual(self.chat(fresh)[0], 200)
        self.assertEqual(len(self.call("/spend/logs")[1]), 4)

    def test_reset_is_open_without_a_master_key(self):
        self.start()
        self.assertEqual(self.chat(None)[0], 200)
        self.assertEqual(self.call("/_jjap/reset", "POST", key=None)[:2], (200, {"reset": True}))
        self.assertEqual(self.call("/v1/_jjap/reset", "GET", key=None)[0], 405)


if __name__ == "__main__":
    unittest.main()
