import base64
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from litellm_server import DEFAULT_MODELS, Gateway, Handler, Server


class Upstream(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.calls.append((self.path, self.headers.get("Authorization"), body))
        if self.path.startswith("/unavailable/"):
            self.send_error(503)
            return
        response = {
            "model": body["model"], "usage": {"prompt_tokens": 2, "completion_tokens": 3},
            "choices": [],
        }
        stream = body.get("stream")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream" if stream else "application/json")
        self.end_headers()
        output = json.dumps(response).encode()
        self.wfile.write(b"data: " + output + b"\n\ndata: [DONE]\n\n" if stream else output)

    def log_message(self, *_args):
        pass


class LiteLLMTests(unittest.TestCase):
    def serve(self, server):
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return "http://127.0.0.1:" + str(server.server_port)

    def gateway(self, *, config=DEFAULT_MODELS, db=":memory:", key=None):
        server = Server(("127.0.0.1", 0), Handler)
        server.gateway = Gateway(config, db, key)
        self.addCleanup(server.gateway.db.close)
        self.base = self.serve(server)
        return server

    def request(self, path, body=None, *, key=None, code=200, stream=False):
        headers = {"Authorization": "Bearer " + key} if key else {}
        raw = None
        if body is not None:
            raw = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        try:
            response = urlopen(Request(self.base + path, data=raw, headers=headers), timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            data = response.read()
            self.assertEqual(response.status, code, data)
            return data.decode() if stream else json.loads(data)

    def test_mock_chat_embeddings_and_responses(self):
        self.gateway()
        models = self.request("/v1/models")
        self.assertEqual([model["id"] for model in models["data"]], ["mock-chat", "mock-embedding"])
        request = {"model": "mock-chat", "messages": [{"role": "user", "content": "hello gateway"}]}
        reply = self.request("/v1/chat/completions", request)
        self.assertEqual(reply["choices"][0]["message"]["content"], "hello gateway")
        chat = self.request("/v1/chat/completions", {
            **request, "stream": True, "stream_options": {"include_usage": True},
        }, stream=True)
        self.assertIn("hello gateway", chat)
        self.assertIn('"total_tokens":4', chat)
        self.assertTrue(chat.endswith("data: [DONE]\n\n"))
        embedding_request = {"model": "mock-embedding", "input": ["abc", "xyz"], "dimensions": 8,
                             "encoding_format": "base64"}
        embedding = self.request("/v1/embeddings", embedding_request)
        self.assertEqual(embedding, self.request("/v1/embeddings", embedding_request))
        self.assertEqual(len(base64.b64decode(embedding["data"][0]["embedding"])), 32)
        tool_response = self.request("/v1/responses", {
            "model": "mock-chat", "input": "provide a tool result", "tool_choice": "required",
            "tools": [{"type": "function", "name": "work", "parameters": {"type": "object"}}],
        })
        tool = tool_response["output"][0]
        self.assertEqual(tool["name"], "work")
        self.assertEqual(json.loads(tool["arguments"]), {})
        next_response = self.request("/v1/responses", {
            "model": "mock-chat", "previous_response_id": tool_response["id"],
            "input": [{"type": "function_call_output", "call_id": tool["call_id"], "output": "working"}],
            "stream": True,
        }, stream=True)
        self.assertIn("event: response.output_text.delta", next_response)
        self.assertIn('"delta":"working"', next_response)
        self.assertIn("event: response.completed", next_response)

    def test_keys_usage_and_revocation_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "gateway.sqlite3"
            primary = self.gateway(db=db, key="master-fixture")
            self.request("/v1/models", code=401)
            key = self.request("/key/generate", {"models": ["mock-chat"]}, key="master-fixture")["key"]
            embeddings = {"model": "mock-embedding", "input": "blocked"}
            self.request("/v1/embeddings", embeddings, key=key, code=403)
            self.request("/v1/responses", {"model": "mock-chat", "input": "hello"}, key=key)
            primary.shutdown()

            self.gateway(db=db, key="master-fixture")
            self.assertEqual(len(self.request("/v1/models", key=key)["data"]), 1)
            logs = self.request("/spend/logs", key="master-fixture")
            self.assertEqual(logs[0]["total_tokens"], 2)
            self.assertNotIn(key.encode(), db.read_bytes())
            self.request("/key/delete", {"keys": [key]}, key="master-fixture")
            self.request("/v1/models", key=key, code=401)

    def test_upstream_alias_fallback_stream_and_header_isolation(self):
        upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        upstream.calls = []
        base = self.serve(upstream)
        config = {"model_list": [
            {"model_name": "alias", "litellm_params": {"model": "openai/fallback", "api_base": base + "/unavailable/v1"}},
            {"model_name": "alias", "litellm_params": {
                "model": "openai/backend-model", "api_base": base + "/v1", "api_key": "upstream-fixture",
            }},
        ]}
        self.gateway(config=config, key="master-fixture")
        request = {"model": "alias", "messages": [{"role": "user", "content": "hello"}]}
        result = self.request("/v1/chat/completions", request, key="master-fixture")
        self.assertEqual(result["model"], "backend-model")
        self.assertEqual([call[0] for call in upstream.calls],
                         ["/unavailable/v1/chat/completions", "/v1/chat/completions"])
        self.assertEqual(upstream.calls[-1][1], "Bearer upstream-fixture")
        streamed = self.request("/v1/chat/completions", {**request, "stream": True}, key="master-fixture", stream=True)
        self.assertIn("data: [DONE]", streamed)
        for entry in self.request("/spend/logs", key="master-fixture"):
            self.assertEqual(entry["model"], "alias")
            self.assertEqual(entry["total_tokens"], 5)


if __name__ == "__main__":
    unittest.main()
