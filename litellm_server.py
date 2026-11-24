#!/usr/bin/env python3
"""LiteLLM-compatible local gateway with fixture models and HTTP upstreams."""

import argparse
import base64
import datetime
import hashlib
import hmac
import http.client
import json
import math
import os
import re
import secrets
import sqlite3
import struct
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import SplitResult, unquote, urlsplit


ENDPOINTS = ("/chat/completions", "/embeddings", "/responses")
MAX_BODY = 8 * 1024 * 1024
MAX_UPSTREAM_BODY = 32 * 1024 * 1024
DEFAULT_MODELS = {
    "model_list": [
        {"model_name": "mock-chat", "litellm_params": {"model": "mock/chat"},
         "model_info": {"supported_endpoints": ["/v1/chat/completions", "/v1/responses"]}},
        {"model_name": "mock-embedding", "litellm_params": {"model": "mock/embeddings"},
         "model_info": {"supported_endpoints": ["/v1/embeddings"]}},
    ]
}


class GatewayError(ValueError):
    def __init__(self, message, status=400, code=None):
        super().__init__(message)
        self.status = status
        self.code = code


def object_value(value, name):
    if not isinstance(value, dict):
        raise GatewayError(f"{name} must be an object")
    return value


def json_bytes(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()


def invalid_json_constant(value):
    raise ValueError(f"Invalid JSON number: {value}")


def token_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def new_id(prefix):
    return prefix + secrets.token_hex(12)


@dataclass
class Deployment:
    model: str
    endpoints: tuple[str, ...]
    base: SplitResult | None
    token: str
    response: str | None
    tool_arguments: dict

    @classmethod
    def from_config(cls, row, *, resolve_credentials=True):
        row = object_value(row, "model_list entry")
        params = object_value(row.get("litellm_params"), "litellm_params")
        info = object_value(row.get("model_info", {}), "model_info")
        alias, model = row.get("model_name"), params.get("model")
        if not all(isinstance(value, str) and value for value in (alias, model)):
            raise GatewayError("Each deployment requires model_name and litellm_params.model")
        endpoints = info.get("supported_endpoints", ["/v1" + part for part in ENDPOINTS])
        if (not isinstance(endpoints, list) or not endpoints or
                any(not isinstance(part, str) or part.removeprefix("/v1") not in ENDPOINTS for part in endpoints)):
            raise GatewayError("Invalid supported_endpoints")
        endpoints = tuple(part.removeprefix("/v1") for part in endpoints)
        token, base = "", None
        if not model.startswith("mock/"):
            base_url = params.get("api_base")
            if not isinstance(base_url, str):
                raise GatewayError("Upstream deployments require an api_base URL")
            base = urlsplit(base_url)
            if (base.scheme not in ("http", "https") or not base.hostname or base.username is not None or
                    base.query or base.fragment):
                raise GatewayError("api_base must be an HTTP(S) base URL without credentials or query parameters")
            _ = base.port
            model = model.removeprefix("openai/")
            token = params.get("api_key", "")
            if not isinstance(token, str):
                raise GatewayError("api_key must be a string or an os.environ/ reference")
            if token.startswith("os.environ/") and resolve_credentials:
                variable = token.split("/", 1)[1]
                if not os.environ.get(variable):
                    raise GatewayError(f"Missing upstream credential in {variable}")
                token = os.environ[variable]
        response = params.get("mock_response")
        if response is not None and not isinstance(response, str):
            raise GatewayError("mock_response must be a string")
        arguments = object_value(params.get("mock_tool_arguments", {}), "mock_tool_arguments")
        return alias, cls(model, endpoints, base, token, response, arguments)


class Gateway:
    def __init__(self, config, db_file, master_token):
        self.master_hash = token_hash(master_token) if master_token else None
        self.routes = {}
        self.positions = {}
        self.lock = threading.Lock()
        models = object_value(config, "Settings").get("model_list")
        if not isinstance(models, list) or not models:
            raise GatewayError("Settings require a nonempty model_list")
        for row in models:
            alias, deployment = Deployment.from_config(row)
            self.routes.setdefault(alias, []).append(deployment)
        self.db = sqlite3.connect(db_file, check_same_thread=False, timeout=15)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS gateway_keys (
                token TEXT PRIMARY KEY, alias TEXT, models TEXT NOT NULL, expires REAL
            );
            CREATE TABLE IF NOT EXISTS gateway_usage (
                id TEXT PRIMARY KEY, model TEXT NOT NULL, token TEXT NOT NULL, at INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL, completion_tokens INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gateway_responses (
                id TEXT PRIMARY KEY, token TEXT NOT NULL, payload TEXT NOT NULL
            );
        """)

    def access(self, header, *, admin=False):
        scheme, _, token = header.partition(" ")
        digest = token_hash(token) if scheme.lower() == "bearer" and token else ""
        if self.master_hash and hmac.compare_digest(digest, self.master_hash):
            return digest, None
        if admin:
            raise GatewayError("A master key is required", 401, "invalid_api_key")
        if not self.master_hash:
            return "", None
        with self.lock:
            row = self.db.execute("SELECT models, expires FROM gateway_keys WHERE token = ?", (digest,)).fetchone()
        if row is None or (row[1] is not None and row[1] <= time.time()):
            raise GatewayError("Invalid or expired API key", 401, "invalid_api_key")
        return digest, json.loads(row[0]) or None

    def reset(self):
        """Forget virtual keys, usage, and stored responses; the configured models and master key stay."""
        with self.lock, self.db:
            for table in ("gateway_keys", "gateway_usage", "gateway_responses"):
                self.db.execute(f"DELETE FROM {table}")
            self.positions.clear()

    def create_key(self, body):
        unsupported = body.keys() - {"models", "key_alias", "duration"}
        if unsupported:
            raise GatewayError(f"Unsupported key settings: {', '.join(sorted(unsupported))}")
        models = body.get("models", [])
        if not isinstance(models, list) or any(not isinstance(name, str) or name not in self.routes for name in models):
            raise GatewayError("models must list configured aliases")
        alias = body.get("key_alias")
        if alias is not None and not isinstance(alias, str):
            raise GatewayError("key_alias must be a string")
        duration = body.get("duration")
        expires = None
        if duration is not None:
            match = re.fullmatch(r"([1-9][0-9]{0,7})([smhd])", duration) if isinstance(duration, str) else None
            if match is None:
                raise GatewayError("duration must be a positive number followed by s, m, h, or d")
            expires = time.time() + int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]
        key = "sk-" + secrets.token_urlsafe(32)
        with self.lock, self.db:
            self.db.execute("INSERT INTO gateway_keys VALUES (?, ?, ?, ?)", (token_hash(key), alias, json.dumps(models), expires))
        return {"key": key, "key_alias": alias, "models": models, "expires": iso_time(expires)}

    def delete_keys(self, body):
        keys = body.get("keys")
        if not isinstance(keys, list) or not keys or any(not isinstance(key, str) for key in keys):
            raise GatewayError("keys must be a nonempty array of strings")
        with self.lock, self.db:
            self.db.executemany("DELETE FROM gateway_keys WHERE token = ?", ((token_hash(key),) for key in keys))
        return {"deleted_keys": keys}

    def model_deployments(self, model, endpoint, allowed):
        if not isinstance(model, str) or model not in self.routes:
            raise GatewayError("Unknown model", 404, "model_not_found")
        if allowed is not None and model not in allowed:
            raise GatewayError("API key cannot access this model", 403, "model_access_denied")
        candidates = [route for route in self.routes[model] if endpoint in route.endpoints]
        if not candidates:
            raise GatewayError("Model does not support this endpoint", 400, "unsupported_model_endpoint")
        with self.lock:
            key = (model, endpoint)
            start = self.positions.get(key, 0)
            self.positions[key] = (start + 1) % len(candidates)
        return candidates[start:] + candidates[:start]

    def get_response(self, identifier, digest):
        with self.lock:
            row = self.db.execute(
                "SELECT payload FROM gateway_responses WHERE id = ? AND token = ?", (identifier, digest),
            ).fetchone()
        if row is None:
            raise GatewayError("Response was not found", 404, "response_not_found")
        return json.loads(row[0])

    def record(self, identifier, model, digest, usage, stored=None):
        usage = usage if isinstance(usage, dict) else {}
        prompt = usage.get("prompt_tokens", usage.get("input_tokens", 0))
        completion = usage.get("completion_tokens", usage.get("output_tokens", 0))
        prompt, completion = (value if type(value) is int and 0 <= value < 2**63 else 0
                              for value in (prompt, completion))
        with self.lock, self.db:
            self.db.execute("INSERT INTO gateway_usage VALUES (?, ?, ?, ?, ?, ?)",
                            (identifier, model, digest, int(time.time()), prompt, completion))
            if stored is not None:
                self.db.execute("INSERT INTO gateway_responses VALUES (?, ?, ?)",
                                (stored["id"], digest, json_bytes(stored)))

    def spend_logs(self):
        with self.lock:
            rows = self.db.execute("""
                SELECT id, model, token, at, prompt_tokens, completion_tokens
                FROM gateway_usage ORDER BY at DESC, rowid DESC LIMIT 1000
            """).fetchall()
        return [
            {"request_id": row[0], "model": row[1], "api_key": row[2], "startTime": iso_time(row[3]),
             "prompt_tokens": row[4], "completion_tokens": row[5], "total_tokens": row[4] + row[5], "spend": 0.0}
            for row in rows
        ]


def iso_time(timestamp):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp)) if timestamp is not None else None


def token_count(text):
    return len(re.findall(r"\S+", text))


def input_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(filter(None, map(input_text, value)))
    if isinstance(value, dict):
        return input_text(value.get("content", value.get("text", value.get("output", ""))))
    return ""


def mock_tool(body, deployment):
    choice = body.get("tool_choice", "auto")
    if choice in ("auto", "none"):
        return None
    tools = body.get("tools", [])
    if not isinstance(tools, list):
        raise GatewayError("tools must be an array")
    functions = []
    for entry in tools:
        if isinstance(entry, dict) and entry.get("type") == "function":
            function = entry.get("function", entry)
            if isinstance(function, dict) and isinstance(function.get("name"), str):
                functions.append(function["name"])
    if choice == "required" and functions:
        name = functions[0]
    elif isinstance(choice, dict) and choice.get("type") == "function":
        name = object_value(choice.get("function", choice), "tool_choice").get("name")
        if name not in functions:
            raise GatewayError("Forced tool must be a declared function")
    else:
        raise GatewayError("Unsupported tool_choice or missing function tools")
    arguments = deployment.tool_arguments.get(name, {})
    return name, json_bytes(arguments).decode()


def mock_chat(body, deployment):
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages or any(not isinstance(item, dict) for item in messages):
        raise GatewayError("messages must be a nonempty array of objects")
    text = input_text(messages[-1])
    answer = deployment.response if deployment.response is not None else text
    tool = mock_tool(body, deployment)
    message = {"role": "assistant", "content": answer}
    finish = "stop"
    if tool:
        name, arguments = tool
        message.update(content=None, tool_calls=[
            {"id": new_id("call_"), "type": "function", "function": {"name": name, "arguments": arguments}},
        ])
        answer, finish = arguments, "tool_calls"
    prompt, completion = token_count(input_text(messages)), token_count(answer)
    return {
        "id": new_id("chatcmpl-"), "object": "chat.completion", "created": int(time.time()), "model": body["model"],
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion},
    }


def chat_events(result, include_usage):
    envelope = {key: result[key] for key in ("id", "created", "model")}
    envelope["object"] = "chat.completion.chunk"
    choice = result["choices"][0]
    message = choice["message"]
    chunks = [({"role": "assistant"}, None)]
    if message.get("tool_calls"):
        chunks.append(({"tool_calls": [{"index": 0, **message["tool_calls"][0]}]}, None))
    else:
        chunks.extend(({"content": message["content"][start:start + 32]}, None)
                      for start in range(0, len(message["content"]), 32))
    chunks.append(({}, choice["finish_reason"]))
    for delta, reason in chunks:
        yield sse({**envelope, "choices": [{"index": 0, "delta": delta, "finish_reason": reason}]})
    if include_usage:
        yield sse({**envelope, "choices": [], "usage": result["usage"]})
    yield b"data: [DONE]\n\n"


def mock_embeddings(body):
    values = body.get("input")
    if isinstance(values, str) or (isinstance(values, list) and values and all(type(item) is int for item in values)):
        values = [values]
    if not isinstance(values, list) or not values or len(values) > 2048:
        raise GatewayError("input must be a nonempty string, token array, or batch")
    dimensions, encoding = body.get("dimensions", 16), body.get("encoding_format", "float")
    if type(dimensions) is not int or not 1 <= dimensions <= 1024 or encoding not in ("float", "base64"):
        raise GatewayError("Invalid embedding dimensions or encoding_format")
    result, count = [], 0
    for index, value in enumerate(values):
        if isinstance(value, str):
            count += token_count(value)
        elif isinstance(value, list) and value and all(type(item) is int and item >= 0 for item in value):
            count += len(value)
        else:
            raise GatewayError("Invalid embedding input")
        digest = hashlib.shake_256(json_bytes(value)).digest(dimensions * 2)
        vector = [item / 32767.5 - 1 for (item,) in struct.iter_unpack("<H", digest)]
        if encoding == "base64":
            vector = base64.b64encode(struct.pack(f"<{dimensions}f", *vector)).decode()
        result.append({"object": "embedding", "index": index, "embedding": vector})
    return {"object": "list", "model": body["model"], "data": result,
            "usage": {"prompt_tokens": count, "total_tokens": count}}


def mock_response(body, deployment):
    source = body.get("input")
    if not isinstance(source, (str, list)):
        raise GatewayError("input must be a string or an array")
    text = input_text(source)
    answer = deployment.response if deployment.response is not None else text
    tool = mock_tool(body, deployment)
    if tool:
        name, answer = tool
        output = {"id": new_id("fc_"), "type": "function_call", "call_id": new_id("call_"),
                  "name": name, "arguments": answer, "status": "completed"}
    else:
        output = {"id": new_id("msg_"), "type": "message", "role": "assistant", "status": "completed",
                  "content": [{"type": "output_text", "text": answer, "annotations": []}]}
    prompt, completion = token_count(text), token_count(answer)
    return {
        "id": new_id("resp_"), "object": "response", "created_at": int(time.time()), "status": "completed",
        "model": body["model"], "output": [output], "error": None, "incomplete_details": None,
        "parallel_tool_calls": body.get("parallel_tool_calls", True), "tool_choice": body.get("tool_choice", "auto"),
        "tools": body.get("tools", []), "metadata": body.get("metadata", {}),
        "previous_response_id": body.get("previous_response_id"),
        "usage": {"input_tokens": prompt, "output_tokens": completion, "total_tokens": prompt + completion,
                  "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}},
    }


def sse(payload, event=None):
    prefix = f"event: {event}\n".encode() if event else b""
    return prefix + b"data: " + json_bytes(payload) + b"\n\n"


def response_events(result):
    item = result["output"][0]
    coordinates = {"content_index": 0, "item_id": item["id"], "output_index": 0}
    created = {**result, "status": "in_progress", "output": [], "usage": None}
    events = [("response.created", {"response": created}), ("response.in_progress", {"response": created})]
    if item["type"] == "message":
        part = item["content"][0]
        events.extend([
            ("response.output_item.added", {"output_index": 0, "item": {**item, "status": "in_progress", "content": []}}),
            ("response.content_part.added", {**coordinates, "part": {**part, "text": ""}}),
            ("response.output_text.delta", {**coordinates, "delta": part["text"], "logprobs": []}),
            ("response.output_text.done", {**coordinates, "text": part["text"], "logprobs": []}),
            ("response.content_part.done", {**coordinates, "part": part}),
        ])
    else:
        tool = {key: coordinates[key] for key in ("item_id", "output_index")}
        events.extend([
            ("response.output_item.added", {"output_index": 0, "item": {**item, "status": "in_progress", "arguments": ""}}),
            ("response.function_call_arguments.delta", {**tool, "delta": item["arguments"]}),
            ("response.function_call_arguments.done", {**tool, "name": item["name"], "arguments": item["arguments"]}),
        ])
    events.extend([
        ("response.output_item.done", {"output_index": 0, "item": item}),
        ("response.completed", {"response": result}),
    ])
    for sequence, (kind, fields) in enumerate(events):
        yield sse({"type": kind, "sequence_number": sequence, **fields}, kind)


class Handler(BaseHTTPRequestHandler):
    server_version = "jjap-litellm/1"
    timeout = 30

    def request_json(self):
        size = self.headers.get("Content-Length", "")
        if not size.isascii() or not size.isdecimal() or len(size) > 10 or self.headers.get("Transfer-Encoding"):
            raise GatewayError("Requests require a valid Content-Length")
        count = int(size)
        if count > MAX_BODY:
            raise GatewayError("Request body is too large", 413)
        raw = self.rfile.read(count)
        if len(raw) != count:
            raise GatewayError("Incomplete request body")
        try:
            return object_value(json.loads(raw, parse_constant=invalid_json_constant), "Request body")
        except (ValueError, RecursionError) as exc:
            raise GatewayError("Invalid JSON request body") from exc

    def respond(self, status, payload):
        if not isinstance(payload, bytes):
            payload = json_bytes(payload)
        self.response_started = True
        self.send_response(status)
        if status == 405 and self.path_name == "/_jjap/reset":
            self.send_header("Allow", "POST")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def stream(self, events):
        self.response_started = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for event in events:
            self.wfile.write(event)
            self.wfile.flush()

    def dispatch(self):
        api = self.server.gateway
        url = urlsplit(self.path)
        path = url.path.rstrip("/").removeprefix("/v1/")
        path = self.path_name = "/" + path.lstrip("/")
        method = self.command
        if method == "GET" and path in ("/health/liveliness", "/health/readiness", "/health"):
            self.respond(200, {"status": "healthy"})
            return
        header = self.headers.get("Authorization", "")
        if not header and (key := self.headers.get("x-litellm-api-key")):
            header = key if " " in key else "Bearer " + key
        if path == "/_jjap/reset":
            if api.master_hash:
                api.access(header, admin=True)
            if method != "POST":
                raise GatewayError("Use POST to reset the gateway", 405, "method_not_allowed")
            api.reset()
            self.respond(200, {"reset": True})
            return
        if path in ("/key/generate", "/key/delete", "/spend/logs"):
            api.access(header, admin=True)
            if method == "POST" and path == "/key/generate":
                self.respond(200, api.create_key(self.request_json()))
            elif method == "POST" and path == "/key/delete":
                self.respond(200, api.delete_keys(self.request_json()))
            elif method == "GET" and path == "/spend/logs":
                self.respond(200, api.spend_logs())
            else:
                raise GatewayError("Unsupported method", 405)
            return
        digest, models = api.access(header)
        if method == "GET" and path == "/models":
            self.respond(200, {"object": "list", "data": [
                {"id": name, "object": "model", "created": 0, "owned_by": "litellm"}
                for name in api.routes if models is None or name in models
            ]})
        elif method == "GET" and path.startswith("/responses/"):
            result = api.get_response(unquote(path.split("/", 2)[2]), digest)
            self.respond(200, result)
        elif method == "POST" and path in ENDPOINTS:
            self.infer(api, path, digest, models)
        else:
            raise GatewayError("Endpoint was not found", 404)

    def infer(self, api, path, digest, models):
        body = self.request_json()
        alias = body.get("model")
        stream = body.get("stream", False)
        if type(stream) is not bool or (path == "/embeddings" and stream):
            raise GatewayError("Invalid stream setting")
        deployments = api.model_deployments(alias, path, models)
        if (path == "/responses" and body.get("previous_response_id") is not None and
                len(deployments) > 1 and any(deployment.base is not None for deployment in deployments)):
            raise GatewayError("Stateful upstream Responses requests require a single deployment for the model")
        identifier = new_id("req_")
        for index, deployment in enumerate(deployments):
            if deployment.base is None:
                self.serve_mock(api, deployment, path, digest, body, identifier, stream)
                return
            try:
                status = self.serve_upstream(
                    api, deployment, path, digest, body, identifier, stream, index + 1 < len(deployments),
                )
            except ConnectionRefusedError:
                if index + 1 < len(deployments):
                    continue
                raise GatewayError("Could not connect to the upstream model", 502) from None
            if status:
                return

    def serve_mock(self, api, deployment, path, digest, body, identifier, stream):
        stored = None
        if path == "/responses":
            previous = body.get("previous_response_id")
            if previous is not None:
                if not isinstance(previous, str):
                    raise GatewayError("previous_response_id must be a string")
                api.get_response(previous, digest)
            if type(body.get("store", True)) is not bool:
                raise GatewayError("store must be a boolean")
            result = mock_response(body, deployment)
            if body.get("store", True):
                stored = result
            events = response_events(result)
        elif path == "/chat/completions":
            options = object_value(body.get("stream_options") or {}, "stream_options")
            result = mock_chat(body, deployment)
            events = chat_events(result, options.get("include_usage", False))
        else:
            result = mock_embeddings(body)
        api.record(identifier, body["model"], digest, result.get("usage"), stored)
        if stream:
            self.stream(events)
        else:
            self.respond(200, result)

    def serve_upstream(self, api, deployment, path, digest, body, identifier, stream, has_fallback):
        base = deployment.base
        connection_type = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(base.hostname, base.port, timeout=20)
        headers = {"Content-Type": "application/json", "Accept": "text/event-stream" if stream else "application/json"}
        if deployment.token:
            headers["Authorization"] = "Bearer " + deployment.token
        try:
            connection.request("POST", base.path.rstrip("/") + path, json_bytes({**body, "model": deployment.model}), headers)
            response = connection.getresponse()
            status = response.status
            if has_fallback and status in (429, 502, 503, 504):
                return False
            if status == 200 and stream:
                if "text/event-stream" not in response.getheader("Content-Type", ""):
                    raise GatewayError("Upstream did not return an event stream", 502)
                self.forward_stream(response, api, identifier, body["model"], digest)
                return True
            payload = response.read(MAX_UPSTREAM_BODY + 1)
            if len(payload) > MAX_UPSTREAM_BODY:
                raise GatewayError("Upstream response is too large", 502)
            if not 200 <= status < 300:
                try:
                    error = json.loads(payload)
                    if isinstance(error, dict) and "error" in error and 400 <= status <= 599:
                        self.respond(status, error)
                        return True
                except (ValueError, RecursionError):
                    pass
                raise GatewayError("Upstream model request failed", status if 400 <= status <= 599 else 502)
            try:
                result = object_value(json.loads(payload), "Upstream response")
            except (ValueError, RecursionError) as exc:
                raise GatewayError("Invalid upstream JSON response", 502) from exc
            api.record(identifier, body["model"], digest, result.get("usage"))
            self.respond(status, result)
            return True
        except (http.client.HTTPException, TimeoutError, ConnectionResetError) as exc:
            raise GatewayError("Upstream model connection failed", 502) from exc
        finally:
            connection.close()

    def forward_stream(self, response, api, identifier, model, digest):
        usage = None

        def lines():
            nonlocal usage
            while line := response.readline(MAX_BODY + 1):
                if len(line) > MAX_BODY:
                    break
                if line.startswith(b"data:"):
                    try:
                        event = json.loads(line[5:])
                        if isinstance(event, dict):
                            value = event.get("response", event)
                            if isinstance(value, dict) and value.get("usage") is not None:
                                usage = value["usage"]
                    except (ValueError, RecursionError):
                        pass
                yield line

        try:
            self.stream(lines())
        except (OSError, http.client.HTTPException):
            self.close_connection = True
        api.record(identifier, model, digest, usage)

    def handle_http(self):
        self.response_started = False
        self.path_name = None
        try:
            self.dispatch()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except (ValueError, RecursionError, sqlite3.Error, OSError) as exc:
            if self.response_started:
                self.close_connection = True
                return
            if isinstance(exc, GatewayError):
                message, status, code = str(exc), exc.status, exc.code
            elif isinstance(exc, (ValueError, RecursionError)):
                message, status, code = "Invalid request", 400, None
            elif isinstance(exc, sqlite3.Error):
                message, status, code = "Gateway storage operation failed", 500, None
            else:
                message, status, code = "Gateway operation failed", 502, None
            kind = "server_error" if status >= 500 else "invalid_request_error"
            try:
                self.respond(status, {"error": {"message": message, "type": kind, "param": None, "code": code}})
            except OSError:
                self.close_connection = True

    do_GET = do_POST = handle_http

    def other_method(self):
        if urlsplit(self.path).path.rstrip("/").removeprefix("/v1") == "/_jjap/reset":
            self.handle_http()
        else:
            self.send_error(501, f"Unsupported method ({self.command!r})")

    do_PUT = do_DELETE = do_PATCH = other_method


class Server(ThreadingHTTPServer):
    daemon_threads = False


# LiteLLM proxy configuration and virtual key export


MASTER_KEY_REFERENCE = "os.environ/LITELLM_MASTER_KEY"
MOCK_TEXT = "This is a mock request"
MOCK_DIMENSIONS = 16
# PyYAML rejects these characters unescaped, or folds them as YAML 1.1 line breaks.
YAML_UNPRINTABLE = re.compile("[\x7f-\x9f\u2028\u2029\ud800-\udfff\ufeff\ufffe\uffff]")
GATEWAY_COLUMNS = {
    "gateway_keys": {"token", "alias", "models", "expires"},
    "gateway_usage": {"id", "model", "token", "at", "prompt_tokens", "completion_tokens"},
    "gateway_responses": {"id", "token", "payload"},
}


def yaml_json(value, indent=0):
    """Return JSON that PyYAML's YAML 1.1 loader, used by the LiteLLM proxy, reads back as the same value."""
    pad = " " * (indent + 2)
    if isinstance(value, dict) and value:
        items = []
        for key, item in value.items():
            if len(json.dumps(key)) > 1000:
                raise ValueError("YAML limits mapping keys to 1024 characters")
            items.append(f"{pad}{yaml_json(key)}: {yaml_json(item, indent + 2)}")
        return "{\n" + ",\n".join(items) + "\n" + " " * indent + "}"
    if isinstance(value, list) and any(isinstance(item, (dict, list)) and item for item in value):
        return "[\n" + ",\n".join(pad + yaml_json(item, indent + 2) for item in value) + "\n" + " " * indent + "]"
    if isinstance(value, list):
        return "[" + ", ".join(map(yaml_json, value)) + "]"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("The configuration contains a non-finite number")
        text = repr(value)
        # YAML 1.1 reads a float without a fractional part, such as 1e+20, as a string.
        return text.replace("e", ".0e", 1) if "e" in text and "." not in text else text
    if isinstance(value, str):
        return YAML_UNPRINTABLE.sub(lambda match: f"\\u{ord(match[0]):04x}", json.dumps(value, ensure_ascii=False))
    return json.dumps(value)


def litellm_deployment(row, warnings):
    alias, deployment = Deployment.from_config(row, resolve_credentials=False)
    entry = json.loads(json.dumps(row))
    params = entry["litellm_params"]
    # jjap sends every upstream request in OpenAI format with the model id after any openai/ prefix.
    params["model"] = "openai/" + deployment.model
    if deployment.base is not None:
        # LiteLLM's OpenAI client requires a key; jjap sends no Authorization header without one.
        params.setdefault("api_key", "none")
        for field in ("mock_response", "mock_tool_arguments"):
            if params.pop(field, None) is not None:
                warnings.append(f"removed {field} from upstream deployment {alias!r}, which jjap ignores there")
        return entry
    if params.pop("mock_tool_arguments", None):
        warnings.append(f"dropped mock_tool_arguments of {alias!r}; LiteLLM mocks cannot answer forced tool calls")
    if deployment.endpoints == ("/embeddings",):
        params["mock_response"] = [0.0] * MOCK_DIMENSIONS
        warnings.append(f"mock deployment {alias!r} returns one constant {MOCK_DIMENSIONS}-dimension embedding "
                        "in LiteLLM instead of a vector per input")
        return entry
    if "/embeddings" in deployment.endpoints:
        warnings.append(f"mock deployment {alias!r} cannot serve embeddings in LiteLLM alongside text responses")
    if deployment.response is None:
        params["mock_response"] = MOCK_TEXT
        warnings.append(f"mock deployment {alias!r} echoes input in jjap; LiteLLM returns {MOCK_TEXT!r}")
    return entry


def litellm_config(config, warnings):
    config = object_value(config, "Settings")
    models = config.get("model_list")
    if not isinstance(models, list) or not models:
        raise GatewayError("Settings require a nonempty model_list")
    result = json.loads(json.dumps(config, allow_nan=False))
    result["model_list"] = [litellm_deployment(row, warnings) for row in models]
    general = result.get("general_settings", {})
    if not isinstance(general, dict):
        raise GatewayError("general_settings must be an object")
    if general.get("master_key", MASTER_KEY_REFERENCE) != MASTER_KEY_REFERENCE:
        warnings.append(f"replaced general_settings.master_key with {MASTER_KEY_REFERENCE}")
    result["general_settings"] = {**general, "master_key": MASTER_KEY_REFERENCE}
    return result


def read_gateway_snapshot(filename):
    path = Path(filename).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"LiteLLM test server database not found: {path}")
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, isolation_level=None)
    try:
        try:
            connection.execute("BEGIN")
            for table, columns in GATEWAY_COLUMNS.items():
                if not columns <= {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}:
                    raise ValueError
            keys = connection.execute("SELECT token, alias, models, expires FROM gateway_keys ORDER BY rowid").fetchall()
            usage, responses = connection.execute(
                "SELECT (SELECT count(*) FROM gateway_usage), (SELECT count(*) FROM gateway_responses)").fetchone()
        except (ValueError, sqlite3.DatabaseError) as exc:
            raise ValueError(f"{path} is not a LiteLLM test server SQLite persistence file") from exc
    finally:
        connection.close()
    return keys, usage, responses


def sql_text(value):
    if "\0" in value:
        raise ValueError("PostgreSQL text cannot contain NUL characters")
    return "'" + value.replace("'", "''") + "'"


def verification_token_sql(keys):
    """Insert statements for LiteLLM_VerificationToken in the LiteLLM proxy's Prisma schema on PostgreSQL."""
    lines = ["-- Virtual keys exported from the jjap LiteLLM test server.",
             "-- token holds SHA-256 hex digests of the sk- keys, which is LiteLLM's hash_token, so clients keep their keys.",
             "BEGIN;"]
    for token, alias, models, expires in keys:
        models = json.loads(models) if isinstance(models, str) else None
        if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{64}", token):
            raise ValueError("A stored key is not a SHA-256 hex digest compatible with LiteLLM's hash_token")
        if not isinstance(models, list) or any(not isinstance(model, str) for model in models):
            raise ValueError("A stored key has an invalid model list")
        if alias is not None and not isinstance(alias, str) or expires is not None and type(expires) not in (int, float):
            raise ValueError("A stored key has an invalid alias or expiry")
        array = "ARRAY[" + ", ".join(map(sql_text, models)) + "]::TEXT[]" if models else "'{}'::TEXT[]"
        moment = "NULL"
        if expires is not None:
            moment = datetime.datetime.fromtimestamp(expires, datetime.timezone.utc).replace(tzinfo=None)
            moment = f"TIMESTAMP '{moment.isoformat(' ', 'milliseconds')}'"
        lines.append('INSERT INTO "LiteLLM_VerificationToken" ("token", "key_alias", "models", "expires")')
        lines.append(f"VALUES ({sql_text(token)}, {'NULL' if alias is None else sql_text(alias)}, {array}, {moment})")
        lines.append('ON CONFLICT ("token") DO NOTHING;')
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def export_main(argv):
    parser = argparse.ArgumentParser(prog="litellm_server.py export",
                                     description="Write a LiteLLM proxy config.yaml and the virtual keys as SQL.")
    parser.add_argument("--config", type=Path, help="JSON file containing model_list (default: the mock models)")
    parser.add_argument("--db-file", type=Path, help="existing SQLite persistence file with virtual keys")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    args = parser.parse_args(argv)
    destination = args.out.expanduser().absolute()
    warnings, keys = [], []
    try:
        if os.path.lexists(destination):
            raise FileExistsError(f"Output directory already exists: {destination}")
        config = json.loads(args.config.read_text(encoding="utf-8")) if args.config else DEFAULT_MODELS
        settings = litellm_config(config, warnings)
        if args.db_file is not None:
            keys, usage, responses = read_gateway_snapshot(args.db_file)
            if usage or responses:
                warnings.append(f"skipped {usage} usage records and {responses} stored responses; LiteLLM_SpendLogs "
                                "needs the call type and timing jjap does not record, and responses are not portable")
        files = {"config.yaml": "# LiteLLM proxy configuration exported from the jjap LiteLLM test server.\n"
                                + yaml_json(settings) + "\n"}
        if keys:
            files["keys.sql"] = verification_token_sql(keys)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".litellm-export-", dir=destination.parent) as temporary:
            staged = Path(temporary) / "litellm"
            staged.mkdir(mode=0o700)
            for name, text in files.items():
                with (staged / name).open("x", encoding="utf-8") as file:
                    file.write(text)
            if os.name == "nt":
                staged.rename(destination)
            else:
                destination.mkdir(mode=0o700)
                try:
                    staged.replace(destination)
                except BaseException:
                    destination.rmdir()
                    raise
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"LiteLLM export failed: {exc}\n")
    for warning in warnings:
        print(f"LiteLLM export warning: {warning}", file=sys.stderr)
    print(f"Exported {len(settings['model_list'])} deployments to config.yaml"
          + (f" and {len(keys)} virtual keys to keys.sql" if keys else "") + f" in {destination}")
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["export"]:
        return export_main(argv[1:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4000)
    parser.add_argument("--db-file", type=Path, help="SQLite path for keys, response fixtures, and usage")
    parser.add_argument("--config", type=Path, help="JSON file containing model_list")
    parser.add_argument("--password-file", type=Path, help="File containing the optional master API key")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    gateway = None
    try:
        config = json.loads(args.config.read_text(encoding="utf-8")) if args.config else DEFAULT_MODELS
        master = args.password_file.read_text(encoding="utf-8").rstrip("\r\n") if args.password_file else None
        if master is not None and not master:
            parser.error("--password-file must contain a nonempty master key")
        if args.db_file:
            args.db_file.parent.mkdir(parents=True, exist_ok=True)
        gateway = Gateway(config, args.db_file or ":memory:", master)
        with Server((args.host, args.port), Handler) as server:
            server.gateway = gateway
            print(f"LiteLLM test server listening on http://{args.host}:{server.server_port}", flush=True)
            server.serve_forever()
    except (ValueError, sqlite3.Error, OSError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    except KeyboardInterrupt:
        pass
    finally:
        if gateway is not None:
            gateway.db.close()


if __name__ == "__main__":
    raise SystemExit(main())
