#!/usr/bin/env python3
"""Move data between the jjap servers and the real products in Docker, then compare both sides.

Each check builds a fixture, exports or migrates it into the real product, reads it back, and where the server
has an import command, imports the real product's own dump back into a new jjap file. Run one check per call:

    python3 .github/scripts/real_products.py mongo|neo4j|kafka|opensearch|iceberg|nexus
"""

import argparse
import base64
import contextlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMAGES = {
    "mongo": "mongo:8.0.38",
    "neo4j": "neo4j:5.26.34-community",
    "kafka": "apache/kafka:4.4.0",
    "opensearch": "opensearchproject/opensearch:2.19.7",
    "iceberg": "apache/iceberg-rest-fixture:1.10.1",
    "nexus": "sonatype/nexus3:3.76.1",
}


def run(*command, cwd=None, check=True, timeout=600):
    print("+", " ".join(map(str, command)), flush=True)
    result = subprocess.run([str(part) for part in command], cwd=cwd, text=True, capture_output=True, timeout=timeout)
    if result.stderr.strip():
        print(result.stderr.strip()[-4000:], file=sys.stderr, flush=True)
    if check and result.returncode:
        raise AssertionError(f"{command[0]} exited with {result.returncode}: {result.stdout[-2000:]}")
    return result.stdout


def jjap(script, *args, **kwargs):
    return run(sys.executable, "-B", ROOT / script, *args, **kwargs)


@contextlib.contextmanager
def jjap_server(script, *args):
    """Start a jjap server on a free port and stop it with SIGTERM, which lets it persist its state."""
    command = [sys.executable, "-B", "-u", str(ROOT / script), "--port", "0", *map(str, args)]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
        try:
            for line in process.stdout:
                print(f"[{script}] {line.rstrip()}", flush=True)
                if match := re.search(r"127\.0\.0\.1:(\d+)", line):
                    break
            else:
                raise AssertionError(f"{script} exited during startup")
            yield int(match[1])
        finally:
            process.terminate()
            process.wait(timeout=30)


@contextlib.contextmanager
def container(name, image, *options, command=()):
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    run("docker", "run", "-d", "--name", name, *options, image, *command)
    try:
        yield name
    except BaseException:
        subprocess.run(["docker", "logs", "--tail", "200", name])
        raise
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def wait_until(what, probe, timeout=300):
    deadline = time.monotonic() + timeout
    while True:
        try:
            if probe():
                return
        except (OSError, AssertionError, ValueError, urllib.error.URLError):
            pass
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out waiting for {what}")
        time.sleep(2)


def http(method, url, body=None, headers=None, auth=None):
    data = body if isinstance(body, bytes) or body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=dict(headers or {}))
    if data is not None and "Content-Type" not in request.headers:
        request.add_header("Content-Type", "application/json")
    if auth:
        request.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        payload, status = error.read(), error.code
    try:
        return status, json.loads(payload) if payload else None
    except ValueError:
        return status, payload


def expect(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"ok: {label}", flush=True)


def port_open(port):
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0


# MongoDB: export → mongorestore, mongodump --archive → import

def check_mongo(work):
    source = work / "mongo.sqlite3"
    documents = [
        {"_id": 1, "name": "first", "count": {"$numberLong": "12"}, "price": {"$numberDecimal": "13.50"},
         "when": {"$date": {"$numberLong": "1700000000000"}}, "tags": ["a", "b"], "blob": {"$binary": {"base64": "AP8=", "subType": "00"}}},
        {"_id": 2, "name": "second", "nested": {"ratio": 1.5, "flag": True}},
    ]

    def shell(port, command):
        reply = json.loads(jjap("mongo_server.py", "shell", "--port", port, "-d", "app", "--canonical", "-e", json.dumps(command)))
        if reply.get("ok") not in (1, 1.0, {"$numberInt": "1"}, {"$numberDouble": "1.0"}):
            raise AssertionError(f"MongoDB command failed: {reply}")
        return reply

    def snapshot(port):
        items = shell(port, {"find": "items", "sort": {"_id": 1}})["cursor"]["firstBatch"]
        indexes = sorted(index["name"] for index in shell(port, {"listIndexes": "items"})["cursor"]["firstBatch"])
        views = [item["name"] for item in shell(port, {"listCollections": 1, "filter": {"type": "view"}})["cursor"]["firstBatch"]]
        return items, indexes, views

    with jjap_server("mongo_server.py", "--db-file", source) as port:
        shell(port, {"insert": "items", "documents": documents})
        shell(port, {"createIndexes": "items", "indexes": [{"key": {"name": 1}, "name": "name_1", "unique": True}]})
        shell(port, {"create": "recent", "viewOn": "items", "pipeline": [{"$match": {"_id": {"$gt": 1}}}]})
        expected = snapshot(port)
    dump = work / "dump"
    jjap("mongo_server.py", "export", "--db-file", source, "--out", dump)
    with container("jjap-mongo", IMAGES["mongo"], "--network", "host", "-v", f"{work}:/work",
                   command=("--port", "27018", "--bind_ip", "127.0.0.1")):
        wait_until("mongod", lambda: port_open(27018))
        run("docker", "exec", "jjap-mongo", "mongorestore", "--port", "27018", "/work/dump")
        expect("mongorestore of the export", snapshot(27018), expected)
        user = f"{os.getuid()}:{os.getgid()}"
        run("docker", "exec", "--user", user, "jjap-mongo", "mongodump", "--port", "27018", "--db", "app",
            "--archive=/work/app.archive")
        run("docker", "exec", "--user", user, "jjap-mongo", "mongodump", "--port", "27018", "--db", "app", "--gzip",
            "--out", "/work/realdump")
    for label, options in (("archive", ("--archive", work / "app.archive")), ("gzip dump", ("--dump", work / "realdump"))):
        imported = work / f"imported-{label.replace(' ', '-')}.sqlite3"
        jjap("mongo_server.py", "import", "--db-file", imported, *options)
        with jjap_server("mongo_server.py", "--db-file", imported) as port:
            expect(f"import of a real mongodump {label}", snapshot(port), expected)


# Neo4j: export → neo4j-admin import and cypher-shell, APOC export → import

NEO4J_FIXTURE = """
import runpy, sys
neo4j = runpy.run_path(sys.argv[1])
S, graph = neo4j["Structure"], neo4j["Graph"]()
ann = graph.add_node({"Person", "Admin"}, {"name": "Ann", "age": 41, "score": 1.5, "tags": ["a;b", "c"], "active": True,
    "born": S(0x44, (3685,)), "seen": S(0x49, (1700000000, 123000000, 3600)), "span": S(0x45, (14, 3, 5, 0)),
    "home": S(0x58, (4326, 12.5, 55.25)), "zone": S(0x69, (1700000000, 0, "Europe/Paris")), "clock": S(0x54, (3723000000001, 0))})
bob = graph.add_node({"Person"}, {"name": "Bob", "age": "unknown"})
graph.add_edge(ann, bob, "KNOWS", {"since": 2020})
graph.add_edge(ann, ann, "SELF", {})
graph.schema["person_name"] = {"name": "person_name", "kind": "constraint", "label": "Person", "properties": ["name"],
                               "unique": True, "required": False}
graph.schema["person_age"] = {"name": "person_age", "kind": "index", "label": "Person", "properties": ["age"],
                              "unique": False, "required": False}
store = neo4j["Store"](sys.argv[2]); store.commit(graph, 0, True); store.close()
"""
NEO4J_TYPES = ("MATCH (n:Person {name: 'Ann'}) RETURN valueType(n.age), valueType(n.score), valueType(n.tags), "
               "valueType(n.born), valueType(n.seen), valueType(n.span), valueType(n.home), valueType(n.zone), "
               "valueType(n.clock), toString(n.seen), toString(n.zone), toString(n.span), n.tags")


def cypher(name, query, port):
    return run("docker", "exec", name, "cypher-shell", "-a", f"bolt://127.0.0.1:{port}", "--format", "plain", query)


def neo4j_summary(name, port):
    return {
        "counts": cypher(name, "MATCH (n) WITH count(n) AS nodes MATCH ()-[r]->() RETURN nodes, count(r)", port),
        "labels": cypher(name, "MATCH (n) RETURN n.name, labels(n) ORDER BY n.name", port),
        "types": cypher(name, NEO4J_TYPES, port),
        "constraints": cypher(name, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name", port),
        "indexes": cypher(name, "SHOW INDEXES YIELD name, type WHERE type = 'RANGE' RETURN name ORDER BY name", port),
        "leftovers": cypher(name, "MATCH (n:__jjap_export) RETURN count(n)", port),
    }


def check_neo4j(work):
    source = work / "graph.sqlite3"
    run(sys.executable, "-B", "-c", NEO4J_FIXTURE, ROOT / "neo4j_server.py", source)
    jjap("neo4j_server.py", "export", "--db-file", source, "--out", work / "cypher")
    jjap("neo4j_server.py", "export", "--db-file", source, "--out", work / "csv", "--format", "csv")
    summaries = {}
    for label, port in (("csv", 17687), ("cypher", 17688)):
        name = volume = f"jjap-neo4j-{label}"
        subprocess.run(["docker", "volume", "rm", "-f", volume], capture_output=True)
        try:
            if label == "csv":
                run("docker", "run", "--rm", "-v", f"{volume}:/data", "-v", f"{work}:/work:ro", "-w", "/work/csv",
                    "--entrypoint", "neo4j-admin", IMAGES["neo4j"], "database", "import", "full", "neo4j",
                    "--report-file=/tmp/import.report", "@import.args")
            options = ["--network", "host", "-v", f"{volume}:/data", "-v", f"{work}:/work",
                       "-e", "NEO4J_AUTH=none", "-e", 'NEO4J_PLUGINS=["apoc"]', "-e", "NEO4J_server_directories_import=/work",
                       "-e", "NEO4J_apoc_export_file_enabled=true", "-e", "APOC_EXPORT_FILE_ENABLED=true",
                       "-e", f"NEO4J_server_bolt_listen__address=127.0.0.1:{port}",
                       "-e", f"NEO4J_server_http_listen__address=127.0.0.1:{port + 1000}"]
            with container(name, IMAGES["neo4j"], *options):
                wait_until("Neo4j", lambda: cypher(name, "RETURN 1", port) is not None, timeout=300)
                script = "/work/csv/schema.cypher" if label == "csv" else "/work/cypher/neo4j.cypher"
                run("docker", "exec", name, "cypher-shell", "-a", f"bolt://127.0.0.1:{port}", "-f", script)
                summaries[label] = neo4j_summary(name, port)
                if label == "cypher":
                    cypher(name, "CALL apoc.export.cypher.all('apoc.cypher', {format: 'cypher-shell', "
                                 "useOptimizations: {type: 'UNWIND_BATCH', unwindBatchSize: 20}})", port)
        finally:
            subprocess.run(["docker", "volume", "rm", "-f", volume], capture_output=True)
    expect("neo4j-admin import of the CSV export matches cypher-shell of the Cypher export",
           summaries["csv"], summaries["cypher"])
    types = summaries["cypher"]["types"].splitlines()[1]
    for kind in ("INTEGER NOT NULL", "FLOAT NOT NULL", "LIST<STRING NOT NULL> NOT NULL", "DATE NOT NULL",
                 "ZONED DATETIME NOT NULL", "DURATION NOT NULL", "POINT NOT NULL", "ZONED TIME NOT NULL"):
        if kind not in types:
            raise AssertionError(f"Neo4j property types are missing {kind}: {types}")
    expect("no temporary import key left", summaries["cypher"]["leftovers"].splitlines()[1], "0")
    imported = work / "from-apoc.sqlite3"
    jjap("neo4j_server.py", "import", "--db-file", imported, "--cypher", work / "apoc.cypher")
    restored = json.loads(run(sys.executable, "-B", "-c", (
        "import json, runpy, sys\n"
        "neo4j = runpy.run_path(sys.argv[1])\n"
        "graph = neo4j['read_graph_snapshot'](sys.argv[2])\n"
        "print(json.dumps(sorted([sorted(n.labels), sorted(n.props)] for n in graph.nodes.values())))"),
        ROOT / "neo4j_server.py", imported))
    expect("import of a real apoc.export.cypher script", restored,
           [[["Admin", "Person"], ["active", "age", "born", "clock", "home", "name", "score", "seen", "span", "tags", "zone"]],
            [["Person"], ["age", "name"]]])


# Kafka: real cluster A → import → migrate → real cluster B

KAFKA_ENV = ("KAFKA_NODE_ID=1", "KAFKA_PROCESS_ROLES=broker,controller", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
             "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
             "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
             "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0")


def kafka_options(port):
    environment = [*KAFKA_ENV, f"KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{port + 1}",
                   f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
                   f"KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:{port + 1}"]
    return ["--network", "host", *(item for pair in (("-e", value) for value in environment) for item in pair)]


def kafka_tool(name, tool, *args, check=True):
    return run("docker", "exec", "-i", name, f"/opt/kafka/bin/{tool}", *args, check=check)


def kafka_records(name, port, topic):
    output = kafka_tool(name, "kafka-console-consumer.sh", "--bootstrap-server", f"127.0.0.1:{port}", "--topic", topic,
                        "--from-beginning", "--timeout-ms", "15000", "--property", "print.key=true",
                        "--property", "print.headers=true", "--property", "print.timestamp=true",
                        "--property", "print.partition=true", "--property", "print.offset=true", check=False)
    return sorted(line for line in output.splitlines() if line.startswith("CreateTime"))


def kafka_group(name, port, group):
    output = kafka_tool(name, "kafka-consumer-groups.sh", "--bootstrap-server", f"127.0.0.1:{port}",
                        "--describe", "--group", group)
    rows = [line.split() for line in output.splitlines() if line.startswith(group)]
    return sorted((row[1], row[2], row[3]) for row in rows)


def check_kafka(work):
    with container("jjap-kafka-a", IMAGES["kafka"], *kafka_options(19092)), \
            container("jjap-kafka-b", IMAGES["kafka"], *kafka_options(19094)):
        for name, port in (("jjap-kafka-a", 19092), ("jjap-kafka-b", 19094)):
            wait_until("Kafka", lambda: kafka_tool(name, "kafka-topics.sh", "--bootstrap-server",
                                                   f"127.0.0.1:{port}", "--list") is not None)
        kafka_tool("jjap-kafka-a", "kafka-topics.sh", "--bootstrap-server", "127.0.0.1:19092", "--create",
                   "--topic", "events", "--partitions", "3", "--config", "retention.ms=604800000")
        lines = "".join(f"source:jjap,n:{index}\tkey-{index % 4}\tvalue {index} é\n" for index in range(60))
        process = subprocess.run(
            ["docker", "exec", "-i", "jjap-kafka-a", "/opt/kafka/bin/kafka-console-producer.sh",
             "--bootstrap-server", "127.0.0.1:19092", "--topic", "events", "--property", "parse.key=true",
             "--property", "key.separator=\t", "--property", "parse.headers=true",
             "--property", "headers.delimiter=\t", "--property", "headers.separator=,",
             "--property", "headers.key.separator=:"], input=lines, text=True, capture_output=True, timeout=120)
        if process.returncode:
            raise AssertionError(process.stderr)
        kafka_tool("jjap-kafka-a", "kafka-console-consumer.sh", "--bootstrap-server", "127.0.0.1:19092",
                   "--topic", "events", "--group", "readers", "--from-beginning", "--max-messages", "25",
                   "--timeout-ms", "20000")
        source = kafka_records("jjap-kafka-a", 19092, "events")
        expect("records produced", len(source), 60)
        imported = work / "kafka.sqlite3"
        jjap("kafka_server.py", "import", "--db-file", imported, "--bootstrap-server", "127.0.0.1:19092")
        jjap("kafka_server.py", "migrate", "--db-file", imported, "--bootstrap-server", "127.0.0.1:19094")
        expect("records after import and migrate", kafka_records("jjap-kafka-b", 19094, "events"), source)
        expect("committed group offsets", kafka_group("jjap-kafka-b", 19094, "readers"),
               kafka_group("jjap-kafka-a", 19092, "readers"))
        configs = kafka_tool("jjap-kafka-b", "kafka-configs.sh", "--bootstrap-server", "127.0.0.1:19094",
                             "--describe", "--entity-type", "topics", "--entity-name", "events")
        if "retention.ms=604800000" not in configs:
            raise AssertionError(f"Topic configs were not migrated: {configs}")


# OpenSearch: export → migrate into a cluster, --source import back

def check_opensearch(work):
    source = work / "search.sqlite3"
    bulk = "".join(json.dumps(line) + "\n" for index in range(40) for line in (
        {"index": {"_index": "notes", "_id": str(index)}},
        {"title": f"note {index}", "rank": index, "tags": ["even" if index % 2 == 0 else "odd"]}))
    with jjap_server("opensearch_server.py", "--db-file", source) as port:
        base = f"http://127.0.0.1:{port}"
        http("PUT", f"{base}/notes", {"mappings": {"properties": {"title": {"type": "text"}, "rank": {"type": "integer"},
                                                                     "tags": {"type": "keyword"}}},
                                       "aliases": {"current": {"filter": {"term": {"tags": "even"}}}}})
        status, reply = http("POST", f"{base}/_bulk?refresh=true", bulk.encode(), {"Content-Type": "application/x-ndjson"})
        expect("jjap bulk load", (status, reply["errors"]), (200, False))
    options = ["--network", "host", "-e", "discovery.type=single-node", "-e", "DISABLE_SECURITY_PLUGIN=true",
               "-e", "DISABLE_INSTALL_DEMO_CONFIG=true", "-e", "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
               "-e", "http.port=19200", "-e", "transport.port=19300"]
    with container("jjap-opensearch", IMAGES["opensearch"], *options):
        target = "http://127.0.0.1:19200"
        wait_until("OpenSearch", lambda: http("GET", f"{target}/_cluster/health")[0] == 200)
        jjap("opensearch_server.py", "migrate", "--db-file", source, "--target", target)
        expect("document count", http("GET", f"{target}/notes/_count")[1]["count"], 40)
        expect("alias filter", http("GET", f"{target}/current/_count")[1]["count"], 20)
        mapping = http("GET", f"{target}/notes/_mapping")[1]["notes"]["mappings"]["properties"]
        expect("mapping", {key: value["type"] for key, value in mapping.items()},
               {"title": "text", "rank": "integer", "tags": "keyword"})
        search = {"query": {"range": {"rank": {"gte": 35}}}, "sort": [{"rank": "asc"}]}
        hits = [hit["_id"] for hit in http("POST", f"{target}/notes/_search", search)[1]["hits"]["hits"]]
        expect("range search", hits, ["35", "36", "37", "38", "39"])
        imported = work / "imported.sqlite3"
        jjap("opensearch_server.py", "import", "--db-file", imported, "--source", target)
    with jjap_server("opensearch_server.py", "--db-file", imported) as port:
        base = f"http://127.0.0.1:{port}"
        expect("count after --source import", http("GET", f"{base}/notes/_count")[1]["count"], 40)
        expect("alias after --source import", http("GET", f"{base}/current/_count")[1]["count"], 20)
        hits = [hit["_id"] for hit in http("POST", f"{base}/notes/_search", search)[1]["hits"]["hits"]]
        expect("range search after --source import", hits, ["35", "36", "37", "38", "39"])


# Iceberg: migrate (register) into the Iceberg REST fixture, --source import back

def check_iceberg(work):
    warehouse = work / "warehouse"
    schema = work / "schema.json"
    schema.write_text(json.dumps([{"id": 1, "name": "id", "required": True, "type": "long"},
                                  {"id": 2, "name": "label", "required": False, "type": "string"}]), encoding="utf-8")
    jjap("iceberg_server.py", "--warehouse", warehouse, "create-table", "demo.events", "--schema", schema)
    rows = "".join(json.dumps({"id": index, "label": f"row {index}"}) + "\n" for index in range(5))
    process = subprocess.run([sys.executable, "-B", str(ROOT / "iceberg_server.py"), "--warehouse", str(warehouse),
                              "append", "demo.events", "-"], input=rows, text=True, capture_output=True)
    if process.returncode:
        raise AssertionError(process.stderr)
    expected = jjap("iceberg_server.py", "--warehouse", warehouse, "scan", "demo.events")
    options = ["--network", "host", "-v", f"{work}:{work}", "-e", f"CATALOG_WAREHOUSE={warehouse.as_uri()}",
               "--user", f"{os.getuid()}:{os.getgid()}"]
    with container("jjap-iceberg", IMAGES["iceberg"], *options):
        target = "http://127.0.0.1:8181"
        wait_until("Iceberg REST fixture", lambda: http("GET", f"{target}/v1/config")[0] == 200)
        jjap("iceberg_server.py", "--warehouse", warehouse, "migrate", "--target", target)
        status, table = http("GET", f"{target}/v1/namespaces/demo/tables/events")
        expect("registered table", status, 200)
        expect("current snapshot rows", table["metadata"]["snapshots"][-1]["summary"]["total-records"], "5")
        imported = work / "imported.sqlite3"
        jjap("iceberg_server.py", "--warehouse", warehouse, "--db-file", imported, "import", "--source", target)
    expect("scan after --source import",
           jjap("iceberg_server.py", "--warehouse", warehouse, "--db-file", imported, "scan", "demo.events"), expected)


# Nexus Repository: migrate into Nexus, --source import back

def check_nexus(work):
    source = work / "nexus.sqlite3"
    payload = bytes(range(256)) * 64
    with jjap_server("nexus_server.py", "--db-file", source) as port:
        base = f"http://127.0.0.1:{port}"
        for path in ("fixtures/data.bin", "docs/readme ü.txt"):
            expect(f"jjap upload {path}", http("PUT", f"{base}/repository/raw-hosted/{urllib.request.quote(path)}",
                                              payload)[0], 201)
        pom = b"<project><modelVersion>4.0.0</modelVersion><groupId>org.example</groupId>" \
              b"<artifactId>demo</artifactId><version>1.0</version></project>"
        for name, data in (("demo-1.0.jar", payload), ("demo-1.0.pom", pom)):
            expect(f"jjap upload {name}", http("PUT", f"{base}/repository/maven-releases/org/example/demo/1.0/{name}",
                                              data)[0], 201)
    options = ["-p", "127.0.0.1:18081:8081", "-e", "INSTALL4J_ADD_VM_PARAMS=-Xms1g -Xmx1g -XX:MaxDirectMemorySize=1g"]
    with container("jjap-nexus", IMAGES["nexus"], *options):
        target = "http://127.0.0.1:18081"
        wait_until("Nexus", lambda: http("GET", f"{target}/service/rest/v1/status/writable")[0] == 200, timeout=600)
        password = work / "admin.password"
        wait_until("the Nexus admin password", lambda: run("docker", "exec", "jjap-nexus", "cat",
                                                           "/nexus-data/admin.password").strip())
        password.write_text(run("docker", "exec", "jjap-nexus", "cat", "/nexus-data/admin.password").strip(),
                            encoding="utf-8")
        jjap("nexus_server.py", "migrate", "--db-file", source, "--target", target, "--username", "admin",
             "--password-file", password)
        auth = f"admin:{password.read_text(encoding='utf-8')}"
        for path in ("raw-hosted/fixtures/data.bin", "maven-releases/org/example/demo/1.0/demo-1.0.jar"):
            status, body = http("GET", f"{target}/repository/{path}", auth=auth)
            expect(f"Nexus serves {path}", (status, body), (200, payload))
        imported = work / "imported.sqlite3"
        jjap("nexus_server.py", "import", "--db-file", imported, "--source", target, "--username", "admin",
             "--password-file", password, "--repository", "raw-hosted", "--repository", "maven-releases")
    with jjap_server("nexus_server.py", "--db-file", imported) as port:
        status, body = http("GET", f"http://127.0.0.1:{port}/repository/raw-hosted/docs/{urllib.request.quote('readme ü.txt')}")
        expect("content after --source import", (status, body), (200, payload))


CHECKS = {"mongo": check_mongo, "neo4j": check_neo4j, "kafka": check_kafka, "opensearch": check_opensearch,
          "iceberg": check_iceberg, "nexus": check_nexus}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("product", choices=sorted(CHECKS))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix=f"jjap-{args.product}-", ignore_cleanup_errors=True) as directory:
        work = Path(directory)
        work.chmod(0o777)
        CHECKS[args.product](work)
    print(f"{args.product}: all checks passed")


if __name__ == "__main__":
    main()
