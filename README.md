# jjap

Single-file builds of Pi, OpenCode, and Hermes, plus Python servers for local integration tests.

## Standalone builds

The single-file builds are in [`standalone/`](standalone/).

| Build | Files | Runtime |
| --- | --- | --- |
| Pi 1.3.0 preview.2 | [`pi-standalone.mjs`](standalone/pi-standalone.mjs) | Node.js 22.19 or later |
| OpenCode 2.1.2-standalone.1 | [Full](standalone/opencode-standalone.js) · [Lite](standalone/opencode-standalone-lite.js) | Node.js 26.4 or later |
| Hermes | [Full .py](standalone/hermes-full.py) · [Lite .py](standalone/hermes-lite.py) · [Full .pyz](standalone/hermes-full.pyz) · [Lite .pyz](standalone/hermes-lite.pyz) | CPython 3.14 or later |

Clone the repository.

```sh
git clone https://github.com/seo-rii/jjap.git
cd jjap
```

The full Hermes files use Git LFS. Install Git LFS and run `git lfs pull` in the clone to retrieve them. [Git LFS](https://git-lfs.com/)

Run the build you want from the repository directory.

```sh
node standalone/pi-standalone.mjs
node standalone/opencode-standalone.js
python3.14 -I -S standalone/hermes-lite.py
```

To verify all eight files, run `sha256sum --check SHA256SUMS` from `standalone/`. [Checksums](standalone/SHA256SUMS)

- The full OpenCode build includes the web UI. The lite build provides the CLI, TUI, and HTTP API.
- The full Hermes build includes local Piper voice models, dictionaries, and the full Google API discovery cache. The lite build bundles one Google API discovery document for Google Chat. The `.py` and `.pyz` builds accept the same options.

## Server quick start

The servers run with Python 3.10 or later and the standard library.

From the repository directory, start the MongoDB test server on `127.0.0.1:27017`.

```sh
python3 mongo_server.py --db-file test.sqlite3
```

In a second terminal, run a ping from the repository directory.

```sh
python3 mongo_server.py shell --eval '{"ping": 1}'
```

## Test servers

| Server | Default endpoint | Source |
| --- | --- | --- |
| MongoDB | `mongodb://127.0.0.1:27017` | [`mongo_server.py`](mongo_server.py) |
| Neo4j | `bolt://127.0.0.1:7687` | [`neo4j_server.py`](neo4j_server.py) |
| Redis | `redis://127.0.0.1:6379` | [`redis_server.py`](redis_server.py) |
| Nexus Repository | `http://127.0.0.1:8081` | [`nexus_server.py`](nexus_server.py) |
| Apache Iceberg | `http://127.0.0.1:8181` | [`iceberg_server.py`](iceberg_server.py) |
| Apache Polaris | `http://127.0.0.1:8181/api/catalog` | [`iceberg_server.py serve-polaris`](iceberg_server.py) |
| OpenLineage | `http://127.0.0.1:5000` | [`openlineage_server.py`](openlineage_server.py) |
| LiteLLM | `http://127.0.0.1:4000/v1` | [`litellm_server.py`](litellm_server.py) |
| OpenSearch | `http://127.0.0.1:9200` | [`opensearch_server.py`](opensearch_server.py) |
| Apache Airflow | `http://127.0.0.1:8080/api/v2` | [`airflow_server.py`](airflow_server.py) |
| OpenMetadata | `http://127.0.0.1:8585/api/v1` | [`openmetadata_server.py`](openmetadata_server.py) |

The servers bind to loopback by default. Use `--port 0` to pick an available port and read it from the startup message. MongoDB, Neo4j, Redis, and Nexus Repository accept `--password-file` for their test accounts. These four servers keep data in memory by default, and the examples use `--db-file` to persist it.

### MongoDB

Connect with a MongoDB client or use the built-in `shell` command.

Run `export` to capture a committed SQLite snapshot. It writes `.bson` and `.metadata.json` files into a new directory for `mongorestore`. The dump preserves BSON types, collection options, index definitions, and views. Use `--db` to select one database. [MongoDB dump format](https://www.mongodb.com/docs/database-tools/mongodump/mongodump-behavior/)

```sh
python3 mongo_server.py export --db-file test.sqlite3 --out ./dump
```

To export a WiredTiger `dbPath`, install `mongod` and `mongorestore` on a system with Unix domain sockets. Open the result with the same version of `mongod` that created it. Use `--mongod` and `--mongorestore` to specify executable paths.

```sh
python3 mongo_server.py export --db-file test.sqlite3 --out ./mongo-data --format dbpath
mongod --dbpath ./mongo-data --bind_ip 127.0.0.1 --port 27018
```

Exports cover ordinary collections and views. The exporter converts nonstandard collection UUIDs from older test server builds to UUID v4 in the exported metadata. It preserves existing UUID v4 values and reads the SQLite source without writing to it.

### Neo4j

```sh
python3 neo4j_server.py --db-file graph.sqlite3
```

The server supports Bolt 4.4 and 5.0–5.4. Use `bolt://` for a direct connection or `neo4j://` for routing to the same node. The Cypher subset covers node and relationship operations, parameters, path matching, aggregation, transactions, and selected index and constraint commands. Index definitions are stored as metadata and queries scan the graph. Tests that rely on full Cypher, APOC, or distributed clustering require Neo4j. [Bolt protocol specification](https://neo4j.com/docs/bolt/current/bolt/handshake/)

### Redis

```sh
python3 redis_server.py --db-file redis.json
```

The server supports RESP2 and RESP3, strings, hashes, lists, sets, sorted sets, streams, key expiration, scans, transactions with `WATCH`, and Pub/Sub. Pass `--cluster` to assign all slots to a single node for Redis Cluster clients. With `--db-file`, `SAVE` and clean shutdown write a JSON snapshot. Tests that rely on replication, Sentinel, Lua, or modules require Redis. [RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

### Nexus Repository

```sh
python3 nexus_server.py --db-file nexus.sqlite3
```

The server provides hosted and group repositories for Raw, Maven, PyPI, and npm, HTTP proxy caching for Raw and Maven, a Docker/OCI registry, and REST endpoints for repositories, components, and assets. UI, role-based access control, and scheduled jobs require Nexus Repository. [Nexus Repository API](https://help.sonatype.com/en/api-reference.html) · [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)

| Format | Default repositories or address |
| --- | --- |
| Raw | `raw-hosted` |
| Maven | `maven-releases`, `maven-snapshots`, `maven-public` |
| PyPI | `/repository/pypi-hosted/simple/` |
| npm | `/repository/npm-hosted/` |
| Docker/OCI | `127.0.0.1:8081/docker-hosted/<image>:<tag>` |

Upload and fetch a local `fixture.bin` with the Raw repository. Specify `--tls-cert` and `--tls-key` when a client requires HTTPS.

```sh
curl --fail --upload-file fixture.bin http://127.0.0.1:8081/repository/raw-hosted/fixture.bin
curl --fail http://127.0.0.1:8081/repository/raw-hosted/fixture.bin
```

### Apache Iceberg

```sh
python3 iceberg_server.py --warehouse ./warehouse serve
```

The server exposes an Iceberg v2 REST catalog and provides CLI commands for local file operations. SQLite tracks the current metadata location. The warehouse holds versioned JSON metadata, Avro manifests and manifest lists, and Parquet data files. The catalog and its clients need access to the same local `file://` paths. The server returns HTTP `409` when a commit requirement fails. [Iceberg table specification](https://iceberg.apache.org/spec/) · [REST catalog specification](https://iceberg.apache.org/rest-catalog-spec/)

Use the same warehouse for the CLI and the REST server. If you pass `--db-file`, use the same SQLite path for both. Save this field list as `schema.json` in the repository directory.

```json
[
  {"id": 1, "name": "id", "required": true, "type": "long"},
  {"id": 2, "name": "label", "required": false, "type": "string"}
]
```

```sh
python3 iceberg_server.py --warehouse ./warehouse create-table demo.events --schema schema.json
printf '%s\n' '{"id":1,"label":"first"}' '{"id":2,"label":"second"}' |
  python3 iceberg_server.py --warehouse ./warehouse append demo.events -
python3 iceberg_server.py --warehouse ./warehouse scan demo.events --columns id,label
python3 iceberg_server.py --warehouse ./warehouse delete demo.events --where '{"id":1}'
python3 iceberg_server.py --warehouse ./warehouse snapshots demo.events
```

- **Snapshots and deletes.** Read an earlier snapshot with `scan --snapshot-id`. Pass column-value equality predicates as JSON with `--where`. The `delete` command rewrites live data into new files and keeps the files referenced by earlier snapshots. Scans also apply Iceberg v2 position and equality delete files.
- **Types and partitions.** File operations cover flat primitive columns with identity, bucket, truncate, year, month, day, and hour partition transforms. JSON uses ISO strings for dates and times, strings for decimals, and Base64 strings for binary and fixed-width values. Dates and timestamps outside Python's calendar range use integer days and microseconds since the Unix epoch, respectively.
- **Parquet codecs.** The CLI writes uncompressed Parquet and reads uncompressed, Snappy, and GZIP files, as well as ZSTD files on Python 3.14 or later. New catalog tables default to Snappy for client writes. [Parquet file format](https://parquet.apache.org/docs/file-format/) · [Avro specification](https://avro.apache.org/docs/current/specification/)

### Apache Polaris

Start Polaris mode with a file containing the root OAuth2 client secret.

```sh
umask 077
python3 -c 'import secrets; print(secrets.token_urlsafe(32))' > polaris.secret
python3 iceberg_server.py --warehouse ./polaris-warehouse serve-polaris --password-file polaris.secret
```

In a second terminal, get a token and create a local catalog.

```sh
POLARIS_TOKEN=$(curl -fsS --user "root:$(cat polaris.secret)" \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL \
  http://127.0.0.1:8181/api/catalog/v1/oauth/tokens |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
curl -fsS -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"catalog":{"name":"demo","type":"INTERNAL","storageConfigInfo":{"storageType":"FILE"}}}' \
  http://127.0.0.1:8181/api/management/v1/catalogs
```

Configure an Iceberg REST client with the catalog URI from the table, warehouse `demo`, the OAuth2 token endpoint above, and scope `PRINCIPAL_ROLE:ALL`. The management API supports shared catalog grants via principals, principal roles, and catalog roles. The `FILE` warehouse and data files must be accessible to both the server and the client. Catalog grants govern API operations, and filesystem permissions govern direct access to the files. [Polaris REST API guide](https://polaris.apache.org/guides/rest-api/)

### OpenLineage

```sh
python3 openlineage_server.py --db-file lineage.sqlite3
```

Point the OpenLineage HTTP transport at `http://127.0.0.1:5000`. Its default `POST /api/v1/lineage` route accepts JSON and gzip. The server keeps raw run, job, and dataset events, deduplicates exact replays, and projects input and output relationships. Fetch a run at `/api/v1/runs/<run-id>` or query the Marquez-style graph at `/api/v1/lineage` with `nodeId` and `depth` parameters. Set `--password-file` to require a bearer token. [HTTP transport configuration](https://openlineage.io/docs/client/python/configuration/) · [Marquez lineage API](https://marquezproject.ai/docs/api/get-lineage/)

### LiteLLM

```sh
python3 litellm_server.py --db-file gateway.sqlite3
```

Send a chat request from another terminal.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"hello"}]}' \
  http://127.0.0.1:4000/v1/chat/completions
```

The built-in `mock-chat` and `mock-embedding` models support Chat Completions, Responses, and Embeddings. The mock Responses API supports stored responses and function-call follow-ups. Pass `--config` with a JSON `model_list` to route an alias to an OpenAI-compatible HTTP upstream.

```json
{
  "model_list": [
    {
      "model_name": "local-model",
      "litellm_params": {
        "model": "openai/provider-model-id",
        "api_base": "http://127.0.0.1:8000/v1"
      }
    }
  ]
}
```

The gateway streams SSE and passes upstream Responses requests through. Use a single deployment per model for upstream requests with `previous_response_id`. With `--password-file`, the master key can issue or revoke model-scoped virtual keys with optional expiry and read request token usage at `/spend/logs`. [LiteLLM Gateway](https://docs.litellm.ai/docs/simple_proxy/)

### OpenSearch

```sh
python3 opensearch_server.py --db-file search.sqlite3
```

Index and search a document from another terminal.

```sh
curl -fsS -X PUT -H 'Content-Type: application/json' \
  -d '{"title":"local search"}' 'http://127.0.0.1:9200/notes/_doc/1?refresh=true'
curl -fsS 'http://127.0.0.1:9200/notes/_search'
```

The server supports document CRUD, NDJSON bulk requests, mappings, filtered and write aliases, refresh, sequence-number concurrency checks, and common Query DSL filters, sorting, pagination, and aggregations. It uses one primary shard per index and simplified scoring and text analysis. Get requests read the latest document, and search queries read the last refreshed version. [Python client](https://docs.opensearch.org/latest/clients/python-low-level/) · [Query DSL](https://docs.opensearch.org/latest/query-dsl/index/)

### Apache Airflow

Save a local workflow as `dag.json`.

```json
{
  "dag_id": "demo",
  "tasks": [
    {"task_id": "hello", "command": ["python3", "-c", "print('hello from Airflow')"]}
  ]
}
```

```sh
python3 airflow_server.py --dag-file dag.json --db-file airflow.sqlite3
```

Trigger a run from another terminal and poll its state.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"dag_run_id":"manual__demo","logical_date":null}' \
  http://127.0.0.1:8080/api/v2/dags/demo/dagRuns
curl -fsS http://127.0.0.1:8080/api/v2/dags/demo/dagRuns/manual__demo
```

One worker runs commands registered in local JSON DAGs. Tasks can declare `upstream` dependencies, retries, and timeouts. DAGs can use fixed intervals with `schedule_seconds`. The Airflow 3 API exposes DAGs, runs, task instances, and attempt logs. Pass `--openlineage-url http://127.0.0.1:5000/api/v1/lineage` to send task events to the collector. [Airflow public REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

### OpenMetadata

```sh
python3 openmetadata_server.py --db-file metadata.sqlite3
```

Check the API version from another terminal.

```sh
curl -fsS http://127.0.0.1:8585/api/v1/system/version
```

The catalog stores database service, database, schema, table, pipeline, and messaging entities. The API supports UUID and fully qualified name lookups, cursor pagination, JSON Patch, change history, search, and directional lineage queries with optional column-level details. Point an OpenMetadata Python SDK client at `http://127.0.0.1:8585/api` and use `--password-file` to require a bearer token. [Python SDK](https://docs.open-metadata.org/v2.0.x/api-reference/sdk/python) · [Lineage API](https://docs.open-metadata.org/v2.0.x/api-reference/lineage/get)

## Development

Run the network, persistence, and file-format tests with the standard library.

```sh
python3 -B -m unittest discover -s tests -v
```

## Source and license notices

- Pi's corresponding source for the DOOM example ships alongside the build. [Source archive](standalone/doom-corresponding-source.tar.gz) · [Original release](https://github.com/seo-rii/pi-standalone/releases/tag/standalone-v1.3.0-preview.2) · [License and notices](licenses/pi/)
- OpenCode provides third-party notices for both builds. [Full source](https://github.com/seo-rii/opencode-standalone/tree/9b8be1342f1562aa15ec3b724bbbb8d219cdf696) · [Lite source](https://github.com/seo-rii/opencode-standalone/tree/75325db60f7d6c728fdb65881b515ad4328509ec) · [License and notices](licenses/opencode/)
- Hermes embeds third-party licenses in each executable file. [Source](https://github.com/seo-rii/hermes-standalone/tree/fe98d0bd3e3628ac5a32c9cda07eb966b9313911) · [License](licenses/hermes/LICENSE)
- The original MongoDB test server shipped in v0.1.0. [Initial download](https://github.com/seo-rii/jjap/releases/download/v0.1.0/mongo_test_server.py) · [Initial release source](https://github.com/seo-rii/jjap/blob/v0.1.0/mongo_test_server.py)
