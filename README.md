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
| Apache Kafka | `127.0.0.1:9092` | [`kafka_server.py`](kafka_server.py) |
| Apache Spark Standalone | `http://127.0.0.1:6066` | [`spark_server.py`](spark_server.py) |

The servers bind to loopback by default. Use `--port 0` to pick an available port and read it from the startup message. MongoDB, Neo4j, Redis, and Nexus Repository accept `--password-file` for their test accounts. These four servers keep data in memory by default, and the examples use `--db-file` to persist it.

Each server can hand its persisted state to the original product. `export` writes files for the product's own import tool, and `migrate` loads a running instance through its API. Both read the persistence file without changing it, and `export` refuses an existing output path. `migrate` reads credentials only from files and exits with status 1 when any item fails.

| Server | Command | Result |
| --- | --- | --- |
| MongoDB | `export` | `mongorestore` dump or WiredTiger `dbPath` |
| Neo4j | `export` | `cypher-shell` script or `neo4j-admin database import` CSV files |
| Redis | `export` | RDB file, or RESP commands for `redis-cli --pipe` |
| Nexus Repository | `export`, `migrate` | REST create requests, native repository layouts, and OCI image layouts |
| Apache Iceberg and Polaris | `export`, `migrate` | JDBC catalog database, or table registration in a REST catalog |
| OpenLineage | `export`, `migrate` | NDJSON events replayed to `/api/v1/lineage` |
| OpenSearch | `export`, `migrate` | Create-index bodies and `_bulk` files |
| OpenMetadata | `export`, `migrate` | `Create*` and `AddLineage` request bodies in dependency order |
| Apache Kafka | `migrate` | Record batches and committed group offsets replayed into a cluster |

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

Run `export` to move a persisted graph into Neo4j 5. The default `cypher` format writes `neo4j.cypher` for `cypher-shell`. Run it against an empty database. It recreates labels, typed properties, relationships, indexes, and constraints, and removes its temporary import key at the end. [cypher-shell](https://neo4j.com/docs/operations-manual/current/cypher-shell/)

```sh
python3 neo4j_server.py export --db-file graph.sqlite3 --out ./graph-export
cypher-shell -d neo4j -f ./graph-export/neo4j.cypher
```

The `csv` format writes typed CSV files for the offline importer, `import.args` with the matching options, and `schema.cypher`. Stop Neo4j and run the import from the export directory, with the database name before `@import.args`. The target database must be new or empty. Run `schema.cypher` with `cypher-shell` after the next start. [Import](https://neo4j.com/docs/operations-manual/current/import/)

```sh
python3 neo4j_server.py export --db-file graph.sqlite3 --out ./graph-csv --format csv
cd graph-csv
neo4j-admin database import full neo4j @import.args
```

- Byte arrays have no Cypher literal, so graphs that store them need the `csv` format. Empty lists need the `cypher` format.
- Node key and property existence constraints require Neo4j Enterprise Edition. Both formats create them last.
- Lists that mix integers and floats are exported as float lists. Points need finite coordinates.

### Redis

```sh
python3 redis_server.py --db-file redis.json
```

The server supports RESP2 and RESP3, strings, hashes, lists, sets, sorted sets, streams, key expiration, scans, transactions with `WATCH`, and Pub/Sub. Pass `--cluster` to assign all slots to a single node for Redis Cluster clients. With `--db-file`, `SAVE` and clean shutdown write a JSON snapshot. Tests that rely on replication, Sentinel, Lua, or modules require Redis. [RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

Run `export` to convert the JSON snapshot into a file that Redis 7.0 or later loads. The default `rdb` format writes an RDB version 10 file for `redis-server` to load at startup. The `resp` format writes commands for `redis-cli --pipe`, which also load as a legacy append-only file. Both keep every data type, streams with consumer groups and pending entries, and absolute expiration times. Keys that have already expired are skipped. [Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) · [Bulk loading](https://redis.io/docs/latest/develop/clients/patterns/bulk-loading/)

```sh
python3 redis_server.py export --db-file redis.json --out ./data/dump.rdb
redis-server --dir ./data --dbfilename dump.rdb --port 6380
```

```sh
python3 redis_server.py export --db-file redis.json --out fixture.resp --format resp
redis-cli -p 6380 --pipe < fixture.resp
```

The RESP commands replace keys with the same names, and Redis records stream consumer seen times when it runs them. Redis Cluster nodes accept only database 0, so pass `--db 0` for a cluster target.

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

Run `export` to write `repositories.json`, with one Nexus REST create request per repository, and each hosted repository's content in its native layout. Raw and Maven files keep their paths, PyPI files get a PEP 503 index, npm tarballs come with packuments, and each Docker image becomes an OCI image layout. Every asset is checked against its recorded checksum. Add `--include-proxy-cache` to include cached proxy content. [Repositories API](https://help.sonatype.com/en/repositories-api.html) · [OCI image layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md)

```sh
python3 nexus_server.py export --db-file nexus.sqlite3 --out ./nexus-export
skopeo copy --all oci:./nexus-export/content/docker-hosted/example/image:latest docker://nexus:8082/example/image:latest
```

Run `migrate` to copy repositories and content into a running Nexus Repository. It creates missing repositories through the REST API. It uploads raw and Maven files with HTTP PUT, PyPI files through the upload API, and npm versions with npm publish, and pushes Docker images through the registry API. Content that already exists with the same checksum is skipped, and differing content is reported without being overwritten. Docker repositories default to `TARGET/repository/<name>/v2`. Pass `--docker-endpoint NAME=URL` to use an HTTP connector port, which is also configured when the repository is created.

```sh
python3 nexus_server.py migrate --db-file nexus.sqlite3 --target http://nexus:8081 \
  --username admin --password-file nexus-password.txt --docker-endpoint docker-hosted=http://nexus:8082
```

Proxy caches are not migrated. Nexus Repository 3.77 and later accept uploads only after the Community Edition license is accepted. Docker schema 2 manifests in an OCI layout need skopeo 1.19 or later.

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

| Area | Supported scope |
| --- | --- |
| Authentication | OAuth2 client credentials, expiring bearer tokens, and one configurable realm |
| Management API | Root can create, list, inspect, and delete internal catalogs, principals, and roles, assign roles, and grant or revoke catalog-wide privileges |
| Iceberg REST Catalog | Namespace and table operations, staged creation, registration, rename, and atomic commits |
| Table privileges | `TABLE_CREATE` covers staged creation. `TABLE_WRITE_PROPERTIES` covers property-only commits. General metadata changes require broader grants. |
| Storage | Dedicated local `FILE` warehouses with direct metadata paths checked against the catalog root. The client and server share `file://` paths. Filesystem permissions govern direct file access. |
| Client verification | PyIceberg 0.12.0 authenticated, created a table, appended Parquet data, and scanned the result |

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

Configure an Iceberg REST client with the catalog URI from the server list, warehouse `demo`, the OAuth2 token endpoint above, and scope `PRINCIPAL_ROLE:ALL`. [Polaris REST API guide](https://polaris.apache.org/guides/rest-api/)

The Iceberg and Polaris tables are already Iceberg v2 files, so moving them to a real catalog only moves catalog pointers. `export` writes a SQLite database in the [JDBC catalog](https://iceberg.apache.org/docs/latest/jdbc/) schema shared by Iceberg Java's `JdbcCatalog` and [PyIceberg's `SqlCatalog`](https://py.iceberg.apache.org/configuration/#sql-catalog). `migrate` creates the namespaces and registers every table in a running REST catalog. Metadata locations are absolute `file://` URIs, so the target must read the same paths.

```sh
python3 iceberg_server.py --warehouse ./warehouse export --out catalog.db --catalog-name default
python3 iceberg_server.py --warehouse ./warehouse migrate --target http://localhost:8181 --token-file catalog.token
```

For OAuth2 client credentials, pass `--credential-file` with a `client_id:client_secret` line. For a Polaris target, pass the target catalog name as the `migrate` option `--warehouse`. Add `--polaris-catalog NAME` to read one catalog of a `serve-polaris` warehouse. Polaris principals, roles, and grants are not migrated.

### OpenLineage

| Area | Supported scope |
| --- | --- |
| HTTP ingestion | JSON or gzip run, job, and dataset events at `POST /api/v1/lineage`. Submit an event array at `POST /api/v1/lineage/batch`. |
| Event storage | Raw events, exact replay deduplication, and run state projected in event-time order. Use `--db-file` for SQLite persistence. |
| Queries | Namespace, job, and dataset listings, run status and raw events, and a Marquez-style job and dataset graph with `nodeId` and `depth` |
| Authentication | Optional fixed bearer token via `--password-file` |
| Client verification | `openlineage-python` 1.54.0 with gzip HTTP transport and Airflow task events from this repository |

```sh
python3 openlineage_server.py --db-file lineage.sqlite3
```

Point the OpenLineage HTTP transport at `http://127.0.0.1:5000`. Fetch a run at `/api/v1/runs/<run-id>` or query a graph at `GET /api/v1/lineage`. [HTTP transport configuration](https://openlineage.io/docs/client/python/configuration/) · [Marquez lineage API](https://marquezproject.ai/docs/api/get-lineage/)

Run `export` to write the stored events in arrival order, one per line, as NDJSON or as gzip when the name ends in `.gz`. Run `migrate` to post them in order to a running collector such as Marquez. `--batch-size N` posts arrays to `/api/v1/lineage/batch` instead. Marquez does not deduplicate events, so a repeated run adds them again. [OpenLineage API](https://openlineage.io/apidocs/openapi/)

```sh
python3 openlineage_server.py export --db-file lineage.sqlite3 --out events.ndjson.gz
python3 openlineage_server.py migrate --db-file lineage.sqlite3 --target http://localhost:5000 --token-file marquez.token
```

### LiteLLM

| Area | Supported scope |
| --- | --- |
| Inference API | Model listing, Chat Completions and Responses over JSON or SSE, and Embeddings in float or Base64 format |
| Mock models | `mock-chat` reproduces text and forced function calls. `mock-embedding` generates deterministic vectors. Mock Responses support stored response lookup and function-call follow-ups. |
| Upstream routing | OpenAI-compatible HTTP endpoints, model aliases, round-robin deployment selection, and fallback on connection refusal or HTTP `429`, `502`, `503`, or `504` |
| Stateful upstream requests | Requests with `previous_response_id` require a single deployment for that model |
| Keys and usage | Optional master key via `--password-file`, model-scoped virtual keys with optional expiry, key revocation, and token counts at `/spend/logs`. Use `--db-file` to persist keys and usage. |
| Client verification | OpenAI Python SDK 3.57.0 exercised streaming, embeddings, and Responses function-call follow-ups against mock models |

```sh
python3 litellm_server.py --db-file gateway.sqlite3
```

Send a chat request from another terminal.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"hello"}]}' \
  http://127.0.0.1:4000/v1/chat/completions
```

Pass `--config` with a JSON `model_list` to route an alias to an OpenAI-compatible HTTP upstream. [LiteLLM Gateway](https://docs.litellm.ai/docs/simple_proxy/)

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

### OpenSearch

| Area | Supported scope |
| --- | --- |
| Indexes | Index creation and deletion, mappings, settings, and filtered and write aliases. Each index has one primary shard. |
| Documents | CRUD, `_mget`, gzip NDJSON `_bulk` with per-item results, and sequence-number and primary-term concurrency checks |
| Visibility | Realtime document reads, manual and interval refresh, and `refresh=true` or `refresh=wait_for`. Search reads the last refreshed version. |
| Search | `_search` and `_count` with common term, range, Boolean, and text queries, sorting, `search_after`, source projection, and basic aggregations. Text analysis and scoring are simplified. |
| Persistence | `--db-file` stores indexes, live documents, and refreshed search data in SQLite |
| Client verification | `opensearch-py` 3.2.0 exercised index and alias setup, bulk, optimistic updates, and search |

```sh
python3 opensearch_server.py --db-file search.sqlite3
```

Index and search a document from another terminal.

```sh
curl -fsS -X PUT -H 'Content-Type: application/json' \
  -d '{"title":"local search"}' 'http://127.0.0.1:9200/notes/_doc/1?refresh=true'
curl -fsS 'http://127.0.0.1:9200/notes/_search'
```

Use the OpenSearch Python client with the server endpoint above. [Python client](https://docs.opensearch.org/latest/clients/python-low-level/) · [Query DSL](https://docs.opensearch.org/latest/query-dsl/index/)

Run `export` to write one [create index](https://docs.opensearch.org/latest/api-reference/index-apis/create-index/) body and one [`_bulk`](https://docs.opensearch.org/latest/api-reference/document-apis/bulk/) file per index, plus `manifest.json` with the load order. The bulk files hold every live document, including writes that were not yet refreshed. Run `migrate` to load a running cluster. It refuses indexes that already exist unless `--skip-existing` is given, adds aliases after the documents, and refreshes at the end. `--preserve-versions` keeps `_version` through `version_type=external`.

```sh
python3 opensearch_server.py export --db-file search.sqlite3 --out ./search-export
python3 opensearch_server.py migrate --db-file search.sqlite3 --target https://localhost:9200 \
  --username admin --password-file admin.pw --ca-file root-ca.pem
```

### Apache Airflow

| Area | Supported scope |
| --- | --- |
| Airflow 3 REST API | `/api/v2` DAG listing, details and pause, run triggering and status, unmapped task instances, and per-attempt logs |
| Task execution | One worker runs command arrays registered in local JSON DAGs, with upstream dependencies, retries, and timeouts |
| Scheduling | Manual runs and fixed-second intervals via `schedule_seconds`. The first scheduled run is due one interval after registration. |
| State and recovery | `--db-file` stores run, task, and attempt state in SQLite. Interrupted attempts are recorded as failed on restart. |
| OpenLineage | Per-attempt lifecycle events over HTTP, with retries on delivery failure |
| Client verification | `apache-airflow-client` 3.1.0 exercised DAG lookup, run triggering and polling, task instances, and logs |

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

Pass `--openlineage-url http://127.0.0.1:5000/api/v1/lineage` to send task events to the collector. [Airflow public REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

### OpenMetadata

| Area | Supported scope |
| --- | --- |
| Catalog entities | Database, pipeline, and messaging services, databases, schemas, tables, pipelines, topics, and users |
| Entity lifecycle | Create and update, UUID and fully qualified name lookup, cursor pagination, JSON Patch, version history, soft and hard delete, and restore |
| Search | `/api/v1/search/query` and `/api/v1/search/fieldQuery` with basic text and wildcard matching |
| Lineage | Create, read, and delete edges, optionally with column mappings. Traverse up to three hops per direction. |
| Authentication and persistence | Optional fixed bearer token via `--password-file`. `--db-file` stores catalog entities and lineage in SQLite. |
| Client verification | The Python SDK from `openmetadata-ingestion` 2.0.9.0 exercised catalog writes, pagination, search, patch, and lineage |

```sh
python3 openmetadata_server.py --db-file metadata.sqlite3
```

Check the API version from another terminal.

```sh
curl -fsS http://127.0.0.1:8585/api/v1/system/version
```

Point an OpenMetadata Python SDK client at `http://127.0.0.1:8585/api`. [Python SDK](https://docs.open-metadata.org/v2.0.x/api-reference/sdk/python) · [Lineage API](https://docs.open-metadata.org/v2.0.x/api-reference/lineage/get)

Run `export` to write `Create*` and `AddLineage` request bodies in dependency order, with `manifest.json` listing each request. Owners and lineage refer to entities by type and fully qualified name. Run `migrate` to send them to a running OpenMetadata server, which resolves those references to the target's ids. Soft-deleted entities are skipped unless `--include-deleted` is given. Version history is not migrated.

```sh
python3 openmetadata_server.py export --db-file metadata.sqlite3 --out ./metadata-export
python3 openmetadata_server.py migrate --db-file metadata.sqlite3 --target http://localhost:8585/api --token-file bot.jwt
```

### Apache Kafka

| Area | Supported scope |
| --- | --- |
| Broker | Kafka TCP protocol with API version negotiation, topic creation, and partition metadata. One plaintext broker with replication factor 1. `--auto-create-topics` enables topic creation from client metadata requests. |
| Records | Produce and fetch magic 2 record batches with uncompressed or gzip payloads. Keys, headers, and tombstones are preserved. Non-idempotent producer retries can append duplicates. |
| Offsets | Partition offset lookup by timestamp, beginning, or end. Assignment and subscription workflows can commit and fetch group offsets. |
| Consumer groups | Classic group join, sync, heartbeat, leave, and partition reassignment |
| Persistence | `--db-file` stores topics, partition logs, and committed offsets in SQLite. Consumers rejoin their groups after a broker restart. |
| Client verification | `kafka-python` 3.0.13 exercised topic creation, gzip batches, seeks, two-member rebalancing, and recovery of records and group offsets after restart |

Start the broker.

```sh
python3 kafka_server.py --db-file kafka.sqlite3
```

From another terminal with `kafka-python==3.0.13` installed, create a topic on a fresh broker, send a record, and read it back.

```python
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

endpoint = "127.0.0.1:9092"
admin = KafkaAdminClient(bootstrap_servers=endpoint)
admin.create_topics([NewTopic("events", num_partitions=1, replication_factor=1)])
admin.close()

producer = KafkaProducer(bootstrap_servers=endpoint, enable_idempotence=False)
producer.send("events", partition=0, value=b"hello").get(timeout=5)
producer.close()

consumer = KafkaConsumer(bootstrap_servers=endpoint, group_id=None, consumer_timeout_ms=5000)
partition = TopicPartition("events", 0)
consumer.assign([partition])
consumer.seek_to_beginning(partition)
print(next(consumer).value.decode())
consumer.close()
```

Set `--advertised-host` when clients connect using a hostname different from the listen address. [Kafka protocol](https://kafka.apache.org/42/design/protocol/) · [Record batch format](https://kafka.apache.org/42/implementation/message-format/)

Kafka has no offline import format, so run `migrate` to replay a SQLite snapshot into a running cluster. It produces each partition's record batches in order with `acks=all` and keeps keys, values, headers, tombstones, compression, and CreateTime timestamps. Missing topics are created with the source partition count and the broker's default replication factor, which `--replication-factor` overrides. An existing topic is reused only when its partition count matches and it holds no records. `--allow-non-empty-topics` appends to it instead.

```sh
python3 kafka_server.py migrate --db-file kafka.sqlite3 --bootstrap-server broker1:9092 --offset-map offsets.json
```

The target assigns new offsets. `--offset-map` writes the source-to-target mapping as JSON. Committed consumer group offsets are translated through it and committed for groups with no active members on the target. Connections are plaintext. Use `--username` and `--password-file` for SASL/PLAIN.

### Apache Spark Standalone

| Area | Supported scope |
| --- | --- |
| Submission API | Submit, inspect, and cancel jobs through `/v1/submissions`. Set `appResource` to a configured job name and `mainClass` to `jjap.LocalSqlJob`. Jobs can be canceled until output publication starts. |
| SQL | One worker runs read-only SQLite queries over registered tables, including filters, joins, grouping, and aggregation. Queries, destinations, and `timeout_seconds` come from the local config. |
| Files | Typed CSV and JSON Lines with `string`, `int`, `long`, `double`, and `boolean` columns. Relative paths resolve against the config directory. Set `overwrite: true` on a file output to replace an existing file atomically. |
| Iceberg | Read snapshots and append to existing local Iceberg v2 tables using the optional `iceberg_server.py` CLI companion. Set `--iceberg-cli` to select the companion path. |
| State and access | `--db-file` persists submission state. Queued jobs resume with the same configuration, and interrupted active jobs are marked `ERROR`. Set `--password-file` to require a bearer token. |

Save this job as `jobs.json`.

```json
{
  "jobs": {
    "daily-total": {
      "inputs": {
        "events": {
          "format": "csv",
          "path": "events.csv",
          "schema": {"label": "string", "amount": "long"}
        }
      },
      "sql": "SELECT label, sum(amount) AS total FROM events GROUP BY label",
      "output": {
        "format": "jsonl",
        "path": "totals.jsonl",
        "schema": {"label": "string", "total": "long"}
      }
    }
  }
}
```

Create the input beside `jobs.json` and start the server.

```sh
printf 'label,amount\nbooks,2\nbooks,3\n' > events.csv
python3 spark_server.py --config jobs.json --db-file spark.sqlite3
```

Submit the job from another terminal.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{
    "action": "CreateSubmissionRequest",
    "appResource": "daily-total",
    "mainClass": "jjap.LocalSqlJob",
    "clientSparkVersion": "4.1.1",
    "sparkProperties": {},
    "environmentVariables": {},
    "appArgs": []
  }' \
  http://127.0.0.1:6066/v1/submissions/create
```

Use the returned `submissionId` in `GET /v1/submissions/status/<id>` to inspect the job or `POST /v1/submissions/kill/<id>` to cancel it. The completed job writes `totals.jsonl`. [Spark Standalone REST API](https://spark.apache.org/docs/latest/spark-standalone.html#rest-api)

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
