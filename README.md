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

Each server can move its persisted state to and from the original product. `export` writes files for the product's own import tool, and `migrate` loads a running instance through its API. `import` builds a new persistence file from the product's dumps or from a running instance. The commands read their sources without changing them, `export` and `import` refuse an existing output path, and credentials are read only from files. `migrate --dry-run` reports what a migration would create, upload, or report as a conflict without writing anything.

| Server | To the product | From the product |
| --- | --- | --- |
| MongoDB | `export`: `mongorestore` dump or WiredTiger `dbPath` | `import`: `mongodump` directory or archive |
| Neo4j | `export`: `cypher-shell` script or `neo4j-admin database import` CSV files | `import`: `neo4j-admin` CSV files or `cypher-shell` scripts such as `apoc.export.cypher` output |
| Redis | `export`: RDB file, or RESP commands for `redis-cli --pipe` | `import`: RDB file, append-only file, or `appendonlydir` |
| Nexus Repository | `export`, `migrate`: REST create requests, native repository layouts, and OCI image layouts | `import`: export directory, OCI image layout, local Maven repository, or a running Nexus |
| Apache Iceberg and Polaris | `export`, `migrate`: JDBC catalog database or table registration in a REST catalog, optionally relocated | `import`: JDBC or PyIceberg SQL catalog, or a running REST catalog |
| OpenLineage | `export`, `migrate`: NDJSON events replayed to `/api/v1/lineage` | `import`: NDJSON files or a running Marquez |
| LiteLLM | `export`: proxy `config.yaml` and SQL for the virtual keys | |
| OpenSearch | `export`, `migrate`: create-index bodies and `_bulk` files | `import`: export directory, `_bulk` or elasticdump files, or a running cluster |
| Apache Airflow | `export`: Airflow 3 DAG files | |
| OpenMetadata | `export`, `migrate`: `Create*` and `AddLineage` request bodies in dependency order | `import`: export directory or a running server |
| Apache Kafka | `migrate`: record batches and committed group offsets replayed into a cluster | `import`: a running cluster |
| Apache Spark Standalone | `export`: PySpark scripts and `spark-submit` commands | |

Tests can return a running server to its startup state without a restart. The HTTP servers answer `POST /_jjap/reset` with the same credentials as their other write APIs. MongoDB runs `{"jjapReset": 1}` when started with `--enable-test-commands`, and Neo4j runs `CALL jjap.reset()`. Redis has `FLUSHALL`, and Kafka deletes all topics and groups through its admin APIs. With `--db-file`, the reset state is saved.

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

Run `import` to build a new SQLite file from `mongodump` output. `--dump` reads a dump directory or one database directory, and `--archive` reads an archive file or `-` for standard input. Gzip compression is detected. Documents keep their exact BSON bytes, and collections keep their UUIDs, options, indexes, and views. Use `--db` and `--nsInclude` to select namespaces. Users, roles, server metadata, and the oplog are skipped with a warning. Non-simple collations, time series collections, and text or geospatial indexes stop the import, and `--skip-unsupported-indexes` drops those indexes instead. [mongodump](https://www.mongodb.com/docs/database-tools/mongodump/)

```sh
mongodump --uri mongodb://127.0.0.1:27018 --gzip --archive=fixture.archive.gz
python3 mongo_server.py import --db-file test.sqlite3 --archive fixture.archive.gz
```

Add `--replica-set` to serve as the primary of a one-member replica set. Drivers then use multi-document transactions, retryable writes, and change streams. Mongoose transactions, Prisma, which requires a replica set, and `watch()` work against the test server. Connect with the `replicaSet` option, and set `--advertised-host` when clients reach the server under another name. Transactions read a snapshot, see their own writes, and commit atomically. A write to a document that another open transaction holds fails with `WriteConflict` and the `TransientTransactionError` label, and a write outside a transaction waits for that transaction to end. With `--db-file`, retry records and change stream resume tokens survive a restart. `--change-log-size` sets how many change events stay available for resuming (default 10000). [Transactions](https://www.mongodb.com/docs/v8.0/core/transactions/) · [Change streams](https://www.mongodb.com/docs/v8.0/changeStreams/)

```sh
python3 mongo_server.py --db-file test.sqlite3 --replica-set rs0
```

The connection string is then `mongodb://127.0.0.1:27017/?replicaSet=rs0`. Start the server with `--enable-test-commands` to use MongoDB's `failCommand` failpoint and `{"jjapReset": 1}` against the `admin` database. `configureFailPoint` accepts the `off`, `alwaysOn`, `times`, `skip`, and `activationProbability` modes and the `failCommands`, `errorCode`, `errorLabels`, `closeConnection`, `blockConnection`, `blockTimeMS`, `writeConcernError`, `appName`, and `namespace` fields. `jjapReset` drops every database, cursor, failpoint, and open transaction, and requires authentication when a password is set. [failCommand](https://github.com/mongodb/mongo/wiki/The-%22failCommand%22-fail-point)

```sh
python3 mongo_server.py --enable-test-commands
python3 mongo_server.py shell -d admin --eval '{"configureFailPoint": "failCommand", "mode": {"times": 1}, "data": {"failCommands": ["find"], "closeConnection": true}}'
```

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

Run `import` to build a new SQLite file from `neo4j-admin database import` CSV files or from `cypher-shell` scripts. `--csv` reads a directory written by `export --format csv`, and `--args-file`, `--nodes`, and `--relationships` take neo4j-admin arguments for other CSV files with typed headers, ID groups, and labels. `--cypher` runs a script such as `export` output or `apoc.export.cypher` output, including `:param` commands. The Cypher subset also covers `time()`, `localtime()`, `duration()`, time zone names, nanoseconds, `NaN`, and `Infinity`.

```sh
python3 neo4j_server.py import --db-file graph.sqlite3 --csv ./graph-csv
python3 neo4j_server.py import --db-file graph.sqlite3 --cypher apoc-export.cypher
```

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

Run `import` to build a new JSON snapshot from Redis data. It reads RDB files of versions 1 through 12, which Redis 8.4 and earlier write, append-only files, a Redis 7 `appendonlydir` or its manifest, and the RESP commands that `export --format resp` writes. Commands in an append-only file run through the server's own command implementation, as Redis replays them at startup. Keys that have already expired are skipped and counted. An append-only file that ends inside a command is refused unless `--allow-truncated` is given. Function libraries are skipped with a warning, and scripts, module values, and hash fields with expiration times stop the import. The server also supports `SWAPDB`, `DUMP`, `RESTORE`, `XSETID`, `XCLAIM`, and `XAUTOCLAIM`. [RDB format](https://redis.io/docs/latest/operate/oss_and_stack/reference/internals/rdb/)

```sh
redis-cli -p 6380 --rdb ./data/dump.rdb
python3 redis_server.py import --db-file redis.json --in ./data/dump.rdb
```

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

Run `import` to build a new persistence file from an `export` directory, an OCI image layout such as `skopeo copy … oci:DIR` output, a local Maven repository, or a running Nexus Repository. From a running instance it copies the repository settings and hosted content, checks every downloaded asset against its listed checksums, and reads Docker images through the registry API. Proxy caches and unsupported formats are skipped with a warning. [Assets API](https://help.sonatype.com/en/assets-api.html)

```sh
python3 nexus_server.py import --db-file copy.sqlite3 --export-dir ./nexus-export
python3 nexus_server.py import --db-file images.sqlite3 --oci-layout ./layout --repository docker-hosted --image example/app:1.0
python3 nexus_server.py import --db-file live.sqlite3 --source http://nexus:8081 --username admin --password-file nexus-password.txt
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

Run `export --relocate DIR --location-prefix URI` to copy every file of every table into DIR and rewrite the absolute locations inside metadata, manifest lists, manifests, and position delete files to start with URI, for example `s3://bucket/warehouse`. Add `--out FILE` to also write a SQL catalog for the relocated tables. Run `import` to build a catalog from a JDBC or PyIceberg SQL catalog file or from a running REST catalog. Imported table files must be local `file://` paths inside `--warehouse`. On `serve-polaris`, `POST /_jjap/reset` requires a root token and removes all catalogs and principals. Table files stay on disk after a reset. [Table spec](https://iceberg.apache.org/spec/)

```sh
python3 iceberg_server.py --warehouse ./warehouse export --relocate ./moved --location-prefix s3://bucket/warehouse --out moved.db
python3 iceberg_server.py import --db-file catalog.sqlite3 --warehouse ./rest-warehouse --source http://localhost:8181
```

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

Run `import` to build a new persistence file from NDJSON or JSON array event files, such as `export` output, or from a running Marquez server. Gzip input is detected by content. `--source` reads [`GET /api/v1/events/lineage`](https://marquezproject.ai/docs/api/get-lineage-events) in event-time order. That endpoint lists run events only, and the server serves it the same way. Identical events are stored once.

```sh
python3 openlineage_server.py import --db-file copy.sqlite3 --source http://localhost:5000
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

Run `export` to write a LiteLLM proxy `config.yaml`. It keeps `model_list` and the router, LiteLLM, and general settings, and reads the master key from `LITELLM_MASTER_KEY`. With `--db-file`, the export also writes `keys.sql`, which inserts the virtual keys into LiteLLM's PostgreSQL database. Both gateways store SHA-256 digests of the keys, so clients keep their keys. Mock models become `mock_response` deployments. Usage records and stored responses are not exported. [Proxy config](https://docs.litellm.ai/docs/proxy/configs) · [Virtual keys](https://docs.litellm.ai/docs/proxy/virtual_keys)

```sh
python3 litellm_server.py export --config models.json --db-file gateway.sqlite3 --out ./litellm
LITELLM_MASTER_KEY="$(cat master.key)" DATABASE_URL=postgresql://litellm@db/litellm litellm --config ./litellm/config.yaml
psql postgresql://litellm@db/litellm -v ON_ERROR_STOP=1 -f ./litellm/keys.sql
```

Start the proxy once with `DATABASE_URL` so that LiteLLM creates its schema before `keys.sql` runs.

`POST /_jjap/reset` deletes virtual keys, usage, and stored responses, and keeps the `--config` models and the master key.

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

Run `import` to build a new persistence file from `export` output, from `_bulk` or [elasticdump](https://github.com/elasticsearch-dump/elasticsearch-dump) files, or from a running cluster. `--index-body` takes a create-index body or a `GET` index, `_mapping`, or `_settings` response. `--source` copies settings, mappings, aliases, `_id`, `_source`, and `_version` through [point in time](https://docs.opensearch.org/latest/search-plugins/searching-data/point-in-time/) searches, or scroll on multi-shard indexes. It skips hidden and dot-prefixed indexes unless `--index` names them. The server also implements the point in time APIs.

```sh
python3 opensearch_server.py import --db-file copy.sqlite3 --bulk products.ndjson.gz --index-body products.json --default-index products
python3 opensearch_server.py import --db-file copy.sqlite3 --source https://localhost:9200 \
  --username admin --password-file admin.pw --ca-file root-ca.pem --index 'logs-*'
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

Run `export` to write each DAG as an Airflow 3 DAG file. Each task becomes a `BashOperator` that runs the command array through `exec`, so the program receives the same arguments. The files keep dependencies, retries, retry delays, timeouts, tags, the pause state, and `schedule_seconds`. Pass `--db-file` instead of `--dag-file` to export the registered DAGs with their current pause state and next run. Run history is not exported. [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)

```sh
python3 airflow_server.py export --dag-file dag.json --out ./dags
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" airflow dags test demo
```

The first scheduled run is due one interval after the export, or at the next run the server had planned. Airflow creates no tasks for a run whose logical date is earlier than `start_date`, so trigger manual runs with a null logical date. Tasks run in a temporary directory unless you pass `--cwd`. Inputs and outputs become OpenLineage inlets and outlets when `apache-airflow-providers-openlineage` is installed.

`POST /_jjap/reset` kills a running attempt, deletes every run, and registers the `--dag-file` DAGs again with fresh schedules and their initial pause state.

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

Run `import` to build a new persistence file from `export` output or from a running server. `--source` reads users, services, databases, schemas, tables, pipelines, and topics through the list APIs with their ids, versions, and version history, and reads lineage per entity. Other entity types and their edges are skipped. `--include-deleted` also reads soft-deleted entities.

```sh
python3 openmetadata_server.py import --db-file copy.sqlite3 --source http://localhost:8585/api --token-file bot.jwt
```

### Apache Kafka

| Area | Supported scope |
| --- | --- |
| Broker | Kafka TCP protocol with API version negotiation, topic creation, and partition metadata for Kafka 4.x and `kafka-python` clients. One plaintext broker with replication factor 1. `--auto-create-topics` enables topic creation from client metadata requests. |
| Records | Produce and fetch magic 2 record batches. Producers send uncompressed or gzip batches, and imported snappy, lz4, and zstd batches are served unchanged. Keys, headers, and tombstones are preserved. |
| Idempotent producers | The broker hands out producer ids and keeps each partition's producer epoch and last five batch sequences, so a retried batch is acknowledged with its original offset. Transactional producers fail with "Transactions are not supported". |
| Topic configs | Topic creation and `kafka-configs.sh` accept the Kafka 4.4 topic configs. The broker enforces `max.message.bytes`, `message.timestamp.type`, the timestamp limits, `min.insync.replicas` for `acks=all`, keys on compacted topics, `retention.ms`, and `retention.bytes`. Retention deletes whole batches, the default `retention.ms` is `-1`, and compaction is not performed. |
| Offsets | Partition offset lookup by timestamp, beginning, end, or maximum timestamp. Assignment and subscription workflows can commit and fetch group offsets. Deleting records moves the log start offset. |
| Consumer groups | Classic group join, sync, heartbeat, leave, and partition reassignment. Groups can be listed, described, and deleted, and their offsets reset or deleted. |
| Persistence | `--db-file` stores topics, topic configs, partition logs, producer state, and committed offsets in SQLite. Consumers rejoin their groups after a broker restart. |
| Client verification | Kafka 4.4.0 console and perf-test producers and consumers with default settings, `kafka-topics.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh`, `kafka-delete-records.sh`, and `kafka-python` 3.0.13 |

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

The target assigns new offsets. `--offset-map` writes the source-to-target mapping as JSON. Committed consumer group offsets are translated through it and committed for groups with no active members on the target. Topics are created with the source topic configs. Connections are plaintext. Use `--username` and `--password-file` for SASL/PLAIN.

Run `import` to copy a running cluster into a new SQLite file. It reads every non-internal topic, or each `--topic`, from its log start offset to its high watermark, with partition counts, topic config overrides, and the committed offsets of every consumer group, or each `--group`. Batches keep their bytes and source offsets, so committed offsets carry over unchanged. Transaction markers and aborted records are skipped with a warning, and a partition with an open transaction fails the import.

```sh
python3 kafka_server.py import --db-file kafka.sqlite3 --bootstrap-server broker1:9092
```

To return a running broker to an empty state, delete its topics and groups. Deleting a topic also deletes its committed offsets.

```sh
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --delete --topic '.*'
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --delete --all-groups
```

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

Run `export` to write each job as a PySpark script, plus a `submit.sh` with one `spark-submit` command per job. Each script reads the inputs with the configured schema, registers the same table names, runs the SQL with `spark.sql`, and replaces the output file atomically. Iceberg tables are read and written through a REST catalog. Serve the local catalog with `iceberg_server.py serve`, or point `--iceberg-uri` at another catalog. [Submitting applications](https://spark.apache.org/docs/latest/submitting-applications.html)

```sh
python3 spark_server.py export --config jobs.json --out ./spark-jobs
./spark-jobs/submit.sh --master 'local[2]'
```

The scripts use absolute `file://` paths, so the driver and executors must see the same files. Spark SQL differs from SQLite: `5 / 2` returns `2.5`, `LIKE` is case-sensitive, double quotes delimit strings, and division by zero raises an error.

`POST /_jjap/reset` cancels the running submission and forgets every submission, and keeps the `--config` jobs.

## Development

Run the network, persistence, and file-format tests with the standard library.

```sh
python3 -B -m unittest discover -s tests -v
```

Some export tests also load the output into the real product. They are skipped when `redis-server`, `spark-submit`, or `litellm` is not installed. GitHub Actions runs the suite on Python 3.10 through 3.14, loads exports into real Redis, PySpark, LiteLLM, and Airflow, and moves data in both directions between the servers and MongoDB, Neo4j, Kafka, OpenSearch, the Iceberg REST fixture, and Nexus Repository in Docker with [`real_products.py`](.github/scripts/real_products.py).

## Source and license notices

- Pi's corresponding source for the DOOM example ships alongside the build. [Source archive](standalone/doom-corresponding-source.tar.gz) · [Original release](https://github.com/seo-rii/pi-standalone/releases/tag/standalone-v1.3.0-preview.2) · [License and notices](licenses/pi/)
- OpenCode provides third-party notices for both builds. [Full source](https://github.com/seo-rii/opencode-standalone/tree/9b8be1342f1562aa15ec3b724bbbb8d219cdf696) · [Lite source](https://github.com/seo-rii/opencode-standalone/tree/75325db60f7d6c728fdb65881b515ad4328509ec) · [License and notices](licenses/opencode/)
- Hermes embeds third-party licenses in each executable file. [Source](https://github.com/seo-rii/hermes-standalone/tree/fe98d0bd3e3628ac5a32c9cda07eb966b9313911) · [License](licenses/hermes/LICENSE)
- The original MongoDB test server shipped in v0.1.0. [Initial download](https://github.com/seo-rii/jjap/releases/download/v0.1.0/mongo_test_server.py) · [Initial release source](https://github.com/seo-rii/jjap/blob/v0.1.0/mongo_test_server.py)
