# jjap

Single-file test servers for local integration tests, plus standalone builds of Pi, OpenCode, and Hermes. Each test server runs with Python 3.10 or later and the standard library. The standalone builds have their own runtime requirements.

## Quick start

Clone the repository and start the MongoDB test server on `127.0.0.1:27017`.

```sh
git clone https://github.com/seo-rii/jjap.git
cd jjap
python3 mongo_test_server.py --db-file test.sqlite3
```

In a second terminal, run a ping from the repository directory.

```sh
python3 mongo_test_server.py shell --eval '{"ping": 1}'
```

## Test servers

| Server | Default endpoint | Source |
| --- | --- | --- |
| MongoDB | `mongodb://127.0.0.1:27017` | [`mongo_test_server.py`](mongo_test_server.py) |
| Neo4j | `bolt://127.0.0.1:7687` | [`neo4j_test_server.py`](neo4j_test_server.py) |
| Redis | `redis://127.0.0.1:6379` | [`redis_test_server.py`](redis_test_server.py) |
| Nexus Repository | `http://127.0.0.1:8081` | [`nexus_test_server.py`](nexus_test_server.py) |
| Apache Iceberg | `http://127.0.0.1:8181` | [`iceberg_test_server.py`](iceberg_test_server.py) |

The servers bind to loopback by default. Use `--port 0` to pick an available port and read it from the startup message. MongoDB, Neo4j, Redis, and Nexus Repository accept `--password-file` for their test accounts. These four servers keep data in memory by default, and the examples use `--db-file` to persist it.

### MongoDB

Connect with a MongoDB client or use the built-in `shell` command.

Run `export` to capture a committed SQLite snapshot. It writes `.bson` and `.metadata.json` files into a new directory for `mongorestore`. The dump preserves BSON types, collection options, index definitions, and views. Use `--db` to select one database. [MongoDB dump format](https://www.mongodb.com/docs/database-tools/mongodump/mongodump-behavior/)

```sh
python3 mongo_test_server.py export --db-file test.sqlite3 --out ./dump
```

To export a WiredTiger `dbPath`, install `mongod` and `mongorestore` on a system with Unix domain sockets. Open the result with the same version of `mongod` that created it. Use `--mongod` and `--mongorestore` to specify executable paths.

```sh
python3 mongo_test_server.py export --db-file test.sqlite3 --out ./mongo-data --format dbpath
mongod --dbpath ./mongo-data --bind_ip 127.0.0.1 --port 27018
```

Exports cover ordinary collections and views. The exporter converts nonstandard collection UUIDs from older test server builds to UUID v4 in the exported metadata. It preserves existing UUID v4 values and reads the SQLite source without writing to it.

### Neo4j

```sh
python3 neo4j_test_server.py --db-file graph.sqlite3
```

The server supports Bolt 4.4 and 5.0–5.4. Use `bolt://` for a direct connection or `neo4j://` for routing to the same node. The Cypher subset covers node and relationship operations, parameters, path matching, aggregation, transactions, and selected index and constraint commands. Index definitions are stored as metadata and queries scan the graph. Tests that rely on full Cypher, APOC, or distributed clustering require Neo4j. [Bolt protocol specification](https://neo4j.com/docs/bolt/current/bolt/handshake/)

### Redis

```sh
python3 redis_test_server.py --db-file redis.json
```

The server supports RESP2 and RESP3, strings, hashes, lists, sets, sorted sets, streams, key expiration, scans, transactions with `WATCH`, and Pub/Sub. Pass `--cluster` to assign all slots to a single node for Redis Cluster clients. With `--db-file`, `SAVE` and clean shutdown write a JSON snapshot. Tests that rely on replication, Sentinel, Lua, or modules require Redis. [RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

### Nexus Repository

```sh
python3 nexus_test_server.py --db-file nexus.sqlite3
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
python3 iceberg_test_server.py --warehouse ./warehouse serve
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
python3 iceberg_test_server.py --warehouse ./warehouse create-table demo.events --schema schema.json
printf '%s\n' '{"id":1,"label":"first"}' '{"id":2,"label":"second"}' |
  python3 iceberg_test_server.py --warehouse ./warehouse append demo.events -
python3 iceberg_test_server.py --warehouse ./warehouse scan demo.events --columns id,label
python3 iceberg_test_server.py --warehouse ./warehouse delete demo.events --where '{"id":1}'
python3 iceberg_test_server.py --warehouse ./warehouse snapshots demo.events
```

- **Snapshots and deletes.** Read an earlier snapshot with `scan --snapshot-id`. Pass column-value equality predicates as JSON with `--where`. The `delete` command rewrites live data into new files and keeps the files referenced by earlier snapshots. Scans also apply Iceberg v2 position and equality delete files.
- **Types and partitions.** File operations cover flat primitive columns with identity, bucket, truncate, year, month, day, and hour partition transforms. JSON uses ISO strings for dates and times, strings for decimals, and Base64 strings for binary and fixed-width values. Dates and timestamps outside Python's calendar range use integer days and microseconds since the Unix epoch, respectively.
- **Parquet codecs.** The CLI writes uncompressed Parquet and reads uncompressed, Snappy, and GZIP files, as well as ZSTD files on Python 3.14 or later. New catalog tables default to Snappy for client writes. [Parquet file format](https://parquet.apache.org/docs/file-format/) · [Avro specification](https://avro.apache.org/docs/current/specification/)

## Standalone release builds

Download the single-file builds from [v0.1.0](https://github.com/seo-rii/jjap/releases/tag/v0.1.0).

| Build | File | Runtime |
| --- | --- | --- |
| Pi 1.3.0 preview.2 | `pi-standalone.mjs` | Node.js 22.19 or later |
| OpenCode 2.1.2-standalone.1 | `opencode-standalone.js`, `opencode-standalone-lite.js` | Node.js 26.4 or later |
| Hermes | `hermes-full.py`, `hermes-lite.py`, `hermes-full.pyz`, `hermes-lite.pyz` | CPython 3.14 or later |
| MongoDB test server, initial release | `mongo_test_server.py` | Python 3.10 or later |

To download Pi and verify its checksum, run the following commands.

```sh
gh release download v0.1.0 -R seo-rii/jjap -D dist -p pi-standalone.mjs -p SHA256SUMS
(cd dist && sha256sum --ignore-missing --check SHA256SUMS)
```

Run the corresponding command from the directory where you downloaded the file.

```sh
node pi-standalone.mjs
node opencode-standalone.js
python3.14 -I -S hermes-lite.py
python3 mongo_test_server.py --port 27017
```

- The full OpenCode build includes the web UI. The lite build provides the CLI, TUI, and HTTP API.
- The full Hermes build includes local Piper voice models, dictionaries, and the full Google API discovery cache. The lite build bundles one Google API discovery document for Google Chat. The `.py` and `.pyz` builds accept the same options.

For MongoDB export support, use the current [`mongo_test_server.py`](mongo_test_server.py) from this repository.

## Development

Run the network, persistence, and file-format tests with the standard library.

```sh
python3 -B -m unittest discover -s tests -v
```

## Source and license notices

- Pi's corresponding source for the DOOM example ships as `doom-corresponding-source.tar.gz`. [Original release](https://github.com/seo-rii/pi-standalone/releases/tag/standalone-v1.3.0-preview.2) · [License and notices](licenses/pi/)
- OpenCode provides third-party notices for both builds. [Full source](https://github.com/seo-rii/opencode-standalone/tree/9b8be1342f1562aa15ec3b724bbbb8d219cdf696) · [Lite source](https://github.com/seo-rii/opencode-standalone/tree/75325db60f7d6c728fdb65881b515ad4328509ec) · [License and notices](licenses/opencode/)
- Hermes embeds third-party licenses in each executable file. [Source](https://github.com/seo-rii/hermes-standalone/tree/fe98d0bd3e3628ac5a32c9cda07eb966b9313911) · [License](licenses/hermes/LICENSE)
- The original MongoDB test server shipped in v0.1.0. [Initial release source](https://github.com/seo-rii/jjap/blob/v0.1.0/mongo_test_server.py)
