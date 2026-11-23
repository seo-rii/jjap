# jjap

Pi, OpenCode, Hermes의 단일 파일 실행 버전과 MongoDB, Neo4j, Redis, Nexus Repository, Apache Iceberg 호환 테스트 서버를 모았습니다. 필요한 파일 하나를 받아 해당 런타임으로 실행할 수 있습니다.

## 다운로드

아래 실행 파일은 [GitHub Releases v0.1.0](https://github.com/seo-rii/jjap/releases/tag/v0.1.0)에서 받을 수 있습니다. Python 테스트 서버는 저장소에서도 받을 수 있습니다.

| 도구 | 파일 | 런타임 |
| --- | --- | --- |
| Pi 1.3.0 preview.2 | `pi-standalone.mjs` | Node.js 22.19 이상 |
| OpenCode 2.1.2-standalone.1 | `opencode-standalone.js`, `opencode-standalone-lite.js` | Node.js 26.4 이상 |
| Hermes | `hermes-full.py`, `hermes-lite.py`, `hermes-full.pyz`, `hermes-lite.pyz` | CPython 3.14 이상 |
| MongoDB 테스트 서버 | `mongo_test_server.py` | Python 3.10 이상 |

GitHub CLI로 파일을 골라 받고 SHA-256을 확인할 수 있습니다.

```sh
gh release download v0.1.0 -R seo-rii/jjap -D dist -p pi-standalone.mjs -p SHA256SUMS
(cd dist && sha256sum --ignore-missing --check SHA256SUMS)
```

## 실행

다운로드한 파일이 있는 디렉터리에서 실행합니다.

```sh
node pi-standalone.mjs
node opencode-standalone.js
python3.14 -I -S hermes-lite.py
python3 mongo_test_server.py --port 27017
```

- OpenCode 전체판은 웹 UI를 포함합니다. 경량판은 CLI, TUI, HTTP API를 제공합니다.
- Hermes 전체판은 로컬 음성 모델과 전체 Google API 정의 캐시를 포함합니다. 경량판은 로컬 Piper 모델과 사전을 빼고 Google Chat API 정의만 내장합니다. `.py`와 `.pyz`에 같은 실행 옵션을 사용합니다.
- MongoDB 테스트 서버는 기본적으로 `127.0.0.1`에서 메모리로 동작합니다. `mongodb://127.0.0.1:27017`로 접속하거나 내장 셸에서 `python3 mongo_test_server.py shell --eval '{"ping": 1}'`을 실행할 수 있습니다. `--db-file test.sqlite3`를 지정하면 테스트 데이터를 보존합니다.

### MongoDB 내보내기

`export`는 `--db-file`에 커밋된 SQLite 스냅샷을 읽어서 새 디렉터리에 씁니다. 기본 출력은 컬렉션별 `.bson`과 `.metadata.json`이며, 문서의 BSON 타입과 컬렉션 옵션, 인덱스 정의, 뷰를 보존합니다. `--db`로 데이터베이스 하나를 선택할 수 있습니다. [MongoDB 덤프 형식](https://www.mongodb.com/docs/database-tools/mongodump/mongodump-behavior/)

```sh
python3 mongo_test_server.py export --db-file test.sqlite3 --out ./dump
```

Unix 소켓을 지원하는 환경에 `mongod`와 `mongorestore`가 설치되어 있으면 실제 WiredTiger `dbPath`도 만들 수 있습니다. 내보내기에 사용한 `mongod` 버전으로 결과를 엽니다. `--mongod`와 `--mongorestore`로 실행 파일 경로를 지정할 수 있습니다.

```sh
python3 mongo_test_server.py export --db-file test.sqlite3 --out ./mongo-data --format dbpath
mongod --dbpath ./mongo-data --bind_ip 127.0.0.1 --port 27018
```

예전 테스트 서버가 생성한 비표준 컬렉션 UUID는 내보낸 메타데이터에서만 UUID v4로 변환합니다. 기존 UUID v4와 원본 SQLite 데이터는 그대로 보존합니다. 내보내기 대상은 MongoDB에서 복원할 수 있는 일반 컬렉션과 뷰입니다.

## Neo4j, Redis, Nexus Repository 테스트 서버

각 서버는 Python 3.10 이상의 표준 라이브러리로 실행됩니다. 기본 설정은 루프백 주소와 메모리 저장입니다. `--db-file`에 서버별 파일을 지정하면 재시작 후 데이터를 다시 읽을 수 있고, `--port 0`은 비어 있는 포트를 선택해 시작 메시지에 표시합니다.

| 서버 | 기본 주소 | 실행 파일 |
| --- | --- | --- |
| Neo4j | `bolt://127.0.0.1:7687` | [`neo4j_test_server.py`](neo4j_test_server.py) |
| Redis | `redis://127.0.0.1:6379` | [`redis_test_server.py`](redis_test_server.py) |
| Nexus Repository | `http://127.0.0.1:8081` | [`nexus_test_server.py`](nexus_test_server.py) |

```sh
python3 neo4j_test_server.py --db-file graph.sqlite3
python3 redis_test_server.py --db-file redis.json
python3 nexus_test_server.py --db-file nexus.sqlite3
```

서버는 각각 별도 터미널에서 시작합니다. `--password-file`로 테스트 계정의 암호를 지정할 수 있습니다.

- **Neo4j**는 Bolt 4.4와 5.0~5.4, 노드·관계 생성 및 조회, 파라미터, 경로 검색, 집계, 트랜잭션, 일부 인덱스·제약조건 명령을 지원합니다. `bolt://`로 직접 접속하거나 `neo4j://`로 같은 노드에 라우팅할 수 있습니다. 스키마 인덱스는 메타데이터로 기록하며 조회는 그래프를 순회합니다. Cypher 전체 문법, APOC, 분산 클러스터는 지원 범위 밖입니다. [Bolt 명세](https://neo4j.com/docs/bolt/current/bolt/handshake/)
- **Redis**는 RESP2와 RESP3, 문자열·해시·리스트·집합·정렬 집합·스트림, TTL, 스캔, 트랜잭션과 `WATCH`, Pub/Sub를 지원합니다. `--cluster`는 모든 슬롯을 단일 노드에 배정해 Redis Cluster 클라이언트로 연결할 수 있게 합니다. `--db-file` 사용 시 `SAVE` 또는 정상 종료로 JSON 스냅샷을 기록합니다. 복제, Sentinel, Lua, 모듈은 지원 범위 밖입니다. [RESP 명세](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- **Nexus Repository**는 Raw, Maven, PyPI, npm의 호스티드 저장소와 그룹, Raw·Maven HTTP 프록시 캐시, Docker/OCI 레지스트리, 저장소·컴포넌트·에셋 REST API를 제공합니다. 기본 저장소는 `raw-hosted`, `maven-releases`, `maven-snapshots`, `maven-public`, `pypi-hosted`, `npm-hosted`, `docker-hosted`입니다. Nexus 관리 UI, 역할 기반 권한 관리, 백그라운드 작업 스케줄링은 지원 범위 밖입니다. [Nexus Repository API](https://help.sonatype.com/en/api-reference.html) · [OCI Distribution 명세](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)

Nexus의 기본 `raw-hosted` 저장소는 `curl`로 확인할 수 있습니다. PyPI 인덱스는 `/repository/pypi-hosted/simple/`, npm 레지스트리는 `/repository/npm-hosted/`, Docker/OCI 이미지 주소는 `127.0.0.1:8081/docker-hosted/<이미지>:<태그>`를 사용합니다. HTTPS를 요구하는 클라이언트에는 `--tls-cert`와 `--tls-key`를 지정합니다.

```sh
curl --fail --upload-file fixture.bin http://127.0.0.1:8081/repository/raw-hosted/fixture.bin
curl --fail http://127.0.0.1:8081/repository/raw-hosted/fixture.bin
```

네트워크와 재시작 통합 테스트도 별도 패키지 없이 실행됩니다.

```sh
python3 -B -m unittest discover -s tests -v
```

## Apache Iceberg 테스트 서버

[`iceberg_test_server.py`](iceberg_test_server.py)는 Python 3.10 이상 표준 라이브러리로 Iceberg v2 REST 카탈로그와 로컬 파일 처리를 제공합니다. SQLite로 현재 메타데이터를 관리하고, 웨어하우스에 버전별 JSON 메타데이터, Avro 매니페스트·매니페스트 목록, Parquet 데이터 파일을 저장합니다. 카탈로그와 클라이언트는 같은 로컬 `file://` 경로에 접근해야 합니다. [Iceberg 테이블 명세](https://iceberg.apache.org/spec/) · [REST 카탈로그 명세](https://iceberg.apache.org/rest-catalog-spec/)

```sh
python3 iceberg_test_server.py --warehouse ./warehouse serve
```

REST 카탈로그 주소는 `http://127.0.0.1:8181`입니다. 같은 웨어하우스에 CLI로 직접 쓸 수 있고, `--db-file`을 지정했다면 양쪽에 같은 경로를 사용합니다. 커밋 전제 조건이 맞지 않으면 HTTP `409`를 반환해 이전 커밋을 보호합니다.

아래 필드 배열을 `schema.json`에 저장합니다.

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

- `scan --snapshot-id`로 과거 스냅샷을 읽을 수 있습니다. `--where`는 컬럼별 동등 조건을 받습니다. `delete`는 현재 데이터를 새 파일로 다시 쓰고 기존 스냅샷의 파일을 보존합니다. 스캔할 때 v2 position delete와 equality delete 파일도 적용합니다.
- 파일 처리는 평면 기본 타입과 identity·bucket·truncate·year·month·day·hour 파티션을 지원합니다. JSON에서 날짜·시간은 ISO 문자열, decimal은 문자열, binary·fixed는 Base64 문자열을 사용합니다. Python 달력 범위를 벗어난 날짜와 타임스탬프는 각각 Unix epoch 기준 일수와 마이크로초 정수로 보존합니다.
- Parquet 쓰기는 비압축을 사용합니다. 읽기는 비압축·Snappy·GZIP과 Python 3.14 이상의 ZSTD를 지원합니다. 카탈로그가 만드는 테이블은 외부 클라이언트의 기본 쓰기 코덱을 Snappy로 설정합니다. 파일 처리 범위는 로컬 웨어하우스의 평면 Parquet와 Avro 매니페스트입니다. [Parquet 파일 포맷](https://parquet.apache.org/docs/file-format/) · [Avro 명세](https://avro.apache.org/docs/current/specification/)

## 원본과 라이선스

- Pi의 DOOM 예제 대응 소스 `doom-corresponding-source.tar.gz`는 실행 파일과 같은 릴리스에 있습니다. [원본 릴리스](https://github.com/seo-rii/pi-standalone/releases/tag/standalone-v1.3.0-preview.2) · [라이선스 및 고지](licenses/pi/)
- OpenCode는 전체판과 경량판의 제삼자 고지를 각각 제공합니다. [전체판 소스](https://github.com/seo-rii/opencode-standalone/tree/9b8be1342f1562aa15ec3b724bbbb8d219cdf696) · [경량판 소스](https://github.com/seo-rii/opencode-standalone/tree/75325db60f7d6c728fdb65881b515ad4328509ec) · [라이선스 및 고지](licenses/opencode/)
- Hermes의 제삼자 라이선스는 각 실행 파일에 포함되어 있습니다. [원본 소스](https://github.com/seo-rii/hermes-standalone/tree/fe98d0bd3e3628ac5a32c9cda07eb966b9313911) · [라이선스](licenses/hermes/LICENSE)
- MongoDB 테스트 서버는 기존 단일 파일을 그대로 가져왔습니다. [원본 소스](https://github.com/seo-rii/mongo-test-server/tree/207ddeefc8098bd694803cc2f5aeb9e06edd39b5)
