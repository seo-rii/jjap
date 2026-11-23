# jjap

Pi, OpenCode, Hermes와 MongoDB 호환 테스트 서버의 단일 파일 실행 버전을 모았습니다. 필요한 파일 하나를 받아 해당 런타임으로 실행할 수 있습니다.

## 다운로드

모든 실행 파일은 [GitHub Releases v0.1.0](https://github.com/seo-rii/jjap/releases/tag/v0.1.0)에서 받을 수 있습니다. MongoDB 테스트 서버의 Python 파일은 [저장소](mongo_test_server.py)에도 있습니다.

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

## 원본과 라이선스

- Pi의 DOOM 예제 대응 소스 `doom-corresponding-source.tar.gz`는 실행 파일과 같은 릴리스에 있습니다. [원본 릴리스](https://github.com/seo-rii/pi-standalone/releases/tag/standalone-v1.3.0-preview.2) · [라이선스 및 고지](licenses/pi/)
- OpenCode는 전체판과 경량판의 제삼자 고지를 각각 제공합니다. [전체판 소스](https://github.com/seo-rii/opencode-standalone/tree/9b8be1342f1562aa15ec3b724bbbb8d219cdf696) · [경량판 소스](https://github.com/seo-rii/opencode-standalone/tree/75325db60f7d6c728fdb65881b515ad4328509ec) · [라이선스 및 고지](licenses/opencode/)
- Hermes의 제삼자 라이선스는 각 실행 파일에 포함되어 있습니다. [원본 소스](https://github.com/seo-rii/hermes-standalone/tree/fe98d0bd3e3628ac5a32c9cda07eb966b9313911) · [라이선스](licenses/hermes/LICENSE)
- MongoDB 테스트 서버는 기존 단일 파일을 그대로 가져왔습니다. [원본 소스](https://github.com/seo-rii/mongo-test-server/tree/207ddeefc8098bd694803cc2f5aeb9e06edd39b5)
