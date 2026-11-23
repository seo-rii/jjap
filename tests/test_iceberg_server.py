"""Exercise one-file Iceberg v2 reads, writes, and snapshot commits."""

from concurrent.futures import ThreadPoolExecutor
import http.client
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "iceberg_test_server.py"
NAMESPACE = ("sample",)
TABLE = "events"
IDENTIFIER = ".".join((*NAMESPACE, TABLE))
FIELDS = [
    {"id": 1, "name": "id", "required": True, "type": "long"},
    {"id": 2, "name": "label", "required": False, "type": "string"},
]


class IcebergFileTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="jjap-iceberg-test-")
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.warehouse = self.directory / "warehouse"

    def cli(self, *args, rows=None):
        stdin = None if rows is None else "".join(json.dumps(row) + "\n" for row in rows)
        result = subprocess.check_output(
            [sys.executable, "-I", "-S", "-B", str(self.script), "--warehouse", str(self.warehouse), *args],
            cwd=self.directory, input=stdin, text=True, timeout=15,
        )
        return [json.loads(line) for line in result.splitlines()]

    def create(self, fields=FIELDS, spec=None):
        schema_file = self.directory / "schema.json"
        schema_file.write_text(json.dumps(fields), encoding="utf-8")
        options = []
        if spec is not None:
            spec_file = self.directory / "spec.json"
            spec_file.write_text(json.dumps(spec), encoding="utf-8")
            options = ["--partition-spec", str(spec_file)]
        return self.cli("create-table", IDENTIFIER, "--schema", str(schema_file), *options)[0]["metadata"]

    def test_partitioned_parquet_avro_and_time_travel(self):
        fields = FIELDS + [
            {"id": 3, "name": "day", "required": True, "type": "date"},
            {"id": 4, "name": "amount", "required": False, "type": "decimal(20, 2)"},
            {"id": 5, "name": "binary", "required": False, "type": "fixed[2]"},
        ]
        spec = {"spec-id": 0, "fields": [
            {"source-id": 3, "field-id": 1000, "name": "day_partition", "transform": "identity"},
            {"source-id": 1, "field-id": 1001, "name": "id_bucket", "transform": "bucket[4]"},
        ]}
        self.create(fields, spec)
        one = {"id": 1, "label": "한글", "day": "1969-12-31", "amount": "123456789012345678.90", "binary": "AP8="}
        two = {"id": 2, "label": None, "day": "2024-01-01", "amount": None, "binary": None}
        first = self.cli("append", IDENTIFIER, "-", rows=[one, two])[0]
        self.cli("append", IDENTIFIER, "-", rows=[dict(one, id=3)])
        self.assertEqual(self.cli("scan", IDENTIFIER, "--where", '{"day":"1969-12-31"}', "--columns", "id"),
                         [{"id": 1}, {"id": 3}])
        self.assertEqual(self.cli("scan", IDENTIFIER, "--snapshot-id", str(first["snapshot-id"])), [one, two])

        parquet = sorted(self.warehouse.rglob("*.parquet"))
        avro = sorted(self.warehouse.rglob("*.avro"))
        self.assertEqual(len(parquet), 3)
        self.assertEqual(len(avro), 4)
        self.assertTrue(all(file.read_bytes().startswith(b"PAR1") and file.read_bytes().endswith(b"PAR1")
                            for file in parquet))
        self.assertTrue(all(file.read_bytes().startswith(b"Obj\x01") for file in avro))

    def test_copy_on_write_delete_retains_older_files(self):
        self.create()
        rows = [{"id": 1, "label": "old"}, {"id": 2, "label": "kept"}]
        first = self.cli("append", IDENTIFIER, "-", rows=rows)[0]
        self.assertEqual(self.cli("delete", IDENTIFIER, "--where", '{"id":1}')[0]["deleted-rows"], 1)
        self.assertEqual(self.cli("scan", IDENTIFIER), rows[1:])
        self.assertEqual(self.cli("scan", IDENTIFIER, "--snapshot-id", str(first["snapshot-id"])), rows)
        self.assertEqual(self.cli("delete", IDENTIFIER, "--where", '{"id":2}')[0]["deleted-rows"], 1)
        self.assertEqual(self.cli("scan", IDENTIFIER), [])
        self.assertEqual(len(list(self.warehouse.rglob("*.parquet"))), 2)

    def test_preserves_dates_and_timestamps_outside_python_calendar(self):
        fields = [
            {"id": 1, "name": "day", "required": True, "type": "date"},
            {"id": 2, "name": "stamp", "required": True, "type": "timestamptz"},
        ]
        spec = {"spec-id": 0, "fields": [
            {"source-id": 1, "field-id": 1000, "name": "by_year", "transform": "year"},
            {"source-id": 2, "field-id": 1001, "name": "by_month", "transform": "month"},
        ]}
        self.create(fields, spec)
        row = {"day": (1 << 31) - 1, "stamp": -(1 << 63)}
        self.cli("append", IDENTIFIER, "-", rows=[row])
        self.assertEqual(self.cli("scan", IDENTIFIER), [row])

    def test_conflicting_writers_preserve_one_commit(self):
        self.create()
        module = runpy.run_path(str(self.script))
        barrier = threading.Barrier(2, timeout=10)
        base = module["IcebergCatalog"]

        class ConcurrentCatalog(base):
            def commit_table(self, *args, **kwargs):
                barrier.wait()
                return super().commit_table(*args, **kwargs)

        def append(identifier):
            files = module["IcebergFiles"](ConcurrentCatalog(self.warehouse))
            try:
                files.append(NAMESPACE, TABLE, [{"id": identifier}])
                return identifier, 200
            except module["IcebergCatalogError"] as error:
                return identifier, error.status

        with ThreadPoolExecutor(max_workers=2) as threads:
            outcomes = list(threads.map(append, (1, 2)))
        self.assertEqual(sorted(status for _, status in outcomes), [200, 409])
        winner = next(identifier for identifier, status in outcomes if status == 200)
        self.assertEqual(self.cli("scan", IDENTIFIER), [{"id": winner, "label": None}])
        self.assertEqual(len(list(self.warehouse.rglob("*.parquet"))), 1)
        self.assertEqual(len(list(self.warehouse.rglob("*.avro"))), 2)

    def test_rest_load_observes_local_commits_and_rejects_stale_requirements(self):
        self.create()
        module = runpy.run_path(str(self.script))
        catalog = module["IcebergCatalog"](self.warehouse)
        endpoint = "/v1/namespaces/sample/tables/events"
        with ThreadingHTTPServer(("127.0.0.1", 0), module["_IcebergCatalogHandler"]) as server:
            server.catalog = catalog
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()

            def request(method, body=None, headers=None):
                connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    connection.request(method, endpoint, body=body, headers=headers or {})
                    response = connection.getresponse()
                    content = response.read()
                    return response.status, response.headers, json.loads(content) if content else None
                finally:
                    connection.close()

            try:
                status, headers, previous = request("GET")
                self.assertEqual(status, 200)
                etag = headers["ETag"]
                snapshot = self.cli("append", IDENTIFIER, "-", rows=[{"id": 1}])[0]
                status, headers, updated = request("GET", headers={"If-None-Match": etag})
                self.assertEqual(status, 200)
                self.assertNotEqual(previous["metadata-location"], updated["metadata-location"])
                self.assertEqual(updated["metadata"]["current-snapshot-id"], snapshot["snapshot-id"])
                self.assertEqual(request("GET", headers={"If-None-Match": headers["ETag"]})[0], 304)
                stale = json.dumps({
                    "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}],
                    "updates": [{"action": "set-properties", "updates": {"stale": "true"}}],
                })
                status, _, body = request("POST", stale, {"Content-Type": "application/json"})
                self.assertEqual((status, body["error"]["type"]), (409, "CommitFailedException"))
                self.assertNotIn("stale", request("GET")[2]["metadata"]["properties"])
            finally:
                server.shutdown()
                thread.join(timeout=5)

    def test_reads_iceberg_position_and_equality_delete_files(self):
        self.create()
        module = runpy.run_path(str(self.script))
        catalog = module["IcebergCatalog"](self.warehouse)
        files = module["IcebergFiles"](catalog)
        first = files.append(NAMESPACE, TABLE, [{"id": 1}, {"id": 2}, {"id": 3}])
        metadata = catalog.load_table(NAMESPACE, TABLE)["metadata"]
        previous, inventory = module["_ice_inventory"](first)
        _, manifest_schema, header, entries = inventory[0]
        location = catalog.local_path(metadata["location"])
        data_path = entries[0]["data_file"]["file_path"]
        snapshot = {"snapshot-id": 32, "parent-snapshot-id": first["snapshot-id"], "sequence-number": 2,
                    "timestamp-ms": first["timestamp-ms"] + 1, "schema-id": metadata["current-schema-id"],
                    "summary": {"operation": "delete"}}
        descriptors = [
            (1, location / "data" / "position.parquet", [
                {"id": 2147483546, "name": "file_path", "required": True, "type": "string"},
                {"id": 2147483545, "name": "pos", "required": True, "type": "long"},
            ], {"file_path": data_path, "pos": 0}),
            (2, location / "data" / "equality.parquet", FIELDS[:1], {"id": 2}),
        ]
        deletes = []
        for content, path, fields, row in descriptors:
            module["parquet_write"](path, fields, [row])
            info = {"content": content, "file_path": path.as_uri(), "file_format": "PARQUET", "partition": {},
                    "record_count": 1, "file_size_in_bytes": path.stat().st_size}
            if content == 2:
                info["equality_ids"] = [1]
            deletes.append({"status": 1, "snapshot_id": 32, "sequence_number": None,
                            "file_sequence_number": None, "data_file": info})
        header = dict({key: value for key, value in header.items() if not key.startswith("avro.")}, content="deletes")
        delete_manifest = module["_ice_write_manifest"](location / "metadata", manifest_schema, header, deletes,
                                                        metadata["default-spec-id"], 1, snapshot, [])
        root = location / "metadata" / "delete-root.avro"
        module["avro_write"](root, module["iceberg_manifest_list_schema"](), [*previous, delete_manifest])
        snapshot["manifest-list"] = root.as_uri()
        catalog.commit_table(NAMESPACE, TABLE, [
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": first["snapshot-id"]},
        ], [
            {"action": "add-snapshot", "snapshot": snapshot},
            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 32, "type": "branch"},
        ])
        self.assertEqual(self.cli("scan", IDENTIFIER, "--columns", "id"), [{"id": 3}])
        self.cli("append", IDENTIFIER, "-", rows=[{"id": 2}])
        self.assertEqual(self.cli("scan", IDENTIFIER, "--columns", "id"), [{"id": 3}, {"id": 2}])


if __name__ == "__main__":
    unittest.main()
