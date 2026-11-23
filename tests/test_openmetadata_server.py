"""Exercise OpenMetadata catalog and lineage APIs over HTTP."""

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote, urlencode

from test_servers import http_request, running_server


class OpenMetadataTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.headers = {}

    def server(self, *args):
        return running_server("openmetadata_server.py", self.directory,
                              "--db-file", str(self.directory / "catalog.db"), *args)

    def request(self, port, method, path, value=None, expected=200):
        headers = self.headers.copy()
        body = None
        if value is not None:
            headers["Content-Type"] = "application/json-patch+json" if method == "PATCH" else "application/json"
            body = json.dumps(value).encode()
        status, _, response = http_request(port, method, "/api/v1" + path, body, headers)
        self.assertEqual(status, expected, response)
        return json.loads(response) if response else None

    def hierarchy(self, port):
        service = self.request(port, "PUT", "/services/databaseServices",
                               {"name": "warehouse", "serviceType": "CustomDatabase"}, 201)
        database = self.request(port, "PUT", "/databases", {"name": "data", "service": "warehouse"}, 201)
        schema = self.request(port, "PUT", "/databaseSchemas",
                              {"name": "public", "database": "warehouse.data"}, 201)
        self.assertEqual(schema["service"]["id"], service["id"])
        self.assertEqual(schema["database"]["id"], database["id"])
        return schema

    def table(self, port, name, expected=201, method="PUT", **extra):
        return self.request(port, method, "/tables", {
            "name": name, "databaseSchema": "warehouse.data.public",
            "columns": [{"name": "id", "dataType": "BIGINT"}], **extra,
        }, expected)

    def test_catalog_pagination_patch_history_and_restart(self):
        with self.server() as port:
            schema = self.hierarchy(port)
            first = self.table(port, "a.one")
            second = self.table(port, "b")
            self.assertEqual(self.table(port, "b", expected=200)["id"], second["id"])
            self.table(port, "b", expected=409, method="POST")
            self.assertEqual(first["databaseSchema"]["id"], schema["id"])
            self.assertEqual(first["serviceType"], "CustomDatabase")
            self.assertEqual(first["fullyQualifiedName"], 'warehouse.data.public."a.one"')
            page = self.request(port, "GET", "/tables?limit=1&service=warehouse")
            self.assertEqual(page["paging"]["total"], 2)
            self.assertEqual(page["data"][0]["id"], first["id"])
            next_page = self.request(port, "GET", "/tables?" + urlencode({"after": page["paging"]["after"], "limit": 1}))
            self.assertEqual(next_page["data"][0]["id"], second["id"])
            previous = self.request(port, "GET", "/tables?" + urlencode({"before": next_page["paging"]["before"], "limit": 1}))
            self.assertEqual(previous, page)
            path = "/tables/name/" + quote(first["fullyQualifiedName"], safe="")
            patch = [
                {"op": "test", "path": "/columns/0/name", "value": "id"},
                {"op": "add", "path": "/description", "value": "source rows"},
                {"op": "add", "path": "/columns/-", "value": {"name": "label", "dataType": "STRING"}},
            ]
            updated = self.request(port, "PATCH", path, patch)
            self.assertEqual(updated["version"], 0.2)
            self.request(port, "PATCH", path, [
                {"op": "replace", "path": "/description", "value": "should roll back"},
                {"op": "test", "path": "/version", "value": 0.1},
            ], 409)
            self.request(port, "PATCH", path, [{"op": "replace", "path": "/name", "value": "renamed"}], 400)
        with self.server() as port:
            found = self.request(port, "GET", path)
            self.assertEqual(found["columns"][-1]["name"], "label")
            self.assertEqual(found["description"], "source rows")
            history = self.request(port, "GET", f"/tables/{first['id']}/versions")
            self.assertEqual([version["version"] for version in history["versions"]], [0.2, 0.1])
            original = self.request(port, "GET", f"/tables/{first['id']}/versions/0.1")
            self.assertEqual(len(original["columns"]), 1)
            search = self.request(port, "GET", "/search/fieldQuery?" + urlencode({
                "index": "table_search_index", "fieldName": "fullyQualifiedName", "fieldValue": first["fullyQualifiedName"],
            }))
            self.assertEqual([hit["_id"] for hit in search["hits"]["hits"]], [first["id"]])

    def test_lineage_with_column_details_and_delete(self):
        with self.server() as port:
            self.hierarchy(port)
            source, stage, target = [self.table(port, name) for name in ("source", "stage", "target")]
            details = {"sqlQuery": "INSERT INTO stage SELECT id FROM source", "columnsLineage": [
                {"fromColumns": [source["fullyQualifiedName"] + ".id"], "toColumn": stage["fullyQualifiedName"] + ".id"},
            ]}
            for src, dst in ((source, stage), (stage, target)):
                lineage = self.request(port, "PUT", "/lineage", {"edge": {
                    "fromEntity": {"id": src["id"], "type": "table"},
                    "toEntity": {"id": dst["id"], "type": "table"},
                    "description": "Copy columns",
                    "lineageDetails": details if src == source else None,
                }})
                self.assertEqual(lineage["entity"]["id"], src["id"])
        with self.server() as port:
            query = f"/lineage/table/name/{target['fullyQualifiedName']}?upstreamDepth=2&downstreamDepth=0"
            graph = self.request(port, "GET", query)
            self.assertCountEqual([node["id"] for node in graph["nodes"]], [source["id"], stage["id"]])
            self.assertEqual(graph["upstreamEdges"][0]["description"], "Copy columns")
            self.assertEqual(graph["upstreamEdges"][1]["lineageDetails"], details)
            edge_path = f"/lineage/table/{source['id']}/table/{stage['id']}"
            self.request(port, "DELETE", edge_path)
            graph = self.request(port, "GET", query)
            self.assertEqual([node["id"] for node in graph["nodes"]], [stage["id"]])

    def test_bearer_owners_and_recursive_deletion(self):
        token_file = self.directory / "token"
        token_file.write_text("test-token\n", encoding="ascii")
        with self.server("--password-file", str(token_file)) as port:
            version = self.request(port, "GET", "/system/version")
            self.assertEqual(version["version"], "2.0.9")
            self.request(port, "GET", "/tables", expected=401)
            self.headers["Authorization"] = "Bearer test-token"
            schema = self.hierarchy(port)
            user = self.request(port, "POST", "/users", {"name": "owner", "email": "owner@example.test"}, 201)
            table = self.table(port, "owned", owners=[{"id": user["id"], "type": "user"}])
            self.assertEqual(table["owners"][0]["name"], "owner")
            table_path = f"/tables/{table['id']}"
            self.assertNotIn("owners", self.request(port, "GET", table_path + "?fields=tags"))
            self.assertIn("owners", self.request(port, "GET", table_path + "?fields=owners"))
            schema_path = f"/databaseSchemas/{schema['id']}"
            self.request(port, "DELETE", schema_path, expected=409)
            deleted = self.request(port, "DELETE", schema_path + "?recursive=true")
            self.assertTrue(deleted["deleted"])
            self.request(port, "GET", table_path, expected=404)
            self.assertTrue(self.request(port, "GET", table_path + "?include=deleted")["deleted"])
            self.request(port, "PUT", "/tables/restore", {"id": table["id"]}, 409)
            self.request(port, "PUT", "/databaseSchemas/restore", {"id": schema["id"]})
            self.assertFalse(self.request(port, "PUT", "/tables/restore", {"id": table["id"]})["deleted"])
            self.request(port, "DELETE", table_path + "?hardDelete=true", expected=204)
            self.request(port, "GET", table_path + "?include=all", expected=404)


if __name__ == "__main__":
    unittest.main()
