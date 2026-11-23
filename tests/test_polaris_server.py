import base64
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from iceberg_server import _PolarisHandler, _PolarisState


SCHEMA = {
    "type": "struct", "schema-id": 0,
    "fields": [{"id": 1, "name": "id", "type": "long", "required": True}],
}


class PolarisTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.warehouse = Path(directory.name)
        self.start_server()

    def start_server(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _PolarisHandler)
        self.server.polaris = _PolarisState(self.warehouse, None, "test-root-secret", "POLARIS")
        self.base = "http://127.0.0.1:" + str(self.server.server_port)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.root = self.token("root", "test-root-secret", basic=True)

    def request(self, method, path, body=None, headers=None, *, token=None, expected=200):
        headers = dict(headers or {})
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = "Bearer " + token
        req = Request(self.base + path, data=body, method=method, headers=headers)
        try:
            response = urlopen(req, timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            payload = response.read()
            self.assertEqual(response.status, expected, payload)
            return json.loads(payload) if payload else None

    def token(self, client, secret, *, basic=False, scope="PRINCIPAL_ROLE:ALL", status=200):
        form = {"grant_type": "client_credentials", "scope": scope}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if basic:
            auth = base64.b64encode(f"{client}:{secret}".encode()).decode()
            headers["Authorization"] = "Basic " + auth
        else:
            form.update(client_id=client, client_secret=secret)
        result = self.request(
            "POST", "/api/catalog/v1/oauth/tokens", urlencode(form).encode(), headers, expected=status,
        )
        return result.get("access_token", result)

    def manage(self, method, route, body=None, status=200):
        return self.request(method, "/api/management/v1" + route, body,
                            token=self.root, expected=status)

    def catalog(self, name, *, location=None, status=201):
        location = location or (self.warehouse / name).as_uri()
        return self.manage("POST", "/catalogs", {"catalog": {
            "name": name,
            "type": "INTERNAL",
            "properties": {"default-base-location": location},
            "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]},
        }}, status)

    def assign(self, principal, catalog, privileges):
        identity = self.manage("POST", "/principals", {"principal": {"name": principal}}, 201)
        self.manage("POST", "/principal-roles", {"principalRole": {"name": principal}}, 201)
        self.manage("POST", f"/catalogs/{catalog}/catalog-roles", {"catalogRole": {"name": principal}}, 201)
        self.manage("PUT", f"/principals/{principal}/principal-roles", {"principalRole": {"name": principal}}, 201)
        self.manage("PUT", f"/principal-roles/{principal}/catalog-roles/{catalog}",
                    {"catalogRole": {"name": principal}}, 201)
        for privilege in privileges:
            self.manage("PUT", f"/catalogs/{catalog}/catalog-roles/{principal}/grants",
                        {"grant": {"type": "catalog", "privilege": privilege}}, 201)
        return identity["credentials"]

    def catalog_token(self, principal, privileges):
        credentials = self.assign(principal, "demo", privileges)
        return self.token(credentials["clientId"], credentials["clientSecret"])

    def test_management_oauth_and_iceberg_commit(self):
        self.assertEqual(self.token("root", "wrong", status=401), {"error": "invalid_client"})
        self.request("GET", "/api/catalog/v1/config?warehouse=demo", expected=401)
        self.catalog("demo")
        self.catalog("private")
        writer_creds = self.assign("writer", "demo", ["CATALOG_MANAGE_CONTENT"])
        reader_creds = self.assign("reader", "demo", ["NAMESPACE_LIST", "TABLE_LIST", "TABLE_READ_PROPERTIES"])
        writer = self.token(writer_creds["clientId"], writer_creds["clientSecret"], scope="PRINCIPAL_ROLE:writer")
        reader = self.token(reader_creds["clientId"], reader_creds["clientSecret"])
        self.request("GET", "/api/management/v1/catalogs", token=writer, expected=403)
        self.request("GET", "/api/catalog/v1/config?warehouse=private", token=writer, expected=403)
        config = self.request("GET", "/api/catalog/v1/config?warehouse=demo", token=writer)
        self.assertEqual(config["overrides"]["prefix"], "demo")
        prefix = "/api/catalog/v1/demo/namespaces"
        self.request("POST", prefix, {"namespace": ["data"]}, token=writer)
        table = self.request("POST", prefix + "/data/tables", {
            "name": "records",
            "schema": SCHEMA,
        }, token=writer)
        table_path = prefix + "/data/tables/records"
        commit = {
            "requirements": [{"type": "assert-table-uuid", "uuid": table["metadata"]["table-uuid"]}],
            "updates": [{"action": "set-properties", "updates": {"owner": "demo"}}],
        }
        self.request("POST", table_path, commit, token=reader, expected=403)
        updated = self.request("POST", table_path, commit, token=writer)
        self.assertNotEqual(updated["metadata-location"], table["metadata-location"])
        self.assertEqual(self.request("GET", table_path, token=reader)["metadata"]["properties"]["owner"], "demo")
        self.assertEqual(self.request("GET", prefix + "/data/tables", token=reader)["identifiers"],
                         [{"namespace": ["data"], "name": "records"}])
        self.manage("POST", "/catalogs/demo/catalog-roles/reader/grants",
                    {"grant": {"type": "catalog", "privilege": "TABLE_READ_PROPERTIES"}}, 201)
        self.request("GET", table_path, token=reader, expected=403)
        self.manage("DELETE", "/principals/writer/principal-roles/writer", status=204)
        self.request("GET", table_path, token=writer, expected=403)
        self.manage("DELETE", "/catalogs/demo", status=409)

    def test_restart_and_warehouse_isolation(self):
        self.catalog("demo")
        self.catalog("unsafe", location=self.warehouse.parent.as_uri(), status=400)
        self.catalog("overlap", location=(self.warehouse / "demo" / "overlap").as_uri(), status=409)
        creds = self.assign("writer", "demo", ["CATALOG_MANAGE_CONTENT"])
        before = self.token(creds["clientId"], creds["clientSecret"])
        path = "/api/catalog/v1/demo/namespaces"
        self.request("POST", path, {"namespace": ["analytics"]}, token=before)
        self.server.shutdown()
        self.start_server()
        self.request("GET", path, token=before, expected=401)
        after = self.token(creds["clientId"], creds["clientSecret"])
        self.assertEqual(self.request("GET", path, token=after)["namespaces"], [["analytics"]])

    def test_table_metadata_paths_stay_in_the_catalog(self):
        self.catalog("demo")
        self.catalog("private")
        writer = self.catalog_token("writer", ["CATALOG_MANAGE_CONTENT"])
        prefix = "/api/catalog/v1/demo/namespaces"
        self.request("POST", prefix, {"namespace": ["data"]}, token=writer)
        tables = prefix + "/data/tables"
        outside = (self.warehouse / "private" / "data").as_uri()
        inside = (self.warehouse / "demo" / "data").as_uri()
        create = {"name": "records", "schema": SCHEMA}

        for key, staged in (("write.data.path", False), ("write.metadata.path", True)):
            with self.subTest(key=key, staged=staged):
                self.request("POST", tables, {**create, "stage-create": staged, "properties": {key: outside}},
                             token=writer, expected=400)
        self.assertEqual(self.request("GET", tables, token=writer)["identifiers"], [])

        table = self.request("POST", tables, {**create, "properties": {"write.data.path": inside}}, token=writer)
        table_path = tables + "/records"
        invalid_updates = [{"action": "set-properties", "updates": {"write.metadata.path": outside, "owner": "new"}}]
        self.request("POST", table_path, {"requirements": [], "updates": invalid_updates}, token=writer, expected=400)
        self.assertEqual(self.request("GET", table_path, token=writer), table)

        register = prefix + "/data/register"
        invalid_fields = {
            "properties": {"write.data.path": outside},
            "snapshots": [{"snapshot-id": 1, "manifest-list": outside}],
            "metadata-log": [{"timestamp-ms": 1, "metadata-file": outside}],
            "statistics": [{"snapshot-id": 1, "statistics-path": outside}],
            "partition-statistics": [{"snapshot-id": 1, "statistics-path": outside}],
        }
        catalog_dir = self.warehouse / "demo"
        for field, value in invalid_fields.items():
            with self.subTest(metadata_field=field):
                fixture = catalog_dir / f"{field}.metadata.json"
                fixture.write_text(json.dumps({**table["metadata"], field: value}), encoding="utf-8")
                self.request("POST", register, {"name": "imported", "metadata-location": fixture.as_uri()},
                             token=writer, expected=400)
        self.request("GET", tables + "/imported", token=writer, expected=404)
        self.request("POST", register, {"name": "imported", "metadata-location": table["metadata-location"]}, token=writer)
        self.assertEqual(self.request("GET", tables + "/imported", token=writer), table)

    def test_property_writer_and_staged_table_creator_privileges(self):
        self.catalog("demo")
        creator = self.catalog_token("creator", ["TABLE_CREATE"])
        properties = self.catalog_token("properties", ["TABLE_WRITE_PROPERTIES"])
        data = self.catalog_token("data_writer", ["TABLE_WRITE_DATA"])
        prefix = "/api/catalog/v1/demo/namespaces"
        self.request("POST", prefix, {"namespace": ["data"]}, token=self.root)
        tables = prefix + "/data/tables"
        path = tables + "/records"
        staged = self.request("POST", tables, {"name": "records", "schema": SCHEMA, "stage-create": True}, token=creator)
        metadata = staged["metadata"]
        changes = {"requirements": [{"type": "assert-create"}], "updates": [
            {"action": "assign-uuid", "uuid": metadata["table-uuid"]},
            {"action": "add-schema", "schema": metadata["schemas"][0]},
            {"action": "add-spec", "spec": metadata["partition-specs"][0]},
            {"action": "add-sort-order", "sort-order": metadata["sort-orders"][0]},
        ]}
        self.request("POST", path, changes, token=properties, expected=403)
        created = self.request("POST", path, changes, token=creator)
        self.assertEqual(created["metadata"]["table-uuid"], metadata["table-uuid"])
        self.request("POST", path, changes, token=creator, expected=409)
        self.assertEqual(self.request("GET", path, token=properties), created)
        self.assertEqual(self.request("GET", path, token=data), created)

        property_commit = {"requirements": [], "updates": [{"action": "set-properties", "updates": {"owner": "team"}}]}
        self.request("POST", path, property_commit, token=creator, expected=403)
        self.request("POST", path, property_commit, token=data, expected=403)
        updated = self.request("POST", path, property_commit, token=properties)
        self.assertEqual(updated["metadata"]["properties"]["owner"], "team")
        mixed = {"requirements": [], "updates": [
            {"action": "remove-properties", "removals": ["owner"]},
            {"action": "set-location", "location": (self.warehouse / "demo" / "moved").as_uri()},
        ]}
        self.request("POST", path, mixed, token=properties, expected=403)
        self.assertEqual(self.request("GET", path, token=properties), updated)
        removed = self.request("POST", path, {"requirements": [], "updates": mixed["updates"][:1]}, token=properties)
        self.assertNotIn("owner", removed["metadata"]["properties"])


if __name__ == "__main__":
    unittest.main()
