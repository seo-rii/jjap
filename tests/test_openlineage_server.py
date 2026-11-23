"""Exercise OpenLineage ingestion and Marquez-style queries over HTTP."""

import gzip
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode

from test_servers import http_request, running_server


INGEST = "/api/v1/lineage"
PRODUCER = "https://example.test/lineage-test"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json"


def event(kind, **values):
    return {"eventTime": "2026-01-01T01:00:00Z", "producer": PRODUCER,
            "schemaURL": f"{SCHEMA}#/$defs/{kind}", **values}


def run_event(run_id, job, kind, **values):
    return event("RunEvent", run={"runId": run_id}, job=job, eventType=kind, **values)


def post(port, data, path=INGEST, headers=None, compressed=False):
    body = json.dumps(data).encode()
    headers = {"Content-Type": "application/json", **(headers or {})}
    if compressed:
        body = gzip.compress(body)
        headers["Content-Encoding"] = "gzip"
    return http_request(port, "POST", path, body, headers)


class OpenLineageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def server(self, *args):
        return running_server("openlineage_server.py", self.directory,
                              "--db-file", str(self.directory / "lineage.db"), *args)

    def get(self, port, path):
        status, _, body = http_request(port, "GET", path)
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def test_late_start_and_gzip_replay_survive_restart(self):
        run_id = str(uuid.uuid4())
        job = {"namespace": "spark", "name": "project.query"}
        source = {"namespace": "file:///test", "name": "source"}
        target = {"namespace": "file:///test", "name": "target",
                  "facets": {"schema": {"fields": [{"name": "id", "type": "long"}]}},
                  "outputFacets": {"outputStatistics": {"rowCount": 3}}}
        done = run_event(run_id, job, "COMPLETE", eventTime="2026-01-01T01:00:01.250123456Z", outputs=[target])
        start = run_event(run_id, job, "START", eventTime="2025-12-31T20:00:00-05:00", inputs=[source])
        with self.server() as port:
            self.assertEqual(post(port, done, compressed=True)[0], 200)
            self.assertEqual(post(port, start)[0], 200)
            self.assertEqual(post(port, done)[0], 200)
        with self.server() as port:
            run = self.get(port, f"/api/v1/runs/{run_id}")
            self.assertEqual(run["state"], "COMPLETED")
            self.assertEqual(run["durationMs"], 1250)
            self.assertEqual(run["inputs"], [{**source, "inputFacets": {}}])
            self.assertEqual(run["outputs"][0]["outputFacets"], target["outputFacets"])
            self.assertEqual(self.get(port, f"/api/v1/runs/{run_id}/events"), {"events": [start, done]})
            node_id = "job:spark:project.query"
            response = self.get(port, INGEST + "?" + urlencode({"nodeId": node_id, "depth": 1}))
            graph = {node["id"]: node for node in response["graph"]}
            self.assertEqual(graph[node_id]["inEdges"], [{"origin": "dataset:file:///test:source",
                                                          "destination": node_id}])
            self.assertEqual(graph[node_id]["outEdges"], [{"origin": node_id,
                                                           "destination": "dataset:file:///test:target"}])
            namespace = quote(source["namespace"], safe="")
            target_data = self.get(port, f"/api/v1/namespaces/{namespace}/datasets/target")
            self.assertEqual(target_data["fields"], [{"name": "id", "type": "long", "tags": []}])

    def test_batch_and_facet_replacement(self):
        base = {"namespace": "warehouse", "name": "tables/customer"}
        old = event("DatasetEvent", dataset={**base, "facets": {"observations": {"obsolete": 1},
                                                               "schema": {"fields": []}}})
        update = event("DatasetEvent", eventTime="2026-01-01T01:00:01Z",
                       dataset={**base, "facets": {"observations": {"sampleCount": 5}}})
        job = {"namespace": "metadata", "name": "report"}
        planned = event("JobEvent", job=job, inputs=[base])
        with self.server() as port:
            self.assertEqual(post(port, [old, update, planned, {"job": {}}], INGEST + "/batch")[0], 400)
            self.assertEqual(http_request(port, "GET", "/api/v1/namespaces/warehouse")[0], 404)
            self.assertEqual(post(port, [old, update, planned], INGEST + "/batch")[0], 204)
            dataset = self.get(port, "/api/v1/namespaces/warehouse/datasets/tables%2Fcustomer")
            self.assertEqual(dataset["facets"], {"observations": {"sampleCount": 5}, "schema": {"fields": []}})
            job_payload = self.get(port, "/api/v1/namespaces/metadata/jobs/report")
            self.assertEqual(job_payload["inputs"], [base])
            self.assertIsNone(job_payload["latestRun"])

    def test_token_and_conflicting_run_identity(self):
        token_path = self.directory / "token"
        token_path.write_text("fixture-token\n", encoding="ascii")
        run_id = str(uuid.uuid4())
        job = {"namespace": "airflow", "name": "dag.extract"}
        headers = {"Authorization": "Bearer fixture-token"}
        with self.server("--password-file", str(token_path)) as port:
            self.assertEqual(post(port, run_event(run_id, job, "START"))[0], 401)
            self.assertEqual(post(port, run_event(run_id, job, "START"), headers=headers)[0], 200)
            self.assertEqual(post(port, run_event(run_id, {**job, "name": "dag.load"}, "COMPLETE"), headers=headers)[0], 409)
            self.assertEqual(post(port, run_event(run_id, job, "UNKNOWN"), headers=headers)[0], 400)
            response = http_request(port, "GET", f"/api/v1/runs/{run_id}", headers=headers)
            self.assertEqual(response[0], 200)
            self.assertEqual(json.loads(response[2])["state"], "RUNNING")


if __name__ == "__main__":
    unittest.main()
