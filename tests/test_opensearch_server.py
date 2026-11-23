"""Exercise the standalone OpenSearch HTTP server."""

import concurrent.futures
import gzip
import json
import pathlib
import tempfile
import time
import unittest

from test_servers import http_request, running_server


class OpenSearchServerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-opensearch-")
        self.addCleanup(temporary.cleanup)
        self.directory = pathlib.Path(temporary.name)
        self.db_file = str(self.directory / "opensearch.sqlite3")

    def server(self):
        return running_server("opensearch_server.py", self.directory, "--db-file", self.db_file)

    def request(self, port, method, path, body=None, expected=200):
        encoded = json.dumps(body) if body is not None else None
        status, _, response = http_request(port, method, path, encoded, {"Content-Type": "application/json"})
        result = json.loads(response) if response else None
        self.assertEqual(status, expected, result)
        return result

    def test_realtime_get_and_persisted_search_visibility(self):
        with self.server() as port:
            root = self.request(port, "GET", "/")
            self.assertEqual(root["version"]["distribution"], "opensearch")
            self.request(port, "PUT", "/logs", {"settings": {"refresh_interval": "-1", "number_of_replicas": 0}})
            self.request(port, "PUT", "/logs/_doc/a", {"previous": 1}, expected=201)
            self.assertEqual(self.request(port, "GET", "/logs/_doc/a")["_source"], {"previous": 1})
            self.request(port, "GET", "/logs/_doc/a?realtime=false", expected=404)
            self.assertEqual(self.request(port, "GET", "/logs/_count")["count"], 0)
            self.request(port, "POST", "/logs/_refresh")
            self.request(port, "PUT", "/logs/_doc/a", {"current": 2})
            self.assertEqual(self.request(port, "GET", "/logs/_doc/a?realtime=false")["_source"], {"previous": 1})
            self.assertEqual(self.request(port, "GET", "/_cluster/health")["status"], "green")
        with self.server() as port:
            self.assertEqual(self.request(port, "GET", "/logs/_doc/a")["_source"], {"current": 2})
            self.assertEqual(self.request(port, "GET", "/logs/_search")["hits"]["hits"][0]["_source"], {"previous": 1})
            self.request(port, "DELETE", "/logs/_doc/a")
            self.request(port, "GET", "/logs/_doc/a", expected=404)
            self.assertEqual(self.request(port, "GET", "/logs/_count")["count"], 1)
            self.request(port, "POST", "/logs/_refresh")
            self.assertEqual(self.request(port, "GET", "/logs/_count")["count"], 0)

    def test_bulk_item_errors_mappings_and_optimistic_concurrency(self):
        with self.server() as port:
            self.request(port, "PUT", "/events", {"mappings": {"properties": {"count": {"type": "integer"}}}})
            lines = [
                {"create": {"_id": "a"}}, {"count": 1},
                {"create": {"_id": "a"}}, {"count": 2},
                {"index": {"_id": "bad"}}, {"count": "not a number", "bogus_field": 0},
                {"update": {"_id": "b"}}, {"doc": {"count": 3}, "doc_as_upsert": True},
                {"delete": {"_id": "missing"}},
            ]
            bulk = gzip.compress(("\n".join(map(json.dumps, lines)) + "\n").encode())
            status, _, response = http_request(port, "POST", "/events/_bulk?refresh=true", bulk,
                                              {"Content-Type": "application/x-ndjson", "Content-Encoding": "gzip"})
            self.assertEqual(status, 200, response)
            results = json.loads(response)
            self.assertTrue(results["errors"])
            self.assertEqual([next(iter(item.values()))["status"] for item in results["items"]], [201, 409, 400, 201, 404])
            get = self.request(port, "POST", "/events/_mget", {"ids": ["a", "b", "missing"]})["docs"]
            self.assertEqual([item["found"] for item in get], [True, True, False])
            self.assertEqual([item["_source"]["count"] for item in get[:2]], [1, 3])
            mapping = self.request(port, "GET", "/events/_mapping")["events"]["mappings"]["properties"]
            self.assertNotIn("bogus_field", mapping)
            self.assertEqual(mapping["count"]["type"], "integer")
            self.request(port, "PUT", "/events/_mapping", {"properties": {"count": {"type": "keyword"}}}, expected=400)

            path = f"/events/_update/a?if_seq_no={get[0]['_seq_no']}&if_primary_term={get[0]['_primary_term']}"
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as work:
                requests = [work.submit(http_request, port, "POST", path, json.dumps({"doc": {"count": value}}))
                            for value in (10, 20)]
                responses = [future.result(timeout=5) for future in requests]
            self.assertEqual(sorted(status for status, _, _ in responses), [200, 409])
            initial = self.request(port, "GET", "/events/_doc/a")
            result = self.request(port, "POST", "/events/_update/a", {"doc": initial["_source"]})
            self.assertEqual((result["result"], result["_seq_no"]), ("noop", initial["_seq_no"]))

    def test_search_filter_sort_source_projection_and_aggregations(self):
        with self.server() as port:
            mapping = {"category": {"type": "keyword"}, "score": {"type": "integer"}, "title": {"type": "text"}}
            self.request(port, "PUT", "/books", {"mappings": {"properties": mapping}})
            docs = {
                "1": {"title": "Cold Start", "score": 100, "category": "guide", "extra": [0]},
                "2": {"title": "Hot Start", "score": 90, "category": "guide", "extra": [1]},
                "3": {"title": "Hot Deal", "score": 105, "category": "shop", "extra": [2]},
            }
            for doc_id, source in docs.items():
                self.request(port, "PUT", f"/books/_doc/{doc_id}", source, expected=201)
            self.request(port, "POST", "/books/_refresh")
            query = {
                "query": {"bool": {"must": {"match": {"title": "Start deal"}},
                                   "filter": [{"term": {"category": "guide"}}, {"range": {"score": {"gte": 90}}}]}},
                "sort": [{"score": "desc"}], "size": 1, "_source": ["title"],
                "aggs": {"by_category": {"terms": {"field": "category"}, "aggs": {"points": {"sum": {"field": "score"}}}}},
            }
            search = self.request(port, "POST", "/books/_search", query)
            self.assertEqual(search["hits"]["total"], {"value": 2, "relation": "eq"})
            first = search["hits"]["hits"][0]
            self.assertEqual(first["_source"], {"title": "Cold Start"})
            self.assertEqual(first["sort"], [100])
            self.assertEqual(search["aggregations"]["by_category"]["buckets"],
                             [{"key": "guide", "doc_count": 2, "points": {"value": 190}}])
            result = self.request(port, "POST", "/books/_search", {**query, "search_after": first["sort"]})
            self.assertEqual([hit["_id"] for hit in result["hits"]["hits"]], ["2"])
            self.request(port, "POST", "/books/_search", {"query": {"script": {}}}, expected=400)

    def test_filtered_alias_write_target_and_atomic_alias_change(self):
        with self.server() as port:
            self.request(port, "PUT", "/v1", {"aliases": {"live": {"is_write_index": True}}})
            self.request(port, "PUT", "/v2", {"aliases": {"live": {}}})
            document = self.request(port, "PUT", "/live/_doc/a?refresh=true&require_alias=true", {"n": 1}, expected=201)
            self.assertEqual(document["_index"], "v1")
            self.request(port, "POST", "/_aliases", {"actions": [
                {"add": {"index": "v1", "alias": "large", "filter": {"range": {"n": {"gte": 10}}}}},
            ]})
            self.assertEqual(self.request(port, "GET", "/large/_count")["count"], 0)
            self.assertEqual(self.request(port, "GET", "/large/_doc/a")["_source"], {"n": 1})
            self.request(port, "POST", "/_aliases", {"actions": [
                {"remove": {"index": "v1", "alias": "large"}},
                {"add": {"index": "v2", "alias": "live", "is_write_index": True}},
            ]}, expected=400)
            self.request(port, "HEAD", "/_alias/large")
            self.request(port, "POST", "/_aliases", {"actions": [
                {"add": {"index": "v1", "alias": "live", "is_write_index": False}},
                {"add": {"index": "v2", "alias": "live", "is_write_index": True}},
            ]})
            document = self.request(port, "POST", "/live/_doc?refresh=true", {"n": 11}, expected=201)
            self.assertEqual(document["_index"], "v2")
            self.assertEqual(self.request(port, "POST", "/live/_count")["count"], 2)

    def test_wait_for_responds_only_after_refresh(self):
        with self.server() as port:
            self.request(port, "PUT", "/manual", {"settings": {"refresh_interval": "-1"}})
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as work:
                waiting = work.submit(self.request, port, "PUT", "/manual/_doc/a?refresh=wait_for&timeout=3s", {"n": 100}, 201)
                deadline = time.monotonic() + 1
                while time.monotonic() < deadline:
                    if http_request(port, "GET", "/manual/_doc/a")[0] == 200:
                        break
                    time.sleep(0.01)
                else:
                    self.fail("the WAIT_FOR index request did not write its document")
                self.assertFalse(waiting.done())
                self.assertEqual(self.request(port, "GET", "/manual/_count")["count"], 0)
                self.request(port, "POST", "/manual/_refresh")
                self.assertEqual(waiting.result(timeout=2)["result"], "created")
            self.assertEqual(self.request(port, "GET", "/manual/_count")["count"], 1)
            self.request(port, "PUT", "/auto", {"settings": {"refresh_interval": "20ms"}})
            self.request(port, "PUT", "/auto/_doc/a?refresh=wait_for&timeout=3s", {"n": 1}, expected=201)
            self.assertEqual(self.request(port, "GET", "/auto/_count")["count"], 1)
