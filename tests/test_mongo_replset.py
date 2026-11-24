"""Exercise the MongoDB test server's single-member replica set mode over raw OP_MSG."""

import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from uuid import uuid4

SCRIPT = Path(__file__).resolve().parents[1] / "mongo_server.py"
HEADER = struct.Struct("<iiii")


class Client:
    def __init__(self, mongo, port):
        self.mongo = mongo
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.request_id = 0

    def close(self):
        self.sock.close()

    def send(self, db, command):
        body = b"\0\0\0\0\0" + self.mongo["encode_bson"]({**command, "$db": db})
        self.request_id += 1
        self.sock.sendall(HEADER.pack(HEADER.size + len(body), self.request_id, 0, 2013) + body)

    def receive(self):
        def read(size):
            data = b""
            while len(data) < size:
                chunk = self.sock.recv(size - len(data))
                if not chunk:
                    raise EOFError("connection closed")
                data += chunk
            return data

        size = HEADER.unpack(read(HEADER.size))[0]
        return self.mongo["decode_bson"](read(size - HEADER.size)[5:])[0]

    def __call__(self, db, command):
        self.send(db, command)
        return self.receive()


class ReplicaSetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mongo = runpy.run_path(str(SCRIPT))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-mongo-replset-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)
        self.db_file = self.directory / "replset.sqlite3"

    def server(self, *args, replica_set=True):
        command = [sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0", *args]
        if replica_set:
            command += ["--replica-set", "rs0"]
        env = {key: value for key, value in os.environ.items() if not key.endswith("_TEST_PASSWORD")}
        process = subprocess.Popen(
            command, cwd=self.directory, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )

        def stop():
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()

        line = process.stdout.readline()
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        if match is None:
            stop()
            self.fail(f"server did not start: {line}{process.stdout.read()}")
        self.addCleanup(stop)
        return int(match[1]), stop

    def client(self, port):
        client = Client(self.mongo, port)
        self.addCleanup(client.close)
        return client

    def lsid(self):
        return {"id": self.mongo["Binary"](uuid4().bytes, 4)}

    def txn(self, session, number, **fields):
        return {"lsid": session, "txnNumber": self.mongo["Int64"](number), "autocommit": False, **fields}

    def assertCode(self, reply, code, label=None):
        self.assertEqual((reply.get("ok"), reply.get("code")), (0.0, code), reply)
        if label is not None:
            self.assertIn(label, reply.get("errorLabels", []), reply)

    @staticmethod
    def watch(client, db, target, options=None, *stages):
        pipeline = [{"$changeStream": options or {}}, *stages]
        return client(db, {"aggregate": target, "pipeline": pipeline, "cursor": {}})

    def events(self, client, db, cursor_id, collection, wait=500):
        reply = client(db, {"getMore": cursor_id, "collection": collection, "maxTimeMS": wait})
        self.assertEqual(reply["ok"], 1.0, reply)
        return reply["cursor"]

    def test_hello_reports_a_one_member_primary_and_gossips_cluster_time(self):
        port, _ = self.server()
        client = self.client(port)
        hello = client("admin", {"hello": 1})
        me = f"127.0.0.1:{port}"
        self.assertEqual(
            (hello["setName"], hello["hosts"], hello["primary"], hello["me"]), ("rs0", [me], me, me)
        )
        self.assertIs(hello["isWritablePrimary"], True)
        self.assertIs(hello["secondary"], False)
        self.assertIn("majorityOpTime", hello["lastWrite"])
        self.assertEqual(hello["$clusterTime"]["signature"]["keyId"], 0)
        legacy = client("admin", {"isMaster": 1})
        self.assertIs(legacy["ismaster"], True)
        first = client("app", {"insert": "items", "documents": [{"_id": 1}]})
        second = client("app", {"insert": "items", "documents": [{"_id": 2}]})
        timestamp = self.mongo["ts_value"]
        self.assertGreater(timestamp(second["operationTime"], "t"), timestamp(first["operationTime"], "t"))
        self.assertEqual(second["opTime"]["ts"], second["operationTime"])
        status = client("admin", {"replSetGetStatus": 1})
        self.assertEqual((status["set"], status["members"][0]["stateStr"]), ("rs0", "PRIMARY"))
        self.assertEqual(client("admin", {"replSetGetConfig": 1})["config"]["members"][0]["host"], me)
        self.assertCode(client("admin", {"replSetInitiate": {}}), 23)
        started = time.monotonic()
        streamed = client(
            "admin", {"hello": 1, "topologyVersion": hello["topologyVersion"], "maxAwaitTimeMS": 300}
        )
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertEqual(streamed["topologyVersion"], hello["topologyVersion"])
        concern = client("app", {"insert": "items", "documents": [{"_id": 3}], "writeConcern": {"w": 2}})
        self.assertEqual((concern["n"], concern["writeConcernError"]["code"]), (1, 100))

    def test_transactions_isolate_commit_atomically_and_report_states(self):
        port, _ = self.server()
        client, other = self.client(port), self.client(port)
        client(
            "bank",
            {"insert": "accounts", "documents": [{"_id": "a", "balance": 10}, {"_id": "b", "balance": 0}]},
        )
        session = self.lsid()
        reply = client(
            "bank",
            {
                "update": "accounts",
                "updates": [{"q": {"_id": "a"}, "u": {"$inc": {"balance": -5}}}],
                **self.txn(session, 1, startTransaction=True, readConcern={"level": "snapshot"}),
            },
        )
        self.assertEqual(reply["nModified"], 1, reply)
        client(
            "bank", {"insert": "accounts", "documents": [{"_id": "c", "balance": 5}], **self.txn(session, 1)}
        )
        other("bank", {"insert": "audit", "documents": [{"_id": "outside"}]})
        inside = client("bank", {"find": "accounts", "sort": {"_id": 1}, **self.txn(session, 1)})
        self.assertEqual([doc["balance"] for doc in inside["cursor"]["firstBatch"]], [5, 0, 5])
        self.assertEqual(
            client("bank", {"find": "audit", **self.txn(session, 1)})["cursor"]["firstBatch"], []
        )
        outside = other("bank", {"find": "accounts", "sort": {"_id": 1}})
        self.assertEqual([doc["balance"] for doc in outside["cursor"]["firstBatch"]], [10, 0])
        self.assertCode(client("bank", {"count": "accounts", **self.txn(session, 1)}), 263)
        self.assertCode(
            client(
                "bank",
                {
                    "insert": "accounts",
                    "documents": [{}],
                    "writeConcern": {"w": 1},
                    **self.txn(session, 1),
                },
            ),
            72,
        )
        self.assertEqual(client("admin", {"commitTransaction": 1, **self.txn(session, 1)})["ok"], 1.0)
        self.assertEqual(client("admin", {"commitTransaction": 1, **self.txn(session, 1)})["ok"], 1.0)
        self.assertCode(client("admin", {"abortTransaction": 1, **self.txn(session, 1)}), 256)
        committed = other("bank", {"find": "accounts", "sort": {"_id": 1}})["cursor"]["firstBatch"]
        self.assertEqual([doc["balance"] for doc in committed], [5, 0, 5])

        client(
            "bank",
            {
                "insert": "accounts",
                "documents": [{"_id": "d"}],
                **self.txn(session, 2, startTransaction=True),
            },
        )
        self.assertEqual(client("admin", {"abortTransaction": 1, **self.txn(session, 2)})["ok"], 1.0)
        self.assertCode(
            client("bank", {"find": "accounts", **self.txn(session, 2)}), 251, "TransientTransactionError"
        )
        self.assertCode(
            client("bank", {"find": "accounts", **self.txn(session, 9)}), 251, "TransientTransactionError"
        )
        self.assertCode(
            client("bank", {"find": "accounts", **self.txn(session, 1, startTransaction=True)}), 225
        )
        self.assertEqual(other("bank", {"count": "accounts", "query": {"_id": "d"}})["n"], 0)
        self.assertCode(
            client("bank", {"find": "accounts", "lsid": session, "txnNumber": self.mongo["Int64"](10)}), 50768
        )

        client(
            "bank",
            {
                "insert": "accounts",
                "documents": [{"_id": "e"}],
                **self.txn(session, 11, startTransaction=True),
            },
        )
        client("admin", {"endSessions": [session]})
        self.assertCode(client("admin", {"commitTransaction": 1, **self.txn(session, 11)}), 251)
        client("admin", {"setParameter": 1, "transactionLifetimeLimitSeconds": 1})
        client(
            "bank",
            {
                "insert": "accounts",
                "documents": [{"_id": "f"}],
                **self.txn(session, 12, startTransaction=True),
            },
        )
        time.sleep(1.3)
        self.assertCode(
            client("admin", {"commitTransaction": 1, **self.txn(session, 12)}),
            251,
            "TransientTransactionError",
        )

    def test_write_conflicts_abort_transactions_and_block_other_writers(self):
        port, _ = self.server()
        first, second, plain = self.client(port), self.client(port), self.client(port)
        plain("app", {"insert": "docs", "documents": [{"_id": 1, "v": 0}, {"_id": 2, "v": 0}]})
        one, two = self.lsid(), self.lsid()
        update = lambda key, value: {
            "update": "docs",
            "updates": [{"q": {"_id": key}, "u": {"$set": {"v": value}}}],
        }
        first("app", {**update(1, 1), **self.txn(one, 1, startTransaction=True)})
        conflict = second("app", {**update(1, 2), **self.txn(two, 1, startTransaction=True)})
        self.assertCode(conflict, 112, "TransientTransactionError")
        self.assertCode(second("app", {"find": "docs", **self.txn(two, 1)}), 251)

        snapshot = second("app", {"find": "docs", **self.txn(two, 2, startTransaction=True)})
        self.assertEqual(snapshot["cursor"]["firstBatch"][1]["v"], 0)
        plain("app", update(2, 5))
        reread = second("app", {"find": "docs", "filter": {"_id": 2}, **self.txn(two, 2)})
        self.assertEqual(reread["cursor"]["firstBatch"][0]["v"], 0)
        self.assertCode(second("app", {**update(2, 6), **self.txn(two, 2)}), 112, "TransientTransactionError")

        self.assertCode(plain("app", {**update(1, 9), "maxTimeMS": 200}), 50)
        self.assertCode(plain("app", {"drop": "docs", "maxTimeMS": 200}), 50)
        timer = threading.Timer(0.3, lambda: first("admin", {"commitTransaction": 1, **self.txn(one, 1)}))
        timer.start()
        started = time.monotonic()
        waited = plain("app", {"update": "docs", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"v": 10}}}]})
        timer.join()
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertEqual(waited["nModified"], 1)
        self.assertEqual(
            plain("app", {"find": "docs", "filter": {"_id": 1}})["cursor"]["firstBatch"][0]["v"], 11
        )

    def test_transactions_read_a_point_in_time_catalog(self):
        port, _ = self.server()
        client, other = self.client(port), self.client(port)
        client("pit", {"insert": "kept", "documents": [{"_id": 1}]})
        client("pit", {"insert": "dropped", "documents": [{"_id": 2}]})
        session = self.lsid()
        client("pit", {"find": "kept", **self.txn(session, 1, startTransaction=True)})
        other("pit", {"drop": "dropped"})
        other("pit", {"insert": "created", "documents": [{"_id": 3}]})
        old = client("pit", {"find": "dropped", **self.txn(session, 1)})["cursor"]["firstBatch"]
        self.assertEqual(old, [{"_id": 2}])
        self.assertEqual(
            client("pit", {"find": "created", **self.txn(session, 1)})["cursor"]["firstBatch"], []
        )
        insert = other("pit", {"insert": "kept", "documents": [{"_id": 4}]})
        at = insert["operationTime"]
        other("pit", {"delete": "kept", "deletes": [{"q": {}, "limit": 0}]})
        past = other("pit", {"find": "kept", "readConcern": {"level": "snapshot", "atClusterTime": at}})
        self.assertEqual(past["cursor"]["firstBatch"], [{"_id": 1}, {"_id": 4}])
        self.assertEqual(past["cursor"]["atClusterTime"], at)
        self.assertCode(other("pit", {"count": "kept", "readConcern": {"level": "snapshot"}}), 72)
        self.assertCode(
            other(
                "pit",
                {
                    "find": "kept",
                    "readConcern": {"level": "snapshot", "atClusterTime": self.mongo["Timestamp"](1, 1)},
                },
            ),
            239,
        )

    def test_retryable_writes_execute_once_and_survive_restart(self):
        port, stop = self.server("--db-file", str(self.db_file))
        client = self.client(port)
        session = self.lsid()
        Int64 = self.mongo["Int64"]
        insert = {
            "insert": "jobs",
            "documents": [{"_id": 1}, {"_id": 2}],
            "lsid": session,
            "txnNumber": Int64(1),
        }
        client.send("work", insert)
        client.close()
        client = self.client(port)
        retried = client("work", insert)
        self.assertEqual((retried["n"], retried["retriedStmtIds"]), (2, [0, 1]))
        modify = {
            "findAndModify": "jobs",
            "query": {"_id": 1},
            "update": {"$inc": {"runs": 1}},
            "new": True,
            "lsid": session,
            "txnNumber": Int64(2),
        }
        self.assertEqual(client("work", modify)["value"], {"_id": 1, "runs": 1})
        self.assertEqual(client("work", modify)["retriedStmtId"], 0)
        multi = client(
            "work",
            {
                "update": "jobs",
                "updates": [{"q": {}, "u": {"$set": {"x": 1}}, "multi": True}],
                "lsid": session,
                "txnNumber": Int64(3),
            },
        )
        self.assertEqual(multi["writeErrors"][0]["code"], 72)
        self.assertCode(client("work", {**insert, "txnNumber": Int64(1)}), 225)
        stop()
        port, _ = self.server("--db-file", str(self.db_file))
        client = self.client(port)
        again = client("work", modify)
        self.assertEqual((again["value"], again["retriedStmtId"]), ({"_id": 1, "runs": 1}, 0))
        self.assertEqual(
            client("work", {"find": "jobs", "filter": {"_id": 1}})["cursor"]["firstBatch"][0]["runs"], 1
        )

    def test_change_streams_report_document_and_collection_events(self):
        port, _ = self.server()
        client, writer = self.client(port), self.client(port)
        writer("feed", {"create": "items", "changeStreamPreAndPostImages": {"enabled": True}})
        opened = client(
            "feed",
            {
                "aggregate": "items",
                "pipeline": [
                    {
                        "$changeStream": {
                            "fullDocument": "required",
                            "fullDocumentBeforeChange": "whenAvailable",
                        }
                    }
                ],
                "cursor": {},
            },
        )
        stream = opened["cursor"]["id"]
        self.assertEqual(opened["cursor"]["firstBatch"], [])
        self.assertIn("_data", opened["cursor"]["postBatchResumeToken"])
        database = self.watch(client, "feed", 1, {}, {"$match": {"operationType": {"$ne": "insert"}}})
        cluster = self.watch(client, "admin", 1, {"allChangesForCluster": True}, {"$project": {"ns": 1}})
        writer(
            "feed", {"insert": "items", "documents": [{"_id": 1, "tags": [1, 2], "sub": {"a": 1, "b": 2}}]}
        )
        writer(
            "feed",
            {
                "update": "items",
                "updates": [
                    {
                        "q": {"_id": 1},
                        "u": {"$set": {"sub.a": 5}, "$unset": {"sub.b": ""}, "$push": {"tags": 3}},
                    }
                ],
            },
        )
        writer("feed", {"update": "items", "updates": [{"q": {"_id": 1}, "u": {"_id": 1, "replaced": True}}]})
        writer("feed", {"delete": "items", "deletes": [{"q": {"_id": 1}, "limit": 1}]})
        writer("admin", {"renameCollection": "feed.items", "to": "feed.renamed"})
        batch = self.events(client, "feed", stream, "items")
        kinds = [event["operationType"] for event in batch["nextBatch"]]
        self.assertEqual(kinds, ["insert", "update", "replace", "delete", "rename", "invalidate"])
        self.assertEqual(batch["id"], 0)
        update = batch["nextBatch"][1]
        self.assertEqual(
            update["updateDescription"],
            {
                "updatedFields": {"sub.a": 5, "tags.2": 3},
                "removedFields": ["sub.b"],
                "truncatedArrays": [],
            },
        )
        self.assertEqual(update["fullDocument"], {"_id": 1, "tags": [1, 2, 3], "sub": {"a": 5}})
        self.assertEqual(update["fullDocumentBeforeChange"]["sub"], {"a": 1, "b": 2})
        self.assertEqual(batch["nextBatch"][4]["to"], {"db": "feed", "coll": "renamed"})
        later = self.events(client, "feed", database["cursor"]["id"], "$cmd.aggregate")["nextBatch"]
        self.assertEqual(
            [event["operationType"] for event in later], ["update", "replace", "delete", "rename"]
        )
        everything = self.events(client, "admin", cluster["cursor"]["id"], "$cmd.aggregate")["nextBatch"]
        self.assertEqual(len(everything), 5)
        self.assertEqual(set(everything[0]), {"_id", "ns"})

        token = batch["nextBatch"][0]["_id"]
        resumed = self.watch(client, "feed", "items", {"resumeAfter": token})
        self.assertEqual(
            [event["operationType"] for event in resumed["cursor"]["firstBatch"]],
            ["update", "replace", "delete", "rename", "invalidate"],
        )
        invalidate = batch["nextBatch"][-1]["_id"]
        self.assertCode(self.watch(client, "feed", "items", {"resumeAfter": invalidate}), 260)
        after = self.watch(client, "feed", "items", {"startAfter": invalidate})
        self.assertEqual((after["ok"], after["cursor"]["firstBatch"]), (1.0, []))
        self.assertCode(self.watch(client, "feed", "items", {}, {"$group": {"_id": 1}}), 20)
        self.assertCode(self.watch(client, "feed", 1, {"allChangesForCluster": True}), 72)
        modified = self.watch(client, "feed", "renamed", {}, {"$project": {"_id": 0}})
        writer("feed", {"insert": "renamed", "documents": [{"_id": 2}]})
        failure = client(
            "feed", {"getMore": modified["cursor"]["id"], "collection": "renamed", "maxTimeMS": 500}
        )
        self.assertCode(failure, 280)

    def test_change_streams_describe_collection_moves_like_mongodb(self):
        port, _ = self.server()
        client, writer = self.client(port), self.client(port)
        writer("one", {"insert": "source", "documents": [{"_id": 1}]})
        writer("two", {"insert": "target", "documents": [{"_id": 9}]})
        writer("two", {"insert": "rows", "documents": [{"_id": 5, "v": 1}, {"_id": 6, "v": 2}]})
        streams = {
            "one": self.watch(client, "one", 1),
            "two": self.watch(client, "two", 1, {"showExpandedEvents": True}),
        }
        writer("admin", {"renameCollection": "one.source", "to": "two.target", "dropTarget": True})
        writer(
            "two", {"aggregate": "rows", "pipeline": [{"$match": {"v": 2}}, {"$out": "best"}], "cursor": {}}
        )
        writer("two", {"createIndexes": "best", "indexes": [{"key": {"v": 1}, "name": "v_1"}]})
        def shape(events):
            names = [event["ns"].get("coll", "") for event in events]
            names = [re.sub(r"^tmp\w{5}\.renameCollection$", "copy", name) for name in names]
            names = [re.sub(r"^tmp\.agg_out\.[0-9a-f-]{36}$", "out", name) for name in names]
            return [(event["operationType"], name) for event, name in zip(events, names)]

        first = self.events(client, "one", streams["one"]["cursor"]["id"], "$cmd.aggregate")["nextBatch"]
        self.assertEqual(shape(first), [("drop", "source")])
        second = self.events(client, "two", streams["two"]["cursor"]["id"], "$cmd.aggregate")["nextBatch"]
        self.assertEqual(
            shape(second),
            [
                ("create", "copy"),
                ("insert", "copy"),
                ("rename", "copy"),
                ("create", "out"),
                ("insert", "out"),
                ("rename", "out"),
                ("createIndexes", "best"),
            ],
        )
        self.assertEqual(second[2]["to"], {"db": "two", "coll": "target"})
        self.assertEqual(second[5]["to"], {"db": "two", "coll": "best"})
        self.assertIn("collectionUUID", second[6])
        cursor = client("two", {"find": "rows", "batchSize": 1, "readConcern": {"level": "snapshot"}})[
            "cursor"
        ]
        more = client(
            "two",
            {
                "getMore": cursor["id"],
                "collection": "rows",
                "readConcern": {"level": "snapshot", "atClusterTime": cursor["atClusterTime"]},
            },
        )
        self.assertEqual(
            (more["cursor"]["nextBatch"], more["cursor"]["atClusterTime"]),
            ([{"_id": 6, "v": 2}], cursor["atClusterTime"]),
        )

    def test_change_streams_wait_follow_transactions_and_resume_after_restart(self):
        port, stop = self.server("--db-file", str(self.db_file), "--change-log-size", "5")
        client, writer = self.client(port), self.client(port)
        opened = self.watch(client, "feed", "orders", {"fullDocument": "updateLookup"})
        stream = opened["cursor"]["id"]
        started = time.monotonic()
        self.assertEqual(self.events(client, "feed", stream, "orders", wait=300)["nextBatch"], [])
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        session = self.lsid()
        writer(
            "feed",
            {
                "insert": "orders",
                "documents": [{"_id": 1, "n": 1}],
                **self.txn(session, 1, startTransaction=True),
            },
        )
        writer(
            "feed",
            {
                "update": "orders",
                "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}],
                **self.txn(session, 1),
            },
        )
        self.assertEqual(self.events(client, "feed", stream, "orders", wait=100)["nextBatch"], [])
        timer = threading.Timer(
            0.2, lambda: writer("admin", {"commitTransaction": 1, **self.txn(session, 1)})
        )
        timer.start()
        batch = self.events(client, "feed", stream, "orders", wait=3000)
        timer.join()
        events = batch["nextBatch"]
        self.assertEqual([event["operationType"] for event in events], ["insert", "update"])
        self.assertEqual(events[0]["clusterTime"], events[1]["clusterTime"])
        self.assertEqual(events[1]["txnNumber"], 1)
        self.assertEqual(events[1]["lsid"]["id"], session["id"])
        self.assertEqual(events[1]["fullDocument"], {"_id": 1, "n": 2})
        token = batch["postBatchResumeToken"]
        stop()

        port, stop = self.server("--db-file", str(self.db_file), "--change-log-size", "5")
        client = self.client(port)
        self.assertEqual(client("admin", {"commitTransaction": 1, **self.txn(session, 1)})["ok"], 1.0)
        client("feed", {"insert": "orders", "documents": [{"_id": 2}]})
        resumed = self.watch(client, "feed", "orders", {"resumeAfter": token})
        self.assertEqual([event["documentKey"] for event in resumed["cursor"]["firstBatch"]], [{"_id": 2}])
        client("feed", {"insert": "orders", "documents": [{"_id": key} for key in range(3, 12)]})
        lost = self.watch(client, "feed", "orders", {"resumeAfter": token})
        self.assertCode(lost, 286)

    def test_standalone_mode_still_rejects_replica_set_features(self):
        port, _ = self.server(replica_set=False)
        client = self.client(port)
        hello = client("admin", {"hello": 1})
        self.assertNotIn("setName", hello)
        self.assertNotIn("$clusterTime", hello)
        session = self.lsid()
        self.assertCode(
            client("app", {"insert": "x", "documents": [{}], **self.txn(session, 1, startTransaction=True)}),
            20,
        )
        self.assertCode(self.watch(client, "app", "x"), 40573)
        self.assertCode(client("admin", {"replSetGetStatus": 1}), 76)


if __name__ == "__main__":
    unittest.main()
