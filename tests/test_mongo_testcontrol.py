"""Exercise the MongoDB test server's failCommand failpoint and jjapReset over the wire protocol."""

import contextlib
import os
from pathlib import Path
import queue
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "mongo_server.py"
LISTENING = re.compile(r"mongodb://127\.0\.0\.1:(\d+)")


class MongoTestControlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mongo = runpy.run_path(str(SCRIPT))
        cls.MongoError = cls.mongo["MongoError"]

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="jjap-mongo-testcontrol-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.script = self.directory / SCRIPT.name
        shutil.copyfile(SCRIPT, self.script)

    @contextlib.contextmanager
    def server(self, *args):
        env = {key: value for key, value in os.environ.items() if not key.endswith("_TEST_PASSWORD")}
        command = [sys.executable, "-I", "-S", "-B", "-u", str(self.script), "--port", "0", *args]
        with subprocess.Popen(command, cwd=self.directory, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True) as process:
            lines = queue.Queue()
            reader = threading.Thread(target=lambda: [lines.put(line) for line in process.stdout] + [lines.put(None)],
                                      daemon=True)
            reader.start()
            try:
                while True:
                    line = lines.get(timeout=10)
                    self.assertIsNotNone(line, "server exited during startup")
                    if match := LISTENING.search(line):
                        yield int(match[1])
                        break
            finally:
                process.terminate()
                process.wait(timeout=10)
                reader.join(timeout=5)

    def connect(self, port):
        connection = self.mongo["ShellConnection"]("127.0.0.1", port).__enter__()
        self.addCleanup(connection.close)
        return connection

    def fail(self, connection, mode, **data):
        return connection.command("admin", {"configureFailPoint": "failCommand", "mode": mode, "data": data})

    def error(self, connection, db, command):
        with self.assertRaises(self.MongoError) as caught:
            connection.command(db, command)
        return caught.exception

    def test_test_commands_require_the_serve_flag(self):
        with self.server() as port:
            client = self.connect(port)
            for command in ({"configureFailPoint": "failCommand", "mode": "off"}, {"jjapReset": 1}):
                self.assertEqual(self.error(client, "admin", command).code, 59)
            self.assertNotIn("configureFailPoint", client.command("admin", {"listCommands": 1})["commands"])
            self.assertIs(client.command("admin", {"getParameter": 1, "enableTestCommands": 1})["enableTestCommands"], False)

    def test_fail_command_modes_counts_and_filters(self):
        with self.server("--enable-test-commands") as port:
            client = self.connect(port)
            self.assertEqual(self.error(client, "test", {"configureFailPoint": "failCommand", "mode": "off"}).code, 13)
            self.assertEqual(self.error(client, "admin", {"configureFailPoint": "nope", "mode": "off"}).code, 266)
            self.assertEqual(self.error(client, "admin", {"configureFailPoint": "failCommand"}).code, 20)
            for mode, data, code in (("sometimes", {}, 2), ({"times": -1}, {}, 2), ({"bogus": 1}, {}, 2), ("off", 1, 14)):
                configure = {"configureFailPoint": "failCommand", "mode": mode, "data": data}
                self.assertEqual(self.error(client, "admin", configure).code, code)
            self.assertEqual(self.error(client, "admin", {"setParameter": 1, "enableTestCommands": 0}).code, 20)
            self.assertIs(client.command("admin", {"getParameter": 1, "enableTestCommands": 1})["enableTestCommands"], True)

            self.assertEqual(self.fail(client, {"times": 2}, failCommands=["ping", "configureFailPoint"],
                                       errorCode=91, errorLabels=["Custom"])["count"], 0)
            client.command("test", {"find": "c"})
            error = self.error(client, "admin", {"ping": 1})
            self.assertEqual((error.code, error.details["codeName"], error.details["errorLabels"]),
                             (91, "ShutdownInProgress", ["Custom"]))
            self.assertEqual(str(error), "Failing command via 'failCommand' failpoint")
            self.assertEqual(self.fail(client, "off")["count"], 1)
            self.assertEqual(self.fail(client, {"skip": 1}, failCommands=["ping"], errorCode=12345)["count"], 1)
            client.command("admin", {"ping": 1})
            error = self.error(client, "admin", {"ping": 1})
            self.assertEqual((error.details["codeName"], "errorLabels" in error.details), ("Location12345", False))
            self.fail(client, {"times": 0}, failCommands=["ping"], errorCode=2)
            self.error(client, "admin", {"ping": 1})
            client.command("admin", {"ping": 1})
            self.fail(client, {"activationProbability": 0}, failCommands=["ping"], errorCode=2)
            client.command("admin", {"ping": 1})
            self.fail(client, {"activationProbability": 1}, failCommands=["ping"], errorCode=2)
            self.error(client, "admin", {"ping": 1})

            self.fail(client, "alwaysOn", failCommands=["find"], namespace="test.target", errorCode=2)
            client.command("test", {"find": "other"})
            self.error(client, "test", {"find": "target"})
            self.fail(client, "alwaysOn", failCommands=["ping"], appName="special", errorCode=2)
            client.command("admin", {"ping": 1})
            tagged = self.connect(port)
            tagged.command("admin", {"hello": 1, "client": {"application": {"name": "special"}}})
            self.error(tagged, "admin", {"ping": 1})
            self.fail(client, "alwaysOn", failCommands=["ping"], errorCode=11000)
            self.assertEqual(self.error(client, "admin", {"ping": 1}).code, 40671)
            self.fail(client, "alwaysOn", failCommands=["ping"], errorCode=11000,
                      errorExtraInfo={"keyPattern": {"a": 1}, "keyValue": {"a": 1}})
            self.assertEqual(self.error(client, "admin", {"ping": 1}).details["keyPattern"], {"a": 1})
            self.fail(client, "off")

    def test_close_block_and_write_concern_error(self):
        with self.server("--enable-test-commands") as port:
            client = self.connect(port)
            victim = self.connect(port)
            self.fail(client, {"times": 1}, failCommands=["find"], closeConnection=True, errorCode=2)
            with self.assertRaises((OSError, ValueError)):
                victim.command("test", {"find": "c"})
            self.assertEqual(self.connect(port).command("test", {"find": "c"})["cursor"]["firstBatch"], [])

            self.fail(client, {"times": 1}, failCommands=["find"], blockConnection=True, blockTimeMS=300)
            started = time.monotonic()
            client.command("test", {"find": "c"})
            self.assertGreaterEqual(time.monotonic() - started, 0.3)
            self.fail(client, {"times": 1}, failCommands=["find"], blockConnection=True, blockTimeMS=300)
            self.assertEqual(self.error(client, "test", {"find": "c", "maxTimeMS": 50}).code, 50)

            failure = {"code": 91, "errmsg": "Replication is being shut down"}
            self.fail(client, "alwaysOn", failCommands=["insert", "find"], writeConcernError=failure, errorLabels=["L1"])
            reply = client.command("test", {"insert": "c", "documents": [{"_id": 1}]})
            self.assertEqual((reply["n"], reply["writeConcernError"], reply["errorLabels"]), (1, failure, ["L1"]))
            reply = client.command("test", {"insert": "c", "documents": [{"_id": 1}]})
            self.assertEqual((reply["writeErrors"][0]["code"], reply["writeConcernError"]), (11000, failure))
            self.assertNotIn("writeConcernError", client.command("test", {"find": "c"}))
            # Blocking consumes the single activation, so the write concern error is not added as well.
            self.fail(client, {"times": 1}, failCommands=["insert"], blockConnection=True, blockTimeMS=10,
                      writeConcernError=failure)
            self.assertNotIn("writeConcernError", client.command("test", {"insert": "c", "documents": [{"_id": 2}]}))
            self.assertEqual(self.fail(client, "off")["count"], 6)

    def test_automatic_error_labels_follow_mongod(self):
        labels = self.mongo["fail_command_labels"]
        retryable = {"insert": "c", "txnNumber": 1}
        transaction = {"insert": "c", "txnNumber": 1, "autocommit": False}
        commit = {"commitTransaction": 1, "txnNumber": 1, "autocommit": False}
        self.assertEqual(labels({}, retryable, "insert", {}, code=91), ["RetryableWriteError"])
        self.assertEqual(labels({}, retryable, "insert", {}, code=2), [])
        self.assertEqual(labels({}, retryable, "insert", {}, write_concern_code=91), ["RetryableWriteError"])
        self.assertEqual(labels({}, retryable, "insert", {"internalClient": True}, code=91), [])
        self.assertEqual(labels({}, {"insert": "c"}, "insert", {}, code=91), [])
        self.assertEqual(labels({}, transaction, "insert", {}, code=112), ["TransientTransactionError"])
        self.assertEqual(labels({}, transaction, "insert", {}, code=91), ["TransientTransactionError"])
        self.assertEqual(labels({}, commit, "committransaction", {}, code=91), ["RetryableWriteError"])
        self.assertEqual(labels({}, commit, "committransaction", {}, code=251), ["TransientTransactionError"])
        self.assertEqual(labels({"errorLabels": []}, retryable, "insert", {}, code=91), [])

    def test_reset_drops_data_cursors_and_failpoints_and_persists(self):
        database = self.directory / "state.sqlite3"
        password = self.directory / "password.txt"
        password.write_text("reset-secret\n", encoding="utf-8")
        args = ("--enable-test-commands", "--db-file", str(database), "--password-file", str(password),
                "--ttl-interval", "30")
        with self.server(*args) as port:
            anonymous = self.connect(port)
            self.assertEqual(self.error(anonymous, "admin", {"jjapReset": 1}).code, 13)
            client = self.connect(port)
            client.authenticate("admin", "admin", "reset-secret")
            client.command("app", {"insert": "items", "documents": [{"_id": index, "v": index} for index in range(5)]})
            client.command("app", {"createIndexes": "items", "indexes": [{"key": {"v": 1}, "name": "v_1", "unique": True}]})
            client.command("other", {"create": "view", "viewOn": "items", "pipeline": []})
            cursor = client.command("app", {"find": "items", "batchSize": 2})["cursor"]["id"]
            client.command("admin", {"setParameter": 1, "ttlMonitorSleepSecs": 5})
            self.fail(client, "alwaysOn", failCommands=["ping"], errorCode=2)
            self.assertEqual(self.error(client, "app", {"jjapReset": 1}).code, 13)
            self.assertEqual(client.command("admin", {"jjapReset": 1})["reset"], True)
            self.assertEqual(client.command("admin", {"listDatabases": 1})["databases"], [])
            self.assertEqual(self.error(client, "app", {"getMore": cursor, "collection": "items"}).code, 43)
            client.command("admin", {"ping": 1})
            parameters = client.command("admin", {"getParameter": 1, "ttlMonitorSleepSecs": 1})
            self.assertEqual(parameters["ttlMonitorSleepSecs"], 30)
            self.assertEqual(self.fail(client, "off")["count"], 0)
            reply = client.command("app", {"insert": "fresh", "documents": [{"_id": 1}]})
            self.assertEqual(reply["n"], 1)
            record = client.command("app", {"find": "fresh", "showRecordId": True})["cursor"]["firstBatch"][0]
            self.assertEqual(record["$recordId"], 1)
            client.command("admin", {"jjapReset": 1})
        with self.server("--db-file", str(database)) as port:
            self.assertEqual(self.connect(port).command("admin", {"listDatabases": 1})["databases"], [])


if __name__ == "__main__":
    unittest.main()
