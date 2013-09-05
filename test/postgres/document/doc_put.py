import json
import psycopg2
import unittest

import test_data

DOCUMENT_PUT = "SELECT document_put(data, %s, %s, %s)::text from test;"
DOCUMENT_PUT_INT = "SELECT document_put_int(data, %s, %s)::text from test;"
DOCUMENT_PUT_STRING = "SELECT document_put_text(data, %s, %s)::text from test;"
DOCUMENT_PUT_BOOL = "SELECT document_put_bool(data, %s, %s)::text from test;"
DOCUMENT_PUT_FLOAT = "SELECT document_put_float(data, %s, %s)::text from test;"
NEW_KEY = "NEW_KEY"

class TestSerde(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("dbname=test user=postgres password=postgres")
        self.cur = conn.cursor()

    def teardown(self):
        cur.close()
        conn.close()

    def test_new(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_PUT_STRING, (NEW_KEY, TEST_STRING))
        new_dict = flat_dict.deepcopy().put("NEW_KEY", TEST_STRING)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

        self.cur.execute(DOCUMENT_PUT_INT, (NEW_KEY, TEST_INT))
        new_dict = flat_dict.deepcopy().put("NEW_KEY", TEST_INT)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

        self.cur.execute(DOCUMENT_PUT_FLOAT, (NEW_KEY, TEST_FLOAT))
        new_dict = flat_dict.deepcopy().put(NEW_KEY, TEST_FLOAT)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

        self.cur.execute(DOCUMENT_PUT_BOOL, (NEW_KEY, TEST_BOOL))
        new_dict = flat_dict.deepcopy().put(NEW_KEY, TEST_BOOL)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_overwrite(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_PUT_INT, (INT_KEY, TEST_INT))
        new_dict = flat_dict.deepcopy().put(INT_KEY, TEST_INT)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_array(self):
        arr = ["a", "bcd", "e"]
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_PUT, (NEW_KEY, "text[]", str(arr)))
        new_dict = flat_dict.deepcopy().put(NEW_KEY, arr)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_doc(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_PUT, (NEW_KEY, "document", json.dumps(flat_dict)))
        new_dict = flat_dict.deepcopy().put(NEW_KEY, flat_dict)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_nested_document_put(self):
        new_int = 10
        self.cur.execute(INSERT, nested_dict)
        self.cur.execute(DOCUMENT_PUT_INT, ("document.int", new_int))
        new_dict = nested_dict.deepcopy()
        new_dict["document"]["int"] = new_int
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_nested_document_put_failure(self):
        self.cur.execute(INSERT, flat_dict)
        with self.assertRaises(DatabaseError):
            self.cur.execute(DOCUMENT_PUT_INT, ("document.int", new_int))
