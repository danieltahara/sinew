import json
import psycopg2
import unittest

import test_data

DOCUMENT_GET = "SELECT document_get(data, %s, %s) FROM test;"
DOCUMENT_GET_INT = "SELECT document_get_int(data, %s) FROM test;"
DOCUMENT_GET_STRING = "SELECT document_get_text(data, %s) FROM test;"
DOCUMENT_GET_BOOL = "SELECT document_get_bool(data, %s) FROM test;"
DOCUMENT_GET_FLOAT = "SELECT document_get_float(data, %s) FROM test;"

class TestSerde(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("dbname=test user=postgres password=postgres")
        self.cur = conn.cursor()

    def teardown(self):
        cur.close()
        conn.close()

    def test_no_match(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_GET_INT, ("doesnotexist"))
        result = (self.cur.fetchone())[0]
        assertEqual(None, result)

    def test_flat_lookups(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_GET_STRING, (STRING_KEY))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_STRING, result)

        self.cur.execute(DOCUMENT_GET_INT, (INT_KEY))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_INT, result)

        self.cur.execute(DOCUMENT_GET_BOOL, (BOOL_KEY))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_BOOL, result)

        self.cur.execute(DOCUMENT_GET_FLOAT, (FLOAT_KEY))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_FLOAT, result)

    def test_array(self):
        self.cur.execute(INSERT, array_dict)
        self.cur.execute(DOCUMENT_GET, ("STRING_ARRAY", "text[]"))
        result = (self.cur.fetchone())[0]
        self.assertEqual(str(["a", "b", "cdef"]), result)

    def test_doc(self):
        self.cur.execute(INSERT, nested_dict)
        self.cur.execute("SELECT document_get(data, %s, %s)::text FROM test;", (DOCUMENT_KEY, "document"))
        result = (self.cur.fetchone())[0]
        self.assertEqual(flat_dict, json.loads(result))

    def test_array_deref(self):
        self.cur.execute(INSERT, array_dict)
        # Fixed length type
        self.cur.execute(DOCUMENT_GET_INT, ("INT_ARRAY[3]"))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_INT, result)

        # Variable length type
        self.cur.execute(DOCUMENT_GET_STRING, ("STRING_ARRAY[1]"))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_STRING, result)

        # Array OOB
        self.cur.execute(DOCUMENT_GET_INT, ("INT_ARRAY[20]"))
        result = (self.cur.fetchone())[0]
        assertEqual(None, result)

    def test_nested_array_deref(self):
        self.cur.execute(INSERT, nested_array_dict)
        self.cur.execute(DOCUMENT_GET_INT, ("NESTED_INT_ARRAY[1][0]]"))
        result = (self.cur.fetchone())[0]
        assertEqual(2, result)

        self.cur.execute(DOCUMENT_GET_STRING, ("NESTED_STRING_ARRAY[1][0][1]"))
        result = (self.cur.fetchone())[0]
        assertEqual("idk", result)

    def test_nested_document_deref(self):
        self.cur.execute(INSERT, double_nested_dict)
        self.cur.execute(DOCUMENT_GET_FLOAT, ("document.document.float"))
        result = (self.cur.fetchone())[0]
        assertEqual(TEST_FLOAT, result)

    def test_lookup_multiple_records(self):
        self.cur.execute(INSERT, nested_dict)
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(INSERT, array_dict)
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_GET_INT, (INT_KEY))
        result = self.cur.fetchmany(10)
        assertEqual(4, len(result))
        assertEqual([(None), (TEST_INT), (None), (TEST_INT)], result)
