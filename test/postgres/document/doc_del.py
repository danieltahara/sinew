import json
import psycopg2
import unittest

import test_data

DOCUMENT_DELETE = "SELECT document_delete(data, %s, %s) FROM test;"

class TestSerde(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("dbname=test user=postgres password=postgres")
        self.cur = conn.cursor()

    def teardown(self):
        cur.close()
        conn.close()

    def test_empty(self):
        self.cur.execute(INSERT, empty_dict)
        self.cur.execute(DOCUMENT_DELETE, (INT_KEY, INT_TYPE))
        result = (self.cur.fetchone())[0]
        assertEqual(empty_dict, json.loads(result))

    def test_leave_empty(self):
        one_element_dict = { INT_KEY : TEST_INT }
        self.cur.execute(INSERT, one_element_dict)
        self.cur.execute(DOCUMENT_DELETE, (INT_KEY, INT_TYPE))
        result = (self.cur.fetchone())[0]
        assertEqual(empty_dict, json.loads(result))

    def test_flat(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_DELETE, (STRING_KEY, STRING_TYPE))
        new_dict = flat_dict.deepcopy().delete(STRING_KEY)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_nomatch(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_DELETE, ("KEYDOESNOTEXIST", TEST_INT))
        result = (self.cur.fetchone())[0]
        assertEqual(flat_dict, json.loads(result))

    def test_array(self):
        self.cur.execute(INSERT, array_dict)
        self.cur.execute(DOCUMENT_DELETE, ("STRING_ARRAY", "text[]"))
        new_dict = flat_dict.deepcopy().delete("STRING_ARRAY")
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_doc(self):
        self.cur.execute(INSERT, nested_dict)
        self.cur.execute(DOCUMENT_DELETE, (DOCUMENT_KEY, "document"))
        new_dict = flat_dict.deepcopy().delete(DOCUMENT_KEY)
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_nested_document(self):
        self.cur.execute(INSERT, nested_dict)
        self.cur.execute(DOCUMENT_DELETE, ("document.int", new_int))
        new_dict = flat_dict.deepcopy()
        new_dict["document"].delete("int")
        result = (self.cur.fetchone())[0]
        assertEqual(new_dict, json.loads(result))

    def test_nested_document_nomatch(self):
        self.cur.execute(INSERT, flat_dict)
        self.cur.execute(DOCUMENT_DELETE, ("document.int", new_int))
        result = (self.cur.fetchone())[0]
        assertEqual(flat_dict, json.loads(result))
