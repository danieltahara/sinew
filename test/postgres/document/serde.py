import psycopg2
import json
import unittest

import test_data

SELECT = "SELECT data::text FROM TEST;"

# See: http://initd.org/psycopg/docs/usage.html 
class TestSerde(unittest.TestCase):

    def insert_and_select(self, json_dict):
        self.cur.execute(INSERT, json.dumps(json_dict))
        self.cur.execute(SELECT)
        return json.loads((self.cur.fetchone())[0])

    def setUp(self):
        self.conn = psycopg2.connect("dbname=test user=postgres password=postgres")
        self.cur = conn.cursor()

    def teardown(self):
        cur.close()
        conn.close()

    def test_empty(self):
        result_dict = insert_and_select(empty_dict)
        self.assertEqual(empty_dict, result_dict)

    def test_flat(self):
        result_dict = self.insert_and_select(flat_dict)
        self.assertEqual(flat_dict, result_dict)

    def test_nested_document(self):
        result_dict = self.insert_and_select(nested_dict)
        self.assertEqual(nested_dict, result_dict)

        result_dict = self.insert_and_select(double_nested_dict)
        self.assertEqual(double_nested_dict, result_dict)

    def test_empty_array(self):
        self.assertTrue(False)

    def test_arrays(self):
        result_dict = self.insert_and_select(array_dict)
        self.assertEqual(array_dict, result_dict)

        result_dict = self.insert_and_select(doc_array_dict)
        self.assertEqual(doc_array_dict, result_dict)

    def test_nested_arrays(self):
        result_dict = self.insert_and_select(nested_array_dict)
        self.assertEqual(nested_array_dict, result_dict)

    def test_bad_input(self):
        with self.assertRaises(DatabaseError):
            invalid_key_dict = '{"hello":"world", 3:"invalid key"}'
            self.cur.execute(INSERT, invalid_key_dict)

        with self.assertRaises(DatabaseError):
            missing_value_dict = '{"hello":"world", "key2":}'
            self.cur.execute(INSERT, missing_value_dict)

        with self.assertRaises(DatabaseError):
            incomplete_dict = '{"hello":"world"'
            self.cur.execute(INSERT, incomplete_dict)

if __name__ == '__main__':
    unittest.main()
