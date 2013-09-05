INSERT = "INSERT INTO test(data) VALUES(%s);"

STRING_KEY = "string"
INT_KEY = "int"
BOOL_KEY = "bool"
FLOAT_KEY = "float"
ARRAY_KEY = "array"
DOCUMENT_KEY = "document"

TEST_STRING = "hello, world"
TEST_INT = 123
TEST_BOOL = False
TEST_FLOAT = 1.2345

STRING_TYPE = "text"
INT_TYPE = "bigint"
BOOL_TYPE = "boolean"
FLOAT_TYPE = "double precision"

empty_dict = {}
flat_dict = {
            STRING_KEY : TEST_STRING,
            INT_KEY : TEST_INT,
            BOOL_KEY : TEST_BOOL,
            FLOAT_KEY : TEST_FLOAT,
            "string2" : TEST_STRING
        }
nested_dict = {
            STRING_KEY : TEST_STRING,
            DOCUMENT_KEY : flat_dict,
            FLOAT_KEY : TEST_FLOAT
        }
double_nested_dict = {
            STRING_KEY : TEST_STRING,
            DOCUMENT_KEY : nested_dict,
            FLOAT_KEY : TEST_FLOAT
        }
array_dict = {
            STRING_KEY : TEST_STRING,
            "STRING_ARRAY" : ["a", "b", "cdef"],
            "INT_ARRAY" : [1, 2, 3, 4, 5, 26],
            "FLOAT_ARRAY" : [1.23, 4.5, 6, 7.8],
            # "FLOAT_ARRAY" : [1, 2.2]
            "BOOL_ARRAY" : ["a", "b", "cdef"],
            FLOAT_KEY : TEST_FLOAT
        }
doc_array_dict = {
            "DOC_ARRAY" : [flat_dict, nested_dict, array_dict]
        }
nested_array_dict = {
            STRING_KEY : TEST_STRING,
            "NESTED_INT_ARRAY" : [[1,2], [2,3,4], [5,6,7,8], [9]],
            "NESTED_STRING_ARRAY": [[["hello","hi"],["sup","nm, you just chilling","pee"],[]], [["sup","idk"],["were bffaeaeaeaeae...."]]]
        }

