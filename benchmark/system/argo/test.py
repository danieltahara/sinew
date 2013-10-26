import demo_init
import json
try:
    import readline
except:
    pass
from time import time

db = demo_init.get_db()
import os

# Execute queries
queries = os.listdir("queries")
for query in queries:
    print "Query " + query + ":"
    with open("queries/" + query) as f:
        qstr = f.read()
        print qstr
        for i in range(4):
            start = time()
            try:
                db.execute_sql(qstr)
            except Exception, e:
                print "ERROR: " + str(e)
            end = time()
            print end - start

