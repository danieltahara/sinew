var start = Date.now();
db.test.find({ "$or" : [ { "sparse_110" : {"$exists" : true} },
                         { "sparse_220" : {"$exists" : true} } ] },
                  ["sparse_110", "sparse_220"]).forEach(function() {});
var end = Date.now() - start;
end;
