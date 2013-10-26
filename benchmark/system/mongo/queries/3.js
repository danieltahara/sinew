var start = Date.now();
db.test.find({ "$or" : [ { "sparse_110" : {"$exists" : true} },
                         { "sparse_119" : {"$exists" : true} } ] },
                  ["sparse_110", "sparse_119"]).forEach(function() {});
var end = Date.now() - start;
end;
