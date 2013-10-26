var start = Date.now();
db.test.find({ "$and": [{ "dyn1" : { "$gte" : 10000 } },
                        { "dyn1" : { "$lt" : 50000 } }]}).forEach(function() {});
var end = Date.now() - start;
end;
