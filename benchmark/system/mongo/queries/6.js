var start = Date.now();
db.test.find({ "$and": [{ "num" : { "$gte" : 10000 } },
                        { "num" : { "$lt" : 50000 } }]}).forEach(function() {});
var end = Date.now() - start;
end;
