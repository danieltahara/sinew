var start = Date.now();
db.test.group( { key: {"thousandth" : true },
                 cond: { "$and": [{ "num" : { "$gte" : 10000 } },
                                  { "num" : { "$lt" : 20000 } }]},
                 initial: { "total" : 0 },
                 reduce: function(obj, prev) { prev.total += 1; }
               } ).forEach(function(){});
var end = Date.now() - start;
end;
