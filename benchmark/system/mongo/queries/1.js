var start = Date.now();
db.test.find({}, ["str1", "num"]).forEach(function() {});
var end = Date.now() - start;
end;
