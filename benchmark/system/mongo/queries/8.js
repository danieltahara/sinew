var start = Date.now();
db.test.find({ "nested_arr" : "times" }).forEach(function() {});
var end = Date.now() - start;
end;
