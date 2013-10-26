var start = Date.now();
db.test.find({}, ["nested_obj.str", "nested_obj.num"]).forEach(function() {});
var end = Date.now() - start;
end;
