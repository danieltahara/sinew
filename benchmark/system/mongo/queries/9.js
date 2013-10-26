var start = Date.now();
db.test.find({ "sparse_333" : "GBRDCMA=" }).forEach(function() {});
var end = Date.now() - start;
end;
