var start = Date.now();
db.test.find({ "str1" : "GBRDCMBQGAYTAMJQGEYTCMJRGEYDAMBRGAYDCMJRGA======" }).forEach(function() {});
var end = Date.now() - start;
end;
