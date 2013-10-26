map = function() {
    if ((this.num >= 10000) && (this.num < 2500000)) {
        emit(this.str1, { left: [this], right: [] });
    }
    emit(this.nested_obj.str, { left: [], right: [this] });
}

reduce = function(key, values) {
    var result = { left: [], right: [] };
    values.forEach(function(value) {
        result.left = result.left.concat(value.left);
        result.right = result.right.concat(value.right);
    });
    return result;
}

db.runCommand( { mapReduce: 'test',
                 map: mapFunction,
                 reduce: reduceFunction,
                 out: 'joined' } );
