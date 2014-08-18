SELECT _MAP['str1_str'] AS str1,
    _MAP['num_int'] AS num
FROM "/path/to/file";

-- SELECT _MAP['nested_obj_obj.str'] AS str,
--     _MAP['nested_obj_obj.num'] AS num
-- FROM "/path/to/file":

SELECT _MAP['sparse_110_str'] AS sparse_110,
    _MAP['sparse_119_str'] AS sparse_119
FROM "/path/to/file";

SELECT _MAP['sparse_110_str'] AS sparse_110,
    _MAP['sparse_220_str'] AS sparse_220
FROM "/path/to/file";

SELECT *
FROM "/path/to/file"
WHERE CAST(_MAP['str1_str'] AS VARCHAR) =
    'DCMBRGEYTCMBQGEYTAMBQGEYTCMBQGAYTCMJQGA======';

SELECT *
FROM "/path/to/file"
WHERE CAST(_MAP['num_int'] AS INT) BETWEEN 10000 AND 50000;

SELECT *
FROM "/path/to/file"
WHERE CAST(_MAP['dyn1_int'] AS INT) BETWEEN 10000 AND 50000;

-- SELECT *
-- FROM "/path/to/file"
-- WHERE 'times' = ANY CAST(_MAP['nested_arr_arr'] AS VARCHAR[]);

SELECT *
FROM "/path/to/file"
WHERE CAST(_MAP['sparse_333_str'] AS VARCHAR) = 'GBRDCMA=';

SELECT COUNT(*)
FROM (
    SELECT _MAP['thousandth_int'] as thousandth
    FROM "/path/to/file"
    WHERE CAST(_MAP['num_int'] AS INT) BETWEEN 10000 AND 20000
    GROUP BY _MAP['thousandth_int']
  ) t;

-- SELECT *
-- FROM "/path/to/file" t1 INNER JOIN "/path/to/file" t2
--     ON t1._MAP['nested_obj_obj.str'] = t2._MAP['str1_str']
-- WHERE CAST(t1._MAP['num'] AS INT) BETWEEN 10000 AND 5000000;
