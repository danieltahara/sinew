-- SELECT * FROM test AS l INNER JOIN test AS r ON (l.nested_obj.str = r.str1) WHERE (l.num >= 10000) AND (l.num <= 50000);

\timing
BEGIN;

CREATE TEMPORARY TABLE nobench_intermediate AS
  SELECT test_a.objid AS objid_a,
         test_b.objid AS objid_b
  FROM argo_test_data AS test_a,
       argo_test_data AS test_b
  WHERE test_a.keystr = 'nested_obj.str' AND
        test_b.keystr = 'str1' AND
        test_a.valstr = test_b.valstr AND
        test_a.objid in (SELECT objid
                               FROM argo_test_data AS test
                               WHERE test.valnum >= 10000 and
                                     test.valnum < 50000 AND
                                     test.keystr = 'num');

CREATE TEMPORARY TABLE nobench_result(objid_a BIGINT NOT NULL, keystr_a TEXT
NULL, keystr_b TEXT NULL, valstr TEXT NULL, valnum DOUBLE PRECISION NULL,
valbool BOOLEAN NULL);

INSERT INTO nobench_result(objid_a, keystr_a, valstr, valnum, valbool)
  SELECT objid, keystr, valstr, valnum, valbool
  FROM argo_test_data WHERE objid IN (SELECT objid_a FROM nobench_intermediate);
INSERT INTO nobench_result(objid_a, keystr_b, valstr, valnum, valbool)
  SELECT objid, keystr, valstr, valnum, valbool
  FROM argo_test_data WHERE objid IN (SELECT objid_b FROM nobench_intermediate);

END;
