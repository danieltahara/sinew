\timing
CREATE TEMPORARY TABLE nobench_intermediate AS SELECT objid FROM argo_test_data WHERE keystr = 'num' AND valnum >= 10000 and valnum < 50000;
CREATE TEMPORARY TABLE nobench_result AS SELECT valnum, COUNT(*) FROM argo_test_data WHERE keystr = 'thousandth' AND objid IN (SELECT objid FROM nobench_intermediate) GROUP BY valnum;
