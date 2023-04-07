CREATE EXTENSION query_advisor;

-- Make sure that installcheck won't find previous data
SELECT query_advisor_qualstats_reset();

-- Make sure sure we'll see at least one qual
SET query_advisor.sample_rate = 1;

CREATE TABLE pgqs AS SELECT id, 'a'::text val FROM generate_series(1, 100000) id;
ANALYZE pgqs;
SELECT COUNT(*) FROM pgqs WHERE id = 1;
SELECT lrelid::regclass::text, lattnum, occurences, execution_count,
    nbfiltered, eval_type
FROM query_advisor_qualstats();
SELECT COUNT(*) > 0 FROM query_advisor_qualstats;
SELECT COUNT(*) > 0 FROM query_advisor_qualstats();
SELECT COUNT(*) > 0 FROM query_advisor_workload_queries();
SELECT query_advisor_qualstats_reset();
SELECT COUNT(*) FROM query_advisor_qualstats();
-- OpExpr sanity checks
-- subquery_var operator const, shouldn't be tracked
SELECT * FROM (SELECT * FROM pgqs LIMIT 0) pgqs WHERE pgqs.id = 0;
SELECT COUNT(*) FROM query_advisor_qualstats();
-- const non_commutable_operator var, should be tracked, var found on RHS
SELECT * FROM pgqs WHERE 'meh' ~ val;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum FROM query_advisor_qualstats();
SELECT query_advisor_qualstats_reset();
-- opexpr operator var and commuted, shouldn't be tracked
SELECT * FROM pgqs WHERE id % 2 = 3;
SELECT * FROM pgqs WHERE 3 = id % 2;
SELECT COUNT(*) FROM query_advisor_qualstats();
-- same query with handled commuted qual, which should be found as identical
SELECT * FROM pgqs WHERE id = 0;
SELECT * FROM pgqs WHERE 0 = id;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum, sum(occurences)
FROM query_advisor_qualstats()
GROUP by 1, 2, 3, 4;
SELECT COUNT(DISTINCT qualnodeid) FROM query_advisor_qualstats();
-- (unique)qualid behavior
SELECT query_advisor_qualstats_reset();
-- There should be one group of 2 AND-ed quals, and 1 qual alone
SELECT COUNT(*) FROM pgqs WHERE (id = 1) OR (id > 10 AND id < 20);
SELECT CASE WHEN qualid IS NULL THEN 'OR-ed' ELSE 'AND-ed' END kind, COUNT(*)
FROM query_advisor_qualstats() GROUP BY 1 ORDER BY 2 DESC;

----------------
-- index advisor
----------------

-- check that empty arrays are returned rather than NULL values
SELECT query_advisor_qualstats_reset();
SELECT * FROM query_advisor_index_recommendations(50);
-- Test some naive scenario
CREATE TABLE adv (id1 integer, id2 integer, id3 integer, val text);
INSERT INTO adv SELECT i, i, i, 'line ' || i from generate_series(1, 100000) i;
ANALYZE adv;
SELECT query_advisor_qualstats_reset();
SELECT * FROM adv WHERE id1 < 0;
SELECT count(*) FROM adv WHERE id1 < 500;
SELECT * FROM adv WHERE val = 'meh';
SELECT * FROM adv WHERE id1 = 0 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and id2 = 2 AND val = 'meh';
SELECT * FROM adv WHERE id1 = 6 and id2 = 6 AND id3 = 6 AND val = 'meh';
SELECT COUNT(*) FROM pgqs WHERE id = 1;
-- non optimisable statements
SELECT * FROM adv WHERE val ILIKE 'moh';
SELECT count(*) FROM adv WHERE val ILIKE 'moh';
SELECT * FROM adv WHERE val LIKE 'moh';

