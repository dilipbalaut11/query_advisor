/*"""
.. function:: query_advisor_index_recommendations()

  Generates index advises based on quals collected so far
*/

CREATE FUNCTION query_advisor_index_recommendations(
	min_filter integer DEFAULT 1000,
	min_selectivity integer DEFAULT 30,
	OUT index text,
	OUT estimated_size_in_bytes int8,
	OUT estimated_pct_cost_reduction float4)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'query_advisor_index_recommendations'
LANGUAGE C;


/*"""
.. function:: query_advisor_qualstats_reset()

  Resets statistics gathered by advisor.
*/
CREATE FUNCTION query_advisor_qualstats_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_qualstats_reset'
LANGUAGE C;

/*"""
.. function query_advisor_workload_queries()

  Returns all the workload queries with their associated queryid
*/
CREATE FUNCTION query_advisor_workload_queries(OUT queryid bigint, OUT query text, OUT frequency int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_qualstats_example_queries'
LANGUAGE C;

/*"""
.. function:: query_advisor_qualstats()

  Returns:
    A SETOF record containing the data gathered by pg_qualstats

    Attributes:
      userid (oid):
        the user who executed the query
      dbid (oid):
        the database on which the query was executed
      lrelid (oid):
        oid of the relation on the left hand side
      lattnum (attnum):
        attribute number of the column on the left hand side
      opno (oid):
        oid of the operator used in the expression
      rrelid (oid):
        oid of the relation on the right hand side
      rattnum (attnum):
        attribute number of the column on the right hand side
      qualid(bigint):
        hash of the parent ``AND`` expression, if any. This is useful for identifying
        predicates which are used together.
      uniquequalid(bigint):
        hash of the parent ``AND`` expression, if any, including the constant
        values.
      qualnodeid(bigint):
        the predicate hash.
      uniquequalnodeid(bigint):
        the predicate hash. Everything (down to constants) is used to compute this hash
      occurences (bigint):
        the number of times this predicate has been seen
      execution_count (bigint):
        the total number of execution of this predicate.
      nbfiltered (bigint):
        the number of lines filtered by this predicate
      min_err_estimate_ratio(double precision):
        the minimum selectivity estimation error ratio for this predicate
      max_err_estimate_ratio(double precision):
        the maximum selectivity estimation error ratio for this predicate
      mean_err_estimate_ratio(double precision):
        the mean selectivity estimation error ratio for this predicate
      stddev_err_estimate_ratio(double precision):
        the standard deviation for selectivity estimation error ratio for this predicate
      min_err_estimate_num(bigint):
        the minimum number of line for selectivity estimation error for this predicate
      max_err_estimate_num(bigint):
        the maximum number of line for selectivity estimation error for this predicate
      mean_err_estimate_num(double precision):
        the mean number of line for selectivity estimation error for this predicate
      stddev_err_estimate_num(double precision):
        the standard deviation for number of line for selectivity estimation error for this predicate
      constant_position (int):
        the position of the constant in the original query, as filled by the lexer.
      queryid (bigint):
        the queryid identifying this query, as generated by pg_stat_statements
      constvalue (varchar):
        a string representation of the right-hand side constant, if
        any, truncated to 80 bytes.
      eval_type (char):
        the evaluation type. Possible values are ``f`` for execution as a filter (ie, after a Scan)
        or ``i`` if it was evaluated as an index predicate. If the qual is evaluated as an index predicate,
        then the nbfiltered value will most likely be 0, except if there was any rechecked conditions.
*/
CREATE FUNCTION query_advisor_qualstats(
  OUT userid oid,
  OUT dbid oid,
  OUT lrelid oid,
  OUT lattnum smallint,
  OUT opno oid,
  OUT rrelid oid,
  OUT rattnum smallint,
  OUT qualid  bigint,
  OUT uniquequalid bigint,
  OUT qualnodeid    bigint,
  OUT uniquequalnodeid bigint,
  OUT occurences bigint,
  OUT execution_count bigint,
  OUT nbfiltered bigint,
  OUT min_err_estimate_ratio double precision,
  OUT max_err_estimate_ratio double precision,
  OUT mean_err_estimate_ratio double precision,
  OUT stddev_err_estimate_ratio double precision,
  OUT min_err_estimate_num bigint,
  OUT max_err_estimate_num bigint,
  OUT mean_err_estimate_num double precision,
  OUT stddev_err_estimate_num double precision,
  OUT constant_position int,
  OUT queryid    bigint,
  OUT constvalue varchar,
  OUT eval_type  "char"
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_qualstats_2_0'
LANGUAGE C STRICT VOLATILE;

/*"""
.. function query_advisor_workload_queries()

  Returns all the workload queries with their associated queryid
*/
CREATE FUNCTION query_advisor_qualstats_memory_usage(OUT storage_name text, OUT usage_percentages float4)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_qualstats_status'
LANGUAGE C;


-- Register a view on the function for ease of use.
/*"""
.. view:: query_advisor_qualstats

  This view is just a simple wrapper on the :func:`query_advisor_qualstats()` function, filtering on the current database for convenience.
*/
CREATE VIEW query_advisor_qualstats AS
  SELECT qs.* FROM query_advisor_qualstats() qs
  INNER JOIN pg_database on qs.dbid = pg_database.oid
  WHERE pg_database.datname = current_database();


GRANT SELECT ON query_advisor_qualstats TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION query_advisor_qualstats_reset() FROM PUBLIC;

/*"""
.. view:: query_advisor_qualstats_pretty

  This view resolves oid "on the fly", for the current database.

  Returns:
    left_schema (name):
      the name of the left-hand side relation's schema.
    left_table (name):
      the name of the left-hand side relation.
    left_column (name):
      the name of the left-hand side attribute.
    operator (name):
      the name of the operator.
    right_schema (name):
      the name of the right-hand side relation's schema.
    right_table (name):
      the name of the right-hand side relation.
    right_column (name):
      the name of the operator.
    execution_count (bigint):
      the total number of time this qual was executed.
    nbfiltered (bigint):
      the total number of tuples filtered by this qual.
*/
CREATE VIEW query_advisor_qualstats_pretty AS
  select
        nl.nspname as left_schema,
        al.attrelid::regclass as left_table,
        al.attname as left_column,
        opno::regoper::text as operator,
        nr.nspname as right_schema,
        ar.attrelid::regclass as right_table,
        ar.attname as right_column,
        sum(occurences) as occurences,
        sum(execution_count) as execution_count,
        sum(nbfiltered) as nbfiltered
  from query_advisor_qualstats qs
  left join (pg_class cl inner join pg_namespace nl on nl.oid = cl.relnamespace) on cl.oid = qs.lrelid
  left join (pg_class cr inner join pg_namespace nr on nr.oid = cr.relnamespace) on cr.oid = qs.rrelid
  left join pg_attribute al on al.attrelid = qs.lrelid and al.attnum = qs.lattnum
  left join pg_attribute ar on ar.attrelid = qs.rrelid and ar.attnum = qs.rattnum
  group by al.attrelid, al.attname, ar.attrelid, ar.attname, opno, nl.nspname, nr.nspname
;


CREATE OR REPLACE VIEW query_advisor_qualstats_all AS
  SELECT dbid, relid, userid, queryid, array_agg(distinct attnum) as attnums,
    opno, max(qualid) as qualid, sum(occurences) as occurences,
    sum(execution_count) as execution_count, sum(nbfiltered) as nbfiltered,
    coalesce(qualid, qualnodeid) as qualnodeid
  FROM (
    SELECT
          qs.dbid,
          CASE WHEN lrelid IS NOT NULL THEN lrelid
               WHEN rrelid IS NOT NULL THEN rrelid
          END as relid,
          qs.userid as userid,
          CASE WHEN lrelid IS NOT NULL THEN lattnum
               WHEN rrelid IS NOT NULL THEN rattnum
          END as attnum,
          qs.opno as opno,
          qs.qualid as qualid,
          qs.qualnodeid as qualnodeid,
          qs.occurences as occurences,
          qs.execution_count as execution_count,
          qs.nbfiltered as nbfiltered,
          qs.queryid
    FROM query_advisor_qualstats() qs
    WHERE lrelid IS NOT NULL or rrelid IS NOT NULL
  ) t GROUP BY dbid, relid, userid, queryid, opno, coalesce(qualid, qualnodeid)
;

/*"""
.. type:: qual

  Attributes:

    relid (oid):
      the relation oid
    attnum (integer):
      the attribute number
    opno (oid):
      the operator oid
    eval_type (char):
      the evaluation type. See :func:`query_advisor_qualstats()` for an explanation of the eval_type.
*/
CREATE TYPE qual AS (
  relid oid,
  attnum integer,
  opno oid,
  eval_type "char"
 );

/*"""
.. type:: qualname

  Pendant of :type:`qual`, but with names instead of oids

  Attributes:

    relname (text):
      the relation oid
    attname (text):
      the attribute number
    opname (text):
      the operator name
    eval_type (char):
      the evaluation type. See :func:`query_advisor_qualstats()` for an explanation of the eval_type.
*/
CREATE TYPE qualname AS (
  relname text,
  attnname text,
  opname text,
  eval_type "char"
);

CREATE TYPE adv_quals AS (
    qualnodeids bigint[],
    queryids bigint[]
);

