query_advisor
============

query_advisor is a PostgreSQL extension for providing index potetial useful
rcommendations based on keeping statistics on predicates(found
in ```WHERE``` statements and ```JOIN``` clauses) and workload queries and analyzing
the cost benefit to actual workload queries by replanning them with the hypothetical
index.  Hypothetical indexes are created based on the predicates collected from
the workload queries.

This is useful if you want to be able to analyze what are the most useful indexes which
can benefit the workload queries without actually creating all possible indexes.


The predicate collection works by looking for known patterns in queries. Currently, this
includes:

 - Binary OpExpr where at least one side is a column from a table. Whenever
   possible, the predicate will be swaped so that CONST OP VAR expressions are
   turned into VAR COMMUTED_OP CONST.
   AND and OR expression members are counted as separate entries.
   Ex: WHERE column1 = 2, WHERE column1 = column2, WHERE 3 = column3

 - ScalarArrayOpExpr where the left side is a VAR, and the right side is an
   array constant. Those will be counted one time per element in the array.
   Ex: WHERE column1 IN (2, 3) will be counted as 2 occurences for the (column1,
   '=') operator pair

 - BooleanTest where the expression is a simple boolean column reference
   Ex: WHERE column1 IS TRUE
   Please not that clauses like WHERE columns1, WHERE NOT column1 won't be
   processed by query_advisor (yet)

This extension also saves the first query text, as-is, for each distinct
queryid executed in workload table.

Please not that the gathered data are not saved when the PostgreSQL server is
restarted.

Installation
------------

- Compatible with PostgreSQL 12 or later
- Needs postgresql header files
- sudo make install
- Add query_advisor to the shared preload libraries:

```
   shared_preload_libraries = 'query_advisor'
```

Configuration
-------------

The following GUCs can be configured, in postgresql.conf:

- *query_advisor.enabled* (boolean, default true): whether or not query_advisor
  should be enabled
- *query_advisor.max_qual_entries*: the maximum number of predicates tracked
  (defaults to 1000)
- *query_advisor.max_workload_entries*: the maximum number of workload queries tracked
  (defaults to 1000)
- *query_advisor.sample_rate* (double, default -1): the fraction of queries that
  should be sampled. For example, 0.1 means that only one out of ten queries
  will be sampled. The default (-1) means automatic, and results in a value of 1
  / max_connections, so that statiscally, concurrency issues will be rare.
- *query_advisor.max_workload_query_size* - max size of the workload query
  (defaults to 1024 bytes)


Usage
-----

- Create the extension in any database:

```
   CREATE EXTENSION query_advisor;
```

### Functions


The extension defines the following functions:

 - **query_advisor_index_recommendations(min_filter, min_selectivity)**:
   Perform a global index suggestion.  By default, only predicates filtering at
   least 1000 rows and 30% of the rows in average will be considered, but this
   can be passed as parameter.  This will internally generate the one and two
   column index candidates based on the predicates it has collected.  And it will
   replan all related workload queries in presence of hypothetical index with
   respect to each candidate.  And finally recoomends the list of indexes which will
   bring most value to the workload.  This will also show the estimated index size and
   precentage cost reduction in the workload queries.  So based on the size and benefit
   ratio user can decide which indexes are most useful for them.

  Example:

```
postgres[64811]=# select * from query_advisor_index_recommendations(0,0);
                              index                               | estimated_size_in_bytes | estimated_pct_cost_reduction
------------------------------------------------------------------+-------------------------+------------------------------
 CREATE INDEX ON nation USING btree (n_name);                     |                    8192 |                    14.619857
 CREATE INDEX ON supplier USING btree (s_suppkey,s_nationkey);    |                  933888 |                   0.63003576
 CREATE INDEX ON partsupp USING btree (ps_suppkey);               |                50159616 |                    13.254544
(3 rows)

```

 - **query_advisor_qualstats**: returns the counts for every qualifier, identified by the
   expression hash. This hash identifies each expression.
   - *userid*: oid of the user who executed the query.
   - *dbid*: oid of the database in which the query has been executed.
   - *lrelid*, *lattnum*: oid of the relation and attribute number of the VAR
     on the left hand side, if any.
   - *opno*: oid of the operator used in the expression
   - *rrelid*, *rattnum*: oid of the relation and attribute number of the VAR
     on the right hand side, if any.
   - *qualid*: normalized identifier of the parent "AND" expression, if any.
     This identifier is computed excluding the constants.  This is useful for
     identifying predicates which are used together.
   - *uniquequalid*: unique identifier of the parent "AND" expression, if any.
     This identifier is computed including the constants.
   - *qualnodeid*: normalized identifier of this simple predicate.  This
     identifier is computed excluding the constants.
   - *uniquequalnodeid*: unique identifier of this simple predicate.  This
     identifier is computed including the constats.
   - *occurences*: number of time this predicate has been invoked, ie. number
     of related query execution.
   - *execution_count*: number of time this predicate has been executed, ie.
     number of rows it processed.
   - *nbfiltered*: number of tuples this predicate discarded.
   - *queryid*: if pg_stats_statements is installed, the queryid identifying
     this query, otherwise NULL.
   - *eval_type*: evaluation type. 'f' for a predicate evaluated after a scan
     or 'i' for an index predicate.

   Example:

```
postgres[19793]=# select * from query_advisor_qualstats();
 userid | dbid | lrelid | lattnum | opno | rrelid | rattnum | qualid | uniquequalid | qualnodeid | uniquequalnodeid | occurences | execution_count | nbfiltered | min_err_estimate_ratio | max_err_estimate_ratio |
 mean_err_estimate_ratio | stddev_err_estimate_ratio | min_err_estimate_num | max_err_estimate_num | mean_err_estimate_num | stddev_err_estimate_num |       queryid        | eval_type 
--------+------+--------+---------+------+--------+---------+--------+--------------+------------+------------------+------------+-----------------+------------+------------------------+------------------------+
-------------------------+---------------------------+----------------------+----------------------+-----------------------+-------------------------+----------------------+-----------
     10 |    5 |  16501 |       1 |   96 |        |         |        |              |  262905824 |        262905824 |          1 |         3000000 |    2999997 |                      3 |                      3 |
                       3 |                         0 |                    2 |                    2 |                     2 |                       0 | -3469822585968758916 | f
```
 - **query_advisor_workload_queries**: return all the stored query texts.
 - **query_advisor_qualstats_memory_usage**: return the percentage usage of the workload and qual hash table.
 - **query_advisor_qualstats_reset**: reset the internal counters and forget about every
   encountered quals and workload entry.

### Views

In addition to that, the extension defines some views on top of the pg_qualstats
function:

  - **query_advisor_qualstats**: filters calls to query_advisor_qualstats() by the current database.
  - **query_advisor_qualstats_pretty**: performs the appropriate joins to display a readable
    aggregated form for every attribute from the query_advisor_qualstats view

    Example:

```
ro=# select * from query_advisor_qualstats_pretty;
 left_schema |    left_table    | left_column |   operator   | right_schema | right_table | right_column | occurences | execution_count | nbfiltered
-------------+------------------+-------------+--------------+--------------+-------------+--------------+------------+-----------------+------------
 public      | pgbench_accounts | aid         | pg_catalog.= |              |             |              |          5 |         5000000 |    4999995
 public      | pgbench_tellers  | tid         | pg_catalog.= |              |             |              |         10 |        10000000 |    9999990
 public      | pgbench_branches | bid         | pg_catalog.= |              |             |              |         10 |         2000000 |    1999990
 public      | t1               | id          | pg_catalog.= | public       | t2          | id_t1        |          1 |           10000 |       9999
```

### Limitation
 - Currently, only single and two columns indexes are considered
 - qual stats and workload is not collected for utility commands.
 - Currently, hash table entries for predicats and workload is static and change need restart.
 - In current version the index overhead is only considered based on index size but in future we can also
   consider the overhead of the index creation based on the updates of the index columns in the workload.
