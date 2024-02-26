# KLIP 58 - LIMIT clause for Pull Queries

**Author**: Chittaranjan Prasad (@cprasad1) | 
**Release Target**: 0.24.0; 7.2.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/8298

**tl;dr:** Users of pull queries don't have any way of restricting the number of rows returned by a pull query. The 
`LIMIT` clause will enable users to save on computation, time and bandwidth costs by giving them an easy way to restrict 
the number of rows returned. In the future, the `LIMIT` clause can also be used to write more expressive queries in 
conjunction with `OFFSET` and `ORDER BY` clauses. 
> **_NOTE:_** ksqlDB pull queries don't support `OFFSET` and `ORDER BY` currently. 




## Motivation and background

Today, users of pull queries don't have a simple way of restricting the amount of data returned by pull queries. If a 
user just wants to see some of the rows in a table or a few records in a stream, they would very often issue queries of 
the form `SELECT * FROM table/stream;`. Issuing pull queries of this form can be very computationally intensive if the 
underlying table/stream on which the pull query is being executed has a large amount of data. This can also lead them to 
incurring large bandwidth costs when all they wanted to do was inspect a small amount of data from the table or stream. 
In the future, the `LIMIT` clause will also enhance the expressivity of pull query SQL syntax in conjunction with 
potential `OFFSET` and `ORDER BY` clauses which can unlock new use cases for pull queries. 

## What is in scope

- `LIMIT` clause should be supported with pull queries on `STREAM` and `TABLE` objects with non-negative integers.
- When a pull query is executed over a stream the `LIMIT` clause should output results from the beginning of the stream.
- When the limit is reached, the underlying query terminates and free up its resources.

## What is not in scope
- Pull queries over `TABLES` don't offer any ordering guarantees today, so the exact subset of data that will returned 
by restricting the output using `LIMIT` won't be covered in this klip.

## Value/Return

The ability to `LIMIT` should help users prototyping in ksqlDB with pull by giving them access to a subset of the results 
that they would expect to see without the `LIMIT` in place. ksqlDB users will not have to issue expensive table scans to
just get a quick look of what the underlying table or stream looks like (the schema and the rows/records).

## Public APIS

##### ksqlDB query language: 
Users will be able to add a `LIMIT n` clause at the end of their pull queries to retrievea subset of the data from tables 
and streams. Earlier, users would get this error if they issued a pull query with a `LIMIT` clause: 
```
Pull queries don't support LIMIT clauses. See https://cnfl.io/queries for more info. 
Add EMIT CHANGES if you intended to issue a push query.
```

Syntax evolution:
```SQL
SELECT select_expr [, ...]                 SELECT select_expr [, ...] 
  FROM table/stream                          FROM table/stream        
  [ WHERE where_condition ]    ────────►     [ WHERE where_condition ]
  [ AND window_bounds ];                     [ AND window_bounds ]    
                                           + [ LIMIT num_records ];  
```                                     
                                               

## Design
### For Pull Queries over Tables
Pull query execution relies on building a logical plan which is then translated into a physical plan. The `LIMIT` clause 
of pull queries will be translated into a `QueryLimitNode` in the logical plan. If present, the `QueryLimitNode` will be 
the parent node of `QueryProjectNode` in the logical plan. The `QueryLimitNode` will then be translated into an operator 
called the `LimitOperator` in the physical plan by the pull query physical plan builder. The `LimitOperator` will wil be 
the parent operator of `ProjectOperator` in the physical plan. The `QueryLimitNode` and `LimitOperator` will store the 
number of rows to return in them. The physical plan will be fanned out to all the hosts (both remote and local) in parallel
to execute with the limit being applied to every host individually. 

Functionally, the `LimitOperator` will stop returning rows flowing down from upstream operators in the physical plan 
topology by keeping a counter of the number of rows returned so far and stop scanning further once the limit is reached. 
This will result in returning the first set of n (where n is the limit requested in the query) rows that it gets from 
upstream operators and stop scanning for rows after that. If the limit isn't reached and we run out of rows to return 
(exhausting the rows that conform to our query), then the operator will just return. 

In other words, if we have three hosts and we execute a query with `LIMIT 5`, we could potentially scan 15 records 
(5 from each host), but then after we stream back the first five records to the caller, we'd just stop the response and 
drop the rest of the queue.

As an example, if we have a `TABLE` called `ridersNearMountainView` with 
`Schema: DISTANCEINMILES DOUBLE KEY, RIDERS ARRAY<STRING>, COUNT BIGINT`, and we execute a pull query of the form:
```SQL
SELECT distanceinmiles, riders 
FROM ridersNearMountainView 
WHERE distanceinmiles < 115 
LIMIT 3;
```
the query gets translated into a logical plan which in turn gets translated into a physical plan that look like:

```
┌────────────────────────┐              ┌───────────────────────┐
│                        │              │                       │
│      Logical Plan      │              │     Physical Plan     │
│     ──────────────     │              │    ───────────────    │
│                        │              │                       │
│   ┌────────────────┐   │              │ ┌───────────────────┐ │
│   │ DataSourceNode │   │              │ │ TableScanOperator │ │
│   └────────┬───────┘   │              │ └─────────┬─────────┘ │
│            │           │              │           │           │
│  ┌─────────▼────────┐  │              │  ┌────────▼───────┐   │
│  │  QueryFilterNode │  │              │  │ SelectOperator │   │
│  └─────────┬────────┘  │              │  └────────┬───────┘   │
│            │           │              │           │           │
│   ┌────────▼───────┐   │              │   ┌───────▼───────┐   │
│   │ QueryLimitNode │   │  ─────────►  │   │ LimitOperator │   │
│   └────────┬───────┘   │              │   └───────┬───────┘   │
│            │           │              │           │           │
│  ┌─────────▼────────┐  │              │  ┌────────▼────────┐  │
│  │ QueryProjectNode │  │              │  │ ProjectOperator │  │
│  └─────────┬────────┘  │              │  └─────────────────┘  │
│            │           │              │                       │
│ ┌──────────▼─────────┐ │              │                       │
│ │ KsqlBareOutputNode │ │              │                       │
│ └────────────────────┘ │              │                       │
│                        │              │                       │
└────────────────────────┘              └───────────────────────┘
```
The output of the above query will look like:
```
+-------------------------------+-------------------------------+
|DISTANCEINMILES                |RIDERS                         |
+-------------------------------+-------------------------------+
|0.0                            |[4ab5cbad, 8b6eae59, 4a7c7b41] |
|10.0                           |[18f4ea86]                     |
|50.0                           |[c2309eec, 4ddad000]           |
Limit Reached
Query terminated
```

### For Pull Queries over Streams
Pull queries over streams will also use the same `QueryLimitNode` in the physical plan. The `QueryLimitNode` will be the 
parent of `FinalProjectNode` whenever pull queries are executed with a `LIMIT` clause. The logical plan will then get 
converted into a physical plan by the persistent query physical plan builder with a topology of the individual execution 
steps. 

The introduction of the `QueryLimitNode` will not result in any additional execution step in the pHysical plan. This means 
that the physical plans for a query with and without the `LIMIT` clause will have the same execution steps. The 
`QueryLimitNode` from the logical plan will just encode the limit information in the `TransientQueryMetadata`. The limit 
information will then be used to create a `TransientQueryQueue` which has a size as big as the limit. The 
`TransientQueryQueue` will continue accepting stream records from the beginning of the stream flowing from the execution 
steps of the physical plan till it is full and then return.

As an example, if we have a `STREAM` called `riderlocations` with 
`Schema: profileId VARCHAR, latitude DOUBLE, longitude DOUBLE`, and we execute a pull query of the form:

```SQL
SELECT profileid, latitude 
FROM riderlocations 
WHERE latitude > 38 
LIMIT 5;
```
then the query gets translated into a logical plan which in turn gets translated into a physical plan that look like:
```
┌────────────────────────┐              ┌──────────────────────────────────┐
│                        │              │                                  │
│      Logical Plan      │              │           Physical Plan          │
│     ──────────────     │              │          ───────────────         │
│                        │              │                                  │
│                        │       ┌──────┼──►`TransientQueryMetadata` has   │
│                        │       │      │         limit information        │
│                        │       │      │                                  │
│                        │       │      │                                  │
│   ┌────────────────┐   │       │      │          ┌──────────────┐        │
│   │ DataSourceNode │   │       │      │          │ StreamSource │        │
│   └────────┬───────┘   │       │      │          └───────┬──────┘        │
│            │           │       │      │                  │               │
│     ┌──────▼─────┐     │       │      │          ┌───────▼──────┐        │
│     │ FilterNode │     │       │      │          │ StreamFilter │        │
│     └──────┬─────┘     │       │      │          └───────┬──────┘        │
│            │           │       │      │                  │               │
│   ┌────────▼───────┐   │       │      │                  │               │
│   │ QueryLimitNode ├───┼───────┘      │                  │               │
│   └────────┬───────┘   │              │                  │               │
│            │           │              │                  │               │
│  ┌─────────▼────────┐  │ ───────────► │          ┌───────▼──────┐        │
│  │ FinalProjectNode │  │              │          │ StreamSelect │        │
│  └─────────┬────────┘  │              │          └──────────────┘        │
│            │           │              │                                  │
│ ┌──────────▼─────────┐ │              │                                  │
│ │ KsqlBareOutputNode │ │              │                                  │
│ └────────────────────┘ │              │                                  │
│                        │              │                                  │
└────────────────────────┘              └──────────────────────────────────┘
```
The output of the above query will look like:
```
+-------------------------------+-------------------------------+
|PROFILEID                      |LATITUDE                       |
+-------------------------------+-------------------------------+
|c2309eeca                      |38.7877                        |
|18f4ea86a                      |38.3903                        |
|4ab5cbada                      |38.3952                        |
|8b6eae59a                      |38.3944                        |
|4a7c7b41a                      |38.4049                        |
Limit Reached
Query terminated
```

If the limit isn't reached and we run out stream records to return (exhausting the records that conform to our query), 
then the `TransientQueryQueue` will just be returned. 

## Test plan
- Unit testing.
- Functional testing to ensure that the queries generate the correct physical and logical plans for pull queries on 
streams and tables.
- Rest query validation tests to ensure that we are returning the correct number of rows and ending the query with 
messages saying `Limit Reached` and `Query terminated` for pull queries on streams and tables.
- Queries with limit clause argument that is a negative integer should throw a ksqlDB syntax exception and output an 
appropriate message to the user.

## LOEs and Delivery Milestones

The implementation can both be broken up as follows:
- Pull queries on windowed and non-windowed tables - 3 days
- Pull queries on streams - 2 days
- Testing according to the test plan - 2 days
- Documentation updates - 1 day
- Manual Testing and buffer time - 2 days

## Documentation Updates

We will update the documentation around pull queries 
[here](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-pull-query/) to reflect the addition of 
the new `LIMIT` clause syntax along with some helpful examples.

## Compatibility Implications

The introduction of the `LIMIT` clause only increases the domain of the pull queries that can be executed, therefore we 
do not expect any compatibility problems. Users will now be able to execute this type of query without seeing an error.

## Security Implications

None
