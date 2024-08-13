# KLIP 54 - Optimized Range Pull Queries 

**Author**: Patrick Stuedi (@pstuedi) | 
**Release Target**: 0.22.0; 7.1.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/7993

**tl;dr:** For range pull queries on tables, use the range interface provided by the state store for retrieving records instead of doing a table scan.
           
## Motivation and background

Currently, range queries on primary keys (e.g., WHERE colX < T) are implemented using a table scan followed by a filtering operation, which is inefficient. Recently, range scan capabilities were added to Kafka streams (KAFKA-4064). Following that work, we should optimize the execution of range queries in ksqlDB by directly leveraging the dedicated range query interface to improve I/O efficiency and performance. 

## What is in scope

* Pull queries on tables with a WHERE clause including ">", ">=", "<", "<=" expressions should be executed using the range interface in the state store. There are restrictions on when this optimization can be applied. See the next section.

## What is not in scope

* Queries with conjunctions or disjunctions of range expressions will not be optimized with the techniques proposed in this KLIP, but instead we will fallback to table scans for such queries. In the future, we plan to optimize queries with multiple range expressions by compacting the various range expressions into fewer expressions, leading to fewer range calls on the state store. 
* There exists a potential for several physical plan optimizations around range queries, such as omitting redundant filtering in selection operators. Such optimizations are not part of this KLIP. 
* Currently, the scope of this KLIP is restricted to String type keys. The reason for this is that the default comparator in rocksdb is lexicographical, thus, the order of binary key types like Integers will not semantically be correct. For instance, negative integers represented in the two-complement binary format may appear greater than some positive integers. As of now, we restrict the range scan optimization to String key types. In the future, we plan to add support for binary keys.  

## Value/Return

With this feature, range queries on primary keys will retrieve the exact range of records from underlying state store, instead of retrieving all the records and dropping unmatched records using filtering. The expected result is better I/O efficiency and improved performance. 

## Public APIS

No new syntax is added. 

## Design

In order to efficiently support range queries on tables, several modifications have to be made with regard to how opportunities for range scans are detected, how the physical plan is generated and how materialization is implemented:
1. Generate KeyConstrains for range expressions (instead of NoKeyConstraints)
2. Enable the existing KeyedTableLookupOperator to handle KeyConstraints of type "<", "<=", ">", ">="
3. Add support for range scans in ksqlDB materialization 

## Test plan

We will add new unit and integration tests to ensure that range queries produce the correct result. We will further enhance the benchmark capabilities to assess the performance improvements due to the range scan optimizations. 

## Documentation Updates

* No updates to the documentation required. 

## Compatibility Implications

This is an optimization transparent to applications, therefore we do not expect any compatibility problems.

## Security Implications

No security implications expected. 
