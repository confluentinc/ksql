# KSQL Improvement Proposals

This is a place to collect improvement proposals for KSQL from the community. Please follow the 
[KLIP Template](klip-template.md) when creating a new KLIP.

We have a two-part process for submitting KLIPs:

## Proposal Intent

This serves as a locking mechanism to make sure that 
- each KLIP has a unique number
- two people are not working on the same KLIP at once

To submit a proposal intent, create a pull request with the following information:
1. title is `docs: intent for klip-<number>: <title-of-klip>`
1. update the number for the next KLIP on this readme
1. add a line in the table below for your new KLIP with status `Proposal`

You can expect this PR to be merged swiftly.

## Design Discussion

This is the guts of our improvement proposal process:

1. Create a new branch off of master on your fork
1. Copy the [`klip-template.md`](klip-template.md) file and rename it with your KLIP number and 
   title (e.g. `klip-1-improve-udf-interfaces.md`)
1. Fill in the template with details from your KLIP
1. Submit a Pull Request from your branch to KSQL:
    1. make sure the title is `docs: klip-<number>: <title>`
    1. update the table entry below from the Proposal Intent step with a link to your KLIP
1. Share a link to the PR in the `#ksqldb` channel on the [confluent community slack](https://slackpass.io/confluentcommunity).
1. The design discussion will happen on the pull request
1. The KLIP is approved and merged if at least two people with write access approve the change

# KLIP Directory

The progression of statuses should be: Proposal, Discussion, Approved, Merged

Next KLIP number: **64**

| KLIP                                                                                                                             |  Status  | Community Release | CP Release | Discussion PR                                                |
|----------------------------------------------------------------------------------------------------------------------------------|:--------:|:-----------------:|:----------:|--------------------------------------------------------------|
| [KLIP-X: Template](klip-template.md)                                                                                             |    -     |         -         |     -      | -                                                            |
| [KLIP-1: Improve UDF Interfaces](klip-1-improve-udf-interfaces.md)                                                               | Approved |                   |            |                                                              |
| [KLIP-2: Insert Into Semantics](klip-2-produce-data.md)                                                                          |  Merged  |         -         |    5.3     |                                                              |
| [KLIP-3: Serialization of single Fields](klip-3-serialization-of-single-fields.md)                                               | Approved |                   |            |                                                              |
| [KLIP-4: Custom Type Registry](klip-4-custom-types.md)                                                                           |  Merged  |         -         |    5.4     |                                                              |
| [KLIP-6: Execution Plans](klip-6-execution-plans.md)                                                                             |  Merged  |       0.8.0       |    5.5     |                                                              |
| [KLIP-7: Kafka Connect Integration](klip-7-connect-integration.md)                                                               |  Merged  |         -         |    5.4     |                                                              |
| [KLIP-8: Queryable State Stores](klip-8-queryable-state-stores.md)                                                               |  Merged  |         -         |    5.4     |                                                              |
| [KLIP-9: Table Functions](klip-9-table-functions.md)                                                                             |  Merged  |         -         |    5.4     |                                                              |
| [KLIP-10: Suppress](klip-10-suppress.md)                                                                                         | Proposal |                   |            |                                                              |
| [KLIP-11: Redesign KSQL query language](klip-11-DQL.md)                                                                          | Proposal |                   |            |                                                              |
| [KLIP-12: Implement High-Availability for Pull queries](klip-12-pull-high-availability.md)                                       | Proposal |                   |            |                                                              |
| [KLIP-13: Expose Connect Worker Properties](klip-13-introduce-KSQL-command-to-print-connect-worker-properties-to-the-console.md) |  Merged  |       0.8.0       |   5.5.0    |                                                              |
| [KLIP-14: ROWTIME as Pseudocolumn](klip-14-rowtime-as-pseudocolumn.md)                                                           |  Merged  |        0.9        |   6.0.0    |                                                              |
| [KLIP-15: KSQLDB new API and Client](klip-15-new-api-and-client.md)                                                              | Proposal |                   |            |                                                              |
| [KLIP-16: Introduce 'K$' dynamic views                                                                                           | Proposal |                   |            |                                                              |
| [KLIP-17: Remove 'INSERT INTO' in favour of SQL Union](klip-17-sql-union.md)                                                     | Proposed |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/4125) |
| [KLIP-18: Distributed Metastore](klip-18-distributed-metastore.md)                                                               | Proposal |                   |            |                                                              |
| [KLIP-19: Introduce Materialize Views](klip-19-materialize-views.md)                                                             | Proposal |                   |            |                                                              |
| [KLIP-20: Remove 'TERMINATE' statements](klip-20_remove_terminate.md)                                                            | Proposal |                   |            |                                                              |
| [KLIP-21: Correct 'INSERT VALUES' semantics](klip-21_correct_insert_values_semantics.md)                                         | Proposal |                   |            |                                                              |
| KLIP-22: Add consumer group id to CREATE STREAM and CREATE TABLE DSL                                                             | Proposal |                   |            |                                                              |
| [KLIP-23: PRIMARY KEYs for tables](klip-23-primary-keys-for-tables.md)                                                           |  Merged  |       0.9.0       |   6.0.0    |                                                              |
| [KLIP-24: KEY column semantics in queries](klip-24-key-column-semantics-in-queries.md)                                           |  Merged  |      0.10.0       |   6.0.0    |                                                              |
| [KLIP-25: Removal of `WITH(KEY)` syntax](klip-25-removal-of-with-key-syntax.md)                                                  |  Merged  |      0.10.0       |   6.0.0    |                                                              |
| [KLIP-26: Java client interfaces](klip-26-java-client-interfaces.md)                                                             |  Merged  |      0.10.0       |   6.0.0    |                                                              |
| [KLIP-27: Enhanced UDF Configuration Options](klip-27-enhanced-udf-configuration-options.md)                                     | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/5269) |
| [KLIP-28: Introduce 'CREATE OR REPLACE' for Query Upgrades](klip-28-create-or-replace.md)                                        | Approved |      0.12.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/5611) |
| [KLIP-29: Explicit Table Primary Keys and Key-less Streams]( klip-29-explicit-keys.md)                                           |  Merged  |      0.10.0       |   6.0.0    | [Discussion](https://github.com/confluentinc/ksql/pull/5530) |
| [KLIP-30: Lambda Functions](klip-30-lambda-functions.md)                                                                         | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/5661) |
| [KLIP-31: Metastore Backups](klip-31-metastore-backups.md)                                                                       |  Merged  |      0.11.0       |   6.0.0    | [Discussion](https://github.com/confluentinc/ksql/pull/5741) |
| [KLIP-32: SQL-based testing tool](klip-32-sql-testing-tool.md)                                                                   | Proposal |                   |            |                                                              |
| [KLIP-33: Key format](klip-33-key-format.md)                                                                                     | Approved |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6017) |
| KLIP-34: Optional WITH for CREATE TABLE and CREATE STREAM                                                                        | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6065) |
| KLIP-35: Dynamic Processing Log Levels                                                                                           | Proposal |                   |            |                                                              |
| [KLIP-36: GRACE period for stream-stream joins](klip-36-grace-stream-stream-joins.md)                                            | Approved |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6175) |
| KLIP-36: GRACE period for stream-stream joins                                                                                    | Proposal |                   |            |                                                              |
| KLIP-37: System Tables                                                                                                           | Proposal |                   |            |                                                              |
| [KLIP-38: Variable Substitution](klip-38-variable-substitution.md)                                                               |  Merged  |      0.14.0       |   6.1.0    | [Discussion](https://github.com/confluentinc/ksql/pull/6259) |
| KLIP-39: Prepared Statements for Java Client                                                                                     | Proposal |                   |            |                                                              |
| [KLIP-41: ksqlDB .NET Client](klip-41-ksqldb-.net-client.md)                                                                     | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6613) |
| KLIP-42: Schema Migrations Tool                                                                                                  | Proposal |                   |            |                                                              |
| [KLIP-43: TIMESTAMP data type support ](klip-43-timestamp-data-type-support.md)                                                  | Approved |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6649) |
| [KLIP-44: ksqlDB Go client](klip-44-ksqldb-golang-client.md)                                                                     | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6825) |
| [KLIP-45: Materializing Tables for Query](klip-45-materializing-tables-for-query.md)                                             | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6997) |
| KLIP-46: DATE and TIME data type support                                                                                         | Proposal |        0.20.0     |            | [Discussion](https://github.com/confluentinc/ksql/pull/7417) |
| KLIP-47: NON NULL columns and component types                                                                                    | Proposal |                   |            |                                                              |
| KLIP-48: Schemas                                                                                                                 | Proposal |                   |            |                                                              |
| [KLIP-49: Add source stream/table semantic](klip-49-add-source-stream-table-semantic.md)                                         | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/7474) |
| KLIP-50: Partition and offset in ksqlDB                                                                                          | Proposal |      0.23.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/7505) |
| [KLIP-51: ksqlDB .NET LINQ provider](klip-51-ksqldb .NET LINQ provider.md)                                                       | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/6883) |
| [KLIP-52: BYTES data type support](klip-52-bytes-data-type-support.md)                                                           | Proposal |      0.21.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/7764) |
| [KLIP-53: Pull Queries on Streams                                                                                                | Proposal |      0.22.0       |            |                                                              |
| [KLIP-54: Optimized Range Pull Queries on Tables                                                                                 | Proposal |      0.22.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/7993) |
| [KLIP-55: Tablespaces](https://github.com/confluentinc/ksql/blob/af5e34f27627b634a33dc8e5bfa4f64ac75069e6/design-proposals/klip-55-tablespaces.md)                                                                                 | Proposal |      TBD       |            | [Discussion](https://github.com/confluentinc/ksql/pull/8109) |
| [KLIP-56: Schema id in create statements](klip-56-schema-id-in-create-statements.md)                                             |  Merged  |      0.24.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/8303) |
| [KLIP-57: Kafka headers](klip-57-kafka-headers.md)                                                                               |  Merged  |      0.24.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/8293) |
| KLIP-58: [`LIMIT` clause for Pull Queries](https://github.com/confluentinc/ksql/pull/8298)                                       | Approved |      0.24.0       |            | [Discussion](https://github.com/confluentinc/ksql/pull/8298) |
| [KLIP-59: JSON functions](klip-59-json-functions.md)                                                                             | Approved |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/8550) | 
| [KLIP-60: Support input topics with multiple message types](klip-60-multiple-message-types.md)                                                                             | Proposal |                   |            |  | 
| [KLIP-61: Bootstrapped TABLE subscriptions](klip-61-bootstrapped-table-subscriptions.md)                                                     | Proposal |                   |            |  | 
| [KLIP-62: ksqlDB Ruby Client](klip-62-ksqldb-ruby-client.md)                                                     | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/8865) | 
| [KLIP-63: PAUSE and RESUME for persistent queries](klip-63-pause-resume-persistent-queries.md)                                                     | Proposal |                   |            | 	| 
| [KLIP-66: Multiple parameter UDAFs](klip-66-multi-param-udafs.md)                                                     | Proposal |                   |            | [Discussion](https://github.com/confluentinc/ksql/pull/9173)| 
