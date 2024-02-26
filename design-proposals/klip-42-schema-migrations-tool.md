# KLIP 42 - Schema Migrations Tool

**Author**: Sergio Peña (@spena) |
**Release Target**: 0.17.0; 6.2.0 |
**Status**: _Merged_ |
**Discussion**: https://github.com/confluentinc/ksql/pull/6721

**tl;dr:** _New tool to provide ksqlDB users for easy and automated schema migrations for their
           ksqlDB environments. This allows users to version control their ksqlDB schema; recreate
           their schema from scratch; and migrate their current schema to newer versions._

## Motivation and background

Schema migrations (also database migrations) refers to the process of performing updates or rollbacks on a database schema to a
newer or older schema version. This practice is pretty common in any application lifecycle. Users use a version control for their
applications which allow them to know if an application requires an upgrade or rolling back a buggy change. The same is necessary
with a database schema. Users look for ways to version control schema changes along the application lifecycle.

Existing tools exist to perform schema migrations on any database (MySQL, Postgres, Oracle, etc). These tools allow users to
version the database schema, then automate the process to easily upgrade the schema to a newer version. If a schema is buggy,
then users can rollback to the previous schema version. One more reason for using these tools is the integration with any
CI/CD system, which allows users to test and verify a schema upgrade will work as expected.

You can learn more about these existing tools:
- [Flyway](https://flywaydb.org/)
- [Liquibase](https://www.liquibase.org/)
- [DBGeni](http://dbgeni.appsintheopen.com/index.html)

ksqlDB requires the same kind of integration with CI/CD systems and automation to deploy ksqlDB schema changes in any environment.
Some tools, like `Flyway`, support plugins to integrate schema migrations with a different non-supported database. However, our
syntax is exclusively to ksqlDB; tools require JDBC support; and tools have several features that we cannot support
(i.e. transactions & databases); so this integration becomes complex or unsupported. For that reason, ksqlDB must have its own
schema migrations tool to provide the same migrations benefits to users who want to easily deploy and automate ksqlDB schema
changes in all their ksqlDB environments.

These benefits include:
- Version the ksqlDB schema environments
- Integrate ksqlDB schema changes with CI/CD environments
- Simplify schema evolution changes across different environments

This KLIP proposes a new tool for schema migrations. You will learn about the design aspects of the tool, and the features
to support for a basic schema migration process.

## In scope

* Discuss design details for a new tool that provides schema migrations support for ksqlDB

    Basic features to support:
    - New CLI that can easily integrate with CI/CD environments
    - Apply migrations on any ksqlDB environments
    - Dry-run operations to verify what migrations will be applied without altering the cluster
    - Version control ksqlDB schema


## Out of scope

* Some features found in existing migrations tools won't be supported

  - `Execute entire migrations in a single transaction`

    ksqlDB has support for a transactional metastore. However, this is limited to DDL statements that are persisted in the
    Command topic. But DML statements, such as INSERT, write directly to the topic and do not work with transactions. This disallows
    our tool to provide of a transaction support for the whole migration process. Also, DDL statements may create or delete topics,
    which falls in the non-transactional process.

  - `Support for simulated executions`

    A simulation requires the tool to know the current state of the ksqlDB schema before attempting to verify the new migrations scripts. This
    requires a ksqlDB metastore exporting tool to work. Also, to make this simulation 100% safe, the tool requires a dummy Kafka and SR
    environment that can validate issues with topics and SR subjects names, as well as security restrictions.

  - `Other features, such as repeatable migrations and callbacks`

    Not required for a basic migration.

* Do performance analysis on migrations

  In other DBs, there are operations that take too much time to complete. Such is the case of ALTER statements, which can add/remove columns
  that would take time to complete on large tables. ksqlDB operations are quick unless an issue with the ksqlDB environment affects
  these executions.

* Squash several migration files into one

  This is a very important functionality users may want to use. Over time, users may have several small migration files that can be squashed
  into one single file. However, this functionality gets out of scope for the migrations tool. It is easier to write a ksqlDB metastore tool
  that exports the cluster metadata to a SQL file, then use this SQL file as a replacement for the user migrations scripts.

* A new Java API for Java developers

  This seems important. However, it is in consideration if supporting a Java API is necessary.

## Future work

* `Undo migrated schemas to previous versions`

  ksqlDB has a few statements that support undo, and some of them are limited. The first version for migrations will not support this feature.
  Detailed information about undo is explained in the document for future reference. Also, the schema metadata (See Schema metadata) needs to
  be prepared with future undo operations.
  
* `baseline` command to export existing streams, tables, queries, and custom types to a migration file

  This relates to the "metastore exporter" tool that we would like to add, separate from the migrations tool.
  The tool will allow users to export the current state of their ksqlDB cluster as a series of SQL statements
  which may be run to bring a fresh cluster to match the current cluster. A `migrations baseline` command would
  use the same export functionality to create a migrations file.

## Value/Return

Users will be able to integrate ksqlDB upgrades testing with any CI/CD environments of their choice. This is a huge benefit for users who want
to automate ksqlDB upgrades with their application lifecycle.

Also, a new tool will let users to easily automate schema evolution and migrations changes. They will be able to deploy new schema changes in
all their environments (Prod, QA, Devel, etc).

## Public APIS

- No changes on current public APIs
- New command line tool for migrations 

## Design

I'm going to adopt `Flyway` and `DBGeni` tool syntax and behavior to design the ksqlDB migrations tool. Users will define a new migration in an SQL
script. This new SQL script describes the changes to do to migrate the cluster from state A to state B. Then run the migration from the
command line to apply the new state in the ksqlDB cluster. Users can also run this migration automatically as part of the build process and/or
testing in a CI/CD environment.

For instance, the following example creates a new file that setups the initial state of the cluster.

`V000001__Initial_setup.sql`
```sql
CREATE STREAM pageviews (
    user_id INTEGER KEY, 
    url STRING, 
    status INTEGER
) WITH (
    KAFKA_TOPIC='pageviews', 
    VALUE_FORMAT='JSON'    
);

CREATE TABLE pageviews_metrics AS
 SELECT url, COUNT(*) AS num_views
  FROM pageviews
  GROUP BY url
  EMIT CHANGES;
```

The user runs the migration tool on a specific ksqlDB cluster. The tool updates the cluster by running the SQL statements from the above file.
Then sets the state of the cluster to version 1.

```shell script
$ ksql-migrations apply
Current version of schema: << Empty Schema >>
Migrating schema to version 1 - Initial setup
```

Later, the user needs new changes on the cluster. All previous migrations files are immutable. So, any changes on the cluster require a new migration
file (or SQL script). Let's create one to create the users table.

`V000002__Add_users.sql`
```sql
CREATE TABLE users (
   ID BIGINT PRIMARY KEY, 
   STRING NAME, 
   ADDRESS ADDRESS_TYPE
 ) WITH (
   KAFKA_TOPIC='users', 
   VALUE_FORMAT='JSON' 
 );
```

The user runs the migration tool on the same ksqlDB cluster. The tool detects the cluster has already version 1, so it executes only the newer version 2 migration
file. It then sets the state of the cluster to version 2.

```shell script
$ ksql-migrations apply
Current version of schema: 1
Migrating schema to version 2 - Add users
```

All migration files will support only integer versions (no decimal versions). Integer versions are easier to sort when found in the file name. Also, there are
too few cases that require decimal versioning (i.e. `1.1`) in schema changes. We don't expect users to use decimal versions on ksqlDB migrations.

Version numbers need not be consecutive. In the event that earlier version numbers are added after a later version has already been migrated
to, the tool will ignore the newly added versions by default, though we will support a flag to change this behavior and enable out-of-order migrations.

The benefit of this tool is that it detects the required updates to execute in the ksqlDB cluster. So, users don't need to know which SQL statements need to perform
to update the cluster. It also makes it easy to work with multiple clusters. Say that you have devel, stag and prod clusters. The tool will manage and track the version
of these clusters and apply the right SQL operations.

To be able to do that, the tool will use a metadata stream and table that contains the current schema version and all executed updates.

### Schema metadata

Each ksqlDB cluster requires metadata objects where to track the current schema state. This is not only useful for the tool to know the migrations files to apply, but also for
users who can quickly verify if the cluster requires changes to fix a schema bug or add a major/minor improvement for their applications.

The tool creates two metadata objects, a stream and table. automatically during the first migration; or when the user runs the tool with a parameter to initialize it.

i.e.
```
$ ksql-migrations initialize
Schema metadata initialized successfully
```

Due to some query limitations (lack of ORDER BY clause and pull queries working only on materialized views) in ksqlDB, the metadata will be stored in two places;  
One stream (MIGRATION_EVENTS) and one table (SCHEMA_VERSION).

The stream and table require topics unique for the cluster. In this case, these topics names will use the same convention as the processing log. It's not going
to be an internal topic because it will be a user topic. The topic name is: `{clusterID}ksql_{StreamOrTableName}`.

The user can specify a different name for the MIGRATION_EVENTS stream and SCHEMA_VERSION table. This can be done through the tool configuration file. See `Configurations` for more details.

The `MIGRATION_EVENTS` stream will keep track of every migration change (including undo changes). This will contain the history of changes the user has done. Also, this stream
will contain a key to the current version of the schema (specified as `CURRENT` version). Every time a new migration or undo happens, the tool will insert a new event with the
`CURRENT` key pointing to the current version.

This is the `CREATE` statement for the `MIGRATION_EVENTS` stream:
```sql
CREATE STREAM migration_events ( /* name will be configurable */
  version_key  STRING KEY,
  version      STRING,
  name         STRING,
  state        STRING,  
  checksum     STRING,
  started_on   STRING, /* may use the new TIMESTAMP type, depending on availability at time of release */
  completed_on STRING, /* may use the new TIMESTAMP type, depending on availability at time of release */
  previous     STRING
) WITH (  
  KAFKA_TOPIC='default_ksql_migration_events', /* topic name will be configurable */
  VALUE_FORMAT='JSON',
  PARTITIONS=1,
  REPLICAS=1 /* value will be configurable */
);
```

The `version_key` column has the version of the migration applied or undone. A special value of the `version_key`, `CURRENT`, will be reserved for internal purposes.
The `version` column has the version of the migration applied.
The `name` column has the name of the migration.
The `state` column has the state of the migration process. It can be any of `Pending`, `Running`, `Migrated`, `Error`, `Undone`.
The `checksum` column has the MD5 checksum of the migration file. It is used to validate the schema migrations with the local files.
The `started_on` column has the date and time when the migration started.
The `completed_on` column has the date and time when the migration finished.
The `previous` column has the previous version applied.

The `SCHEMA_VERSION` table will also keep track of every migration change (including future work for undo changes), but with the difference that being a table the tool will see quickly if a schema
version has been migrated or undone. It will also give us a quick view of the `CURRENT` state of the schema. The major advantage is that the tool will use pull queries in this materialized
view to get the `CURRENT` state of the cluster.

*Note:*
The below schema is designed so we support undo operations in the future. When an undo happens, the `CURRENT` key will point to the previous version found.

This is the `CREATE` statement for the `SCHEMA_VERSION` table:
```sql
CREATE TABLE schema_version /* name will be configurable */
  WITH (
    KAFKA_TOPIC='default_ksql_schema_version', /* topic will be configurable */
  )
  AS SELECT 
    version_key, 
    latest_by_offset(version) as version, 
    latest_by_offset(name) AS name, 
    latest_by_offset(state) AS state,     
    latest_by_offset(checksum) AS checksum, 
    latest_by_offset(started_on) AS started_on, 
    latest_by_offset(completed_on) AS completed_on, 
    latest_by_offset(previous) AS previous
  FROM migration_events 
  GROUP BY version_key;
```

The following are the outputs that we'll see on each stream and table, and how the tool will figure out the current schema version using pull queries.

The `MIGRATION_EVENTS` stream output:
```shell script
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
| version_key | version | name          | state    | checksum   | started_on          | completed_on        | previous |
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
| 1           | 1       | Initial setup | Running  | <MD5-sum>  | 12-01-2020 03:48:00 | null                | null     |
| CURRENT     | 1       | Initial setup | Running  | <MD5-sum>  | 12-01-2020 03:48:00 | null                | null     |
| 1           | 1       | Initial setup | Migrated | <MD5-sum>  | 12-01-2020 03:48:00 | 12-01-2020 03:48:05 | null     |
| CURRENT     | 1       | Initial setup | Migrated | <MD5-sum>  | 12-01-2020 03:48:00 | 12-01-2020 03:48:05 | null     |
| 2           | 2       | Add users     | Running  | <MD5-sum>  | 12-03-2020 10:34:30 | null                | 1        |
| CURRENT     | 2       | Add users     | Running  | <MD5-sum>  | 12-03-2020 10:34:30 | null                | 1        |
| 2           | 2       | Add users     | Migrated | <MD5-sum>  | 12-03-2020 10:34:30 | 12-03-2020 10:34:34 | 1        |
| CURRENT     | 2       | Add users     | Migrated | <MD5-sum>  | 12-03-2020 10:34:30 | 12-03-2020 10:34:34 | 1        |
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
```

The `SCHEMA_VERSION` table output:
```shell script
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
| version_key | version | name          | state    | checksum   | started_on          | completed_on        | previous |
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
| 1           | 1       | Initial setup | Migrated | <MD5-sum>  | 12-01-2020 03:48:00 | 12-01-2020 03:48:05 | null     |
| 2           | 2       | Add users     | Undone   | <MD5-sum>  | 12-03-2020 10:34:30 | 12-03-2020 10:34:34 | 1        |
| CURRENT     | 2       | Add users     | Undone   | <MD5-sum>  | 12-03-2020 10:34:30 | 12-03-2020 10:34:34 | 1        |
+-------------+---------+---------------+----------+------------+---------------------+---------------------+----------+
```

The tool will later use pull queries to identify the current state of the system:
```sql
SELECT version, state, previous FROM SCHEMA_VERSION WHERE version_key = 'CURRENT'; 
```


### Naming convention

Hope you have noticed the naming rules I followed in the previous examples for naming the migration files. Having a naming convention for files is necessary
for the tool to detect what migration to apply or revert. Naming files are easier than creating configurations or command parameters to specify the same info.

The migration files follow the same `Flyway` naming convention.
`(Prefix)(Version)__(Description).sql`

The `Prefix` specifies whether the file is a new migration (`V`) file or, when support is eventually added, an undo file (`U`).
The `Version` is the version number used for the schema. Versions will not support decimal versions. Integers with 6 digits are used.
The `Description` is a name or description of the new migration. Description uses underscores (automatically replaced by spaces at runtime) or spaces separated the words.

i.e.
```
- V000001__Initial_setup.sql    # a new version to migrate (v1) with name 'Initial setup'
- V000002__Add_users.sql        # a new version to migrate (v2) with name 'Add users'
```

### Dry-runs

A dry-run for ksqlDB migrations will only verify what migrations will be applied in the cluster. It will not attempt to execute or simulate any migration statement found in the files. This
feature will allow users to test that migration files have the right version and description names as well as know what migrations will be applied in a determined cluster.

### Directory structure

The migrations tool will use following directory structure:

<migrations-project-dir>
|
|- ksql-migrations.properties
|- migrations/

When the migrations tool is executed, it will look at the `migrations` directory (by default) for SQL migration files to execute. This directory can be modified in the  
configuration file or command line parameters.

If the `ksql-migrations.properties` exist in the root directory, then it will use the configuration provided by the file.

### Command Syntax

Finally, let's look at the rest of the command parameters that will exist to facilitate schema migrations.

```shell script
Usage:
  ksql-migrations [options] commands
  
Commands
  new <project-path>  Creates a new migrations project, directory structure and config file.

  initialize
   
                 Initializes the schema metadata (stream and table).   
  
  create [-v <version>] <desc> 
  
                 Create a pair of migration files with <desc> as description.
                 Optional: Use the <version> to specify the version to use.
   
                 This will created a pair of empty migration files based
                 on the next schema version.
               
                 i.e.                  
                   $ ksql-migrations create add_users 
                   Created V000002__Add_users.sql

  apply ( all | next | until <target> )
  
              Migrates a schema to new available schema versions (default: all)
              
              If 'all' is specified, then it migrates all newer versions available
              If 'next' is specified, then it migrates only the next available version
              If 'until <target>' is specified, then it migrates all available versions up to and including <target>
  
  info        Displays information about the current and available migrations
  
  clean       Cleans the schema metadata objects                
  
  validate    Validate applied migrations against local files
  
              Compares local files checksum against the current metadata checksums to check for migrations files that have changed.
              This tells the user that their schema might be not valid against their local files.
 
  abort       Aborts the currently ongoing transaction. See "Migration state transitions and potential race conditions" below.
              Note that this does not actually roll back any commands that were applied as part of attempting the migration;
              any such statements will have to be cleaned up by the user outside of the migrations tool.
  
Options
  -c, --config-file  Specifies a configuration file
  
  -d, --define       Define variables for the client session (equivalent to the DEFINE statement). Only applicable for `apply`.
  
  --dry-run          Simulates what migrations will be applied in the cluster. Dry-runs do not execute or simulate each migration
                     statement. Only displays the schema updates that may take place in the cluster without modifying the cluster.
                     Applicable for `apply` as well as `initialize` and `clean` (to confirm the statements that will be executed to
                     manipulate the metadata stream and table).               
 
  --out-of-order     Allow out of order migrations. Only applicable for `apply`. This does not affec the behavior of `next` but will affect `all` and `until`.``

  -h, --help         Shows this help  
    
```

### Configurations

The tool will support a configuration file where all details for the server and tool migrations will be set. The following
are the basic configurations that the tool should support.

Example of `ksql-migrations.properties`
```
# Server URL and authentication
ksql.server.url=http://localhost:8080
ksql.username=user1
ksql.password=pass1

# TLS configs, including for mTLS
ssl.truststore.location=/path/to/truststore
ssl.truststore.password=pass
ssl.keystore.location=/path/to/keystore
ssl.keystore.password=pass
ssl.key.password=pass

# Migrations details
ksql.migrations.stream.name=migration_events
ksql.migrations.table.name=schema_version
ksql.migrations.stream.topic.name=default_ksql_migration_events
ksql.migrations.table.topic.name=default_ksql_schema_version
ksql.migrations.topic.replicas=1
```

The default stream and table metadata topic names will be `{clusterID}ksql_{StreamOrTableName}`, where the migrations
tool with determine the ksqlDB cluster ID by accessing the `/info` endpoint of the ksqlDB server. If desired,
these topic names may be overridden by the user.

Note: The command line options and other configurations will also be defined during the implementation.

### Migration state transitions and potential race conditions

Below we have two potential options for how to manage migration state. We've decided to go with the latter
("Do not implement a transaction mechanism and accept the possibility of race conditions") for the first
version of migrations tool as it should be uncommon (and discouraged) to have multiple users/automated jobs
running migrations simultaneously.

#### Use Kafka transactions to avoid race conditions

To avoid the possibility of multiple migrations being performed at the same time, we can emulate how the
ksqlDB command topic uses Kafka transactions to ensure that only one migration takes place at a time. 
We'd set the producer's transactional ID based on the ksqlDB cluster's service ID (which the migrations
tool can obtain via the `/info` endpoint of the ksqlDB server), and the flow for applying a migration would be:
1. Begin transaction.
1. Check `CURRENT` state is either `Migrated` or `Error` (i.e., not `Running`), by creating a Kafka consumer. Return error and abort if `Running`.
1. Validate checksums for applied migrations
1. Produce `Running` state to migrations stream.
1. Commit transaction.
1. Perform migration. 
1. If successful, produce `Migrated`, else `Error`.

The reason step 2 (checking `CURRENT` state) requires a Kafka consumer rather than simply issuing a pull query
against the migrations table is because the pull query is not guaranteed to be up to date. (For clusters that
have lag reporting enabled, we could specify zero lag tolerance on the pull query, but this is only possible if
the ksqlDB cluster has lag reporting enabled, which the migrations tool cannot control.) By using a Kafka consumer
(as does the command topic transaction protocol), we can ensure the consumer has consumed up to the end offset of 
the topic.

If we do this, I think we'll need to set `isolation.level=read_committed` on the consumer in the query that populates
the migrations table from the migrations stream, to avoid uncommitted data from entering the pull query state stores.

In the event that the migrations tool produces `Running` and crashes before completing the migration, the user will
have to use the `abort` command on the migrations tool to transition the status of the current migration to `Error`
in order to unblock future migrations. Note that this does not actually roll back any commands that were applied as
part of attempting the migration; any such statements will have to be cleaned up by the user outside of the migrations tool. 

#### Do not implement a transaction mechanism and accept the possibility of race conditions

If we want to avoid the complexity of implementing a transaction protocol, we can accept the possibility
of race conditions if a user attempts to perform multiple migrations at once and opt for documentation
warning about this instead.

The flow for applying a migration would be:
1. Check `CURRENT` state is either `Migrated` or `Error` (i.e., not `Running`), by issuing a pull query. Return error and abort if `Running`.
1. Validate checksums for applied migrations, by issuing pull queries for `status='Migrated'`.
1. Check `CURRENT` state has not changed from above, with another pull query. Return error and abort if a change is detected.
1. Produce `Running` state to migrations stream.
1. Perform migration. 
1. If successful, produce `Migrated`, else `Error`.

In the case of multiple migrations (i.e., `migrations apply all` or `migrations apply until <target>`), the last four
steps will be repeated for each migration. (The validation in step 3 will verify the expected state, which is that the
previous migration has status `Migrated`. This validation will be wrapped in a retry loop as pull queries are not
guaranteed to return the latest data.)

As above, in the event that the migrations tool produces `Running` and crashes before completing the migration, the user will
have to use the `abort` command on the migrations tool to transition the status of the current migration to `Error`
in order to unblock future migrations. Note that this does not actually roll back any commands that were applied as
part of attempting the migration; any such statements will have to be cleaned up by the user outside of the migrations tool. 

The pull queries for checking `CURRENT` state in steps 1 and 3 are not guaranteed to return the most up-to-date data.
(If we want, for clusters that have lag reporting enabled, we could specify zero lag tolerance on the pull query.) 
We will accept this possibility as part of the potential race conditions.

Suppose two migrations are attempted concurrently. It's possible that both pass validation and produce `Running`, and both
attempt to perform the actual migration as a result. If the attempted migrations are the same, chances are at most one
will succeed since most ksqlDB statements are only accepted by the server once (e.g., CREATE STREAM/TABLE commands since
stream/table names may not be repeated). In this case, one migration will update the `CURRENT` status to `Error` and the 
other may update the status to `Migrated`. If the order is `Error` then `Migrated`, then the final status is `Migrated`
which is fine. If the order if `Migrated` then `Error`, then the final status is `Error` even though the migration may
have actually been performed. In this case, if the user tries to apply the migration again, that attempt will result in
`Error` and the user will uncover what happened upon investigating based on the error message returned.

If the migrations that were attempted concurrently were different, it's possible both succeed and produce status `Migrated`. 
If users are version-controlling their migration files, it should not be the case that these two migrations have the same
version number. Even if they do, the fact that the two files have different checksums will not fail validation during 
subsequent migrations since the pull query in the validation step will return at most one result for each key (version).

## Test plan

- Verify positive and negative forward migrations
- Verify integration with Github and Jenkins
- Run migrations tool in supported secured environments

## LOEs and Delivery Milestones

Milestones
- Tool with all supported commands completed
- Security configuration (authentication + SSL) completed
- Validate common use cases (i.e. CI/CD, etc) completed
- Quickstart guide and user documentation completed

## Documentation Updates

- New quickstart guide for ksqlDB schema migrations
- New detailed documentation about migrations commands and configurations

## Compatibility Implications

No compatibility implications.

## Security Implications

This tool requires same security configurations as any other ksqlDB client, such as user authentication and SSL
communication with the ksqlDB server.
