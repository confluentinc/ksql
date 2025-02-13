---
layout: page
title: Manage ksqlDB Metadata Schemas
tagline: Manage ksqlDB Metadata Schemas
description: Manage your ksqlDB metadata schemas including streams, tables, and queries.
---

Use the `ksql-migrations` tool to manage metadata schemas for your ksqlDB clusters
by applying statements from *migration files* to your ksqlDB clusters.
This enables you to keep your SQL statements for creating streams, tables, and queries
in version control and manage the versions of your ksqlDB clusters based on the
migration files that have been applied. 

```
usage: ksql-migrations [ {-c | --config-file} <config-file> ] <command> [ <args> ]

Commands are:
    apply                 Migrates the metadata schema to a new schema version.
    create                Creates a blank migration file with the specified description, which can then be populated with ksqlDB statements and applied as the next schema version.
    destroy-metadata      Destroys all ksqlDB server resources related to migrations, including the migrations metadata stream and table and their underlying Kafka topics. WARNING: this is not reversible!
    help                  Display help information
    info                  Displays information about the current and available migrations.
    initialize-metadata   Initializes the migrations schema metadata (ksqlDB stream and table) on the ksqlDB server.
    new-project           Creates a new migrations project directory structure and config file.
    validate              Validates applied migrations against local files.

See 'ksql-migrations help <command>' for more information on a specific
command.
```

The `ksql-migrations` tool supports migrations files containing the following
types of ksqlDB statements:

* `CREATE STREAM`
* `CREATE TABLE`
* `CREATE STREAM ... AS SELECT` 
* `CREATE TABLE ... AS SELECT`
* `CREATE OR REPLACE`
* `INSERT INTO ... AS SELECT`
* `PAUSE <queryID>`
* `RESUME <queryID>`
* `TERMINATE <queryID>`
* `DROP STREAM`
* `DROP TABLE`
* `ALTER STREAM`
* `ALTER TABLE`
* `INSERT INTO ... VALUES`
* `CREATE CONNECTOR`
* `DROP CONNECTOR`
* `CREATE TYPE`
* `DROP TYPE`
* `SET <property>`
* `UNSET <property>`
* `DEFINE <variable>` - available if both `ksql-migrations` and the server are version 0.18 or newer.
* `UNDEFINE <variable>` - available if both `ksql-migrations` and the server are version 0.18 or newer.
* `ASSERT SCHEMA` - available if both `ksql-migrations` and the server are version 0.27 or newer.
* `ASSERT TOPIC` - available if both `ksql-migrations` and the server are version 0.27 or newer.

Any properties or variables set using the `SET`, `UNSET`, `DEFINE` and `UNDEFINE` are applied in the 
current migration file only. They do not carry over to the next migration file, even if multiple
migration files are applied as part of the same `ksql-migrations apply` command

Requirements and Installation
-----------------------------

The `ksql-migrations` tool is available with all ksqlDB versions starting from
ksqlDB 0.17 or {{ site.cp }} 6.2. You can use the tool to manage any 
ksqlDB cluster running version ksqlDB 0.10 ({{ site.cp }} 6.0) or newer.

### Docker

To run the `ksql-migrations` tool with Docker, you may use either the ksqlDB 
server or ksqlDB CLI image. Mount the root directory of your migrations project 
into the container for use by the `ksql-migrations` tool. For example, the 
following command creates a new migrations project in the local 
`./my/migrations/dir` directory to connect to a ksqlDB server listening at 
`http://localhost:8088` (which is accessed from within the Docker container 
at `http://host.docker.internal:8088`):

```bash
docker run -v $PWD/my/migrations/dir:/share/ksql-migrations confluentinc/ksqldb-server:{{ site.ksqldbversion }} ksql-migrations new-project /share/ksql-migrations http://host.docker.internal:8088
```

Similarly, the following command initializes migrations metadata on the ksqlDB server 
for the same setup:

```bash
docker run -v $PWD/my/migrations/dir:/share/ksql-migrations confluentinc/ksqldb-server:{{ site.ksqldbversion }} ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties initialize-metadata
```

See the sections below for more on the different `ksql-migrations` commands.

Setup and Initialization
------------------------

### Initial Setup

To get started with the `ksql-migrations` tool, use the `ksql-migrations new-project`
command to set up the required directory structure and create a config file for
using the migrations tool. 

```
ksql-migrations new-project [--] <project-path> <ksql-server-url>
```

The two required arguments are the path that will be used as the root directory
for your new migrations project, and your ksqlDB server URL. 

```
$ ksql-migrations new-project /my/migrations/project/path http://localhost:8088
```

Your output should resemble:

```
Creating new migrations project at /my/migrations/project/path
Creating directory: /my/migrations/project/path
Creating directory: /my/migrations/project/path/migrations
Creating file: /my/migrations/project/path/ksql-migrations.properties
Writing to config file: ksql.server.url=http://localhost:8088
...
Migrations project directory created successfully
Execution time: 0.0080 seconds
```

This command creates a config file, named `ksql-migrations.properties`,
in the specified directory, and also creates an empty `/migrations` subdirectory.
The config file is initialized with the ksqlDB server URL passed as part of the
command. 

As a convenience, the config file is also initialized with default values 
for other [migrations tool configurations](#config-reference) commented out.
These additional, optional configurations include configs required to access
secure ksqlDB servers, such as credentials for HTTP basic authentication or TLS keystores
and truststores, as well as optional configurations specific to the migrations tool.

See the [config reference](#config-reference) for details on individual configs.
See [here](#connecting-to-confluent-cloud-ksqldb) for the configs required to 
connect to a {{ site.ccloud }} ksqlDB cluster.

### Initialize Migrations Metadata

The `ksql-migrations` tool keeps track of applied migration versions in a ksqlDB
stream and table, the *migrations metadata* stream and table.
To begin managing your ksqlDB cluster from your migrations project, initialize
the migrations metadata stream and table on your ksqlDB cluster by using the
`ksql-migrations initialize-metadata` command.

```
ksql-migrations {-c | --config-file} <config-file> initialize-metadata
```

Provide the path to the config file of your migrations project when you 
run this command.

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties initialize-metadata
```

Your output should resemble:

```
Initializing migrations metadata
Creating stream: MIGRATION_EVENTS
Creating table: MIGRATION_SCHEMA_VERSIONS
Migrations metadata initialized successfully
Execution time: 2.7040 seconds
```

Now that you've initialized the migrations metadata on your ksqlDB cluster,
you're ready to create and apply migrations.

Create Migrations
-----------------

Migration files are located in the `/migrations` subdirectory of your migrations
project and are named according to the convention `V<six digit version>__<description>.sql`. 
Here's an example directory structure:

```
<migrations-project-dir>
|
|- ksql-migrations.properties
|- migrations/
   |
   |- V000001__Initial_setup.sql
   |- V000002__Add_users.sql
```

Use the `ksql-migrations create` command to create a blank migration
file according to the previous naming scheme, which you can populate with ksqlDB
statements and apply to your ksqlDB cluster.

```
ksql-migrations {-c | --config-file} <config-file> create
                [ {-v | --version} <version> ] [--] <description>
```

To use the `ksql-migrations create` command, provide the path to the config file 
of your migrations project along with a description for your new migration file. 
You can optionally pass in a specific version number for the new file as well.
If unspecified, the next available version number is used. 
Note that 0 is not a valid migration version.

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties create Add_users
```

Your output should resemble:

```
Creating migration file: /my/migrations/project/migrations/V000002__Add_users.sql
Migration file successfully created
Execution time: 0.0480 seconds
```

You can now populate the empty migrations file with ksqlDB statements and apply
the migration to your cluster.

Apply Migrations
----------------

The `ksql-migrations apply` command reads ksqlDB statements from your migration
files and applies them to your ksqlDB cluster.

```
ksql-migrations {-c | --config-file} <config-file> apply
                [ {-a | --all} ] 
                [ {-n | --next} ] 
                [ {-u | --until} <untilVersion> ] 
                [ {-v | --version} <version> ]
                [ {-d | --define} <variableName>=<variableValue> ]
                [ --headers <headersFile> ]
                [ --dry-run ] 
```

There are four different modes for specifying which migration file version(s)
to apply:
* `all`: Apply all available migration files, from the latest applied version.
* `next`: Apply the next available migration file, from the latest applied version.
* `until`: Apply all available migration files, from the latest applied version
  through the specified `untilVersion`.
* `version`: Apply the migration file with the specified version only. The supplied
  version cannot be older than the latest applied version.
  
In addition to selecting a mode for `ksql-migrations apply`, you must also provide
the path to the config file of your migrations project as part of the command.

If both your ksqlDB server and migration tool are version 0.18 and newer, you can 
define variables by passing the `--define` flag followed by a string of the form
`name=value` any number of times. For example, the following command

```bash
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties apply --next -d foo=bar -d car=3
```

is equivalent to having the following lines at the beginning of each migration file:

```
DEFINE foo='bar';
DEFINE car='3';
```

You can optionally use the `--dry-run` flag to see which migration file(s) the
command will apply before running the actual `ksql-migrations apply` command
to update your ksqlDB cluster. The dry run does not validate whether the ksqlDB 
server will accept the statements in your migration file(s). Instead, the dry
run only displays the commands that the migrations tool will attempt to
send to the ksqlDB server.  

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties apply --next --dry-run
```

Your output should resemble:

```
This is a dry run. No ksqlDB statements will be submitted to the ksqlDB server.
Validating current migration state before applying new migrations
Loading migration files
1 migration file(s) loaded.
Applying migration version 2: Add users
/my/migrations/project/migrations/V000002__Add_users.sql contents:
CREATE OR REPLACE TABLE users (user_id VARCHAR PRIMARY KEY, email VARCHAR, country VARCHAR) WITH (KAFKA_TOPIC='users', FORMAT='JSON');

Dry run complete. No migrations were actually applied.
Execution time: 1.2270 seconds
```

When you're ready, remove the `--dry-run` flag to submit the statements to your ksqlDB
server:

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties apply --next
```

Your output should resemble:

```
Validating current migration state before applying new migrations
Loading migration files
1 migration file(s) loaded.
Applying migration version 2: Add users
/my/migrations/project/migrations/V000002__Add_users.sql contents:
CREATE OR REPLACE TABLE users (user_id VARCHAR PRIMARY KEY, email VARCHAR, country VARCHAR) WITH (KAFKA_TOPIC='users', FORMAT='JSON');

Successfully migrated
Execution time: 1.2320 seconds
```

The `apply` command does not apply migration files atomically. If a migration file 
containing multiple ksqlDB statements fails during the migration, it's possible that 
some of the statements will have been run on the ksqlDB server while later statements 
have not.

You can optionally pass custom request headers to be sent with all ksqlDB requests 
made as part of the `apply` command by passing the location of a
file containing the custom request headers with the `--headers` flag:

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties apply --next --headers /my/migrations/project/request_headers.txt
```

Format your headers file with one header name and value pair on each line, 
separated either with a colon or an equals sign. Both of the following are valid:
```
X-My-Custom-Header: abcdefg
X-My-Other-Custom-Header: asdfgh
``` 
or
```
X-My-Custom-Header=abcdefg
X-My-Other-Custom-Header=asdfgh
```

View Current Migration Status
-----------------------------

To view your current migration version and the status of applied migrations, use the
`ksql-migrations info` command.

```
ksql-migrations {-c | --config-file} <config-file> info
```

As with the other commands, pass in the path to the config file of 
your migrations project as part of the command.

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties info
```

Your output should resemble:

```
Current migration version: 2
----------------------------------------------------------------------------------------------------------------------------------
 Version | Name          | State    | Previous Version | Started On                  | Completed On                | Error Reason 
----------------------------------------------------------------------------------------------------------------------------------
 1       | Initial setup | MIGRATED | <none>           | 2021-03-03 23:47:50.455 PST | 2021-03-03 23:47:50.689 PST | N/A          
 2       | Add users     | MIGRATED | 1                | 2021-03-03 23:51:42.787 PST | 2021-03-03 23:51:42.973 PST | N/A          
 3       | Add orders    | PENDING  | N/A              | N/A                         | N/A                         | N/A          
 4       | Enrich orders | PENDING  | N/A              | N/A                         | N/A                         | N/A          
----------------------------------------------------------------------------------------------------------------------------------
```

Validate Applied Migrations
---------------------------

Use the `ksql-migrations validate` command to validate that the migrations
that have been applied to your ksqlDB cluster are the same as the migration files
in your migrations project directory.

```
ksql-migrations {-c | --config-file} <config-file> validate
```

When a migration file is applied to your ksqlDB cluster, the `ksql-migrations` 
tool computes the MD5 hash of the migration file and writes the hash into the
migrations metadata stream as a checksum. The `ksql-migrations validate` command
computes hashes for your local migration files and compares them to the checksums
saved in the migrations metadata stream. 

To use the command, provide the path to the config file of your migrations 
project as part of the command.

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties validate
```

Your output should resemble:

```
Successfully validated checksums for migrations that have already been applied
Execution time: 1.0360 seconds
```

The `validate` command validates only the checksums saved in the migrations metadata
against your local files. The command does not perform any verification on the set of 
streams, tables, queries, and connectors present in your ksqlDB cluster, in relation
to the ksqlDB statements contained in your migration files. 

Reset Migration State
---------------------

If you wish to dissociate your migrations project from your ksqlDB cluster, you can
use the `ksql-migrations destroy-metadata` command to remove all migrations metadata 
from your ksqlDB cluster. This does not undo any applied migrations. Instead, ksqlDB 
statements that have already been submitted to the ksqlDB server remain intact,
but the migrations metadata stream and table, along with their underlying {{ site.ak }} topics,
will be cleaned up from your ksqlDB cluster. This action is not reversible, so
exercise caution when using this command.

Once the migrations metadata has been cleaned up, you can use the 
[`ksql-migrations initialize-metadata` command](#initialize-migrations-metadata) 
to re-create the migrations metadata stream and table in order to 
associate your ksqlDB cluster with a new migrations project.

```
ksql-migrations {-c | --config-file} <config-file> destroy-metadata
```

To use the `ksql-migrations destroy-metadata` command to delete migrations 
metadata from your ksqlDB cluster, provide the path to the config file of your 
migrations project as part of the command.

```
$ ksql-migrations --config-file /my/migrations/project/ksql-migrations.properties destroy-metadata
```

Your output should resemble:

```
Cleaning migrations metadata stream and table from ksqlDB server
Found 1 query writing to the metadata table. Query ID: CTAS_MIGRATION_SCHEMA_VERSIONS_3
Terminating query with ID: CTAS_MIGRATION_SCHEMA_VERSIONS_3
Dropping migrations metadata table: MIGRATION_SCHEMA_VERSIONS
Dropping migrations metadata stream: MIGRATION_EVENTS
Migrations metadata cleaned successfully
Execution time: 1.6390 seconds
```

Config Reference
----------------

You can configure the ksqlDB migrations tool by updating your 
`ksql-migrations.properties` file.
The `ksql-migrations new-project` command sets the `ksql.server.url` property upon
creating the properties file, as this property is required. The properties file
is initialized with default values for other properties commented out. 
To enable other properties, add or uncomment the relevant lines in your 
`ksql-migrations.properties` file. 

For a complete list of available configurations, see the [reference](../reference/migrations-tool-configuration.md).

Connecting to {{ site.ccloud }} ksqlDB
--------------------------------------

To use the `ksql-migrations` tool with your [Confluent Cloud ksqlDB](https://docs.confluent.io/cloud/current/get-started/ksql.html)
cluster, set the following configurations in your `ksql-migrations.properties` file,
which is created as part of [setting up your migrations project](#initial-setup).

```properties
ksql.auth.basic.username=<CCLOUD_KSQLDB_APIKEY>
ksql.auth.basic.password=<CCLOUD_KSQLDB_APIKEY_SECRET>
ksql.migrations.topic.replicas=3
ssl.alpn=true
``` 

Troubleshooting
---------------

### Validation Failures

Prior to applying new migrations, the `ksql-migrations` tool validates the current
state of applied migrations including the following:

- The latest migration version has completed, i.e., does not have status `RUNNING`.
- The migration history is valid, i.e., starting from the latest applied migration
  version and repeatedly following the previous migration version saved in the
  migrations metadata with each applied migration leads back to the first applied
  migration version.
- Each applied migration version in the chain of migration versions above has
  status `MIGRATED`.
- The migration file checksum saved for each migrated version matches what's
  currently present on local disk.
  
With the exception of the first bullet, this is the same verification performed
by the [`ksql-migrations validate`](#validate-applied-migrations) command.

If you find yourself in a situation where validation fails and you are unable
to perform further migrations as a result, you can repair your migrations metadata
so that validation once again passes. This type of intervention may be needed
if a `ksql-migrations apply` command is aborted and a migration status is
never transitioned out of `RUNNING` as a result, or if race conditions between
multiple, simultaneous invocations of `ksql-migrations apply` corrupt the
migrations metadata, as the `ksql-migrations` tool does not support performing
simultaneous migrations. 

To repair your migrations metadata, first inspect the metadata with the
[`ksql-migrations info`](#view-current-migration-status) command.
You can also consume the migration metadata table on your ksqlDB cluster
as a regular ksqlDB table. Here's an example push query, where `<MIGRATIONS_TABLE_NAME>`
is the value of the [`ksql.migrations.table.name` config](../reference/migrations-tool-configuration.md#ksqlmigrationstablename),
which defaults to `MIGRATION_SCHEMA_VERSIONS`.

```sql
SELECT * FROM <MIGRATIONS_TABLE_NAME> EMIT CHANGES;
```

You can then update your migrations metadata by inserting into your migrations
metadata stream, where `<MIGRATIONS_STREAM_NAME>` is the value of the
[`ksql.migrations.stream.name` config](../reference/migrations-tool-configuration.md#ksqlmigrationsstreamname), which
defaults to `MIGRATION_EVENTS`, and the other variables represented in angle
brackets are the values to insert:

```sql
INSERT INTO <MIGRATIONS_STREAM_NAME> (
    version_key, 
    version, 
    name, 
    state, 
    checksum, 
    started_on, 
    completed_on, 
    previous, 
    error_reason
) VALUES (
    '<MIGRATION_VERSION or 'CURRENT'>',
    '<MIGRATION_VERSION>',
    '<MIGRATION_NAME>',
    '<MIGRATION_STATE>'
    '<FILE_CHECKSUM>',
    '<START_TIMESTAMP>',
    '<COMPLETION_TIMESTAMP>',
    '<PREVIOUS_MIGIRATION_VERSION>',
    '<ERROR_REASON>'
);
```

For example, if validation fails because the latest migration version has
status `RUNNING`, you can manually transition the migration status to
`ERROR` in order to repair the migrations metadata.

```sql
INSERT INTO MIGRATION_EVENTS (
    version_key, 
    version, 
    name, 
    state, 
    checksum, 
    started_on, 
    completed_on, 
    previous, 
    error_reason
) VALUES (
    '2',
    '2',
    'Add users',
    'ERROR',
    '1bdb2489db5d969dc2f2bc918407f2d6',
    '1615441337127',
    '1615441337408',
    '1',
    'Manual abort after migration stuck RUNNING'
);
```

If you're performing a repair on the metadata for the latest migration version,
you'll want to also perform the repair on the version key `CURRENT`, used for
tracking the latest migration version.

```sql
INSERT INTO MIGRATION_EVENTS (
    version_key, 
    version, 
    name, 
    state, 
    checksum, 
    started_on, 
    completed_on, 
    previous, 
    error_reason
) VALUES (
    'CURRENT',
    '2',
    'Add users',
    'ERROR',
    '1bdb2489db5d969dc2f2bc918407f2d6',
    '1615441337127',
    '1615441337408',
    '1',
    'Manual abort after migration stuck RUNNING'
);
``` 

Once you've updated the migrations metadata stream, the migrations metadata table
will update automatically and metadata validation will be unblocked.

Next Steps
----------

- Blog post: [Online, Managed Schema Evolution with ksqlDB Migrations](https://www.confluent.io/blog/easily-manage-database-migrations-with-evolving-schemas-in-ksqldb/)

