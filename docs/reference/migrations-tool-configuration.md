---
layout: page
title: ksqlDB Migrations Tool Configuration Reference
tagline: Configure the ksqlDB Migrations Tool
description: Parameters for Configuring the ksqlDB Migrations Tool
---

The [`ksql-migrations` tool](../operate-and-deploy/migrations-tool.md) 
enables you to manage metadata schemas for your ksqlDB clusters by applying 
statements saved in local *migration files* to your ksqlDB clusters. 

The following properties can be set in your `ksql-migrations.properties` file.
The [`ksql-migrations new-project` command](../operate-and-deploy/migrations-tool.md#initial-setup) 
sets the `ksql.server.url` property upon creating the properties file, 
as this property is required. The properties file is initialized with 
default values for other properties commented out. 
To enable other properties, add or uncomment the relevant lines in your 
`ksql-migrations.properties` file. 

Properties have the following format:
```
<property-name>=<property-value>
```

Required Configs
----------------

### `ksql.server.url`

The URL for your ksqlDB server. For example, `http://localhost:8088`.

Migrations Metadata Configs
---------------------------

### `ksql.migrations.stream.name`

The name of the migrations metadata stream. Defaults to `MIGRATION_EVENTS`.

### `ksql.migrations.table.name`

The name of the migrations metadata table. Defaults to `MIGRATION_SCHEMA_VERSIONS`. 

### `ksql.migrations.stream.topic.name`

The name of the {{ site.ak }} topic backing the migrations metadata stream. 
Defaults to `<ksql-service-id>ksql_<migrations-stream-name>` where `<ksql-service-id>`
is the service ID of your ksqlDB cluster and `<migrations-stream-name>` is
the value of [`ksql.migrations.stream.name`](#ksqlmigrationsstreamname). 

### `ksql.migrations.table.topic.name`

The name of the {{ site.ak }} topic backing the migrations metadata table. 
Defaults to `<ksql-service-id>ksql_<migrations-table-name>` where `<ksql-service-id>`
is the service ID of your ksqlDB cluster and `<migrations-table-name>` is
the value of [`ksql.migrations.table.name`](#ksqlmigrationstablename). 

### `ksql.migrations.topic.replicas`

The number of replicas for each of the migrations metadata stream and table.
The default is 1. 

TLS Configs
-----------

If [`ksql.server.url`](#ksqlserverurl) specifies an HTTPS listener, the
`ksql-migrations` tool uses TLS when connecting to the ksqlDB server.

### `ssl.truststore.location`

The path to the TLS truststore.

### `ssl.truststore.password`

The password for the TLS truststore.

### `ssl.keystore.location`

The path to the TLS keystore.

### `ssl.keystore.password`

The password for the TLS keystore.

### `ssl.key.password`

The password for the TLS key.

### `ssl.key.alias`

The alias for the TLS key, for look up within the keystore.

### `ssl.alpn`

Specifies whether ALPN should be used. Defaults to `false`.

### `ssl.verify.host`

Specifies whether hostname verification should be performed. Defaults to `true`.

ksqlDB Server Authentication
----------------------------

### `ksql.auth.basic.username`

The username that will be used to connect to the ksqlDB server. A username and
[password](#ksqlauthbasicpassword) will be passed as part of HTTP basic authentication. 

### `ksql.auth.basic.password`

The password that will be used to connect to the ksqlDB server. A 
[username](#ksqlauthbasicusername) and password will be passed as part of 
HTTP basic authentication. 

Migrations Directory Configs
----------------------------

### `ksql.migrations.dir.override`

An optional config that allows you to specify the path to the directory
containing migrations files to be applied. This config is not needed if you
set up your migrations project using the `ksql-migrations new-project` command.

If no override is provided, the migrations directory is inferred relative 
to the migrations configuration file passed when using the `ksql-migrations` tool. 
Specifically, the migrations directory is inferred as a directory with name 
`migrations` contained in the same directory as the migrations configuration file. 
This is the default file structure created by the `ksql-migrations new-project` command.

This configuration is available starting with ksqlDB 0.25.0.
