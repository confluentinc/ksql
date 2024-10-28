---
layout: page
title: Configure Security for ksqlDB
tagline: Set up ksqlDB security  
description: Settings for security ksqlDB
keywords: ksqldb, confguration, security, acl, ssl, sasl, keystore, truststore
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/security.html';
</script>

ksqlDB supports several combinations of encryption and authentication on its
client-facing and internal endpoints. ksqlDB also supports many of the security
features of the other services it communicates with, like {{ site.aktm }} and
{{ site.sr }}.

Securing ksqlDB for Confluent Cloud
------------------------------------

You can use ksqlDB with a {{ site.ak }} cluster in {{ site.ccloud }}. For more
information, see
[Connecting ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/cp-component/ksql-cloud-config.html).

Securing ksqlDB on premise
--------------------------

This section covers how to secure installations of ksqlDB outside of Confluent Cloud,
like on-premises installations or manual installations on other cloud platforms.

To configure security for ksqlDB, add your configuration settings to the
`<path-to-confluent>/etc/ksqldb/ksql-server.properties` file and then
[start the ksqlDB Server](../installing.md#start-the-ksqldb-server) with your
configuration file specified.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksqldb/ksql-server.properties
```

!!! tip
	These instructions assume you are installing {{ site.cp }} by using ZIP
    or TAR archives. For more information, see
    [On-Premises Deployments](https://docs.confluent.io/current/installation/installing_cp/index.html).


The section is split into:

[Securing ksqlDB installation](#securing-ksqldb-installation): Covers how to secure access to ksqlDB itself
and its own internal communication.

[Securing communication with other services](#securing-communication-with-other-services): Covers how to secure
communication with other services and access to the resources they hold.

Securing ksqlDB installation
----------------------------

ksqlDB supports two main deployment modes:

[Securing interactive deployments](#securing-interactive-deployments): Interactive deployments are those where
the ksqlDB servers accept client connections.

[Securing headless deployments](#securing-headless-deployments): headless
deployments are those where the ksqlDB servers do not accept client connections.
They read the SQL statements they should run from a file on-disk.

Securing interactive deployments
--------------------------------

Securing the interactive ksqlDB installation involves securing the HTTP endpoints that the ksqlDB server is listening on.

As well as accepting connections and requests from clients, a multi-node ksqlDB
cluster also requires inter-node communications. You can choose to configure
the external client and internal inter-node communication separately or over
a single listener:

[Securing single listener setup](#securing-single-listener-setup): Ideal for
single-node installations, or where the inter-node communication is over the
same network interface as client communication.

[Securing dual listener setup](#securing-dual-listener-setup): Useful where
inter-node communication is over a different network interface or requires
different authentication or encryption configuration.


Securing single-listener setup
------------------------------

Securing a single listener for ksqlDB is appropriate when both client and
inter-node communication utilize the same authentication and security
configuration.

The supported options include encrypting the connection and authenticating
clients:

[Configuring listener for SSL encryption](#configuring-listener-for-ssl-encryption):
Creates an encrypted connection between the client and server, as well as for
inter-node communication.

[Configuring listener for HTTP-BASIC authentication](#configuring-listener-for-http-basic-authentication):
Uses a username and password for authenticating to ksqlDB.

Configuring Listener for SSL encryption
---------------------------------------

ksqlDB can be configured to use HTTPS rather than the default HTTP for all
communication.

If you haven't already, you will need to
[create SSL key and trust stores](https://docs.confluent.io/current/security/security_tutorial.html#creating-ssl-keys-and-certificates).

Use the following settings to configure the ksqlDB server to use HTTPS:

```properties
listeners=https://hostname:port
ssl.truststore.location=/var/private/ssl/ksql.server.truststore.jks
ssl.truststore.password=zzzz
ssl.keystore.location=/var/private/ssl/ksql.server.keystore.jks
ssl.keystore.password=xxxx
ssl.key.password=yyyy
```

Note the use of the HTTPS protocol in the `listeners` config.

To enable the server to authenticate clients (2-way authentication), use
the following additional setting:

```properties
ssl.client.authentication=REQUIRED
```

!!! note
    The `ssl.client.auth` setting is deprecated.

### Additional server configuration options for HTTPS

Additional settings are available for configuring ksqlDB for HTTPS.

`ssl.enabled.protocols`: A comma-separated list of protocols enabled for SSL connections.
Leave blank to use the default from the {{ site.aktm }} [`SslConfigs.java` file](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/config/SslConfigs.java)
(see `DEFAULT_SSL_ENABLED_PROTOCOLS`).

`ssl.cipher.suites`: A comma-separated list of SSL cipher suites.
Leave blank to use your JVM defaults.

### Configure the CLI for HTTPS

If the ksqlDB server is configured to use HTTPS, CLI instances may need to
be configured with suitable key and trust stores.

If the server's SSL certificate is not signed by a recognized public
Certificate Authority, you must configure the CLI with a trust
store that trusts the server's SSL certificate.

If you haven't already,
[create SSL key and trust stores](https://docs.confluent.io/current/security/security_tutorial.html#creating-ssl-keys-and-certificates).

Use the following settings to configure the CLI server:

```properties
ssl.truststore.location=/var/private/ssl/ksql.client.truststore.jks
ssl.truststore.password=<secure-password>
```

If the server is performing client authentication (2-way
authentication), use the following additional settings:

```properties
ssl.keystore.location=/var/private/ssl/ksql.client.keystore.jks
ssl.keystore.password=xxxx
ssl.key.password=<another-secure-password>

# Used to find the key store entry with the given alias in the keystore
ssl.keystore.alias=<key-store-entry-alias>
```

Settings for the CLI can be stored in a suitable file and passed to the
CLI by using the `--config-file` command-line arguments, for example:

```bash
<ksql-install>bin/ksql --config-file ./config/ksql-cli.properties https://localhost:8088
```

Configuring listener for HTTP-BASIC Authentication/Authorization
----------------------------------------------------------------

ksqlDB can be configured to require users to authenticate using a username
and password via the Basic HTTP authentication mechanism. You can
also provide role-based authorization by specifying which roles can access the ksqlDB server.

!!! note
	If you're using Basic authentication, we recommend that you
    [configure ksqlDB to use HTTPS for secure communication](#configuring-listener-for-ssl-encryption),
    because the Basic protocol passes credentials in plain text.

### Create the `jaas_config.file`

The file `jaas_config.file` defines how the ksqlDB server authenticates users.

An example `jaas_config.file` is:

```java
KsqlServer-Props {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/path/to/password-file"
  debug="false";
};
```

The example `jaas_config.file` above uses the Jetty
`PropertyFileLoginModule`, which itself authenticates users by checking
for their credentials in a password file.

You can also use other implementations of the standard Java
`LoginModule` interface, such as `JDBCLoginModule` for reading
credentials from a database or the `LdapLoginModule`.

In the `PropertyFileLoginModule` example above, the file parameter is the
location of the password file. The format is:

```properties
<username>: <password-hash>[,<rolename> ...]
```

Specify one user and password per line. Roles are optional and are
defined next to the password hash.

The password hash for a user can be obtained by using the
`org.eclipse.jetty.util.security.Password` utility, for example running:

```bash
bin/ksql-run-class org.eclipse.jetty.util.security.Password fred letmein
```

Which results in an output similar to:

```
letmein
OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x
MD5:0d107d09f5bbe40cade3de5c71e9e9b7
CRYPT:frd5btY/mvXo6
```

Where each line of the output is the password encrypted using different
mechanisms, starting with plain text. Copy any of the password encrypted
lines onto the password file.

The role names are defined next to the encrypted password. These
are used by the `LoginModule` to map users to roles.

Here's an example of the password file:

```
fred: OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x,user,admin
harry: changeme,user,developer
tom: MD5:164c88b302622e17050af52c89945d44,user
dick: CRYPT:adpexzg3FUZAk,admin,ksql-user
```

### Configure the ksqlDB server settings

Use the following settings in the `ksql-server.properties` file:

```properties
authentication.method=BASIC
authentication.realm=<KsqlServer-Props-in-jaas_config.file>
authentication.roles=<user-role1>,<user-role2>,...
```

The `authentication.method` property indicates that ksqlDB authenticates users by using `BASIC`
user and password credentials.

The `authentication.realm` config must match a section within
`jaas_config.file`, which provides the login module for authentication.

In this example, the `authentication.realm` config is set to `KsqlServer-Props`, which
was defined in the previous section:

```properties
authentication.realm=KsqlServer-Props
```

The `authentication.roles` config defines a comma-separated list of user
roles with access to the ksqlDB server. To be authorized to use the ksqlDB Server,
an authenticated user must belong to at least one of these roles.

For example, if you define `admin` and `developer` roles, ksqlDB allows access only to users that contain these roles
For more information, see the configuration for the password file in the `jaas_config.file` section.

```properties
authentication.roles=admin,developer
```

!!! note
    You can authorize any role by setting the `authentication.roles` to `**`.

###  Example

Password file:
```
fred: OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x,user,admin
harry: changeme,user,developer
tom: MD5:164c88b302622e17050af52c89945d44,user
dick: CRYPT:adpexzg3FUZAk,admin,ksql-user
```

JAAS config file (`jaas_config.file`):
```java
KsqlServer-Props {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/path/to/password-file"
  debug="false";
};
```

ksqlDB settings:
```properties
authentication.method=BASIC
authentication.realm=KsqlServer-Props
authentication.roles=admin
```

The ksqlDB server authenticates users `fred`, `harry`, `tom`, and `dick` based on the
user and password credentials found in the password file. But only `fred` and `dick`
authorized to access the server because they're under the `admin` role.

### Configure the CLI for Basic HTTP Authentication

If the ksqlDB Server is configured to use Basic authentication, you must
configure CLI instances with suitable valid credentials. You can provide
credentials when starting the CLI by using the `--user` and
`--password` command-line arguments, for example:

```bash
<ksql-install>bin/ksql --user fred --password letmein http://localhost:8088
```

Securing dual-listener setup
----------------------------

Using dual listeners for ksqlDB is appropriate when the client and
inter-node communication utilize different authentication and security
configurations. This is most likely the case when ksqlDB is deployed as an
IaaS service.

The supported setups are SSL-mutual auth for the internal communication
combined with SSL encryption and authentication for the external client:

[Configuring internal for SSL-mutual authentication](#configuring-internal-for-ssl-mutual-authentication):
Creates secure and authenticated connections for inter-node communication,
but leaves the external
client API unsecured. This is most appropriate when clients are trusted, but
the internal APIs are protected from use.

[Configuring internal for SSL-mutual authentication and external for SSL encryption](#configuring-internal-for-ssl-mutual-authentication-and-external-for-ssl-encryption):
Creates secure and authenticated connections for inter-node
communication and uses SSL for the external client API. This is most
likely to be used with authentication below.

[Configuring internal for SSL-mutual authentication and external for HTTP-BASIC authentication](#configuring-internal-for-ssl-mutual-authentication-and-external-for-http-basic-authentication):
Creates secure and authenticated connections for inter-node
communication and uses basic authentication for the external client API.
This is most likely to be used with SSL above.

Configuring internal for SSL-mutual authentication
--------------------------------------------------

ksqlDB supports securing inter-node communication using SSL mutual authentication.

For more information about configuring `ksql.internal.listener`, see [Configuring Listeners of a ksqlDB Cluster](index.md#configuring-listeners-of-a-ksqldb-cluster).

Your key store must contain the key pair for your internal listener set with
`ksql.internal.listener`.  If your internal certificate is not signed by a recognized
public Certificate Authority, a trust store is required to contain certificates for
nodes in your cluster.  Below is an example configuration:

```properties
# Contains the internal key pair for this node.
# (e.g. key pair for node-1.internal.example.com)
ssl.keystore.location=/var/private/ssl/ksql.server.keystore.jks
ssl.keystore.password=xxxx
ssl.key.password=yyyy

# Contains the certificates for nodes in the cluster.
# (e.g. certs for node-1.internal.example.com, node-2.internal.example.com)
ssl.truststore.location=/var/private/ssl/ksql.server.truststore.jks
ssl.truststore.password=zzzz

listeners=http://0.0.0.0:8088
ksql.internal.listener=https://node-1.internal.example.com:8099

# This enables mutual auth checking for the internal listener
ksql.internal.ssl.client.authentication=REQUIRED
```

Configuring internal for SSL-mutual authentication and external for HTTP-BASIC authentication
---------------------------------------------------------------------------------------------

Client-facing basic HTTP authentication can be used alongside authentication for the
internal listener. This ensures that neither the client or internal
APIs can be accessed by unauthorized users.

!!! note
	If you're using Basic authentication, we recommend that you
    [configure ksqlDB to use HTTPS for secure communication](#configuring-listener-for-ssl-encryption),
    because the Basic protocol passes credentials in plain text.

Below is an example configuration:

```properties
ssl.keystore.location=/var/private/ssl/ksql.server.keystore.jks
ssl.keystore.password=xxxx
ssl.key.password=yyyy

ssl.truststore.location=/var/private/ssl/ksql.server.truststore.jks
ssl.truststore.password=zzzz

listeners=http://0.0.0.0:8088
ksql.internal.listener=https://node-1.internal.example.com:8099

ksql.internal.ssl.client.authentication=REQUIRED

authentication.method=BASIC
authentication.roles=admin,developer,user,ksq-user
authentication.realm=KsqlServer-Props

```

For more detail on basic authentication,
[see above](#configure-ksqldb-for-basic-http-authentication).

Configuring internal for SSL-mutual authentication and external for SSL encryption
--------------------------------------------------------------------------------

If you want to use HTTPS on `listeners` as well as use SSL mutual
auth for internal communication on `ksql.internal.listener`, you will likely
require two different key pairs, since your host's identity to clients may be
different from its internal identity. To create such a key store,
refer [below](#setting-up-a-key-store-and-trust-store).

In such a configuration, you must specify which key pair is used for a given
listener by providing a key store alias. For example,
if set, `ksql.ssl.keystore.alias.internal` will be used to find the key store entry
with the given alias when setting up the internal listener. Similarly,
`ksql.ssl.keystore.alias.external` is used for the client listener `listeners`.
Below is an example configuration:

```properties
# Contains the client certificate for this node under the alias 'client'.
# (e.g. key pair for external.example.com)
# Contains the internal certificate for this node under the alias 'internal_node1'.
# (e.g. key pair for node-1.internal.example.com)
ssl.keystore.location=/var/private/ssl/ksql.server.keystore.jks
ssl.keystore.password=xxxx
ssl.key.password=yyyy

# The aliases referenced above
ksql.ssl.keystore.alias.external=client
ksql.ssl.keystore.alias.internal=internal_node1

# Contains the certificates for nodes in the cluster.
# (e.g. certs for node-1.internal.example.com, node-2.internal.example.com)
ssl.truststore.location=/var/private/ssl/ksql.server.truststore.jks
ssl.truststore.password=zzzz

listeners=https://external.example.com:8088
ksql.internal.listener=https://node-1.internal.example.com:8099

# Enable mutual auth checking for the internal listener
ksql.internal.ssl.client.authentication=REQUIRED
# Don't require external clients to authenticate via SSL
ksql.ssl.client.authentication=NONE
```

### Setting up a Key Store and Trust Store

In order to create a keystore with multiple key pairs with aliases, follow the
below examples, depending on the source of the keys.

```bash
# Generated key pairs with aliases 'client' and 'internal_node1'
keytool -genkey -alias client -keyalg RSA -keypass password -storepass password -keystore ksql.server.keystore.p12 -storetype PKCS12
keytool -genkey -alias internal_node1 -keyalg RSA -keypass password -storepass password -keystore ksql.server.keystore.p12 -storetype PKCS12

# Imported key pairs, with aliases 'client' and 'internal_node1'
keytool -importkeystore -deststorepass password -destkeystore ksql.server.keystore.p12 -deststoretype PKCS12 -destalias client -srckeystore client_api.p12 -srcstoretype PKCS12 -srcalias client
keytool -importkeystore -deststorepass password -destkeystore ksql.server.keystore.p12 -deststoretype PKCS12 -destalias internal_node1 -srckeystore internal_node1.p12 -srcstoretype PKCS12 -srcalias internal_node1
```

Also, extracting certificates to add to a trust store can be done with the following
commands:

```bash
keytool -export -alias internal_node1 -storepass password -file node1.cer -keystore internal_node1.p12 -srcstoretype PKCS12
keytool -export -alias internal_node2 -storepass password -file node2.cer -keystore internal_node2.p12 -srcstoretype PKCS12

keytool -import -v -trustcacerts -alias internal_node1 -file node1.cer -keystore ksql.server.truststore.jks -keypass password -storepass password
keytool -import -v -trustcacerts -alias internal_node1 -file node2.cer -keystore ksql.server.truststore.jks -keypass password -storepass password
```

Securing headless deployments
-----------------------------

Unlike interactive deployments, there are no exposed REST APIs, so security
is greatly simplified.

You may still have to [secure communication](#securing-communication-with-other-services) with other services.

Securing communication with other services
------------------------------------------

This section covers how to secure communications of ksqlDB with other services.

The section is split into:

[Configure ksqlDB for Secured Confluent Schema Registry](#configure-ksqldb-for-secured-confluent-schema-registry):
Covers how to secure communication with schema registry.

[Configure ksqlDB for Secured Apache Kafka clusters](#configure-ksqldb-for-secured-apache-kafka-clusters):
Covers how to secure communication with Kafka.


Configure ksqlDB for Secured Confluent Schema Registry
------------------------------------------------------

You can configure ksqlDB to connect to {{ site.sr }} over HTTP by setting
the `ksql.schema.registry.url` to the HTTPS endpoint of {{ site.sr }}.
Depending on your security setup, you might also need to supply
additional SSL configuration. For example, a trustStore is required if
the {{ site.sr }} SSL certificates aren't trusted by the JVM by
default. A keyStore is required if {{ site.sr }} requires mutual
authentication.

You can configure SSL for communication with {{ site.sr }} by using
non-prefixed names, like `ssl.truststore.location`, or prefixed names
like `ksql.schema.registry.ssl.truststore.location`. Non-prefixed names
are used for settings that are shared with other communication channels,
where the same settings are required to configure SSL communication
with both Kafka and {{ site.sr }}. Prefixed names affect communication
with {{ site.sr }} only and override any non-prefixed settings of the same
name.

Use the following to configure ksqlDB for communication with {{ site.sr }}
over HTTPS, where mutual authentication isn't required and {{ site.sr }}
SSL certificates are trusted by the JVM:

```properties
ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
```

Use the following settings to configure ksqlDB for communication with
{{ site.sr }} over HTTPS, with mutual authentication, with an explicit
trustStore, and where the SSL configuration is shared between Kafka and
{{ site.sr }}:

```properties
ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
ksql.schema.registry.ssl.truststore.location=/etc/kafka/secrets/ksql.truststore.jks
ksql.schema.registry.ssl.truststore.password=<your-secure-password>
ksql.schema.registry.ssl.keystore.location=/etc/kafka/secrets/ksql.keystore.jks
ksql.schema.registry.ssl.keystore.password=<your-secure-password>
ksql.schema.registry.ssl.key.password=<your-secure-password>
```

Use the following settings to configure ksqlDB for communication with
{{ site.sr }} over HTTP, without mutual authentication and with an explicit
trustStore. These settings explicitly configure only ksqlDB to {{ site.sr }}
SSL communication.

```properties
ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
ksql.schema.registry.ssl.truststore.location=/etc/kafka/secrets/sr.truststore.jks
ksql.schema.registry.ssl.truststore.password=<your-secure-password>
```

The exact settings will vary depending on the encryption and
authentication mechanisms {{ site.sr }} is using, and how your SSL
certificates are signed.

You can pass authentication settings to the {{ site.sr }} client used by
ksqlDB by adding the following to your ksqlDB Server config.

```properties
ksql.schema.registry.basic.auth.credentials.source=USER_INFO
ksql.schema.registry.basic.auth.user.info=username:password
```

For more information, see
[Schema Registry Security Overview](https://docs.confluent.io/current/schema-registry/security/index.html).

Configure ksqlDB for Secured Apache Kafka clusters
--------------------------------------------------

The following are common configuration examples.

### Configuring Kafka Encrypted Communication

This configuration enables ksqlDB to connect to a Kafka cluster over SSL,
with a user supplied trust store:

```properties
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ssl.truststore.password=confluent
```

The exact settings will vary depending on the security settings of the
Kafka brokers, and how your SSL certificates are signed. For full
details, and instructions on how to create suitable trust stores, please
refer to the
[Security Guide](https://docs.confluent.io/current/security/index.html).

To use separate trust stores for encrypted communication with {{ site.ak }}
and external communication with ksqlDB clients, prefix the SSL truststore configs
with `ksql.streams.`:

```properties
security.protocol=SSL
ksql.streams.ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ksql.streams.ssl.truststore.password=confluent
```

### Configure Kafka Authentication

This configuration enables ksqlDB to connect to a secure Kafka cluster
using PLAIN SASL, where the SSL certificates have been signed by a CA
trusted by the default JVM trust store.

```
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=\
    org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="<ksql-user>" \
    password="<password>";
```

The exact settings will vary depending on what SASL mechanism your Kafka
cluster is using and how your SSL certificates are signed. For more
information, see the
[Security Guide](https://docs.confluent.io/current/security/index.html).

### Configure Authorization of ksqlDB with Kafka ACLs

Kafka clusters can use ACLs to control access to resources. Such
clusters require each client to authenticate as a particular user. To
work with such clusters, ksqlDB must be configured to
[authenticate with the Kafka cluster](#configure-kafka-authentication),
and certain ACLs must be defined in the Kafka cluster to allow the user
ksqlDB is authenticating as access to resources. The list of ACLs that
must be defined depends on the version of the Kafka cluster.

#### Confluent Platform v5.0 (Apache Kafka v2.0) and above

{{ site.cp }} 5.0 simplifies the ACLs required to run ksqlDB against a
Kafka cluster secured with ACLs, (see
[KIP-277](https://cwiki.apache.org/confluence/display/KAFKA/KIP-277+-+Fine+Grained+ACL+for+CreateTopics+API)
and
[KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs)
for details). We strongly recommend using {{ site.cp }} 5.0 or above
for deploying secure installations of Kafka and ksqlDB.

#### ACL definition

Kafka ACLs are defined in the general format of "Principal P is
Allowed/Denied Operation O From Host H on any Resource R matching
ResourcePattern RP".

Principal

:   An authenticated user or group. For example, `"user: Fred"` or
    `"group: fraud"`.

Permission

:   Defines if the ACL allows (`ALLOW`) or denies (`DENY`) access to the
    resource.

Operation

:   The operation that is performed on the resource, for example `READ`.

Resource

:   A resource consists of a resource type and resource name:

    -   `RESOURCE_TYPE`, for example `TOPIC` or consumer `GROUP`.
    -   Resource name, for example the name of a topic or a
        consumer-group.

ResourcePattern

:   A resource pattern matches zero or more Resources and consists
    of a resource type, a resource name and a pattern type.

    -   `RESOURCE_TYPE`, for example `TOPIC` or consumer `GROUP`. The
        pattern will only match resources of the same resource type.
    -   Resource name. How the pattern uses the name to match Resources
        is dependent on the pattern type.
    -   `PATTERN_TYPE`, controls how the pattern matches a Resource's
        name to the patterns. Valid values are:
        -   `LITERAL` pattern types match the name of a resource
            exactly, or, in the case of the special wildcard resource
            name *, resources of any name.
        -   `PREFIXED` pattern types match when the resource's name is
            prefixed with the pattern's name.

    The `CLUSTER` resource type is implicitly a literal pattern with a
    constant name because it refers to the entire Kafka cluster.

The ACLs described below list a `RESOURCE_TYPE`, resource name,
`PATTERN_TYPE`, and `OPERATION`. All ACLs described are `ALLOW` ACLs,
where the principal is the user the ksqlDB Server has authenticated as,
with the Apache Kafka cluster, or an appropriate group that includes the
authenticated ksqlDB user.

!!! tip
	For more information about ACLs, see [Authorization using
    ACLs](https://docs.confluent.io/current/kafka/authorization.html).

##### ACLs on Literal Resource Pattern

A literal resource pattern matches resources exactly. They are
case-sensitive. For example `ALLOW` `user Fred` to `READ` the `TOPIC`
with the `LITERAL` name `users`.

Here, user Fred would be allowed to read from the topic *users* only.
Fred would not be allowed to read from similarly named topics such as
*user*, *users-europe*, *Users* etc.

##### ACLs on Prefixed Resource Pattern

A prefixed resource pattern matches resources where the resource name
starts with the pattern's name. They are case-sensitive. For example
`ALLOW` `user Bob` to `WRITE` to any `TOPIC` whose name is `PREFIXED`
with `fraud-`.

Here, user Bob would be allowed to write to any topic whose name starts
with *fraud-*, for example *fraud-us*, *fraud-testing* and *fraud-*. Bob
would not be allowed to write to topics such as
*production-fraud-europe*, *Fraud-us*, etc.

#### Required ACLs

The ACLs required are the same for both
[Interactive and non-interactive (headless) ksqlDB clusters](index.md#non-interactive-headless-ksqldb-usage).

ksqlDB always requires the following ACLs for its internal operations and
data management:

-   The `DESCRIBE_CONFIGS` operation on the `CLUSTER` resource type.
-   The `ALL` operation on all internal `TOPICS` that are `PREFIXED`
    with `_confluent-ksql-<ksql.service.id>`.
-   The `ALL` operation on all internal `GROUPS` that are `PREFIXED`
    with `_confluent-ksql-<ksql.service.id>`.

Where `ksql.service.id` can be configured in the ksqlDB configuration and
defaults to `default_`.

If ksqlDB is configured to create a topic for the
[record processing log](../../../reference/processing-log.md),
which is the default configuration, the following ACLs are also needed:

-   The `ALL` operation on the `TOPIC` with `LITERAL` name
    `<ksql.logging.processing.topic.name>`.

Where `ksql.logging.processing.topic.name` can be configured in the ksqlDB
configuration and defaults to `<ksql.service.id>ksql_processing_log`.

In addition to the general permissions above, ksqlDB also needs
permissions to perform the actual processing of your data. Here, ksqlDB
needs permissions to read data from your desired input topics and/or
permissions to write data to your desired output topics:

-   The `READ` operation on any input topics.
-   The `WRITE` operation on any output topics.
-   The `CREATE` operation on any output topics that do not already
    exist.

Often output topics from one query form the inputs to others. ksqlDB will
require `READ` and `WRITE` permissions for such topics.

The set of input and output topics that a ksqlDB cluster requires access
to will depend on your use case and whether the ksqlDB cluster is
configured in
[interactive](#interactive-ksqldb-clusters)
or
[non-interactive](#non-interactive-headless-ksqldb-clusters)
mode.

#### Non-Interactive (headless) ksqlDB clusters

[Non-interactive ksqlDB clusters](index.md#non-interactive-headless-ksqldb-usage)
run a known set of SQL statements, meaning the set of input and output
topics is well defined. Add the ACLs required to allow ksqlDB access to
these topics.

For example, given the following setup:

-   A 3-node ksqlDB cluster with ksqlDB servers running on IPs 198.51.100.0,
    198.51.100.1, 198.51.100.2
-   Authenticating with the Kafka cluster as a `KSQL1` user.
-   With `ksql.service.id` set to `production_`.
-   Running queries that read from input topics `input-topic1` and
    `input-topic2`.
-   Writing to output topics `output-topic1` and `output-topic2`.
-   Where `output-topic1` is also used as an input for another query.

Then the following commands would create the necessary ACLs in the Kafka
cluster to allow ksqlDB to operate:

```bash
# Allow ksqlDB to discover the cluster:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --cluster

# Allow ksqlDB to read the input topics (including output-topic1):
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Read --topic input-topic1 --topic input-topic2 --topic output-topic1

# Allow ksqlDB to write to the output topics:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Write --topic output-topic1 --topic output-topic2
# Or, if the output topics do not already exist, the 'create' operation is also required:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Create --operation Write --topic output-topic1 --topic output-topic2

# Allow ksqlDB to manage its own internal topics and consumer groups:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic _confluent-ksql-production_ --group _confluent-ksql-production_

# Allow ksqlDB to manage its record processing log topic, if configured:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic production_ksql_processing_log
```

#### Interactive ksqlDB clusters

[Interactive ksqlDB clusters](/operate-and-deploy/how-it-works#interactive-deployment)
accept SQL statements from users and hence may require access to a wide
variety of input and output topics. Add ACLs to appropriate literal and
prefixed resource patterns to allow ksqlDB access to the input and output
topics, as required.

!!! tip
	To simplify ACL management, you should configure a default custom topic
    name prefix such as `ksql-interactive-` for your ksqlDB cluster via the
    `ksql.output.topic.name.prefix`
    [server configuration setting](index.md#setting-ksqldb-server-parameters).
    Unless a user defines an explicit topic name in a SQL statement, ksqlDB
    will then always prefix the name of any automatically created output
    topics. Then add an ACL to allow `ALL` operations on `TOPICs` that are
    `PREFIXED` with the configured custom name prefix (in the example above:
    `ksql-interactive-`).

For example, given the following setup:

-   A 3-node ksqlDB cluster with ksqlDB servers running on IPs 198.51.100.0,
    198.51.100.1, 198.51.100.2
-   Authenticating with the Kafka cluster as a `KSQL1` user.
-   With `ksql.service.id` set to `fraud_`.
-   Where users should be able to run queries against any input topics
    prefixed with `accounts-`, `orders-` and `payments-`.
-   Where `ksql.output.topic.name.prefix` is set to `ksql-fraud-`
-   And users won't use explicit topic names, i.e. users will rely on
    ksqlDB auto-creating any required topics with auto-generated names.
    (Note: If users want to use explicit topic names, then you must
    provide the necessary ACLs for these in addition to what's shown in
    the example below.)

Then the following commands would create the necessary ACLs in the Kafka
cluster to allow ksqlDB to operate:

```bash
# Allow ksqlDB to discover the cluster:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --cluster

# Allow ksqlDB to read the input topics:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Read --resource-pattern-type prefixed --topic accounts- --topic orders- --topic payments-

# Allow ksqlDB to manage output topics:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic ksql-fraud-

# Allow ksqlDB to manage its own internal topics and consumer groups:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic _confluent-ksql-fraud_ --group _confluent-ksql-fraud_

# Allow ksqlDB to manage its record processing log topic, if configured:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic fraud_ksql_processing_log

# Allow ksqlDB to produce to the command topic:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --producer --transactional-id ksql-fraud_ --topic _confluent-ksql-fraud__command_topic
```

The following table shows the necessary ACLs in the Kafka cluster to
allow ksqlDB to operate in interactive mode.

| Permission  | Operation          | Resource  | Name                                  | Type
|-------------|--------------------|-----------|---------------------------------------|----------
|ALLOW        |DESCRIBE            |CLUSTER    | kafka-cluster                         | LITERAL
|ALLOW        |DESCRIBE_CONFIGS    |CLUSTER    | kafka-cluster                         | LITERAL
|ALLOW        |CREATE              |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |CREATE              |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |CREATE              |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DESCRIBE            |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |DESCRIBE            |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DESCRIBE            |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |ALTER               |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |ALTER               |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |ALTER               |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DESCRIBE_CONFIGS    |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |DESCRIBE_CONFIGS    |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DESCRIBE_CONFIGS    |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |ALTER_CONFIGS       |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |ALTER_CONFIGS       |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |ALTER_CONFIGS       |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |READ                |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |READ                |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |READ                |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |WRITE               |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |WRITE               |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |WRITE               |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DELETE              |TOPIC      | `<ksql-service-id>`                   | PREFIXED
|ALLOW        |DELETE              |TOPIC      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DELETE              |GROUP      | `_confluent-ksql-<ksql-service-id>`   | PREFIXED
|ALLOW        |DESCRIBE            |TOPIC      | `*`                                   | LITERAL
|ALLOW        |DESCRIBE            |GROUP      | `*`                                   | LITERAL
|ALLOW        |DESCRIBE_CONFIGS    |TOPIC      | `*`                                   | LITERAL
|ALLOW        |DESCRIBE_CONFIGS    |GROUP      | `*`                                   | LITERAL
|ALLOW        |DESCRIBE            |TRANSACTIONAL_ID | `<ksql-service-id>`             | LITERAL
|ALLOW        |WRITE               |TRANSACTIONAL_ID | `<ksql-service-id>`             | LITERAL


#### Confluent Platform versions below v5.0 (Apache Kafka < v2.0)

Versions of the {{ site.cp }} below v5.0, (which use Apache Kafka
versions below v2.0), do not benefit from the enhancements found in
later versions of Kafka, which simplify the ACLs required to run ksqlDB
against a Kafka cluster secured with ACLs. This means a much larger, or
wider range, set of ACLs must be defined. The set of ACLs that must be
defined depends on whether the ksqlDB cluster is configured for
[interactive](#interactive-ksqldb-clusters-pre-kafka-20)
or
[non-interactive (headless)](#non-interactive-headless-ksqldb-clusters-pre-kafka-20).

#### ACL definition

Kafka ACLs are defined in the general format of "Principal P is
Allowed/Denied Operation O From Host H on Resource R".

Principal

:   An authenticated user or group. For example, `"user: Fred"` or
    `"group: fraud"`.

Permission

:   Defines if the ACL allows (`ALLOW`) or denies (`DENY`) access to the
    resource.

Operation

:   The operation that is performed on the resource, for example `READ`.

Resource

:   A resource consists of a resource type and resource name:

    -   `RESOURCE_TYPE`, for example `TOPIC` or consumer `GROUP`.
    -   Resource name, where the name is either specific, for example
        `users`, or the wildcard `*`, meaning all resources of this
        type. The name is case-sensitive.

    The `CLUSTER` resource type does not require a resource name because
    it refers to the entire Kafka cluster.

An example ACL might `ALLOW` `user Jane` to `READ` the `TOPIC` named
`users`.

Here, user Jane would be allowed to read from the topic *users* only.
Jane would not be allowed to read from similarly named topics such as
*user*, *users-europe*, *Users* etc.

The ACLs described below list a `RESOURCE_TYPE`, resource name, and
`OPERATION`. All ACLs described are `ALLOW` ACLs, where the principal is
the user the ksqlDB server has authenticated as, with the Apache Kafka
cluster, or an appropriate group that includes the authenticated ksqlDB
user.

!!! tip
	For more information about ACLs, see
    [Authorization using ACLs](https://docs.confluent.io/current/kafka/authorization.html).

#### Interactive ksqlDB clusters pre Kafka 2.0

[Interactive ksqlDB clusters](/operate-and-deploy/how-it-works#interactive-deployment),
(which is the default configuration), require that the authenticated
ksqlDB user has open access to create, read, write, delete topics, and use
any consumer group:

Interactive ksqlDB clusters require these ACLs:

-   The `DESCRIBE_CONFIGS` operation on the `CLUSTER` resource type.
-   The `CREATE` operation on the `CLUSTER` resource type.
-   The `DESCRIBE`, `READ`, `WRITE` and `DELETE` operations on all
    `TOPIC` resource types.
-   The `DESCRIBE` and `READ` operations on all `GROUP` resource types.

It's still possible to restrict the authenticated ksqlDB user from
accessing specific resources using `DENY` ACLs. For example, you can add
a `DENY` ACL to stop SQL queries from accessing a topic that contains
sensitive data.

For example, given the following setup:

-   A 3-node ksqlDB cluster with ksqlDB servers running on IPs 198.51.100.0,
    198.51.100.1, 198.51.100.2
-   Authenticating with the Kafka cluster as a 'KSQL1' user.

Then the following commands would create the necessary ACLs in the Kafka
cluster to allow ksqlDB to operate:

```bash
# Allow ksqlDB to discover the cluster and create topics:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --operation Create --cluster

# Allow ksqlDB access to topics and consumer groups:
bin/kafka-acls --bootstrap-server=localhost:9092 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic '*' --group '*'
```

#### Non-Interactive (headless) ksqlDB clusters pre Kafka 2.0

Because the list of queries are known ahead of time, you can run
[Non-interactive ksqlDB clusters](index.md#non-interactive-headless-ksqldb-usage)
with more restrictive ACLs. Determining the list of ACLs currently
requires a bit of effort.

Standard ACLs

:   The authenticated ksqlDB user always requires:

    -   `DESCRIBE_CONFIGS` permission on the `CLUSTER` resource type.

Input topics

:   An input topic is one that has been imported into ksqlDB using a
    `CREATE STREAM` or `CREATE TABLE` statement. The topic should
    already exist when ksqlDB is started.

    The authenticated ksqlDB user requires `DESCRIBE` and `READ`
    permissions for each input topic.

Output topics

:   ksqlDB creates output topics when you run persistent
    `CREATE STREAM AS SELECT` or `CREATE TABLE AS SELECT` queries.

    The authenticated ksqlDB user requires `DESCRIBE` and `WRITE`
    permissions on each output topic.

    By default, ksqlDB will attempt to create any output topics that do
    not exist. To allow this, the authenticated ksqlDB user requires
    `CREATE` permissions on the `CLUSTER` resource type. Alternatively,
    topics can be created manually before running ksqlDB. To determine the
    list of output topics and their required configuration, like partition
    count, replication factor, and retention policy, you can
    initially run ksqlDB on a Kafka cluster with none or open ACLs first.

Change-log and repartition topics

:   Internally, ksqlDB uses repartition and changelog topics for selected
    operations. ksqlDB requires repartition topics when using either
    `PARTITION BY`, or using `GROUP BY` on non-key values, and requires
    changelog topics for any `CREATE TABLE x AS` statements.

    The authenticated ksqlDB user requires `DESCRIBE`, `READ`, and `WRITE`
    permissions for each changelog and repartition `TOPIC`.

    By default, ksqlDB will attempt to create any repartition or changelog
    topics that do not exist. To allow this, the authenticated ksqlDB user
    requires `CREATE` permissions on the `CLUSTER` resource type.
    Alternatively, you can create topics manually before running ksqlDB.
    To determine the list of output topics and their required
    configuration, (partition count, replication factor, retention
    policy, etc), you can initially run ksqlDB on a Kafka cluster with
    none or open ACLs first.

    All changelog and repartition topics are prefixed with
    `_confluent-ksql-<ksql.service.id>` where `ksql.service.id` defaults
    to `default_`, (for more information, see
    [ksql.service.id](/reference/server-configuration#ksqlserviceid)), and postfixed with
    either `-changelog` or `-repartition`, respectively.

Consumer groups

:   ksqlDB uses Kafka consumer groups when consuming input, change-log and
    repartition topics. The set of consumer groups that ksqlDB requires
    depends on the queries that are being executed.

    The authenticated ksqlDB user requires `DESCRIBE` and `READ`
    permissions for each consumer `GROUP`.

    The easiest way to determine the list of consumer groups is to
    initially run the queries on a Kafka cluster with none or open ACLS
    and then list the groups created. For more information about how to
    list groups, see [Managing Consumer
    Groups](http://kafka.apache.org/documentation.html#basic_ops_consumer_group).

    Consumer group names are formatted like
    `_confluent-ksql-<value of ksql.service.id property>_query_<query id>`,
    where the default of `ksql.service.id` is `default_`.

!!! tip
	For more information about interactive and non-interactive queries, see
    [Non-interactive (Headless) ksqlDB Usage](index.md#non-interactive-headless-ksqldb-usage).

Next Steps
----------

- See the blog post [Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL](https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/)
- Try out the [Kafka Event Streaming Application](https://docs.confluent.io/current/tutorials/cp-demo/docs/index.html) tutorial.
