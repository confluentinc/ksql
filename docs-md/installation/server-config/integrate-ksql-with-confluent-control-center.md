---
layout: page
title: Integrate KSQL with Confluent Control Center
tagline: Set up KSQL Server to communicate with Confluent Control Center  
description: Settings for configuring KSQL Server to integrate with with Confluent Control Center
keywords: ksql, confguration, setup, install, control center
---


Integrate KSQL with {{ site.c3 }}
=================================

You can develop event streaming applications by using the KSQL user
interface provided by {{ site.c3 }}. In {{ site.c3short }}, you can
create {{ site.aktm }} topics and develop persistent queries in the
KSQL query editor. When you install {{ site.cp }}, KSQL Server is
integrated with {{ site.c3short }} by default, and you can configure
{{ site.c3short }} to interact with other KSQL Server instances that run
on separate hosts.

![Screenshot of the KSQL Create Stream interface in Confluent Control Center.](../../img/ksql-interface-create-stream.png)

Configuration Settings for KSQL and Control Center
==================================================

Set up the integration between KSQL and {{ site.c3short }} by assigning
properties in the KSQL Server and {{ site.c3short }} configuration
files.

-   By default, the KSQL Server configuration file is installed at
    `<path-to-confluent>/etc/ksql/ksql-server.properties`.
-   By default, the {{ site.c3short }} configuration file is installed
    at
    `<path-to-confluent>/etc/confluent-control-center/control-center.properties`.

Secure Communication with KSQL Server
=====================================

When you use Basic authentication in KSQL, {{ site.c3short }} allows
passing credentials as part of the KSQL Server URL in {{ site.c3short }}
configuration.

```
# KSQL cluster URL
confluent.controlcenter.ksql.<ksql-cluster-name>.url=http://<username>:<password>@localhost:8088
```

You can set up KSQL Server to communicate securely with other components
in {{ site.cp }}. For more information, see [Configure Security for KSQL](security.md).

Network Configuration for KSQL and Control Center
=================================================

These are the configuration settings that you assign to set up network
connectivity between KSQL and {{ site.c3short }}.

-   In the KSQL Server configuration file, set the
    [listeners](config-reference.md#listeners) property to the IP address of the REST
    API endpoint for KSQL Server. Typical values are
    `http://0.0.0.0:8088` and `http://localhost:8088`.
-   In the {{ site.c3short }} configuration file, set the
    `confluent.controlcenter.ksql.<ksql-cluster-name>.url` property to
    the hostnames and listener ports for the KSQL cluster specified by
    `<ksql-cluster-name>`. This setting specifies how {{ site.c3short }}
    communicates with KSQL Server for regular HTTP requests. For more
    information, see [Control Center Configuration
    Reference](https://docs.confluent.io/current/control-center/installation/configuration.html#ksql-settings).
-   If KSQL Server communicates over an internal DNS that is not
    externally resolvable or routeable, set the
    `confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url`
    property in the {{ site.c3short }} configuration file. This setting
    specifies how the browser communicates with KSQL Server for
    websocket requests. For more information, see [Control Center
    Configuration
    Reference](https://docs.confluent.io/current/control-center/installation/configuration.html#ksql-settings).

When KSQL Server and {{ site.c3 }} run on the same host, you can use the
default configuration defined by {{ site.cp }} setup.

When KSQL and {{ site.c3short }} run on different hosts
--------------------------------------------------------

If KSQL Server and {{ site.c3short }} run on different hosts, you must
specify a configuration that ensures KSQL Server and {{ site.c3short }}
can communicate. This is necessary when KSQL Server and {{ site.c3short }}
are deployed in the following situations:

-   KSQL Server and {{ site.c3short }} run in separate containers.
-   They run in separate virtual machines.
-   They communicate over a virtual private network (VPN).
-   The KSQL Server host publishes a public URL that\'s different from
    the private URL for KSQL Server.

!!! note
	When KSQL and {{ site.c3short }} communicate over a virtual private
    network (VPN), {{ site.c3short }} proxies your queries, but query
    results stream directly from KSQL Server back to your browser without
    going through {{ site.c3short }}. Over a VPN, the advertised URL isn't
    `localhost`. Instead, it's the hostname of the remote server.

Assign the following configuration properties to integrate KSQL Server
with {{ site.c3short }} when they run on separate hosts.

KSQL Server Configuration
=========================

In the KSQL Server configuration file, set `listeners` to bind to all
interfaces:

```
listeners=http://0.0.0.0:8088
```

Control Center Configuration
============================

In the {{ site.c3short }} configuration file, set
`confluent.controlcenter.ksql.<ksql-cluster-name>.url` to a list of URLs
for the KSQL Server hosts, which must be reachable from the host that
{{site.c3short }} is installed on. Replace `<ksql-cluster-name>` with the
name that {{ site.c3short }} uses to identify the KSQL cluster.

```
confluent.controlcenter.ksql.<ksql-cluster-name>.url=<internally-resolvable-hostname1>, <internally-resolvable-hostname2>, ...
```

Also, set
`confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url` to the
public IP addresses published by the KSQL Server hosts, which must be a
list of URLs that the browser can resolve through externally available
DNS.

```
confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url=<externally-resolvable-hostname1>, <externally-resolvable-hostname2>, ...
```

The {{ site.c3short }} configuration must match the KSQL Server
`listeners` values.

Use the `curl` command to check whether these URLs are reachable.
Depending on your deployment, you may need to check from two different
hosts:

-   Check from the host where {{ site.c3short }} is running, which is
    relevant for the
    `confluent.controlcenter.ksql.<ksql-cluster-name>.url` setting.
-   Check from the host where the browser is running, which is relevant
    for the
    `confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url`
    setting.

On both hosts, run the following command to confirm that the KSQL Server
cluster is reachable. The `hostname` value is one of the hosts in the
listed in the `confluent.controlcenter.ksql.<ksql-cluster-name>.url` and
`confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url`
configuration settings.

```bash
curl http://<hostname>:8088/info \
{"KsqlServerInfo":{"version":"{{ site.release }}","kafkaClusterId":"<ksql-cluster-name>","ksqlServiceId":"default_"}}%
```

!!! note
	You must specify the ports in the KSQL URL settings. For example, if the
    public URL is
    `http://ksql-server-677739697.us-east-1.elb.amazonaws.com:80`, be sure
    to include port `80`, or the {{ site.c3short }} connection to KSQL
    Server will fail.

Check Network Connectivity Between KSQL and Control Center
==========================================================

Use a web browser to check the configuration of an advertised URL. Make
sure that your browser can reach the `info` endpoint at
`http://<ksql.advertised.url>/info`. If the configuration is wrong, and
the browser can\'t resolve the URL of the KSQL Server host, you'll
receive an error:
`Websocket error when communicating with <ksql.advertised.url>`.

Check KSQL Server Network Binding
=================================

If {{ site.c3short }} doesn't connect with your KSQL Server instance,
check the network binding on the KSQL Server host:

```bash
sudo netstat -plnt|grep $(ps -ef|grep KsqlServerMain|grep -v grep|awk '')
```

If your KSQL server is bound to `localhost` only, your output should
resemble:

```bash
tcp6 0 0 127.0.0.1:8088 :::* LISTEN 64383/java
tcp6 0 0 :::34791 :::* LISTEN 64383/java
```

If `0.0.0.0` isn't listed, KSQL Server isn't accepting external
connections. In the `ksql-server.properties` file, set
`listeners=http://0.0.0.0:8088` and restart KSQL Server.

Next Steps
----------

-   [Connecting KSQL to Confluent
    Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html)
-   [Configure Security for KSQL](security.md)
