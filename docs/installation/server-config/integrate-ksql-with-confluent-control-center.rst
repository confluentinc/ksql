.. _integrate-ksql-with-confluent-control-center:

Integrate KSQL with |c3|
########################

You can develop event streaming applications by using the KSQL user interface
provided by |c3|. In |c3-short|, you can create |ak-tm| topics and develop
persistent queries in the KSQL query editor. When you install |cp|, KSQL Server
is integrated with |c3-short| by default, and you can configure |c3-short| to
interact with other KSQL Server instances that run on separate hosts.

.. image:: ../../../../images/ksql-interface-create-stream.png
     :width: 600px
     :align: center
     :alt: Screenshot of the KSQL Create Stream interface in Confluent Control Center.

Configuration Settings for KSQL and |c3|
****************************************

Set up the integration between KSQL and |c3| by assigning
properties in the KSQL Server and |c3-short| configuration files.

* By default, the KSQL Server configuration file is installed at
  ``<path-to-confluent>/etc/ksql/ksql-server.properties``.
* By default, the |c3-short| configuration file is installed at
  ``<path-to-confluent>/etc/confluent-control-center/control-center.properties``.

These are the configuration settings that you assign to set up network
connectivity between KSQL and |c3-short|.

* In the KSQL Server configuration file, set the :ref:`ksql-listeners` property
  to the IP address of the REST API endpoint for KSQL Server. Typical values
  are ``http://0.0.0.0:8088`` and ``http://localhost:8088``.
* In the |c3-short| configuration file, set the ``confluent.controlcenter.<name>.ksql.url``
  property to the hostnames and listener ports for the KSQL cluster specified by ``<name>``.
  This setting specifies how |c3-short| communicates with KSQL Server for regular HTTP
  requests. For more information, see :ref:`controlcenter_ksql_settings`.
* If KSQL Server communicates over an internal DNS that is not externally
  resolvable or routeable, set the ``confluent.controlcenter.ksql.<name>.advertised.url``
  property in the |c3-short| configuration file. This setting specifies how the
  browser communicates with KSQL Server for websocket requests. For more information,
  see :ref:`controlcenter_ksql_settings`.

When KSQL Server and |c3| run on the same host, you can use the default
configuration defined by |cp| setup.

When KSQL and |c3| run on different hosts
*****************************************

If KSQL Server and |c3-short| run on different hosts, you must specify a
configuration that ensures KSQL Server and |c3-short| can communicate. This
is necessary when KSQL Server and |c3-short| are deployed in the following
situations:

* KSQL Server and |c3-short| run in separate containers.
* They run in separate virtual machines.
* They communicate over a virtual private network (VPN).
* The KSQL Server host publishes a public URL that's different from the
  private URL for KSQL Server.

.. note::

   When KSQL and |c3| communicate over a virtual private network (VPN),
   |c3-short| proxies your queries, but query results stream directly
   from KSQL Server back to your browser, without going through
   |c3-short|. Over a VPN, the advertised URL isn't ``localhost``. Instead,
   it's the hostname of the remote server.

Assign the following configuration properties to integrate KSQL Server with
|c3-short| when they run on separate hosts.

KSQL Server Configuration
=========================

In the KSQL Server configuration file, set ``listeners`` to bind to all
interfaces:

::

    listeners=http://0.0.0.0:8088

|c3-short| Configuration
========================

In the |c3-short| configuration file, set ``confluent.controlcenter.<name>.ksql.url``
to a list of URLs for the KSQL Server hosts, which must be reachable from the host
that |c3-short| is installed on. Replace ``<name>`` with the name |c3-short| uses
to identify the KSQL cluster.

Also, set ``confluent.controlcenter.ksql.<name>.advertised.url`` to the public
IP addresses published by the KSQL Server hosts, which must be a list of URLs
that the browser can resolve through externally available DNS.

::

    confluent.controlcenter.<ksql-cluster-name>.ksql.url=<private-url1>, <private-url2>, ...
    confluent.controlcenter.ksql.<ksql-cluster-name>.advertised.url=<public-url1>, <public-url2>, ...

.. note::

   You must specify the ports in the KSQL URL settings. For example, if the
   public URL is ``http://ksql-server-677739697.us-east-1.elb.amazonaws.com:80``,
   be sure to include port ``80``, or the |c3-short| connection to KSQL Server
   will fail.

Check Network Connectivity Between KSQL and |c3|
================================================

Use a web browser to check the configuration of an advertised URL. Make sure
that your browser can reach the ``info`` endpoint at ``http://<ksql.advertised.url>/info``.
If the configuration is wrong, and the browser can't resolve the URL of the
KSQL Server host, you'll receive an error:
``Websocket error when communicating with <ksql.advertised.url>``.

Check KSQL Server Network Binding
=================================

If |c3| doesn't connect with your KSQL Server instance, check the network
binding on the KSQL Server host:

.. code:: bash

   sudo netstat -plnt|grep $(ps -ef|grep KsqlServerMain|grep -v grep|awk '')

If your KSQL server is bound to ``localhost`` only, your output should
resemble:

.. code:: bash

   tcp6 0 0 127.0.0.1:8088 :::* LISTEN 64383/java
   tcp6 0 0 :::34791 :::* LISTEN 64383/java

If ``0.0.0.0`` isn't listed, KSQL Server isn't accepting external
connections. In the ``ksql-server.properties`` file, set
``listeners=http://0.0.0.0:8088`` and restart KSQL Server.

Next Steps
**********

* :ref:`install_ksql-ccloud`