.. _install_ksql:

Installation and Configuration
------------------------------

KSQL is a component of |cp| and is automatically installed and running when you install |cp|. For |cp| installation,
see the :ref:`quickstart`.

Interoperability
    .. include:: ../../../includes/installation.rst
        :start-line: 84
        :end-line: 95


To start the KSQL CLI, enter this command:

.. code:: bash

    $ <path-to-confluent>/bin/ksql

After KSQL is started, your terminal should resemble this.

.. include:: ../../includes/ksql-includes.rst
    :start-line: 17
    :end-line: 38


For advanced installation and configuration, see the following topics.

.. toctree::
    :maxdepth: 1

    architecture
    installing
    server-config/index
    cli-config
    common-scenarios
    upgrading
    

