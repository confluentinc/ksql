.. _install_ksql:

Installation and Configuration
------------------------------

KSQL is a component of |cp| and the KSQL binaries are located at `https://www.confluent.io/download/ <https://www.confluent.io/download/>`_
as a part of the |cp| bundle.

KSQL must have access to a running Kafka cluster, which can be on prem, |ccloud|, etc.

Interoperability
    KSQL code is open source `https://github.com/confluentinc/ksql <https://github.com/confluentinc/ksql>`_. 

    .. include:: ../../../includes/installation.rst
        :start-line: 84
        :end-line: 95

Docker support
    You can deploy KSQL in Docker, however the current release does not yet ship with ready-to-use KSQL Docker images for
    production. These images are coming soon.

.. toctree:: Contents
    :maxdepth: 1

    architecture
    installing
    server-config/index
    cli-config
    common-scenarios
    upgrading
    

