.. _ksql_tutorials:

KSQL Tutorials and Examples
===========================

.. toctree::
    :hidden:

    basics-docker
    basics-local
    clickstream-docker
    examples

KSQL Basics
    This tutorial demonstrates a simple workflow using KSQL to write streaming queries against messages in Kafka.

    Get started with these instructions:

    - :ref:`ksql_quickstart-docker`
    - :ref:`ksql_quickstart-local`

Clickstream Data Analysis Pipeline
    Clickstream analysis is the process of collecting, analyzing, and
    reporting aggregate data about which pages a website visitor visits and
    in what order. The path the visitor takes though a website is called the
    clickstream.

    This tutorial focuses on building real-time analytics of users to determine:

    * General website analytics, such as hit count and visitors
    * Bandwidth use
    * Mapping user-IP addresses to actual users and their location
    * Detection of high-bandwidth user sessions
    * Error-code occurrence and enrichment
    * Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

    The tutorial uses standard streaming functions (i.e., min, max, etc) and enrichment using child tables, stream-table join, and different
    types of windowing functionality.

    Get started now with these instructions:
    
    - :ref:`ksql_clickstream-docker`

KSQL Examples
    :ref:`These examples <ksql_examples>` provide common KSQL usage operations.






