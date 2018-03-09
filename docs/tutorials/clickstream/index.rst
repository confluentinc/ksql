.. _ksql_clickstream-overview:

Clickstream Analysis Tutorial
=============================

Clickstream analysis is the process of collecting, analyzing, and
reporting aggregate data about which pages a website visitor visits and
in what order. The path the visitor takes though a website is called the
clickstream.

This demo focuses on building real-time analytics of users to determine:

* General website analytics, such as hit count and visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The demo uses standard streaming functions (i.e., min, max, etc) and enrichment using child tables, stream-table join, and different
types of windowing functionality.

Get started now with these instructions:

.. toctree::
    :titlesonly:
    :maxdepth: 1

    docker-clickstream
    ksql-clickstream-demo

.. include:: ../../includes/ksql-includes.rst
    :start-line: 6
    :end-line: 15
