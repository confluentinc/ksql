# Clickstream Analysis

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | Demo | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|

Clickstream analysis is the process of collecting, analyzing, and reporting aggregate data about which pages a website visitor visits and in what order. The path the visitor takes though a website is called the clickstream.

This demo focuses on building real-time analytics of users to determine:
* General website analytics, such as hit count & visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The demo uses standard streaming functions (i.e., min, max, etc), as well as enrichment using child tables, table-stream joins and different types of windowing functionality.

Get started now with these instructions:

- [Clickstream Analysis using Docker](/ksql-clickstream-demo/docker-clickstream.md#clickstream-analysis-using-docker)
- [Clickstream Analysis not using Docker](/ksql-clickstream-demo/non-docker-clickstream.md#clickstream-analysis)


Click [here](https://youtu.be/A45uRzJiv7I) to watch a screencast of the KSQL demo on YouTube.
<a href="https://youtu.be/A45uRzJiv7I" target="_blank"><img src="../screencast.jpg" alt="KSQL screencast"></a></p>
