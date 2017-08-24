# Clickstream Analysis

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  | [Roadmap](/docs/roadmap.md#roadmap) | Demo |
|---|----|-----|----|----|----|----|----|

### Introduction
Clickstream analysis is the process of collecting, analyzing and reporting aggregate data about which pages a website visitor visits -- and in what order. The path the visitor takes though a website is called the clickstream.

This application focuses on building real-time analytics of users to determine:
* General website analytics, such as hit count & visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The application makes use of standard streaming functions (i.e. min, max, etc), as well as enrichment using child tables, table-stream joins and different types of windowing functionality.


- [Follow these instructions if you are using Docker](/ksql-clickstream-demo/docker-clickstream.md)
- [Follow these instructions if you are not using Docker](/ksql-clickstream-demo/non-docker-clickstream.md)

