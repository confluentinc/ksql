# Roadmap

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  | Roadmap | [Demo](/ksql-clickstream-demo/) |
|---|----|-----|----|----|----|----|----|

* Improved quality, stability, and operations: We will continue to focus on refining the overall experience of working with KSQL. As such this will involve streamlining processes, further expand our testing efforts and improving visibility and feedback mechanisms.


* Support for ‘point-in-time’ SELECT. This feature will introduce the ability to retrieve a snapshot of the current state of a Table. In doing so making it possible to retrieve data snapshots at various times throughout the day. This would support the need for audits, adhoc requests, reference tables and other use-cases.


* Support INSERT into STREAM. Inserting data directly into Streams will continue to lowering the barrier to entry and complete the data-in, data-out semantics.


* Additional aggregate functions. We will continue to expand the set of analytics functions, if there are any you would like to see - pls send it to the mailing list, raise a Jira or create a PR.


* Testing tools. Many data-platforms suffer from an inherent inability to test. With KSQL testing capability a primary focus and we will provide frameworks to support continuous integration and unit test.


* Scale stream processing via Cluster mode and containers. We will release a set of docker images & configuration that is targeted at running KSQL as a cluster using Docker Compose. 
