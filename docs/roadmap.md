# Roadmap

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  | Roadmap | 
|---|----|-----|----|----|----|----|----|

* Improved quality, stability, and operations: We will continue to focus on refining the overall experience of working with KSQL. As such this will involve streamlining processes, further expand our testing efforts and improving visibility and feedback mechanisms.


* Support for ‘point-in-time’ SELECT. This feature will introduce the ability to retrieve a snapshot of the current state of a Table. In doing so making it possible to retrieve data snapshots at various times throughout the day. This would support the need for audits, ad hoc requests, reference tables and other use-cases.


* Support INSERT into STREAM. Inserting data directly into Streams will continue to lower the barrier to entry and complete the data-in, data-out semantics.


* Additional aggregate functions. We will continue to expand the set of analytics functions. If you have suggestions that you would like to see, please send them to the mailing list or create a [GitHub issue](https://github.com/confluentinc/ksql/issues).


* Testing tools. Many data-platforms suffer from an inherent inability to test. With KSQL testing capability is a primary focus and we will provide frameworks to support continuous integration and unit test.


* Scale stream processing via cluster mode and containers. We will release a set of Docker images and configuration that is targeted at running KSQL as a cluster using Docker Compose. 

