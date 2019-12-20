# KLIP 19 - Introduce Materialize Views

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: TBD

**tl;dr:** 
KSQL currently allows users to insert into any Kafka topic. In the case of table data this can be semantically incorrect,
leading to bugs and poor UX. This limitation can force organisations to rely on Kafka ACLs to limit such actions. 
However, ACLs should be used for access control, not to enforce correct semantics. Introducing Materialized Views will 
clean up the semantics around `INSERT VALUES` into tables and allow users to express intended mutability of data sources.    

## Motivation and background

### Why might it be bad to allow inserts into tables?

Let's consider two examples:
1. A table in an upstream db is being replicated to a topic in Kafka, e.g. via Connect, and users want this 
   table available in KSQL.
1. A user wants to create a table in KSQL that is derived from other source(s), as currently done using a 
   `CREATE TABLE AS SELECT` statement.
   
In the first example, the _source of truth_ for the data set is the upstream db, not in Kafka. Any change to the data 
needs to be done in the upstream db. Simply writing a new row to the Kafka topic is semantically incorrect. The row will
be overwritten if/when the same row in the database is next updated. What's more, that same topic may be used by other 
systems that expect it to be an exact copy of the data in the database table. 

In the second example, the rows written to the sink topic are a _derived dataset_.  Simply writing a new row to the Kafka 
topic is also semantically incorrect. Any row written would potentially be overwritten by an upstream change.

### When is it OK to insert into a table?

We _can_ allow inserts into tables where KSQL is the only writer of that data and we can ensure correct table semantics.

At the moment, users can create a new table in KSQL using a `CREATE TABLE` statement with `PARTITIONS` and, optionally,
`REPLICAS`. This will create a new topic. As no other system writes to this topic, KSQL can support `INSERT`s into it.
Likewise, a `CREATE TABLE` statement can be used to import an existing topic into KSQL. As long as no other system is 
writing to that topic, (and that includes another KSQL topology), KSQL can offer correct `INSERT` semantics.

The defining factor here is that it is only possible to have correct `INSERT` semantics _if no other system updates the table_.

**Side note**: It would be easy to support correct `DELETE` semantics too. However, correct `UPDATE` row semantics will
require more work. Likely, it will involve proxying calls to the KSQL node that owns the key, ensuring a single writer
pattern.

### How can we express this difference in SQL?   

To ensure correct table semantics KSQL needs to differentiate between sources where it is the sole writer and those updated
by other systems. Tables where KSQL is the only writer are _source datasets_. Topic containing data updated by another system, 
including another KSQL query, should be treated as _read-only_ as KSQL can not offer correct table semantics on `INSERT`s.

SQL already has concepts to differentiate: 
 * a `TABLE` is a source dataset. The data in the table can be mutated via `INSERT`/`UPDATE`/`DELETE`s.
 * a `VIEW` is a derived and read-only dataset. It's state is _derived_ from one or more sources. 
  `INSERT`/`UPDATE`/`DELETE`s are not supported. 
 * A `MATERIALIZED VIEW` is a `VIEW` where the cost of calculating the derived dataset is done on the write path, rather 
   than the read.

We propose introducing a `CREATE MATERIALIZED VIEW` statement to allow users to import table datasets which KSQL should
treat as read-only.  

The existing `CREATE TABLE` statement will be repurposed for defining new tables or importing an existing topic as a table. 
Note: if `CREATE TABLE` is used to import a topic which is updated by another system then the behaviour is _undefined_, 
i.e. table semantics can not be guaranteed.
   
KSQL currently _materializes_ the results of `CREATE TABLE AS SELECT` statements into the sink topic. 
Hence we propose renaming `CTAS` to `CREATE MATERIALIZED VIEW AS SELECT` to make it clear this is a _derived_ dataset,
and can not be updated directly.

The existing `CREATE TABLE AS SELECT` statement will be removed from KSQL syntax and reserved for future use.
If/when reintroduced it may create a table snapshot, i.e. a `TABLE`, not a `VIEW`, that is initialized from the 
supplied query, i.e. a one-time initialization, not a continuous query/update. Though such functionality may 
be better expressed using SQL `INSERT INTO` syntax. Alternatively, `CTAS` could be reintroduced as a continuously 
updating table if we were to also offer correct `INSERT`, and later `UPDATE` and `DELETE` semantics, which may be possible.

In case you spotted the edge-case, let's discuss it: what about the case where the data in Kafka is the _source dataset_, 
but its managed by another system? As we know, KSQL can not offer correct `INSERT` table semantics, but it's still a 
_source_, not a _derived dataset_, right? So how does this fit in the `TABLE` vs `VIEW` world? Well, KSQL may be reading 
from the _source dataset_, but its essentially making a copy when it reads the data. That copy is a `VIEW`. Hence, users
should import such a data set using `CREATE MATERIALIZED VIEW`.

### What about streams? 

Streams don't have the complexities of per-key update semantics that tables have. So, it's OK to insert into them, right?

Well, streams are certainly not as clear cut as tables. But maybe there's a case for differentiating between a stream and
a read-only stream view. 

Consider a stream coming from some upstream system, e.g a click stream from a web-tier. Should a KSQL user be able to 
add rows to such a stream? Probably not, but the true answer is most likely 'it depends'. It depends on how the organisation
if using KSQL and modeling data in Kafka. Its possible the clickstream should be seen as read-only to KSQL users as its 
owned by the web tier that produces it, and its maybe used by other systems too. On the flip side, its also possible that
appending new rows to the stream is something they want to allow in KSQL.

It can be argued that using ACLs are a good fit for controlling if a KSQL user can insert into a stream. There's nothing
semantically wrong with allow the insert: a stream is just an append-only list of immutable 'facts'. Appending one more 
fact does not change the previous or following facts. However, supporting read-only stream views may provide an easier 
mental model for users to work with. If a user does a `CREATE MATERIALIZED STREAM VIEW AS SELECT` rather than  
`CREATE STREAM AS SELECT` then it will be clear KSQL does not allow `INSERT`s into the stream.

As a discussion point, and as a mirror of the proposed `MATERIALIZED VIEW` changes above, we propose introducing a 
`CREATE MATERIALIZED STREAM VIEW` statement to allow users to import stream datasets as read-only.

The existing `CREATE STREAM` statement will be for defining new streams or importing an existing topic as a stream, not a view.  

We propose renaming `CSAS` to `CREATE MATERIALIZED STREAM VIEW AS SELECT` to make it clear this is a _derived_ and read-only 
dataset.

The existing `CREATE STREAM AS SELECT` will be removed from KSQL syntax and reserved for future use, or potentially maintained 
with current functionality, i.e. to create a dervied stream that also allows `INSERT`s.

## What is in scope

* Enhancements to SQL syntax to differentiate a `TABLE` from a read-only `MATERIALIZED VIEW`.
* Enhancements to SQL syntax to differentiate a `STREAM` from a read-only `MATERIALIZED STREAM VIEW`.
* `INSERT VALUES` semantics fixed to only allow inserts into mutable `TABLE`s and `STREAM`s, not `VIEW`s.
* CLI or server enhancements to return useful error message on any statements that are no longer supported. 

TBD:
* Maintain or drop `CREATE STREAM AS SELECT` functionality?
 
## What is not in scope

* No `VIEW` functionality, only `MATERIALIZED VIEW`s. `VIEW`s have the potential of being useful, as they would allow
  user to define multiple statements that can later be materialized in a single KS topology. But such work is outside the 
  scope of this KLIP.
* New `CREATE TABLE AS SELECT` functionality. This can be done later.

## Value/Return

Differentiating between mutable and read-only data sources will make KSQL easier for both engineers and end-users
to reason about, the SQL more explicit and is needed to to fix the inconsistencies in `INSERT VALUES` functionality.  
  
## Public APIS

We propose the following SQL syntax and associated functionality:

### Tables:

* Add `CREATE MATERIALIZED VIEW` to import an existing topic containing a table. 
    * `INSERT`s will *not* be allowed into the view, as views are read-only.
    * `PARTITIONS` and `REPLICAS` *not* supported in `WITH` clause.
* Re-purpose `CREATE TABLE` to create a mutable table backed by a new or existing topic.
    * `INSERT`s into the table will be supported.
    * `PARTITIONS` and `REPLICAS` supported in `WITH` clause to control customisation of any new topic. 
* Replace `CREATE TABLE AS SELECT` with `CREATE MATERIALIZED VIEW AS SELECT`.
    * `INSERT`s will *not* be allowed into the view, as views are read-only.
    * `PARTITIONS` and `REPLICAS` supported in `WITH` clause to control customisation of the sink topic.   
* Remove `CREATE TABLE AS SELECT`.
    
For those worried about the excessive typing required for `MATERIALIZED VIEW`, we could add a shorthand `CREATE MV` etc. 

### Streams:

Standard SQL already has `MATERIALIZED VIEW`, but has no stream equivalent. 
We propose a stream equivalent of `MATERIALIZED STREAM VIEW`.

* Add `CREATE MATERIALIZED STREAM VIEW` to import an existing topic containing a stream.
    * `INSERT`s will *not* be allowed into the view, as views are read-only.
    * `PARTITIONS` and `REPLICAS` *not* supported in `WITH` clause.
* Re-purpose `CREATE STREAM` to create a mutable stream backed by a new or existing topic.
    * `INSERT`s into the stream will be supported.
    * `PARTITIONS` and `REPLICAS` supported in `WITH` clause to control customisation of any new topic. 
* Replace `CREATE STREAM AS SELECT` with `CREATE MATERIALIZED STREAM VIEW AS SELECT`.
    * `INSERT`s will *not* be allowed into the view, as views are read-only.
    * `PARTITIONS` and `REPLICAS` supported in `WITH` clause to control customisation of the sink topic.   
* Either `CREATE STREAM AS SELECT` will be removed, or maintained as creating a mutable derived stream. 
    * `INSERT`s into the stream will be supported.
    * `PARTITIONS` and `REPLICAS` supported in `WITH` clause to control customisation of the sink topic.
    
For those worried about the excessive typing required for `MATERIALIZED STREAM VIEW`, we could add a shorthand `CREATE MSV` etc.

## Design

Design is mostly covered above by public API changes. Changes to KSQL to implement this KLIP should be straight forward.

All new terminology will be supported in interactive-mode. Headless mode will support all new statements, but will 
remove support for `CREATE TABLE` statements, as these are only of use when combined with `INSERT VALUES` statements, 
which, are not yet supported in headless mode.

The proposed new syntax for tables is Ansi SQL compliant, though obviously the `WITH` syntax part if not.
(This incompatibility could be fixed by defining defaults for formats and using source names for topics,
 but this is out of scope for this KLIP).

The new `VIEW` variants will support the same `WITH` properties that their non-`VIEW` variants do today, 
unless otherwise stated in the [Public APIS section](#public_apis) above. 

The CLI or Server will be updated to detect any removed statements and inform the user they are no longer supported,
and what to do instead, linking to more documentation. This can be done through a general pattern that can 
be used for other removed statement types.

## Test plan

Proposed changes in this KLIP are mostly just syntax changes, with the exception of fixing semantics for `INSERT VALUES`.
Hence, updating existing test cases will cover most of the work.  Test will be added to cover fixes to `INSERT VALUES`.

Not system / muckrake tests will be added.

## Documentation Updates

Existing documentation will be overhauled to take new syntax into account, with special care taken to call out:
 * `VIEW`s being readonly
 * The undefined behaviour if `CREATE TABLE` is used with a topic that is updated by another system
 * Proper table update semantics for `INSERT VALUES` for non-`VIEW` sources.

Existing demos will need updating. 

Updating existing blogs will be left up to the blog author. 

# Compatibility Implications

The KLIP proposes removing support for `CREATE TABLE AS SELECT`, and potentially `CREATE STREAM AS SELECT`.
This is a breaking change and will need to be communicated in release notes and mitigated by the proposed
enhancements to display a useful error message should a user issue them. 

## Performance Implications

None.

## Security Implications

None.
