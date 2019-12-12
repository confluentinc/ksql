# KLIP 20 - Remove 'TERMINATE' statements

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: TBD

**tl;dr:** 
Requiring users to `TERMINATE` a persistent query before dropping a source causes friction in the 
user experience. The id of the query(s) that need terminating are not determinist or accessible 
through SQL. We propose dropping the source should automatically drop persistent query(s) that write
to it.

*NB*: is most likely dependent on [KLIP-17](klip-17-sql-union.md).

## Motivation and background

See https://github.com/confluentinc/ksql/issues/3967 and the issues it links to for more background.

In short, dropping streams and tables created using `CSAS` or `CTAS` is currently painful, and 
practically impossible from a script. 

If done manually, the user must run `DESCRIBE <source>` to get the list of queries writing to the 
source and then stop each of these in turn, via a `TERMINATE` statement, before finally being able 
to drop the stream or table. 

From a script, it is not possible to determine the id of the queries that need to be stopped, and 
those query ids will change from one run to another in most cases. Hence it is generally not 
possible to drop a table or stream in a script.

The problem is compounded by the fact that _mutiple_ persistent queries can be writing into the
source or table.  However, [KLIP-17](klip-17-sql-union.md) proposes to correct this, so that there
will be, at-most, a single persistent query writing to any table or stream.

There also seems to be no clear benefit of being able to terminate a query without dropping the 
associated stream or table. Especially, given there is no means to restart it.

Consider that `CSAS` and `CTAS` statements are equivalent to `MATERIALIZED VIEW`s used in RDBSs, 
(as proposed in [KLIP-19](klip-19-materialize-views.md)). In a traditional RDBS the underlying 
process by which the materialized view is kept up to data is not exposed to the user.  There is
no 'persistent query' to either list or terminate. Hence we propose we should remove the 
problematic `TERMINATE` from our syntax and instead allow users to drop the view, even if there 
are queries writing to it. If such queries exist they will simply be stop as part of the `DROP`.   

## What is in scope

* Removing `TERMINATE` from our SQL syntax.
* Allowing streams and tables to be dropped while there are active queries writing to them.
* Automatically stopping any persistent query that write to a stream or table when that stream or table is dropped.

## What is not in scope

* Allowing streams and tables to be dropped while they are used as a source to downstream views. 
This check will remain, as it is needed to ensure downstream views don't break. 
In the future, we may support a `DROP TABLE CASCADE` sytnax to help drop a hierarchy of MVs.
* Removal of `show queries` and displaying query ids in `DESCRIBE`. Until such time that we can automatically 
recover failed persistent queries we should continue to expose query state to users.
In time, with automated recovery, it may be possible to completely hide this complexity from users.

## Value/Return

Remove `TERMINATE` will make KSQL easier to operate, which will help drive adoption.

## Public APIS

* `TERMINATE` will be removed from our SQL syntax.
* `DROP STREAM|TABLE` will allow users to drop sources that still have active upstream queries writing to them.


## Design

This KLIP could potentially be done independently of [KLIP-17](klip-17-sql-union.md), which restores 
the (1 - 0..1) relationship between source and persistent query. However, this is likely more complex 
as there are potentially multiple queries writing to a source. This may make recovering from the 
command topic more challenging.

With [KLIP-17](klip-17-sql-union.md) complete there are _at-most_ a single persistent query updating each 
source. The engine could track this relationship. When the user attempts to drop the source the engine will 
check if the source is used in any downstream queries. If it is, then the drop will be denied, as it does today. 
The engine will also check for upstream queries writing to the source. If found, instead of failing, 
the query will be terminated.

Any `TERMINATE` messages found in the command topic will be ignored, (and a warning logged). This may 
result in queries running after a restart that were previously stopped. 

The CLI will be updated to detect `TERMINATE` statements and inform the user they are no longer supported,
and what to do instead, linking to more documentation. This can be done through a general pattern that can 
be used for other removed statement types.  

## Test plan

Suitable tests will be added to ensure sources created via `CTAS` and `CSAS` statements can be dropped. 

## Documentation Updates

KSQL documentation updates should simply be a case of removing any reference to `TERMINATE`.  Updating the wider
each system may be a little more involved. However, this is mitigated by the CLI enhancement to return a rich
error message if `TERMINATE` is used, rather than the default parser error. 

# Compatibility Implications

Removing `TERMINATE` from our syntax is a breaking change.

We could choose to keep `TERMINATE` in our syntax for a period of time and have both the CLI and server
print/log a message explaining the statement is not longer needed and will be ignored. However, this two
stage approach is more work and ROI of the additional work may be low.

The main compatibility implication is around existing `TERMINATE` commands in the command topic. 
It should be fine to log a warning and ignore such statements, (though we'll need special code to 
detect them as they won't parse).  It should be safe to ignore as any `DROP` statement that comes 
after will still succeed. Ignoring them should not result in a corrupted metastore. The worst that
can happen is a query that was previously terminated, but whose table was not dropped, would be
restarted on a restart of the server. This seems acceptable.

## Performance Implications

None.

## Security Implications

None.