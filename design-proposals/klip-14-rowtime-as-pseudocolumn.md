# KLIP 14 - ROWTIME as Pseudocolumn

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: TBD

**tl;dr:**
_`ROWTIME` is currently part of a source's schema. `SELECT *` style queries against a source include `ROWTIME`
as a column. As the number of such meta-columns in due to increase, we propose removing `ROWTIME` from a source's
schema and instead having it as a pseudocolumn, akin to Oracle's `ROWNUM`.  

WIP