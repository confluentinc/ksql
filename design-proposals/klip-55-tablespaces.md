# KLIP 55 - Tablespaces

**Author**: @MichaelDrogalis | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: _to be added_

Tablespaces help you use knowledge of the usage pattern of a set of ksqlDB objects to optimize performance.

## Motivation and background

Real-world SQL programs make use of many kinds of objects. Sometimes,
a subset of those objects have a specific usage pattern. If you could tune for
that usage pattern on a granular level, ksqlDB could treat those objects more efficiently.

Tablespaces are a construct for defining groups of configuration that can be applied
to one or more database objects. They exist in
[Postgres](https://www.postgresql.org/docs/10/manage-ag-tablespaces.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/general-tablespaces.html),
[Oracle](https://docs.oracle.com/cd/A57673_01/DOC/server/doc/SCN73/ch4.htm), and so on.
In the context on ksqlDB, a tablespace would allow you to override default server
configuration for one or more objects.

This is particularly important given the impact on how ksqlDB's runtime is being rearchitected.
When multiple persistent queries begin to share a runtime, it will become more challenging to
vary configuration parameters across them. Tablespaces give you a logical boundary to hint to
ksqlDB where it must respect varying parameters.

Tablespaces don't enforce namespacing or scoping: two objects in different tablespaces
can be used with one another.

## What is in scope

- The grammar to support tablespace commands
- The behavior of those commands
- Backward compatibility

## What is not in scope

- How it's implemented

## Value/Return

- Improved program performance
- Isolation of functional parameters to a subset of objects

## Public APIs

**Using the default tablespace**

Creating an object works like normal—it's placed into a default tablespace so
that you never need to use this feature unless you want to.

```sql
CREATE stream object WITH (
    ...
);
```

The default tablespace is `ksql_default`.

**Creating a tablespace**

You can supply one or more key/value parameters. These parameters are server-level
configuration parameters that you want to group together.

```sql
CREATE TABLESPACE tablespace_name WITH (
    property_name = expression [,
    ...
    ]
);
```

By default, no objects are placed within a new tablespace.

- TODO: what happens if `tablespace_name` already exists?

**Adding a newly created object to a tablespace**

Explicitly define what tablespace a stream or table should reside in.

```sql
CREATE [STREAM|TABLE] obj WITH (
    'tablespace' = 'tablespace_name',
    ...
);
```

If `tablespace_name` doesn't exist, `obj`'s creation is rejected with a clear error message why.

**Seeing what tablespace an object is in**

```sql
DESCRIBE obj EXTENDED;
```

Output fragment:

```
Name                 : KSQL_PROCESSING_LOG
Type                 : STREAM
Timestamp field      : Not set - using <ROWTIME>
Tablespace           : tablespace_name
```

**Setting a default tablespace**

You can override the default tablespace so you don't
need to supply it to every proceeding object that gets created.

```sql
SET 'ksql.default.tablespace' = 'tablespace_name';
```

If `tablespace_name` doesn't exist, `obj`'s creation is rejected with a clear error message why.

**Seeing the current default tablespace**

```sql
SHOW PROPERTIES;
```

TODO: can we get more granular than that?

**Changing an existing object's tablespace**

```sql
ALTER <STREAM|TABLE> obj_name SET 'tablespace' = 'tablespace_name';
```

**Changing a tablespace's configuration**

Causes all objects in the tablespace to be immediately reconfigured
with the new parameters.

```sql
ALTER TABLESPACE tablespace_name SET 'key' = 'value';
```

TBD: should this support bulk updating parameters?

**Viewing a tablespace's configuration**

```sql
DESCRIBE tablespace_name [EXTENDED];
```

Output:

```sql
+-----------+--------+
| parameter | value  |
+-----------+--------+
|       k1  | v1     |
|       k2  | v2     |
|       k3  | v3     |
+-----------+--------+
```

**Dropping a tablespace**

`DROP` only succeeds if it has no objects in it. If it has objects, it lists
out which ones need to be dropped first.

```sql
DROP TABLESPACE [IF EXISTS] tablespace_name;
```

**Seeing all objects in a tablespace**

TBD.

**Precedence**

Parameters take the following precedence:

1. Query-level configuration
2. Tablespace configuration
3. Server configuration

## Design

TBD.

## Test plan

TBD.

## LOEs and Delivery Milestones

TBD.

## Documentation Updates

- Add a new how-to guide
- Update the language reference section with the new grammar

## Compatibility Implications

All existing objects get placed into the default tablespace.

## Security Implications

TBD.

## Open questions

- Should there be a reserved "system" tablespace? What would go in it?
- Other TODOs in the document.
