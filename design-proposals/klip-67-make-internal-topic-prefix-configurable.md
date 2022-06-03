# KLIP 67 - Make internal topic prefix configurable

**Author**: Haruki Okada (ocadaruma) | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**:

**tl;dr:** In a multi-tenant Kafka cluster, topic creation/deletion is often restricted to allow only for topics which belongs to the specific tenant by prefixed-ACL.
Currently, reserved internal topic's prefix is hardcoded value `_confluent-`. However, this requires another ACL for `_confluent-ksql-{service.id}`, which is sometimes
prohibited by company's policy. We should make internal topic's prefix configurable to meet the requirements.

## Motivation and background

Currently, reserved internal topic's prefix is hardcoded value `_confluent-`.

However, this would be a problem in multi-tenant Kafka cluster.

Multi-tenancy in Kafka is often implemented as like below:
- Cluster-admin allocates a prefix for each tenant
- Cluster-admin sets prefixed-ACL for a tenant to prevent an application affects other tenant's applications (regardless of intentionally or accidentally)

Since KSQL's internal topics are prefixed with `_confluent-`, they can't be recognized as the tenant's topic.

One possible option we can take is to ask Cluster-admin to accept `_confluent-ksql-{service.id}` prefix, but it's not always possible.

refs: https://github.com/confluentinc/ksql/issues/4954#issuecomment-1087544882  

## What is in scope

All internal topic's prefix configurable.

That is,

- command_topic
- configs

## What is not in scope

N/A

## Value/Return

After this KLIP has been implemented, we can expect further growth of KSQL usage because currently there are users who hesitate to use KSQL (e.g #4954) due to the lack of this feature.

## Public APIS

Add new `ksql.internal.topic.name.prefix` config in `KsqlConfig`.

## Design

`ksql.internal.topic.name.prefix` will be used as the prefix for all internal topics. (`command_topic` and `configs` for now)

## Test plan

N/A

## LOEs and Delivery Milestones

This should be able to deliver in single milestone.

## Documentation Updates

`docs/reference/server-configuration.md` need to be updated.

## Compatibility Implications

When `ksql.internal.topic.name.prefix` is not supplied, current `_confluent-ksql-` will be used as the default prefix.

Any breaking change will not be introduced.

## Security Implications

N/A
