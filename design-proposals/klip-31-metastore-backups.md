# KLIP 31 - Metastore Backups

**Author**: Sergio Peña (@spena) |
**Release Target**: 0.11.0; 6.0.0 |
**Status**: _Merged_ |
**Discussion**: https://github.com/confluentinc/ksql/pull/5741

**tl;dr:** _KSQL should keep a local backup of the metastore (or command_topic) to allow users recover from an accidental
metastore disaster._

## Motivation and background

There are users that may cause KSQL metastore data loss accidentally. A recent use case where a user set the retention.ms to 0 on the KSQL command_topic caused a severe problem because all the metadata
of streams, tables, etc. were lost without any way to recover them. Users should have a plan for Disaster and Recovery in such situations, like configuring backups in Kafka. However, not all KSQL users
have this plan in place. To mitigate the risk of these unrecoverable scenarios, we need KSQL to keep a backup of the metastore (or command_topic) somewhere so it can be recovered in case of an accident.

This KLIP proposes a simple solution to automatically backup the metastore by replaying the command_topic onto a file located in the same KSQL node. Users can use this file to restore their command_topic
if necessary.

Note: The proposal does not take into account any future changes related to [KLIP-18: Metastore Database](https://github.com/confluentinc/ksql/pull/4659). If KLIP-18 is implemented, then this backup
proposal will not work.

## What is in scope

* Automatically create KSQL metastore backups
* Document steps to manually restore the KSQL command_topic

## What is not in scope

* `Managing automatic restores`. This is complicated due to unknown reasons of the metastore clean-up and distributed systems challenges. KSQL does not know if the command_topic was cleaned
  intentionally or by accident. Also, restoring automatically while KSQL is running may cause conflicts with other nodes in the KSQL cluster.
* `Remote backup locations`. Users may want to store their backups in a remote location, but this comes with issues when a cluster is running with multiple nodes. All nodes might be writing
to the same remote location causing corruption in the backup files. To keep things simple, we better remove this support.

## Value/Return

If the KSQL metastore (or command_topic) is accidentally deleted, then the users will be able to recover the metastore from a backup.

## Public APIS

Two new configurations are required:

* `ksql.enable.metastore.backup` as Boolean to enable backups (default: False).
* `ksql.metastore.backup.location` as String to specify a directory location for backups (default: an internal KSQL directory)

## Design

When backups are enabled, KSQL will start replaying the command_topic messages to a local file in the specified backup location. Every new command added to the
command_topic while KSQL is running will be appended to the local file to keep a live metastore backup.

The `ksql.metastore.enable.backup` config will be used to enable backups. It is better to let users enable/disable backups with this config than disabling by setting an empty backup location.
Otherwise, a user setting the location empty just to disable a backup might forgot what the location was when enabling it back.

The `ksql.metastore.backup.location` will specify the directory where the backup files exist. Only a directory in the local file system is accepted (no remote locations). This is to avoid multiple
nodes in the KSQL cluster writing to the same remote location. The location is a directory where KSQL will write one or more files.

The backup file name will be formed using the command_topic name and the date of the initial backup (i.e. _confluent-ksql-default__command_topic_1593540976.bak).

The messages will be serialized using the command_topic JSON serializer. The backup file content will look pretty similar to what is seen in the command_topic messages. It will have one
command per line. Each line has the key (CommandId) and the value (Command) separated by colon.

i.e. (backup: _confluent-ksql-default__command_topic_1593545289.bak)
```
"stream/`TEST1`/create":{"statement":"CREATE STREAM TEST1(ID INT) WITH(KAFKA_TOPIC='test1', VALUE_FORMAT='JSON');","streamsProperties":{}, ... }
"stream/`TEST2`/create":{"statement":"CREATE STREAM TEST2(ID INT) WITH(KAFKA_TOPIC='test2', VALUE_FORMAT='JSON');","streamsProperties":{}, ... }
"stream/`TEST1`/drop":{"statement":"DROP STREAM TEST1;","streamsProperties":{}, ... }
```

This format will allow users to easily restore the command_topic using the Kafka producer scripts.

i.e.
```
$ kafka-console-producer --broker-list localhost:9092 --topic _confluent-ksql-default__command_topic \
    --property "parse.key=true" --property "key.separator=:" < _confluent-ksql-default__command_topic_1593545289.bak
```

### Workflow

When KSQL starts, it always reads the command topic from the beginning. If the backup file does not exist yet, then it will create a new file and start appending each command_topic message to it.
A backup file is not found if it is the first time backups are enabled, or if the KSQL service.id is different from the ones found in the backup filenames.

If a backup file with the same KSQL service.id exists, then KSQL will append only the new command_topic messages to that file.

KSQL will compare each consumed command_topic message against the next line from the backup file. If both messages are different, then it considers the command_topic as a new restored
topic, so KSQL will create a new backup file, copy all read messages to the file, and continue appending the new messages to it. If there are no more messages read from the backup file (EOF),
then KSQL considers this file is the current one and will continue appending messages to it.

KSQL will not use offsets to compare against the backup file. It will assume each offset starts from 1, beginning consuming messages from the topic and reading messages from the file.

### Performance

The KSQL metastore is not expect to grow into a big file. Appending each new command to a file is faster. KSQL command messages are not large to cause slowness during backup file writing.
It will certainly add extra time during KSQL restarts, but considering the average I/O latency of 10ms per write, then 500 commands would take around 5s to create the initial backup. Also, using
the same example of 500 commands and an average size of 4k per command (using the ksql_processing_log stream command size as example), then this backup file would grow to 2Mb.

## Test plan

- Unit tests
- Integration tests to verify backups and restore steps work

## LOEs and Delivery Milestones

It is a small feature that may take a week or less.

## Documentation Updates

Documentation about how to enable backups and restore a command topic manually will be written.

## Compatibility Implications

This is a new feature and does not affect any other feature of KSQL or CP components.

## Security Implications

Backups will be out of the Kafka environment. If encryption is configured in Kafka, then the KSQL metastore will not be encrypted in the local filesystem.
The user would need to provide an environment for file-level or disk-level encryption to comply with the security requirements of their organization.

In case of ACLs are configured in Kafka, then the file will be exposed to not-authorized users who have access to the KSQL cluster.
However, this is not considered a security problem if the backup is located in the same KSQL private directories because those users will have access to the
ksql-server.properties anyway, which contains user credentials to access the command topic in Kafka. This access restriction is part of the system
admin configuration to allow certain users to access KSQL files. But, if the location is out of the KSQL directory, then users would need to be warned (in docs)
to provide the right security rules to protect their metastore backup from unauthorized views.
