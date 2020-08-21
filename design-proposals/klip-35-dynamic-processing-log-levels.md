# KLIP-35: Dynamic Processing Log Levels

## Motivation/Background
It can be difficult for users to make sense of what a running query is doing under the hood. In particular, it can be hard to understand why the results of a query don’t match expectations or in some cases why there aren’t any results at all. Consider the following examples:
- A user has a stream it wants to join with a table. The table’s record key contains the correct data, but its formatted as bytes. When the join is applied and the stream is rekeyed, the stream’s key is formatted as a string and the join returns empty results.
- A user has started a windowed query, but doesn’t see the expected windows. It turns out this is due to an error generating timestamps in the producer.
- A user has a statement with a bunch of nested UDFs and is seeing null results, but doesn’t understand which UDF is failing.

This is a common error scenario users face. I did a (I’ll admit unscientific) scan through the community slack channel and classified inquiries as:
1. General inquiries about functionality or feasibility
1. Errors issuing a ksqlDB statement
1. Errors or confusion with query results (what this KLIP addresses)
Out of 62 inquiries there were 37 general questions, 6 questions about statement errors, and 6 questions about confusing/bad query results. The rest were misc (bugs, perf issues).

ksqlDB already includes a nice mechanism for reporting internal details of query execution to the user - the record processing log. However, to help a user advance past the scenarios described above, the log messages required would need to be logged at a very verbose (e.g. DEBUG) level to avoid flooding the log with records. It’s not practical to run a real workload with the log configured this way.

There have also been incidents where even the current error-only level of logging is too verbose. In such cases, it should be possible to turn the processing log off at runtime.

In this KLIP, I propose a mechanism to dynamically control the log level of the record-processing log. Furthermore, the proposed mechanism makes it possible to change the level with a fine granularity - so you can change the level at runtime for individual queries, or even individual processing nodes of a query.

For important background on the processing log, please read https://docs.ksqldb.io/en/latest/developer-guide/test-and-debug/processing-log/

## In-Scope

- API for getting/setting the global log level
- API for getting/setting query/node-level log levels
- Kafka-based stateful implementation that distributes level settings to the cluster and stores them so they are recovered on a restart.

## Out-Of-Scope

- Thoroughly populating the query engine with log messages to address the problems we’ve already seen. This is super important, but out of scope for this particular KLIP.

## Value/Return

A step toward improving UX when users observe unexpected behavior from running queries.

## Public APIs

### REST API

### PUT /processing_logger/config

Sets the root processing log configuration (currently just supports setting the log level).  This will set the level for all loggers that do not already have a level set.

Request Schema:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “<OFF|FATAL|ERROR|WARN|INFO|DEBUG>”}
    }
}
````

Response Schema:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “The level from the request”}
    }
}
```

### GET /processing_logger/config

Gets the current root processing log level.

Response Schema:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “The level of the root logger”}
    }
}
```

### DELETE /processing_logger/config

Deletes customized config for the root logger (reverts to default config as specified in the log4j config file).

### PUT /processing_logger/<processing logger name>/config

Sets the processing log configuration for the logger with the given processing logger name (currently just supports setting the log level). This will set the level for all child loggers (loggers that share a common name prefix) that do not already have a level set using a longer prefix.

Request Fields:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “<OFF|FATAL|ERROR|WARN|INFO|DEBUG>”}
    }
}
```

Response Fields:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “The level from the request”}
    }
}
```

### GET /processing_logger/<processing logger name>/level

Gets the processing log configuration for the logger with the given processing logger name.

Response Fields:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “The level of the specified logger”}
    }
}
```

### DELETE /processing_logger/<processing logger name>/level

Deletes customized config for the logger with the given processing logger name.

### GET /processing_logger/<processing logger name?>

Lists all processing loggers and configs under the logger name if specified. If not specified, lists all loggers and configs.

Response Fields:
```
{
    “properties”: {
        “loggers”: {
            “type”: “array”,
            “items”: {
                “type" : “object”,
                "properties" : “{
                     “context" : {“type”: “string”, “description”: “the context of this logger”}
                     “config" : {
                         “type" : “object”,
                         "properties" : {
                             “level”: {“type”: “string”, “description”: “The level of the logger”}
                         }
                     }
                 }
            }
        }
    }
}
```

### ksqlDB Statements

As a convenience, in addition to the API we’ll include ksqlDB statements for getting/setting the level. This makes it easy to get/set the level from ksqlDB user interfaces (such as the Confluent Cloud or C3 UI) while those products implement their own interface to the processing log (if they choose to do so).

- `SET_PLL root <level>`: sets the processing log level for the root logger
- `UNSET_PLL root`: unsets the processing log level for the root logger
- `GET_PLL`: lists all processing log levels.
- `SET_PLL <query context> <level>`: sets the processing log level for the given logger
- `UNSET_PLL <query context> <level>`: unsets the processing log level for the given logger
- `GET_PLL <query context>`: lists all processing log levels with the given logger prefix

## Design

### Storing Log Levels

The log levels will be stored on a Kafka topic called the processing log config topic (PLCT). The key of the topic’s records will be the processing logger name. We will use the name “root” for the root logger. The value of the topic will be a JSON object representing the logger’s config. For now, it will contain a single field called “level”:
```
{
    “properties”: {
        “level”: {“type”: “string”, “description”: “<OFF|FATAL|ERROR|WARN|INFO|DEBUG>”}
    }
}
```

The Kafka topic will be compacted.

The name of the topic will be controlled by a configuration named “ksql.logging.processing.config.topic.name”.

### Setting a Log Level

To set the log level, the ksql node receiving the request to set the level will produce a record with the logger’s key and value containing the specified level to the PLCT. Each ksql node will run a thread that consumes the PLCT. For each message consumed, the thread will use the log4j API to set the log level. To set the root level, the thread will call:

```
LogManager.getRootLogger().setLevel(<level>);
```

To set a child logger level, the thread will call:

```
LogManager.getLogger().setLevel(<level>);
```

Unsetting a Log Level

To unset the log level, the thread consuming the PLCT will set the log level for the specified logger to `null`.

### Getting/Listing Log Levels

To get/list log levels, the ksql node receiving the request will list the local loggers in log4j and use them to populate the response.

## Test Plan
In  addition to the usual unit testing, we’ll include an  integration test to validate that the processing log is populated correctly when the level is set.

## LOEs
- ksqlDB syntax: 3 days
- API endpoints: 4 days
- implementation of the backend: 5 days
- adding node-level debug logging of the data seen at each node: 1 day
- docs updates: 2 days

## Docs Updates
- The processing log documentation should be updated to include these features.
- The syntax reference should be updated to include the statements to set/get the log level.
- The API docs should be updated to include the new API endpoints.
