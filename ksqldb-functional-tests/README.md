# KSQL Query Validation Tests

The tests in this directory are used to validate the translation and output of KSQL queries. Each
file contains one or more tests. They are written in a simple json format that describes the queries,
the inputs, and the expected outputs. These queries will be translated by KSQL into a Streams
topology, executed, and verified.

The test cases are run by the `QueryTranslationTest` test class.

## Test Case Plans

Most of the test cases (except those that validate exceptions) work by generating a plan, building
a streams topology from the plan, and then running the topology against the inputs. Over time, the
plan for a given query may evolve. However KSQL still needs to be able to execute older plans
correctly. To test this, most Query Validation Tests work by loading and executing saved plans
from the local tree.

Each time a query plan changes, it's saved under the file
`src/test/resources/historical_plans/<Test Name>/<Version Number>_<Timestamp>`.
`QueryTranslationTest` runs by iterating over the saved plans, building them, and verifying that
queries execute correctly.

### Generating new topology files

Plans evolve over time, and we need to make sure that KSQL tests the latest way of executing
queries. To ensure this, we run a test (called `PlannedTestGeneratorTest`) that ensures that the
latest plan for each test case is saved to the local tree. If it's not, you will need to run the
generator to do so.

To generate new plans, just run `PlannedTestGeneratorTest.manuallyGeneratePlans`

## Topology comparison
These tests also validate the generated topology matches the expected topology,
i.e. a test will fail if the topology has changed from previous runs.
This is needed to detect potentially non-backwards compatible changes to the generated topology.

The expected topologies are stored alongside the query plans described above.

### Running a subset of tests:

`QueryTranslationTest` supports running a subset of test files, for example following example:

```
mvn test -pl ksqldb-functional-tests -Dtest=QueryTranslationTest -Dksql.test.files=sum.json
```

or
```
mvn test -pl ksqldb-functional-tests -Dtest=QueryTranslationTest -Dksql.test.files=sum.json,substring.json
```

The above commands can execute only a single test (sum.json) or multiple tests (sum.json and substring.json).

There is also a version of `QueryTranslationTest` that runs a full KSQL server in order to
verify REST responses: `RestQueryTranslationTest`. These tests are much slower and should
be used sparingly. To run a subset of these tests, supply a regex of the test name:

```
mvn test -pl ksqldb-functional-tests -Dtest=RestQueryTranslationTest -Dksql.functional.test.regex="pull-queries.*join"
```


## Adding new tests

The following is a template test file:

```json
{
  "comments": [
    "Add a description of _what_ are of functionality this file is testing"
  ],
  "tests": [
    {
      "name": "my first positive test",
      "description": "an example positive test where the output is verified",
      "statements": [
        "CREATE STREAM input (ID BIGINT KEY, NAME STRING) WITH (kafka_topic='input_topic', value_format='JSON');",
        "CREATE STREAM output AS SELECT * FROM input WHERE id < 10;"
      ],
      "inputs": [
        {"topic": "input_topic", "key": 8, "value": {"name": "bob"}, "timestamp": 0},
        {"topic": "input_topic", "key": 10, "value": {"name": "pete"}, "timestamp": 10000},
        {"topic": "input_topic", "key": 9, "value": {"name": "vic"}, "timestamp": 30000},
        {"topic": "input_topic", "key": 11, "value": {"name": "jon"}, "timestamp": 40000}
      ],
      "outputs": [
        {"topic": "OUTPUT", "key": 8, "value": {"NAME": "bob"}, "timestamp": 0},
        {"topic": "OUTPUT", "key": 9, "value": {"NAME": "vic"}, "timestamp": 30000}
      ],
      "post": {
        "sources": [
          {"name": "OUTPUT", "type": "stream", "schema": "ID BIGINT KEY, NAME STRING"}
        ]
      }
    },
    {
      "name": "test using insert statements",
      "description": "an example positive test that uses insert into values statements instead of inputs",
      "versions" : {
        "min": "5.0",
        "max": "5.4.1"
      },
      "statements": [
        "CREATE STREAM test (name VARCHAR KEY, number INT) WITH (kafka_topic='input_topic', value_format='JSON');",
        "INSERT INTO test (name, number) VALUES ('foo', 45);",
        "INSERT INTO test (name, number) VALUES ('bar', 646);",
        "CREATE STREAM output AS SELECT name, number FROM test;"
      ],
      "outputs": [
        {"topic": "OUTPUT", "key": "foo", "value": {"NUMBER": 45}},
        {"topic": "OUTPUT", "key": "bar", "value": {"NUMBER": 646}}
      ]
    },
    {
      "name": "my first negative test",
      "description": "an example negative test where the statement will fail to parse",
      "statements": [
        "CREATE STREAM TEST WITH (kafka_topic='test_topic', value_format='DELIMITED');"
      ],
      "expectedException": {
        "type": "io.confluent.ksql.util.KsqlException",
        "message": "The statement does not define any columns."
      }
    }
  ]
}
```

Test cases should be written to be as succinct as possible to make them as readable as possible.
For example:
 - the schemas in DDL statements should be as minimal as possible. So avoid cutting & pasting large schemas between test cases.
 - keep the test of source messages to the minimum required to prove the functionality and any edge cases.
 - keep the definition of source and sink messages succinct too: don't add optional attributes if they are not pertinent to the test case.

Each test case can have the following attributes:

|    Attribute     | Description |
|------------------|:------------|
| name             | (Required) The name of the test case as will be displayed in IDEs and Logs. This would be the function name in a JUnit test |
| description      | (Optional) A description of what the test case is testing. Not used or displayed anywhere |
| versions         | (Optional) A object describing the min and/or max version of KSQL the test is valid for. (See below for more info) |
| format           | (Optional) An array of multiple different formats to run the test case as, e.g. AVRO, JSON, DELIMITED. (See below for more info) |
| config           | (Optional) An array of multiple different config values for a single property to run the test case as (See below for more info) |
| statements       | (Required) The list of statements to execute as this test case |
| properties       | (Optional) A map of property name to value. Can contain any valid Ksql config. The config is passed to the engine when executing the statements in the test case. One property may have value `{CONFIG}` to re-run the test with different config values (cf `config` attribute) |
| topics           | (Optional) An array of the topics this test case needs. Allows more information about the topic to be supplied, e.g. an existing Avro schema (See below for more info) |
| inputs           | (Required if `expectedException` not supplied and statements do not include `INSERT INTO` statements) The set of input messages to be produced to Kafka topic(s), (See below for more info) |
| outputs          | (Required if `expectedException` not supplied) The set of output messages expected in the output topic(s), (See below for more info) |
| expectedException| (Required in `inputs` and `outputs` not supplied) The exception that should be thrown when executing the supplied statements, (See below for more info) |
| post             | (Optional) Defines post conditions that must exist after the statements have run, (See below for more info) |

### Versions
A test can can optionally supply the bounds on Ksql version that the test is valid for.

For example:
```json
[{
  "name": "test only valid before version 5.4",
  "versions": {
     "max": "5.3"  
  }  
},
{
  "name": "test only valid for versions at or after 5.4",
  "versions": {
     "min": "5.4"  
  }  
},
{
  "name": "test only valid between versions 5.2 and 5.4",
  "versions": {
     "min": "5.2",
     "max": "5.4"  
  }  
}]
```

| Attribute | Description |
|-----------|:------------|
| min       | (Optional) lower bound on version, (inclusive) |
| max       | (Optional) upper bound on version, (inclusive) |

### Formats
A test case can optionally supply an array of formats to run the test case as.
The current format will be injected into the statements of the test case and any topics as the `{FORMAT}` variable.

For example:
```json
{
  "name": "my first test that will run with different formats",
  "format": ["AVRO", "JSON"],
  "statements": [
    "CREATE TABLE TEST (ID bigint) WITH (kafka_topic='test_topic', value_format='{FORMAT}');"
  ],
  "topics": [
    {
      "name": "bar",
      "schema": {"type": "array", "items": {"type": "string"}},
      "format": "{FORMAT}"
    }
  ]
}
```

The test will run once for each defined format.

Make use of the `format` option only where the serialized format of the data may affect the functionality.

### Config
A test case can optionally supply an array of config values for a single property to re-run the same test with different configs.
The corresponding property must have value `{CONFIG}`. Note, that only a single property can be `{CONFIG}`.

For example:
```json
{
  "name": "my first test that will run with different configs",
  "config": ["at_least_once", "exactly_once_v2"],
  "properties": {
    "ksql.streams.processing.guarantee": "{CONFIG}"
    "ksql.streams.commit.interval": 1000
  },
  "statements": [
    "CREATE STREAM test (id BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');"
    "CREATE STREAM test2 AS SELECT * FROM test;
  ]
  ...
}
```

### Statements
You can specify multiple statements per test case, i.e. to set up the various streams needed
for joins etc, but currently only the final topology will be verified. This should be enough
for most tests as we can simulate the outputs from previous stages into the final stage. If we
take a modular approach to testing we can still verify that it all works correctly, i.e. if we
verify the output of a select or aggregate is correct, then we can use simulated output to feed
into a join or another aggregate.

If the test case has set the `format` property, then any occurrences of the string `{FORMAT}` 
within the statement text will be replaced with the appropriate format.

### Topics
A test case can optionally supply an array of topics that should exist and additional information about those topics.
It is not necessary to add entries for topics required by the test case unless any of the following attributes need to be controlled:
(Tests will create input and output topics as required).

| Attribute | Description |
|-----------|:------------|
| name      | (Required) the name of the topic |
| format    | (Required) the serialization format of records within the topic, e.g. `AVRO`, or `{FORMAT}` if using `format` property. |
| schema    | (Optional) the schema, registered in the schema store, of the topic. If not supplied, no schema is registered |

### Inputs & Outputs
Each input and output row defines a message either produced to, or expected from, Kafka.
They can define the following attributes:

| Attribute | Description |
|-----------|:------------|
| topic     | (Required) the name of the topic |
| key       | (Optional for streams, Required for tables) the key of the message. If absent the key will be set to `null` |
| value     | (Required) the value of the message |
| timestamp | (Optional) the timestamp of the message. If not supplied it will be set using the system clock if it was from an `INSERT INTO VALUES` statement, and 0 otherwise.\* |
| window    | (Optional) the window information for the message. (See below for more info) |

\* If the timestamp was set using the system clock, it will not verify the timestamp against outputs with no defined timestamp.

#### Windowed messages
A message in either inputs or outputs can be windowed.
This means that the `key` supplied will be wrapped with the supplied window bounds information.

An example windowed message:
```
{"topic": "test_topic", "key": 0, "value": "0,0", "timestamp": 10, "window": {"start": 10, "end": 10, "type": "session"}}
```

The window information can define the following attributes:

| Attribute | Description |
|-----------|:------------|
| start     | (Required) the start of the window in milliseconds |
| end       | (Session only) the end of the window in milliseconds |
| type      | (Required) the window type. Must be one of `hopping`, `tumbling`, or `session` |

### Expected Exception
A test can define an expected exception in the same way as a JUnit test, e.g.

```json
{
  "expectedException": {
    "type": "io.confluent.ksql.util.KsqlException",
    "message": "The statement does not define any columns."
  }
}
```

The expected exception can define the following attributes:

| Attribute | Description |
|-----------|:------------|
| type      | (Optional) The fully qualifid class name of the _exact_ exception |
| message   | (Optional) The full text of the message within the exception, i.e. the error message that would be displayed to the user |

### Post Conditions
A test can define a set of post conditions that must be met for the test to pass, e.g.

```json
{
  "post": {
    "sources": [
      {"name": "INPUT", "type": "stream"}
    ],
    "topics": {
      "blacklist": ".*-not-allowed",
      "topics": [
        {
          "name" : "OUTPUT",
          "keyFormat" : {"format" : "KAFKA"},
          "valueFormat" : {"format" : "DELIMITED"},
          "partitions" : 4
        }
      ]
    }
  }
}
```

Post conditions current support the following checks:

| Attribute | Description |
|-----------|:------------|
| sources   | (Optional) A list of sources that must exist in the metastore after the statements have executed. This list does not need to define every source. |
| topics    | (Optional) Topic post conditions |

#### Sources
A post condition can define the list of sources that must exist in the metastore. A source might be:

```json
{
  "name": "S1",
  "type": "table",
  "schema": "ID BIGINT KEY, FOO STRING",
  "keyFormat": {"format": "KAFKA"},
  "valueFormat": "JSON",
  "keyFeatures": ["UNWRAP_SINGLES"],
  "valueFeatures": ["WRAP_SINGLES"]
}
```

Each source can define the following attributes:

| Attribute    | Description |
|--------------|:------------|
| name         | (Required) The name of the source. |
| type         | (Required) Specifies if the source is a STREAM or TABLE. |
| schema       | (Optional) Specifies the SQL schema for the source. |
| keyFormat    | (Optional) Key serialization format for the source. |
| valueFormat  | (Optional) Value serialization format for the source. |
| keyFeatures  | (Optional) List of expected key serde features for the source. |
| valueFeatures| (Optional) List of expected value serde features for the source. |

#### Topics

A post condition can define a check against the set of topics the case creates

```json
{
  "blacklist": ".*-repartition",
  "topics": [
    {
      "name" : "OUTPUT",
      "keyFormat" : {"format" : "KAFKA", "features": ["UNWRAP_SINGLES"]},
      "valueFormat" : {"format" : "DELIMITED", "features": ["WRAP_SINGLES"]},
      "partitions" : 4
    }
  ]
}
```

The topics object can define the following attributes:

| Attribute   | Description |
|-------------|:------------|
| blacklist   | Regex defining a blacklist of topic names that should not be created. |
| topics      | A list of topics that should be created. |

##### Topic

```json
{
  "name" : "OUTPUT",
  "keyFormat" : {"format" : "KAFKA", "features": ["UNWRAP_SINGLES"]},
  "valueFormat" : {"format" : "DELIMITED", "features": ["WRAP_SINGLES"]},
  "partitions" : 4
}
```

The topic object can define the following attributes:

| Attribute   | Description |
|-------------|:------------|
| name        | The name of the topic. |
| keyFormat   | The key serialization format. |
| valueFormat | The value serialization format. |
| partitions  | (Optional) The number of partitions. |
