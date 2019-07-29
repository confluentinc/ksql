# KSQL Query Validation Tests

The tests in this directory are used to validate the translation and output of KSQL queries. Each
file contains one or more tests. They are written in a simple json format that describes the queries,
the inputs, and the expected outputs. These queries will be translated by KSQL into a Streams
topology, executed, and verified.

The test cases are run by the `QueryTranslationTest` test class.

## Topology comparision
These tests also validate the generated topology matches the expected topology,
i.e. a test will fail if the topology has changed from previous runs.
This is needed to detect potentially non-backwards compatible changes to the generated topology.

The expected topology files, and the configuration used to generated them are found in
`src/test/resources/expected_topology/<Version Number>`

By default, the test will check topology compatibility against all previously released versions
of KSQL (for which expected topology files exist).

### Running a subset of tests:

`QueryTranslationTest` supports running a subset of test files, for example following example:

```
mvn test -pl ksql-functional-tests -Dtest=QueryTranslationTest -Dksql.test.files=sum.json
```

or
```
mvn test -pl ksql-functional-tests -Dtest=QueryTranslationTest -Dksql.test.files=sum.json,substring.json
```

The above commands can execute only a single test (sum.json) or multiple tests (sum.json and substring.json).

### Running against different previous versions:

To run this test against specific previously released versions, set the system property
"topology.versions" to the desired version(s). The property value should be a comma-delimited list of
version number(s) found under the `src/test/resources/expected_topology` directory, for example, `"5.0,5.1"`.

The are two places system properties may be set:
  * Within Intellij
    1. Click Run/Edit configurations
    1. Select the QueryTranslationTest
    1. Enter `-Dtopology.versions=X` in the "VM options:" form entry
       where X is a comma-delimited list of the desired previously released version number(s).
  * From the command line
    1. run `mvn clean package -DskipTests=true` from the base of the KSQL project
    1. Then run `mvn test -Dtopology.versions=X -Dtest=QueryTranslationTest -pl ksql-functional-tests`.
       Again X is a list of the versions you want to run the tests against.

  Note that for both options above the version(s) must exist
  under the `src/test/resources/expected_topology` directory.

### Generating new topology files

For instructions on how to generate new topologies, see `TopologyFileGenerator.java`

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
        "CREATE STREAM intput (ID bigint) WITH (kafka_topic='input_topic', value_format='JSON');",
        "CREATE STREAM output AS SELECT id FROM test WHERE id < 10;"
      ],
      "inputs": [
        {"topic": "input_topic", "key": 0, "value": {"id": 8}, "timestamp": 0},
        {"topic": "input_topic", "key": 0, "value": {"id": 10}, "timestamp": 10000},
        {"topic": "input_topic", "key": 1, "value": {"id": 9}, "timestamp": 30000},
        {"topic": "input_topic", "key": 1, "value": {"id": 11}, "timestamp": 40000}
      ],
      "outputs": [
        {"topic": "OUTPUT", "key": 0, "value": {"id": 8}, "timestamp": 0},
        {"topic": "OUTPUT", "key": 0, "value": {"id": 9}, "timestamp": 30000}
      ]
    },
    {
      "name": "test using insert statements",
      "description": "an example positive test that uses insert into values statements instead of inputs",
      "statements": [
        "CREATE STREAM test (ID name) WITH (kafka_topic='input_topic', value_format='JSON');",
        "INSERT INTO test (name, number) VALUES ('foo', 45)",
        "INSERT INTO test (name, number) VALUES ('bar', 646)",
        "CREATE STREAM output AS SELECT value FROM test;"
      ],
      "outputs": [
        {"topic": "OUTPUT", "key": "foo", "value": {"number": 45}},
        {"topic": "OUTPUT", "key": 0, "value": {"number": 646}}
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
| format           | (Optional) An array of multiple different formats to run the test case as, e.g. AVRO, JSON, DELIMITED. (See below for more info) |
| statements       | (Required) The list of statements to execute as this test case |
| properties       | (Optional) A map of property name to value. Can contain any valid Ksql config. The config is passed to the engine when executing the statements in the test case |
| topics           | (Optional) An array of the topics this test case needs. Allows more information about the topic to be supplied, e.g. an existing Avro schema (See below for more info) |
| inputs           | (Required if `expectedException` not supplied and statements do not include `INSERT INTO` statements) The set of input messages to be produced to Kafka topic(s), (See below for more info) |
| outputs          | (Required if `expectedException` not supplied) The set of output messages expected in the output topic(s), (See below for more info) |
| expectedException| (Required in `inputs` and `outputs` not supplied) The exception that should be thrown when executing the supplied statements, (See below for more info) |
| post             | (Optional) Defines post conditions that must exist after the statements have run, (See below for more info) |

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
| key       | (Optional) the key of the message. If absent the key will be an empty string |
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
      {"name": "INPUT", "type": "stream", "keyField": null}
    ]
  }
}
```

Post conditions current support the following checks:

| Attribute | Description |
|-----------|:------------|
| sources   | (Optional) A list of sources that must exist in the metastore after the statements have executed. This list does not need to define every source. |

#### Sources
A post condition can define the list of sources that must exist in the metastore. A source might be:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"name": "FOO", "legacyName": "KSQL_INTERNAL_COL_0", "legacySchema": {"type": "STRING"}},
  "valueSchema": "STRUCT<ROWTIME BIGINT, ROWKEY STRING, FOO INT, KSQL_COL_1 BIGINT>"
}
```

Each source can define the following attributes:

| Attribute   | Description |
|-------------|:------------|
| name        | (Required) The name of the source. |
| type        | (Required) Specifies if the source is a STREAM or TABLE. |
| keyField    | (Optional) Specifies the keyField for the source. (See below for details of key field) |
| valueSchema | (Optional) Specifies the value SQL schema for the source. |

##### Key Fields

Key field nodes can define the following attributes:

| Attribute   | Description |
|-------------|:------------|
| name        | (Optional) The name of the key field. If present, but set to `null`, the name of the key field is expected to not be set. If not supplied, the name of the key field will not be checked. |
| legacyName  | (Optional) The legacy name of the key field. If present, but set to `null`, the legacy name of the key field is expected to not be set. If not supplied, the legacy name of the key field will not be checked. |
| legacySchema| (Optional) The legacy SQL schema of the key field. If present, but set to `null`, the legacy schema of the key field is expected to not be set. If not supplied, the legacy schema of the key field will not be checked. |

A test case can not test the value of the the key field by not including a keyField node in the source:

```json
{
  "name": "S1",
  "type": "table"
}
```

Which is equivalent to supplying an empty document:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {}
}
```

A test case can require the name of the key field to be set to an expected value:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"name": "expected-name"}
}
```

Or explicitly not set:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"name": null}
}
```

A test case can require the legacy key field to be set:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"legacyName": "OLD_KEY", "legacySchema": "STRING"}
}
```

Or explicitly not set:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"legacyName": null}
}
```

A test case can of course have requirements on both the latest and legacy key field:

```json
{
  "name": "S1",
  "type": "table",
  "keyField": {"name": "expected-new", "legacyName": "expected-old", "legacySchema": "STRING"}
}
```



