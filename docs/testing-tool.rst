.. _ksql-testing-tool:

KSQL Testing Tool
###########################

This describes a command line tool, the KSQL testing tool, that can be used to test a set of KSQL statements without requiring any infrastructure such as Kafka or KSQL clusters.
KSQL testing tool is a great way to design your KSQL pipeline and ensure the expected results are generated. You can colaborate on designing your KSQL statements by sharing the test files.
The input for the tool is a JSON file that describes a set of KSQL statements along with the input data in Kafka topic(s) and the expected output data that will also be written in Kafka topic(s) when you run the statements in a real environment.
You can use the testing tool from terminal by callink the testing took command and passing the test file as a parameter.

.. code:: bash

    $ ksql-testing-tool /path/to/the/test/file.json


Test File Structure
*******************
The test file is a JSON file containing the KSQL statements, input data, desired configurations and the expected results or expected errors.
The following is a simple test file:

.. code:: json
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
            "CREATE STREAM output AS SELECT id FROM intput WHERE id < 10;"
          ],
          "inputs": [
            {"topic": "input_topic", "key": 0, "value": {"id": 8}, "timestamp": 0},
            {"topic": "input_topic", "key": 0, "value": {"id": 10}, "timestamp": 10000},
            {"topic": "input_topic", "key": 1, "value": {"id": 9}, "timestamp": 30000},
            {"topic": "input_topic", "key": 1, "value": {"id": 11}, "timestamp": 40000}
          ],
          "outputs": [
            {"topic": "OUTPUT", "key": 0, "value": {"ID": 8}, "timestamp": 0},
            {"topic": "OUTPUT", "key": 1, "value": {"ID": 9}, "timestamp": 30000}
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


As you can see, you can have multiple tests in one test files. In addition to name, description and statements, each test includes input topics and their data along with the expected output topics and their data.
The test file format is the same as the files KSQL code uses for query translation tests. For more details on the structure of the test file and all possible settings refer to the `README.md <https://github.com/confluentinc/ksql/tree/master/ksql-functional-tests>` in the KSQL repository.

Running Tests
*************

As mentioned above, you can run the tests by passing the test file to the testing tool command in terminal. Let's assume we run the above test file which is stored in test.json file in the home directory.
The following shows how to run the test from the terminal:

.. code:: bash

    $ ksql-testing-tool ~/test.json
     >>> Running test: ksql-test - my first positive test
    	 >>> Test ksql-test - my first positive test passed!
     >>> Running test: ksql-test - my first negative test
    	 >>> Test ksql-test - my first negative test passed!
    All tests passed!
    $

For each test case, the testing tool first creates and populates the input topics in it's internal simulated kafka cluster.
It then compiles and runs the KSQL statements and finally compares the generated results with the expected ones. If the expected results are generated, the test passes, otherwise it fails.
The status of each test is printed out into the terminal.
