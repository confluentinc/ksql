.. _ksql-testing-tool:

KSQL Testing Tool
###########################

Use the KSQL testing tool to test a set of KSQL statements. The KSQL testing tool
is a command line utility that enables testing KSQL statements without requiring any infrastructure, like |ak-tm| and KSQL clusters.
The KSQL testing tool is a great way to design your KSQL pipeline and ensure the expected results are generated.
You can collaborate on designing your KSQL statements by sharing the test files.
You provide a JSON file that describes a set of KSQL statements, along with the input data and expected output data.
Run the testing tool from a terminal and pass the test file as a parameter.

.. code:: bash

    $ ksql-testing-tool /path/to/the/test/file.json


Test File Structure
*******************
The test file is a JSON file containing the KSQL statements, input data, desired configurations, and the expected results or expected errors.
The following is a sample test file:

.. code:: json
    {
      "comments": [
        "Add a description of the functionality that this file tests"
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


You can have multiple tests in one test file. In addition to name, description, and statements, each test includes
input topics and their data along with the expected output topics and their data.
The test file format is the same as the files KSQL code uses for integration tests. For more details on the
structure of the test file and all possible settings, see the `README.md <https://github.com/confluentinc/ksql/tree/master/ksql-functional-tests>` in the KSQL repository.

Running Tests
*************

Assume we run the previous test file, which is stored in test.json in the home directory.
The following command shows how to run the test from the terminal:

.. code:: bash

    ksql-testing-tool ~/test.json


Your output should resemble:

.. code:: bash

     >>> Running test: ksql-test - my first positive test
    	 >>> Test ksql-test - my first positive test passed!
     >>> Running test: ksql-test - my first negative test
    	 >>> Test ksql-test - my first negative test passed!
    All tests passed!


For each test case, the testing tool first creates and populates the input topics in its internal simulated Kafka cluster.
It compiles the KSQL statements, runs them, and compares the generated results with the expected results. If the expected results are generated, the test passes, otherwise it fails.
The status of each test is printed out into the terminal.
