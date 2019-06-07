# KSQL Query Validation Tests

The tests in this directory are used to validate the translation and output of KSQL queries. Each
file contains one or more tests. They are written in a simple json format that describes the queries,
the inputs, and the expected outputs. These queries will be translated by KSQL into a Streams
topology, executed, and verified.

You can specify multiple statements per test case, i.e., to set up the various streams needed,
for joins etc, but currently only the final topology will be verified. This should be enough,
for most tests as we can simulate the outputs from previous stages into the final stage. If we
take a modular approach to testing we can still verify that it all works correctly, i.e, if we,
verify the output of a select or aggregate is correct, we can use simulated output to feed into,
a join or another aggregate.