Changelog
=========

Version 5.0.0
-------------

KSQL 5.0 includes a number of new features, including:
    * User-Defined Functions (UDF).
    * Support for nested data via the new STRUCT data type.
    * Support for writing to existing streams using the new INSERT INTO statement.
    * Stream-Stream Joins.
    * Table-Table Joins.
    * Table Aggregations.
    * A revamped REST API that will be kept compatible from this release onwards.
    * Confluent Platform Docker images for KSQL Server and CLI
    * Java 10 support.

Detailed Changlog
+++++++++++++++++
* `PR-1610 <https://github.com/confluentinc/ksql/pull/1610>`_ - Specify a namespace for avro schemas
* `PR-1570 <https://github.com/confluentinc/ksql/pull/1570>`_ - DOCS-400: Add troubleshooting steps for KSQL server port
* `PR-1588 <https://github.com/confluentinc/ksql/pull/1588>`_ - Add example avro schemas to the ksql packaging build
* `PR-1529 <https://github.com/confluentinc/ksql/pull/1529>`_ - Add a schema inference test
* `PR-1511 <https://github.com/confluentinc/ksql/pull/1511>`_ - Fix NPE when printing null values
* `PR-1547 <https://github.com/confluentinc/ksql/pull/1547>`_ - Fix the bug for null check in comparison expression code gen,
* `PR-1525 <https://github.com/confluentinc/ksql/pull/1525>`_ - Add more unit tests for avro serializer
* `PR-1521 <https://github.com/confluentinc/ksql/pull/1521>`_ - Add unit tests for deserializing a comprehensive set of avro/connect …
* `PR-1519 <https://github.com/confluentinc/ksql/pull/1519>`_ - Extend the query translation test to support parametrized format and add avro to some cases
* `PR-1549 <https://github.com/confluentinc/ksql/pull/1549>`_ - Ensure that the list of interceptors is modifiable before mutating it
* `PR-1528 <https://github.com/confluentinc/ksql/pull/1528>`_ - Include example schemas in the packaging build
* `PR-1531 <https://github.com/confluentinc/ksql/pull/1531>`_ - Recommend the use of STRUCT instead of EXTRACTJSONFIELD
* `PR-1526 <https://github.com/confluentinc/ksql/pull/1526>`_ - Include monitoring interceptor config in streams configs
* `PR-1427 <https://github.com/confluentinc/ksql/pull/1427>`_ - Udf docs
* `PR-1522 <https://github.com/confluentinc/ksql/pull/1522>`_ - Reduce log level when loading blacklist to INFO from ERROR
* `PR-1524 <https://github.com/confluentinc/ksql/pull/1524>`_ - Java 10 support
* `PR-1527 <https://github.com/confluentinc/ksql/pull/1527>`_ - Drop all references to the KSQL UI
* `PR-1479 <https://github.com/confluentinc/ksql/pull/1479>`_ - Write configs to command topic and read back before building queries
* `PR-1512 <https://github.com/confluentinc/ksql/pull/1512>`_ - Data-gen now can create struct columns.
* `PR-1517 <https://github.com/confluentinc/ksql/pull/1517>`_ - Fix spacing in struct schema string
* `PR-1518 <https://github.com/confluentinc/ksql/pull/1518>`_ - Add documentation for structs
* `PR-1516 <https://github.com/confluentinc/ksql/pull/1516>`_ - Fix test not run due to incorrect imports
* `PR-1513 <https://github.com/confluentinc/ksql/pull/1513>`_ - fix incorrect imports
* `PR-1507 <https://github.com/confluentinc/ksql/pull/1507>`_ - Standardize on a single version for the avro-random-genator
* `PR-1481 <https://github.com/confluentinc/ksql/pull/1481>`_ - Consistent schema format between websocket endpoint and ksql endpoint
* `PR-1503 <https://github.com/confluentinc/ksql/pull/1503>`_ - Convert invalid avro field names before serializing
* `PR-1501 <https://github.com/confluentinc/ksql/pull/1501>`_ - sort udf names
* `PR-1500 <https://github.com/confluentinc/ksql/pull/1500>`_ - don't bomb on non-existing plugin dir
* `PR-1495 <https://github.com/confluentinc/ksql/pull/1495>`_ - Pass schema registry client configs onto the schema registry client
* `PR-1482 <https://github.com/confluentinc/ksql/pull/1482>`_ - String masking functions
* `PR-1434 <https://github.com/confluentinc/ksql/pull/1434>`_ - Schema Translation Test + Nested Support in Query Validation Test
* `PR-1486 <https://github.com/confluentinc/ksql/pull/1486>`_ - list and describe updated to include aggregate functions
* `PR-1478 <https://github.com/confluentinc/ksql/pull/1478>`_ - Use ':' for struct field reference.
* `PR-1494 <https://github.com/confluentinc/ksql/pull/1494>`_ - Output more info to help with debugging system tests.
* `PR-1467 <https://github.com/confluentinc/ksql/pull/1467>`_ - Support java based UDAFS
* `PR-1390 <https://github.com/confluentinc/ksql/pull/1390>`_ - Upgrade handling step 1: clean up KSQL configs
* `PR-1453 <https://github.com/confluentinc/ksql/pull/1453>`_ - Struct end to end
* `PR-1471 <https://github.com/confluentinc/ksql/pull/1471>`_ - Renable the build of the cp-ksql-server docker image
* `PR-1465 <https://github.com/confluentinc/ksql/pull/1465>`_ - support null literal in UDFs #1462
* `PR-1458 <https://github.com/confluentinc/ksql/pull/1458>`_ - add jar path to description of functions
* `PR-1457 <https://github.com/confluentinc/ksql/pull/1457>`_ - Simple Security manager for UDFS
* `PR-1449 <https://github.com/confluentinc/ksql/pull/1449>`_ - create module for UDF annotations to minimize dependencies
* `PR-1470 <https://github.com/confluentinc/ksql/pull/1470>`_ - Use a string for formatted time field in clickstream schema
* `PR-1448 <https://github.com/confluentinc/ksql/pull/1448>`_ - Add config for ext dir
* `PR-1461 <https://github.com/confluentinc/ksql/pull/1461>`_ - Fix some doc formatting issues.
* `PR-1463 <https://github.com/confluentinc/ksql/pull/1463>`_ - Handle rename of KafkaConsumerService to ConsumerService
* `PR-1455 <https://github.com/confluentinc/ksql/pull/1455>`_ - Rename SPAN to WITHIN everywhere
* `PR-1435 <https://github.com/confluentinc/ksql/pull/1435>`_ - Support List/Show Functions (UDFs) and Describe Function <name>
* `PR-1417 <https://github.com/confluentinc/ksql/pull/1417>`_ - Add new join types to KSQL
* `PR-1451 <https://github.com/confluentinc/ksql/pull/1451>`_ - KSQL-1093: Fix munged KSQL query code block
* `PR-1430 <https://github.com/confluentinc/ksql/pull/1430>`_ - Updated AST builder to handle dot notation.
* `PR-1442 <https://github.com/confluentinc/ksql/pull/1442>`_ - Simplified Security
* `PR-1445 <https://github.com/confluentinc/ksql/pull/1445>`_ - Prefixed output topics
* `PR-1433 <https://github.com/confluentinc/ksql/pull/1433>`_ - update packaging to include ext dir for udfs
* `PR-1426 <https://github.com/confluentinc/ksql/pull/1426>`_ - Throw exception when calling FunctionRegistry#getUdfFactory(name) if function doesn't exist
* `PR-1429 <https://github.com/confluentinc/ksql/pull/1429>`_ - UDF tidy ups
* `PR-1423 <https://github.com/confluentinc/ksql/pull/1423>`_ - Collect metrics for UDFs
* `PR-1412 <https://github.com/confluentinc/ksql/pull/1412>`_ - DOCS-415: New topic on KSQL custom test data
* `PR-1416 <https://github.com/confluentinc/ksql/pull/1416>`_ - Validate that function names are valid java identifiers
* `PR-1428 <https://github.com/confluentinc/ksql/pull/1428>`_ - Minor style change in comment.
* `PR-1410 <https://github.com/confluentinc/ksql/pull/1410>`_ - Added query rewrite feature using AST.
* `PR-1414 <https://github.com/confluentinc/ksql/pull/1414>`_ - Add missing comma to fix broken REST API example
* `PR-1415 <https://github.com/confluentinc/ksql/pull/1415>`_ - Add year 2018 to the copyright info in the CLI banner
* `PR-1366 <https://github.com/confluentinc/ksql/pull/1366>`_ - Create and load Java based UDFs from an ext directory
* `PR-1362 <https://github.com/confluentinc/ksql/pull/1362>`_ - Return schema description API entities as JSON objects
* `PR-1400 <https://github.com/confluentinc/ksql/pull/1400>`_ - Add struct support for avro by using Connect Converter API
* `PR-1406 <https://github.com/confluentinc/ksql/pull/1406>`_ - #1396 Fix regression issue with string concat using '+' operator.
* `PR-1407 <https://github.com/confluentinc/ksql/pull/1407>`_ - Avoid NPE if not function exists that handles required types.
* `PR-1401 <https://github.com/confluentinc/ksql/pull/1401>`_ - Switch to optional schema fields since we can have null values.
* `PR-1409 <https://github.com/confluentinc/ksql/pull/1409>`_ - Issue #1111: Document schemaRegistryURL arg for ksql-datagen in usage…
* `PR-1402 <https://github.com/confluentinc/ksql/pull/1402>`_ - add test for ws schema serde
* `PR-1408 <https://github.com/confluentinc/ksql/pull/1408>`_ - MINOR: Remove use of deprecated method removed in AK
* `PR-1383 <https://github.com/confluentinc/ksql/pull/1383>`_ - Using Connect data format in KSQL.
* `PR-1140 <https://github.com/confluentinc/ksql/pull/1140>`_ - Do schema inference before writing to output topic
* `PR-1370 <https://github.com/confluentinc/ksql/pull/1370>`_ - Wrap api query id strings in a class
* `PR-1368 <https://github.com/confluentinc/ksql/pull/1368>`_ - Using ExpressionTypeManager to detect the types for function look up.
* `PR-1379 <https://github.com/confluentinc/ksql/pull/1379>`_ - Disable server identification verification for SecureIntegrationTest
* `PR-1378 <https://github.com/confluentinc/ksql/pull/1378>`_ - Update with May 2018 preview release information
* `PR-1347 <https://github.com/confluentinc/ksql/pull/1347>`_ - CP KSQL CLI docker image
* `PR-1372 <https://github.com/confluentinc/ksql/pull/1372>`_ - Drop cp-ksql-server from jenkinsfile as well
* `PR-1371 <https://github.com/confluentinc/ksql/pull/1371>`_ - Remove cp-ksql-server from the build to unblock packaging
* `PR-1369 <https://github.com/confluentinc/ksql/pull/1369>`_ - Using the kafka topic name in delete topic message.
* `PR-1358 <https://github.com/confluentinc/ksql/pull/1358>`_ - Skip building jars for the ksql cp docker image.
* `PR-1363 <https://github.com/confluentinc/ksql/pull/1363>`_ - Print an error if a message fails to produce with datagen
* `PR-1360 <https://github.com/confluentinc/ksql/pull/1360>`_ - Fix checkstyle failures
* `PR-1356 <https://github.com/confluentinc/ksql/pull/1356>`_ - New geo distance function
* `PR-1359 <https://github.com/confluentinc/ksql/pull/1359>`_ - handle array subsrcipts when generating function args
* `PR-1358 <https://github.com/confluentinc/ksql/pull/1358>`_ - Skip building jars for the ksql cp docker image.
* `PR-1353 <https://github.com/confluentinc/ksql/pull/1353>`_ - Support functions with same name but different arguments
* `PR-1354 <https://github.com/confluentinc/ksql/pull/1354>`_ - HOTFIX: Bump POM version for cp-ksql-server
* `PR-1334 <https://github.com/confluentinc/ksql/pull/1334>`_ - Add ksql-examples to the class path when running ksql-datagen
* `PR-1322 <https://github.com/confluentinc/ksql/pull/1322>`_ - CP docker image for KSQL server
* `PR-1285 <https://github.com/confluentinc/ksql/pull/1285>`_ - Refactor WebSocket endpoints and add support for print topic
* `PR-1349 <https://github.com/confluentinc/ksql/pull/1349>`_ - Minor: Add test around extract JSON array field.
* `PR-1350 <https://github.com/confluentinc/ksql/pull/1350>`_ - Child first class loader for UDFs
* `PR-1344 <https://github.com/confluentinc/ksql/pull/1344>`_ - Add syntax ref for INSERT INTO
* `PR-1337 <https://github.com/confluentinc/ksql/pull/1337>`_ - Fix the way `LIMIT` clauses are handled
* `PR-1321 <https://github.com/confluentinc/ksql/pull/1321>`_ - Make FunctionRegistry an interface and make MetaStore implement it
* `PR-1340 <https://github.com/confluentinc/ksql/pull/1340>`_ - Post fix udf instance names to ensure they are unique.
* `PR-1341 <https://github.com/confluentinc/ksql/pull/1341>`_ - Fix system tests (ish)
* `PR-1335 <https://github.com/confluentinc/ksql/pull/1335>`_ - Don't ignore leading spaces when saving history
* `PR-1331 <https://github.com/confluentinc/ksql/pull/1331>`_ - Follow-on updates for KSQL_OPTS
* `PR-1333 <https://github.com/confluentinc/ksql/pull/1333>`_ - Inherit maven-compiler-plugin definition from common
* `PR-1329 <https://github.com/confluentinc/ksql/pull/1329>`_ - Fix run class to work with Java 10 and use ExplicitGCInvokesConcurrent
* `PR-1330 <https://github.com/confluentinc/ksql/pull/1330>`_ - Make KSQL_OPTS more prominent
* `PR-1301 <https://github.com/confluentinc/ksql/pull/1301>`_ - Minor: Admin client leak
* `PR-1315 <https://github.com/confluentinc/ksql/pull/1315>`_ - Consumed imports
* `PR-1277 <https://github.com/confluentinc/ksql/pull/1277>`_ - Ksql 1217 optionally delete kafka topic with drop statement
* `PR-1309 <https://github.com/confluentinc/ksql/pull/1309>`_ - Add crosslink from 'starting KSQL server' section to the headless mode instructions
* `PR-1114 <https://github.com/confluentinc/ksql/pull/1114>`_ - Struct Data Type(Part-1): New struct type, DDL statements and Describe
* `PR-1306 <https://github.com/confluentinc/ksql/pull/1306>`_ - Fix the clickstream demo
* `PR-1307 <https://github.com/confluentinc/ksql/pull/1307>`_ - Rename KSQL HTTP API to KSQL REST API
* `PR-1305 <https://github.com/confluentinc/ksql/pull/1305>`_ - MINOR: Add query validation test for stringtotimestamp with double single quote
* `PR-1303 <https://github.com/confluentinc/ksql/pull/1303>`_ - The DESCRIBE ACL on the __consumer_offsets topic is not required.
* `PR-1091 <https://github.com/confluentinc/ksql/pull/1091>`_ - Add parent reference to ast nodes
* `PR-1296 <https://github.com/confluentinc/ksql/pull/1296>`_ - Remove deprecated punctuate
* `PR-1294 <https://github.com/confluentinc/ksql/pull/1294>`_ - Fix production config docs
* `PR-1167 <https://github.com/confluentinc/ksql/pull/1167>`_ - DOCS-397 - Replace hard-coded version references with variables
* `PR-1249 <https://github.com/confluentinc/ksql/pull/1249>`_ - Include function names in error message for unsupported table aggrega…
* `PR-1220 <https://github.com/confluentinc/ksql/pull/1220>`_ - Add external dependency on Avro Random Generator and remove its source code
* `PR-576 <https://github.com/confluentinc/ksql/pull/576>`_ - Insert into implementation
* `PR-1256 <https://github.com/confluentinc/ksql/pull/1256>`_ - Better reporting of invalid serverAddress or connection issues in CLI
* `PR-1197 <https://github.com/confluentinc/ksql/pull/1197>`_ - KSQL-883: Add KafkaClientSupplier to KSQL API
* `PR-1278 <https://github.com/confluentinc/ksql/pull/1278>`_ - Add Preview Release information, update Latest News
* `PR-1219 <https://github.com/confluentinc/ksql/pull/1219>`_ - Improved error message for Stream/Table and Query relation correctness.
* `PR-1254 <https://github.com/confluentinc/ksql/pull/1254>`_ - Update clickstream to 5.0.0-beta1 (#1248)
* `PR-1242 <https://github.com/confluentinc/ksql/pull/1242>`_ - Work around a race condition in the test ZK instance.
* `PR-1247 <https://github.com/confluentinc/ksql/pull/1247>`_ - Update docker quickstart for 5.0.0-beta1
* `PR-1201 <https://github.com/confluentinc/ksql/pull/1201>`_ - Add versioning to the API (#1151)
* `PR-1240 <https://github.com/confluentinc/ksql/pull/1240>`_ - 5.0 API docs
* `PR-1210 <https://github.com/confluentinc/ksql/pull/1210>`_ - Fix some more rest api inconsistencies
* `PR-1136 <https://github.com/confluentinc/ksql/pull/1136>`_ - Table aggregations
* `PR-1163 <https://github.com/confluentinc/ksql/pull/1163>`_ - Minor: Fix lifecycle of AdminClient and KafkaTopicClientImpl instances.
* `PR-1208 <https://github.com/confluentinc/ksql/pull/1208>`_ - Add a Pull request template
* `PR-1159 <https://github.com/confluentinc/ksql/pull/1159>`_ - Fix Rest API redirects.
* `PR-1185 <https://github.com/confluentinc/ksql/pull/1185>`_ - Fix flaky test, caused by async topic creation.
* `PR-1216 <https://github.com/confluentinc/ksql/pull/1216>`_ - Clarify that KSQL timestamps are in milliseconds.
* `PR-1050 <https://github.com/confluentinc/ksql/pull/1050>`_ - Escape discovered avro field name if it is a ksql lexer token literal #(1043)
* `PR-1198 <https://github.com/confluentinc/ksql/pull/1198>`_ - text is the new binary
* `PR-1147 <https://github.com/confluentinc/ksql/pull/1147>`_ - SourceDescription cleanup + listing with descriptions
* `PR-637 <https://github.com/confluentinc/ksql/pull/637>`_ - [DOC] Small java doc improvement for KsqlAggregateFunction
* `PR-1183 <https://github.com/confluentinc/ksql/pull/1183>`_ - MINOR: Make README ksql blurb consistent with the one on 4.1.x
* `PR-1179 <https://github.com/confluentinc/ksql/pull/1179>`_ - Shutdown cleaning should there be an error on start up.
* `PR-1177 <https://github.com/confluentinc/ksql/pull/1177>`_ - Minor: Fix flakey KafkaTopicClient integration tests
* `PR-1173 <https://github.com/confluentinc/ksql/pull/1173>`_ - Fix flakey CliTest and issue with LIMIT clause not being honoured.
* `PR-1166 <https://github.com/confluentinc/ksql/pull/1166>`_ - Allow tests with Kafka cluster to be run more than once in IDE.
* `PR-1148 <https://github.com/confluentinc/ksql/pull/1148>`_ - Remove unused module types.
* `PR-1174 <https://github.com/confluentinc/ksql/pull/1174>`_ - Fix handling of table tombstones
* `PR-1171 <https://github.com/confluentinc/ksql/pull/1171>`_ - Add back key-constraints
* `PR-1160 <https://github.com/confluentinc/ksql/pull/1160>`_ - Remove .md documentation. Move docs-rst to docs
* `PR-838 <https://github.com/confluentinc/ksql/pull/838>`_ - Rename 'Kafka output topic' to just 'Kafka topic' in describe ext (#838)
* `PR-1155 <https://github.com/confluentinc/ksql/pull/1155>`_ - Update README for KSQL 4.1
* `PR-1158 <https://github.com/confluentinc/ksql/pull/1158>`_ - Fix failing build
* `PR-1109 <https://github.com/confluentinc/ksql/pull/1109>`_ - Ksql 1054 better aggregation with complex expressions
* `PR-652 <https://github.com/confluentinc/ksql/pull/652>`_ - Added referential integrity enforcement for streams/tables and queries
* `PR-756 <https://github.com/confluentinc/ksql/pull/756>`_ - Add the ability to extract the record timestamp from a string field #646
* `PR-1130 <https://github.com/confluentinc/ksql/pull/1130>`_ - Return proper errors from the HTTP server endpoints
* `PR-1134 <https://github.com/confluentinc/ksql/pull/1134>`_ - Add logging to the CliTest
* `PR-1098 <https://github.com/confluentinc/ksql/pull/1098>`_ - Docs on configuring KSQL -> SR over HTTPS
* `PR-944 <https://github.com/confluentinc/ksql/pull/944>`_ - Ksql 660 schema registry clean up
* `PR-1126 <https://github.com/confluentinc/ksql/pull/1126>`_ - Don't depend on the internal 'PlainSaslServer' class from kafka
* `PR-1103 <https://github.com/confluentinc/ksql/pull/1103>`_ - Return more stuff from the rest API
* `PR-1122 <https://github.com/confluentinc/ksql/pull/1122>`_ - fix spelling curretnly -> currently
* `PR-1120 <https://github.com/confluentinc/ksql/pull/1120>`_ - update error messages when failing to parse an avro schema
* `PR-1108 <https://github.com/confluentinc/ksql/pull/1108>`_ - add websockets query endpoint
* `PR-1107 <https://github.com/confluentinc/ksql/pull/1107>`_ - Build clickstream docker img with 4.1 cp
