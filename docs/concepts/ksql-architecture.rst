.. _ksql-architecture:

KSQL Architecture
#################

For more information, see :ref:`streams_architecture`.

KSQL Deployment Modes
*********************

KSQL Server comprises the KSQL Engine, which processes KSQL queries, and the
REST API, which enables access to the engine.

Deployment
Two modes: non-interactive, aka "headless", 
Long-running production deployments
Resource isolation
No REST API
KSQL servers with a SQL file


Interactive service, which enables data exploration and pipeline design.
REST API
Command topic
(animated gif)

KSQL Query Lifecycle
********************

KSQL Engine executes KSQL statements

Data Definition Language (DDL) statements
only metadata updates

CREATE TABLE
CREATE STREAM
DROP STREAM
...

Data Manipulation Language (DML) statements

Compiled in Kafka Streams applications

CREATE TABLE AS SELECT
CREATE STREAM AS SELECT
SELECT
INSERT INTO



