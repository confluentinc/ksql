# Logging

- operational log and processing log. Can be routed differently
- former is for ops issues: is the cluster up and running?
- latter is info about what ksqlDB is doing as it processes records
- Ops log uses Log4j via a JVM variable to pass a logging config
- example logging configuration
- can't change log level at runtime