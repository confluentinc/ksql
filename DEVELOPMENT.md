# Developer notes

## Running locally

To build and run KSQL locally, run the following commands:

```
$ mvn clean package -DskipTests
$ ./bin/ksql-server-start -daemon config/ksql-server.properties
$ ./bin/ksql
```

This will start the KSQL server in the background and the KSQL CLI in the
foreground.

If you would rather have the KSQL server logs spool to the console, then
drop the `-daemon` switch and start the CLI in a second console.

## Testing changes locally

To build and test changes locally, run the following commands:

```
$ mvn clean install checkstyle:check integration-test