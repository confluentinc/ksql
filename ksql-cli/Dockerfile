FROM confluentinc/cp-base

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/confluent/ksql-cli-1.0-SNAPSHOT-standalone.jar", "local", "--bootstrap-server=kafka:29092"]

ADD target/ksql-cli-1.0-SNAPSHOT-standalone.jar /usr/share/confluent/ksql-cli-1.0-SNAPSHOT-standalone.jar
