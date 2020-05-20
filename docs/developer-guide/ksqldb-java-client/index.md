---
layout: page
title: Java Client for ksqlDB
tagline: Java client for ksqlDB
description: Send requests to ksqlDB from your Java app
---

- [Receive query results one row at a time (streamQuery())](./stream-query.md)
- [Receive query results in a single batch (executeQuery())](./execute-query.md)
- [Terminate a push query (terminatePushQuery())](./terminate-push-query.md)

Getting Started
---------------

Start by creating a `pom.xml` for your Java application:

```xml
<?xml version="1.0" encoding="UTF-8"?>

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>my.ksqldb.app</groupId>
    <artifactId>my-ksqldb-app</artifactId>
    <version>0.0.1</version>

    <properties>
        <!-- Keep versions as properties to allow easy modification -->
        <java.version>8</java.version>
        <ksqldb.version>{{ site.release }}</ksqldb.version>
        <!-- Maven properties for compilation -->
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.confluent.ksql</groupId>
            <artifactId>ksqldb-api-client</artifactId>
            <version>${ksqldb.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-compiler-plugin</artifactId>
              <version>3.8.1</version>
              <configuration>
                <source>${java.version}</source>
                <target>${java.version}</target>
                <compilerArgs>
                  <arg>-Xlint:all</arg>
                </compilerArgs>
              </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

Create your example app at `src/main/java/my/ksqldb/app/ExampleApp.java`:

```java
package my.ksqldb.app;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ExampleApp {

  public static final String KSQLDB_SERVER_HOST = "localhost";
  public static final int KSQLDB_SERVER_HOST_PORT = 8088;

  static {
    PUSH_QUERY_PROPERTIES = new HashMap<>();
    PUSH_QUERY_PROPERTIES.put("auto.offset.reset", "earliest");
  }
  
  public static void main(String[] args) {
    final ClientOptions options = ClientOptions.create()
        .setHost(KSQLDB_SERVER_HOST)
        .setPort(KSQLDB_SERVER_HOST_PORT);
    final Client client = Client.create(options);
    
    // Send requests with the client by following the other examples
    
    client.close();
  }
}
```
