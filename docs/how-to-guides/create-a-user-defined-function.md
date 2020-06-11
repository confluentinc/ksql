# How to create a user-defined function

## Context

You have a piece of logic for transforming or aggregating events that ksqlDB can’t currently express. You want to extend ksqlDB to apply that logic in your persistent queries. To do that, ksqlDB exposes hooks so that you can add new logic through Java programs. This functionality is called *user-defined functions*, or UDFs for short.

## In action

```java
package my.company.ksqldb.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two non-nullable INTs.")
  public long multiply(@UdfParameter(value = "v1") final int v1, @UdfParameter(value="v2") final int v2) {
    return v1 * v2;
  }

}
```

## Set up a Java project

To implement a user-defined function, the first thing that you need to do is create a Java project with a dependency on ksqlDB’s UDF library. This library contains the annotations that you’ll use to signal that the classes you’re implementing aren’t just any old classes, they represent UDFs that ksqlDB can use. You can manage your Java project with any build tool, but this guide demonstrates how it works with Gradle. In the end, all that matters is that you’re able to put an uberjar on ksqlDB’s classpath.

In a fresh directory, create the following `build.gradle` file:

```
buildscript {
    repositories {
        jcenter()
    }
}

plugins {
    id "java"
}

sourceCompatibility = "1.8"
targetCompatibility = "1.8"
version = "0.0.1"

repositories {
    mavenCentral()
    jcenter()

    maven {
        url "http://packages.confluent.io/maven"
    }
}

dependencies {
    compile 'io.confluent.ksql:ksql-udf:5.5.0'
}

task copyJar(type: Copy) {
    from jar
    into "extensions/"
}

build.dependsOn copyJar
```

## Implement the classes

There are three kinds of UDFs for manipulating rows in different ways: plain functions, tabular functions, and aggregation functions. Each is demonstrated below. Start by creating a directory to house the class files:

```
mkdir -p src/main/java/com/example
```

### Functions

A plain function takes one row as input and produces one row as output. Use this when you want to simply transform a value.

### Tabular functions

A tabular function takes one row as input and produces zero or more rows as output. This is sometimes called "flat map" or "mapcat" in different programming languages. Use this when a value implicitly represents many other values and needs to be "exploded" to be useful.

### Aggregation functions

## Add the uberjar to the classpath

## Invoke the functions