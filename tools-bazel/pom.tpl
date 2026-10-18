<?xml version="1.0" encoding="UTF-8"?>
<!-- POM template for published ksqlDB artifacts (rules_jvm_external
     java_export pom_template). The groupId / artifactId / version /
     dependencies placeholders below are substituted by the build; the version
     resolves to $(maven_version) (the CP nano-version). Do not spell the
     placeholders in this comment: they would be substituted here too. -->
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd"
         xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <modelVersion>4.0.0</modelVersion>

    <groupId>{groupId}</groupId>
    <artifactId>{artifactId}</artifactId>
    <version>{version}</version>
    <packaging>jar</packaging>
    <url>https://www.confluent.io/</url>
    <licenses>
        <license>
            <name>Confluent Community License</name>
            <url>https://www.confluent.io/confluent-community-license</url>
            <distribution>repo</distribution>
        </license>
    </licenses>

    <dependencies>
{dependencies}
    </dependencies>
</project>
