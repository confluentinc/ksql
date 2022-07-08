# ksqlDB UDF / UDAF Quickstart
This project includes a Maven archetype that can be used for quickly bootstrapping custom KSQL UDFs and UDAFs. Generating a project from this archetype will produce a couple of example functions (a UDF named `REVERSE` and a UDAF named `SUMMARY_STATS`) that are ready to be deployed to your KSQL server. Feel free to use these examples as a starting point for your own custom KSQL functions.

# Usage
First, run the following command to generate a new project from this archetype.

```bash
$ mvn archetype:generate -X \
    -DarchetypeGroupId=io.confluent.ksql \
    -DarchetypeArtifactId=ksqldb-udf-quickstart \
    -DarchetypeVersion=6.2.0-SNAPSHOT \
    -DgroupId=com.example.ksql.functions \
    -DartifactId=my-udf \
    -Dauthor=Bob \
    -Dversion=0.1.0-SNAPSHOT
```

This will copy an example UDF and UDAF into the `my-udf` directory. Be sure to update the `groupId`, `artifactId`, `author` and `version` properties in the command above to the appropriate values for your project.

Once you're ready to deploy your custom functions, run the following command to build an uber JAR.

```bash
$ mvn clean package
```

Now, all you need to do is copy the JAR to the KSQL extension directory (see the `ksql.extension.dir` setting).

```bash
# copy the JAR to the KSQL extenstion directory
$ cp target/my-udf-0.1.0-SNAPSHOT.jar /path/to/ext/
```

Finally, run `SHOW FUNCTIONS` from the CLI and you should see your new UDF / UDAF in the list :)

# Development workflow

## Install Locally
You can install this archetype to a local maven repo for testing changes. Simply run the following:

```bash
$ mvn install
```

## Deploy artifacts
First, ensure the Nexus credentials are added to `$HOME/.m2/settings.xml`.

```bash
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                       https://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <username>...</username>
            <password>...</password>
            <id>confluent</id>
        </server>
        <server>
            <username>...</username>
            <password>...</password>
            <id>confluent-snapshots</id>
        </server>
  </servers>
</settings>
```

Then, simply run:

```bash
$ mvn deploy
```
