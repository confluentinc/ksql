/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.serde.csv.KSQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DDLEngine {

  KSQLEngine ksqlEngine;

  public DDLEngine(KSQLEngine ksqlEngine) {
    this.ksqlEngine = ksqlEngine;
  }

  public KSQLTopic createTopic(final CreateTopic createTopic) {

    String topicName = createTopic.getName().getSuffix();
    if (ksqlEngine.getMetaStore().getTopic(topicName) != null) {
      if (createTopic.isNotExists()) {
        System.out.println("Topic already exists.");
      } else {
        throw new KSQLException("Topic already exists.");
      }
      return null;
    }

    if (createTopic.getProperties().size() == 0) {
      throw new KSQLException("Create topic statement needs WITH clause.");
    }

    if (createTopic.getProperties().get(DDLConfig.FORMAT_PROPERTY) == null) {
      throw new KSQLException("Topic format(format) should be set in WITH clause.");
    }
    String serde = createTopic.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString();
    serde = enforceString(DDLConfig.FORMAT_PROPERTY, serde);

    if (createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY) == null) {
      throw new KSQLException("Corresponding kafka topic should be set in WITH clause.");
    }
    String
        kafkaTopicName =
        createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
    KSQLTopicSerDe topicSerDe;

    // TODO: Find a way to avoid calling toUpperCase() here; if the property can be an unquoted identifier, then
    // capitalization will have already happened
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        if (createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) == null) {
          throw new KSQLException("Avro schema file path should be set for avro topics.");
        }
        String avroSchemaFile = createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
        avroSchemaFile = enforceString(DDLConfig.AVRO_SCHEMA_FILE, avroSchemaFile);
        try {
          String avroSchema = getAvroSchema(avroSchemaFile);
          topicSerDe = new KSQLAvroTopicSerDe(avroSchemaFile, avroSchema);
        } catch (IOException e) {
          throw new KSQLException("Could not read avro schema from file: " + avroSchemaFile);
        }
        break;
      case DataSource.JSON_SERDE_NAME:
        topicSerDe = new KSQLJsonTopicSerDe(null);
        break;
      case DataSource.CSV_SERDE_NAME:
        topicSerDe = new KSQLCsvTopicSerDe();
        break;
      default:
        throw new KSQLException("The specified topic serde is not supported.");
    }
    KSQLTopic ksqlTopic = new KSQLTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putTopic(ksqlTopic);
    return ksqlTopic;
  }

  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KSQLException(propertyName + " value is string and should be enclosed between "
                             + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  public void dropTopic(final DropTable dropTable) {

    String topicName = dropTable.getTableName().getSuffix();
    new DDLUtil().deleteTopic(topicName);
    ksqlEngine.getMetaStore().deleteSource(topicName);
  }

  public KSQLStream createStream(final CreateStream createStream) {

    String streamName = createStream.getName().getSuffix();
    if (ksqlEngine.getMetaStore().getSource(streamName) != null) {
      if (createStream.isNotExists()) {
        System.out.println("Stream already exists.");
      } else {
        throw new KSQLException("Stream already exists.");
      }
      return null;
    }

    if (createStream.getElements().size() == 0) {
      throw new KSQLException("No column was specified.");
    }

    SchemaBuilder streamSchema = SchemaBuilder.struct();
    for (TableElement tableElement : createStream.getElements()) {
      streamSchema = streamSchema.field(tableElement.getName(), getKSQLType(tableElement.getType()));
    }

    if (createStream.getProperties().size() == 0) {
      throw new KSQLException("Create stream statement needs WITH clause.");
    }

    if (createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KSQLException("Topic for the stream should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since topic names (if put in quotes) can be case-sensitive
    String topicName = createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    String keyName = "";
    if (createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) != null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes) can be case-sensitive
      keyName = createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString().toUpperCase();
      keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);
    }

    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KSQLException(String.format("The corresponding topic, %s, does not exist.", topicName));
    }

    KSQLStream
        ksqlStream =
        new KSQLStream(streamName, streamSchema, (keyName.length() == 0) ? null : streamSchema.field(keyName),
                                             ksqlEngine.getMetaStore().getTopic(topicName));

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(ksqlStream);
    return ksqlStream;
  }

  public KSQLTable createTable(final CreateTable createTable) {

    String tableName = createTable.getName().getSuffix();
    if (ksqlEngine.getMetaStore().getSource(tableName) != null) {
      if (createTable.isNotExists()) {
        System.out.println("Table already exists.");
      } else {
        throw new KSQLException("Table already exists.");
      }
      return null;
    }

    if (createTable.getElements().size() == 0) {
      throw new KSQLException("No column was specified.");
    }

    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (TableElement tableElement : createTable.getElements()) {
      tableSchema = tableSchema.field(tableElement.getName(), getKSQLType(tableElement.getType()));
    }

    if (createTable.getProperties().size() == 0) {
      throw new KSQLException("Create table statement needs WITH clause.");
    }

    if (createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KSQLException("Topic for the table should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since topic names (if put in quotes) can be case-sensitive
    String topicName = createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY) == null) {
      throw new KSQLException(
          "State store name for the table should be set in WITH clause.");
    }

    String stateStoreName = createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY)
        .toString();
    stateStoreName = enforceString(DDLConfig.STATE_STORE_NAME_PROPERTY, stateStoreName);

    String keyName = "";
    if (createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) == null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes) can be case-sensitive
      keyName = createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString().toUpperCase();
      keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);
    }


    boolean isWindowed = false;
    if (createTable.getProperties().get(DDLConfig.IS_WINDOWED_PROPERTY) != null) {
      String isWindowedProp = createTable.getProperties().get(DDLConfig.IS_WINDOWED_PROPERTY).toString().toUpperCase();
      try {
        isWindowed = Boolean.parseBoolean(isWindowedProp);
      } catch (Exception e) {
        throw new KSQLException("isWindowed property is not set correctly: " + isWindowedProp);
      }
    }


    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KSQLException("The corresponding topic does not exist.");
    }

    KSQLTable ksqlTable = new KSQLTable(tableName, tableSchema, (keyName.length() == 0) ? null : tableSchema.field(keyName),
                                        ksqlEngine.getMetaStore().getTopic(topicName),
                                        stateStoreName, isWindowed);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(ksqlTable);
    return ksqlTable;
  }

  //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
  private Schema getKSQLType(final String sqlType) {
    switch (sqlType) {
      case "VARCHAR":
      case "STRING":
        return Schema.STRING_SCHEMA;
      case "BOOLEAN":
      case "BOOL":
        return Schema.BOOLEAN_SCHEMA;
      case "INTEGER":
      case "INT":
        return Schema.INT32_SCHEMA;
      case "BIGINT":
      case "LONG":
        return Schema.INT64_SCHEMA;
      case "DOUBLE":
        return Schema.FLOAT64_SCHEMA;

      default:
        if (sqlType.startsWith("ARRAY")) {
          return SchemaBuilder
              .array(getKSQLType(sqlType.substring("ARRAY".length() + 1, sqlType.length() - 1)));
        } else if (sqlType.startsWith("MAP")) {
          //TODO: For now only primitive data types for map are supported. Will have to add
          // nested types.
          String[] mapTypesStrs = sqlType.substring("MAP".length() + 1, sqlType.length() - 1)
              .trim().split(",");
          if (mapTypesStrs.length != 2) {
            throw new KSQLException("Map type is not defined correctly.: " + sqlType);
          }
          String keyType = mapTypesStrs[0].trim();
          String valueType = mapTypesStrs[1].trim();
          return SchemaBuilder.map(getKSQLType(keyType), getKSQLType(valueType));
        }
        throw new KSQLException("Unsupported type: " + sqlType);
    }
  }

  private String getAvroSchema(final String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }
}
