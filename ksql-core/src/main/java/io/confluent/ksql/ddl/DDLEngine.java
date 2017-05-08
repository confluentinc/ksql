/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KQLEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.KQLTopic;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KQLJsonTopicSerDe;
import io.confluent.ksql.util.KQLException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DDLEngine {

  KQLEngine kqlEngine;

  public DDLEngine(KQLEngine kqlEngine) {
    this.kqlEngine = kqlEngine;
  }

  public KQLTopic createTopic(final CreateTopic createTopic) {

    String topicName = createTopic.getName().getSuffix();
    if (kqlEngine.getMetaStore().getTopic(topicName) != null) {
      if (createTopic.isNotExists()) {
        System.out.println("Topic already exists.");
      } else {
        throw new KQLException("Topic already exists.");
      }
      return null;
    }

    if (createTopic.getProperties().size() == 0) {
      throw new KQLException("Create topic statement needs WITH clause.");
    }

    if (createTopic.getProperties().get(DDLConfig.FORMAT_PROPERTY) == null) {
      throw new KQLException("Topic format(format) should be set in WITH clause.");
    }
    String serde = createTopic.getProperties().get(DDLConfig.FORMAT_PROPERTY).toString();
    serde = enforceString(DDLConfig.FORMAT_PROPERTY, serde);

    if (createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY) == null) {
      throw new KQLException("Corresponding kafka topic should be set in WITH clause.");
    }
    String
        kafkaTopicName =
        createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
    KQLTopicSerDe topicSerDe;

    // TODO: Find a way to avoid calling toUpperCase() here; if the property can be an unquoted identifier, then
    // capitalization will have already happened
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        if (createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) == null) {
          throw new KQLException("Avro schema file path should be set for avro topics.");
        }
        String avroSchemaFile = createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
        avroSchemaFile = enforceString(DDLConfig.AVRO_SCHEMA_FILE, avroSchemaFile);
        try {
          String avroSchema = getAvroSchema(avroSchemaFile);
          topicSerDe = new KQLAvroTopicSerDe(avroSchemaFile, avroSchema);
        } catch (IOException e) {
          throw new KQLException("Could not read avro schema from file: " + avroSchemaFile);
        }
        break;
      case DataSource.JSON_SERDE_NAME:
        topicSerDe = new KQLJsonTopicSerDe(null);
        break;
      case DataSource.CSV_SERDE_NAME:
        topicSerDe = new KQLCsvTopicSerDe();
        break;
      default:
        throw new KQLException("The specified topic serde is not supported.");
    }
    KQLTopic kQLTopic = new KQLTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    kqlEngine.getMetaStore().putTopic(kQLTopic);
    return kQLTopic;
  }

  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KQLException(propertyName + " value is string and should be enclosed between "
                             + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  public void dropTopic(final DropTable dropTable) {

    String topicName = dropTable.getTableName().getSuffix();
    new DDLUtil().deleteTopic(topicName);
    kqlEngine.getMetaStore().deleteSource(topicName);
  }

  public KQLStream createStream(final CreateStream createStream) {

    String streamName = createStream.getName().getSuffix();
    if (kqlEngine.getMetaStore().getSource(streamName) != null) {
      if (createStream.isNotExists()) {
        System.out.println("Stream already exists.");
      } else {
        throw new KQLException("Stream already exists.");
      }
      return null;
    }

    if (createStream.getElements().size() == 0) {
      throw new KQLException("No column was specified.");
    }

    SchemaBuilder streamSchema = SchemaBuilder.struct();
    for (TableElement tableElement : createStream.getElements()) {
      streamSchema = streamSchema.field(tableElement.getName(), getKQLType(tableElement.getType()));
    }

    if (createStream.getProperties().size() == 0) {
      throw new KQLException("Create stream statement needs WITH clause.");
    }

    if (createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KQLException("Topic for the stream should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since topic names (if put in quotes) can be case-sensitive
    String topicName = createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) == null) {
      throw new KQLException("Key field name for the stream should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes) can be case-sensitive
    String keyName = createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString().toUpperCase();
    keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);

    if (kqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KQLException(String.format("The corresponding topic, %s, does not exist.", topicName));
    }

    KQLStream
        kqlStream =
        new KQLStream(streamName, streamSchema, streamSchema.field(keyName),
                      kqlEngine.getMetaStore().getTopic(topicName));

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    kqlEngine.getMetaStore().putSource(kqlStream);
    return kqlStream;
  }

  public KQLTable createTable(final CreateTable createTable) {

    String tableName = createTable.getName().getSuffix();
    if (kqlEngine.getMetaStore().getSource(tableName) != null) {
      if (createTable.isNotExists()) {
        System.out.println("Table already exists.");
      } else {
        throw new KQLException("Table already exists.");
      }
      return null;
    }

    if (createTable.getElements().size() == 0) {
      throw new KQLException("No column was specified.");
    }

    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (TableElement tableElement : createTable.getElements()) {
      tableSchema = tableSchema.field(tableElement.getName(), getKQLType(tableElement.getType()));
    }

    if (createTable.getProperties().size() == 0) {
      throw new KQLException("Create table statement needs WITH clause.");
    }

    if (createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KQLException("Topic for the table should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since topic names (if put in quotes) can be case-sensitive
    String topicName = createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY) == null) {
      throw new KQLException(
          "State store name for the table should be set in WITH clause.");
    }

    String stateStoreName = createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY)
        .toString();
    stateStoreName = enforceString(DDLConfig.STATE_STORE_NAME_PROPERTY, stateStoreName);

    if (createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) == null) {
      throw new KQLException("Key field name for the stream should be set in WITH clause.");
    }

    // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes) can be case-sensitive
    String keyName = createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString().toUpperCase();
    keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);

    boolean isWindowed = false;
    if (createTable.getProperties().get(DDLConfig.IS_WINDOWED_PROPERTY) != null) {
      String isWindowedProp = createTable.getProperties().get(DDLConfig.IS_WINDOWED_PROPERTY).toString().toUpperCase();
      try {
        isWindowed = Boolean.parseBoolean(isWindowedProp);
      } catch (Exception e) {
        throw new KQLException("isWindowed property is not set correctly: " + isWindowedProp);
      }
    }


    if (kqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KQLException("The corresponding topic does not exist.");
    }

    KQLTable kqlTable = new KQLTable(tableName, tableSchema, tableSchema.field(keyName),
                                     kqlEngine.getMetaStore().getTopic(topicName),
                                     stateStoreName, isWindowed);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    kqlEngine.getMetaStore().putSource(kqlTable);
    return kqlTable;
  }

  //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
  private Schema getKQLType(final String sqlType) {
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
              .array(getKQLType(sqlType.substring("ARRAY".length() + 1, sqlType.length() - 1)));
        } else if (sqlType.startsWith("MAP")) {
          //TODO: For now only primitive data types for map are supported. Will have to add
          // nested types.
          String[] mapTypesStrs = sqlType.substring("MAP".length() + 1, sqlType.length() - 1)
              .trim().split(",");
          if (mapTypesStrs.length != 2) {
            throw new KQLException("Map type is not defined correctly.: " + sqlType);
          }
          String keyType = mapTypesStrs[0].trim();
          String valueType = mapTypesStrs[1].trim();
          return SchemaBuilder.map(getKQLType(keyType), getKQLType(valueType));
        }
        throw new KQLException("Unsupported type: " + sqlType);
    }
  }

  private String getAvroSchema(final String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }
}
