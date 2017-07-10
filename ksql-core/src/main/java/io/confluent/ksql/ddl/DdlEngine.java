/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class DdlEngine {

  KsqlEngine ksqlEngine;

  public DdlEngine(KsqlEngine ksqlEngine) {
    this.ksqlEngine = ksqlEngine;
  }

  public KsqlTopic registerTopic(final RegisterTopic registerTopic, final Map topicProperties) {

    String topicName = registerTopic.getName().getSuffix();
    if (ksqlEngine.getMetaStore().getTopic(topicName) != null) {
      if (registerTopic.isNotExists()) {
        System.out.println("Topic already exists.");
      } else {
        throw new KsqlException("Topic already exists.");
      }
      return null;
    }

    enforceTopicProperties(registerTopic);
    String serde = registerTopic.getProperties().get(DdlConfig.FORMAT_PROPERTY).toString();
    serde = enforceString(DdlConfig.FORMAT_PROPERTY, serde);

    String
        kafkaTopicName =
        registerTopic.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    kafkaTopicName = enforceString(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
    KsqlTopicSerDe topicSerDe;

    // TODO: Find a way to avoid calling toUpperCase() here;
    // if the property can be an unquoted identifier, then capitalization will have already happened
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        if (!topicProperties.containsKey(DdlConfig.AVRO_SCHEMA)) {
          throw new KsqlException("Avro schema file path should be set for avro topics.");
        }
        String avroSchema = topicProperties.get(DdlConfig.AVRO_SCHEMA).toString();
        topicSerDe = new KsqlAvroTopicSerDe(avroSchema);
        break;
      case DataSource.JSON_SERDE_NAME:
        topicSerDe = new KsqlJsonTopicSerDe(null);
        break;
      case DataSource.DELIMITED_SERDE_NAME:
        topicSerDe = new KsqlDelimitedTopicSerDe();
        break;
      default:
        throw new KsqlException("The specified topic serde is not supported.");
    }
    KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putTopic(ksqlTopic);
    return ksqlTopic;
  }

  private void enforceTopicProperties(final RegisterTopic registerTopic) {
    if (registerTopic.getProperties().size() == 0) {
      throw new KsqlException("Register topic statement needs WITH clause.");
    }

    if (registerTopic.getProperties().get(DdlConfig.FORMAT_PROPERTY) == null) {
      throw new KsqlException("Topic format(format) should be set in WITH clause.");
    }

    if (registerTopic.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY) == null) {
      throw new KsqlException("Corresponding kafka topic should be set in WITH clause.");
    }
  }

  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KsqlException(propertyName + " value is string and should be enclosed between "
          + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  public void dropTopic(final DropTopic dropTopic) {
    String topicName = dropTopic.getTopicName().getSuffix();
    ksqlEngine.getMetaStore().deleteTopic(topicName);
  }

  public void dropTable(final DropTable dropTable) {
    String tableName = dropTable.getTableName().getSuffix();
    ksqlEngine.getMetaStore().deleteSource(tableName);
  }

  public void dropStream(final DropStream dropTable) {
    String streamName = dropTable.getStreamName().getSuffix();
    ksqlEngine.getMetaStore().deleteSource(streamName);
  }

  public KsqlStream createStream(final CreateStream createStream) {

    enforceStreamProperties(createStream);
    String streamName = createStream.getName().getSuffix();
    if (ksqlEngine.getMetaStore().getSource(streamName) != null) {
      throw new KsqlException("Stream already exists.");
    }

    SchemaBuilder streamSchema = getStreamTableSchema(createStream.getElements());

    // TODO: Get rid of call to toUpperCase(),
    // TODO: since topic names (if put in quotes) can be case-sensitive
    String topicName = createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY)
        .toString().toUpperCase();
    topicName = enforceString(DdlConfig.TOPIC_NAME_PROPERTY, topicName);

    String keyColumnName = "";
    if (createStream.getProperties().get(DdlConfig.KEY_NAME_PROPERTY) != null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes)
      // TODO: can be case-sensitive
      keyColumnName = createStream.getProperties().get(DdlConfig.KEY_NAME_PROPERTY)
          .toString().toUpperCase();
      keyColumnName = enforceString(DdlConfig.KEY_NAME_PROPERTY, keyColumnName);
    }

    String timestampColumnName = "";
    if (createStream.getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY) != null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes)
      // TODO: can be case-sensitive
      timestampColumnName = createStream.getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY)
          .toString().toUpperCase();
      timestampColumnName = enforceString(DdlConfig.TIMESTAMP_NAME_PROPERTY, timestampColumnName);
      if (SchemaUtil.getFieldByName(streamSchema, timestampColumnName)
              .get().schema().type() != Schema
          .Type.INT64) {
        throw new KsqlException("Timestamp column, " + timestampColumnName + ", should be LONG"
                                + "(INT64).");
      }
    }

    KsqlStream
        ksqlStream =
        new KsqlStream(streamName, streamSchema,
                       (keyColumnName.length() == 0) ? null : streamSchema.field(keyColumnName),
                       (timestampColumnName.length() == 0) ? null :
                       streamSchema.field(timestampColumnName),
                       ksqlEngine.getMetaStore().getTopic(topicName));

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(ksqlStream.cloneWithTimeKeyColumns());
    return ksqlStream;
  }

  private void enforceStreamProperties(final CreateStream createStream) {
    String topicName = createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY)
        .toString().toUpperCase();
    topicName = enforceString(DdlConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createStream.getElements().size() == 0) {
      throw new KsqlException("No column was specified.");
    }

    if (createStream.getProperties().size() == 0) {
      throw new KsqlException("Create stream statement needs WITH clause.");
    }

    if (createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KsqlException("Topic for the stream should be set in WITH clause.");
    }

    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new
          KsqlException(String.format("The corresponding topic, %s, does not exist.", topicName));
    }
  }

  public KsqlTable createTable(final CreateTable createTable) {

    enforceTableProperties(createTable);

    SchemaBuilder tableSchema = getStreamTableSchema(createTable.getElements());

    // TODO: Get rid of call to toUpperCase(), since topic names (if put in quotes)
    // TODO: can be case-sensitive
    String topicName = createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY)
        .toString().toUpperCase();
    topicName = enforceString(DdlConfig.TOPIC_NAME_PROPERTY, topicName);


    String stateStoreName = createTable.getProperties().get(DdlConfig.STATE_STORE_NAME_PROPERTY)
        .toString();
    stateStoreName = enforceString(DdlConfig.STATE_STORE_NAME_PROPERTY, stateStoreName);

    String keyColumnName = "";
    if (createTable.getProperties().get(DdlConfig.KEY_NAME_PROPERTY) != null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes)
      // TODO: can be case-sensitive
      keyColumnName = createTable.getProperties().get(DdlConfig.KEY_NAME_PROPERTY)
          .toString().toUpperCase();
      keyColumnName = enforceString(DdlConfig.KEY_NAME_PROPERTY, keyColumnName);
    }

    String timestampColumnName = "";
    if (createTable.getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY) != null) {
      // TODO: Get rid of call to toUpperCase(), since field names (if put in quotes)
      // TODO: can be case-sensitive
      timestampColumnName = createTable.getProperties().get(DdlConfig.TIMESTAMP_NAME_PROPERTY)
          .toString().toUpperCase();
      timestampColumnName = enforceString(DdlConfig.TIMESTAMP_NAME_PROPERTY, timestampColumnName);
      if (SchemaUtil.getFieldByName(tableSchema, timestampColumnName)
              .get().schema().type() != Schema
          .Type.INT64) {
        throw new KsqlException("Timestamp column, " + timestampColumnName + ", should be LONG"
                                + "(INT64).");
      }
    }


    boolean isWindowed = false;
    if (createTable.getProperties().get(DdlConfig.IS_WINDOWED_PROPERTY) != null) {
      String isWindowedProp = createTable.getProperties().get(DdlConfig.IS_WINDOWED_PROPERTY)
          .toString().toUpperCase();
      try {
        isWindowed = Boolean.parseBoolean(isWindowedProp);
      } catch (Exception e) {
        throw new KsqlException("isWindowed property is not set correctly: " + isWindowedProp);
      }
    }

    String tableName = createTable.getName().getSuffix();

    KsqlTable ksqlTable = new KsqlTable(tableName, tableSchema,
                                        (keyColumnName.length() == 0) ? null :
                                        tableSchema.field(keyColumnName),
                                        (timestampColumnName.length() == 0) ? null :
                                        tableSchema.field(timestampColumnName),
                                        ksqlEngine.getMetaStore().getTopic(topicName),
                                        stateStoreName, isWindowed);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(ksqlTable.cloneWithTimeKeyColumns());
    return ksqlTable;
  }

  private SchemaBuilder getStreamTableSchema(List<TableElement> tableElementList) {
    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (TableElement tableElement : tableElementList) {
      if (tableElement.getName().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME) || tableElement.getName()
          .equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        throw new KsqlException(SchemaUtil.ROWTIME_NAME + "/" + SchemaUtil.ROWKEY_NAME + " are "
                                + "reserved "
                                + "token for "
                                + "implicit "
                                + "column."
                                + " You cannot use them as a column name.");

      }
      tableSchema = tableSchema.field(tableElement.getName(), getKsqlType(tableElement.getType()));
    }

    return tableSchema;
  }

  private void enforceTableProperties(final CreateTable createTable) {
    String tableName = createTable.getName().getSuffix();
    String topicName = createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY)
        .toString().toUpperCase();
    topicName = enforceString(DdlConfig.TOPIC_NAME_PROPERTY, topicName);
    if (ksqlEngine.getMetaStore().getSource(tableName) != null) {
      throw new KsqlException("Table already exists.");
    }

    if (createTable.getElements().size() == 0) {
      throw new KsqlException("No column was specified.");
    }

    if (createTable.getProperties().size() == 0) {
      throw new KsqlException("Create table statement needs WITH clause.");
    }

    if (createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KsqlException("Topic for the table should be set in WITH clause.");
    }

    if (createTable.getProperties().get(DdlConfig.STATE_STORE_NAME_PROPERTY) == null) {
      throw new KsqlException(
          "State store name for the table should be set in WITH clause.");
    }

    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KsqlException("The corresponding topic does not exist.");
    }
  }

  //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
  private Schema getKsqlType(final String sqlType) {
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
        return getKsqlComplexType(sqlType);
    }
  }

  private Schema getKsqlComplexType(final String sqlType) {
    if (sqlType.startsWith("ARRAY")) {
      return SchemaBuilder
          .array(getKsqlType(sqlType.substring("ARRAY".length() + 1, sqlType.length() - 1)));
    } else if (sqlType.startsWith("MAP")) {
      //TODO: For now only primitive data types for map are supported. Will have to add
      // nested types.
      String[] mapTypesStrs = sqlType.substring("MAP".length() + 1, sqlType.length() - 1)
          .trim().split(",");
      if (mapTypesStrs.length != 2) {
        throw new KsqlException("Map type is not defined correctly.: " + sqlType);
      }
      String keyType = mapTypesStrs[0].trim();
      String valueType = mapTypesStrs[1].trim();
      return SchemaBuilder.map(getKsqlType(keyType), getKsqlType(valueType));
    }
    throw new KsqlException("Unsupported type: " + sqlType);
  }

  private String getAvroSchema(final String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }
}