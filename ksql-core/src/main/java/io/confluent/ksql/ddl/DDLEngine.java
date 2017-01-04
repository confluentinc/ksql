package io.confluent.ksql.ddl;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.ksql.KSQLEngine;
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

  public void createTopic(CreateTopic createTopic) {

    String topicName = createTopic.getName().getSuffix().toUpperCase();
    if (ksqlEngine.getMetaStore().getTopic(topicName) != null) {
      if (createTopic.isNotExists()) {
        System.out.println("Topic already exists.");
      } else {
        throw new KSQLException("Topic already exists.");
      }
      return;
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
    String kafkaTopicName = createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
    kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY,kafkaTopicName);
    KQLTopicSerDe topicSerDe;
    if (serde.equalsIgnoreCase(DataSource.AVRO_SERDE_NAME)) {

      if (createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE) == null) {
        throw new KSQLException("Avro schema file path should be set for avro topics.");
      }
      String avroSchemFile = createTopic.getProperties().get(DDLConfig.AVRO_SCHEMA_FILE).toString();
      avroSchemFile = enforceString(DDLConfig.AVRO_SCHEMA_FILE, avroSchemFile);
      try {
        String avroSchema = getAvroSchema(avroSchemFile);
        topicSerDe = new KQLAvroTopicSerDe(avroSchema);
      } catch (IOException e) {
        throw new KSQLException("Could not read avro schema from file: " + avroSchemFile);
      }
    } else if (serde.equalsIgnoreCase(DataSource.JSON_SERDE_NAME)) {
      topicSerDe = new KQLJsonTopicSerDe();
    } else if (serde.equalsIgnoreCase(DataSource.CSV_SERDE_NAME)) {
      topicSerDe = new KQLCsvTopicSerDe();
    } else {
      throw new KSQLException("The specified topic serde is not supported.");
    }
    KQLTopic KQLTopic = new KQLTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putTopic(KQLTopic);
  }

  private String enforceString(String propertyName, String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KSQLException(propertyName + " value is string and should be enclosed between "
                              + "\"'\".");
    }
    return propertyValue.substring(1,propertyValue.length()-1);
  }

  public void dropTopic(DropTable dropTable) {

    String topicName = dropTable.getTableName().getSuffix().toUpperCase();
    new DDLUtil().deleteTopic(topicName);
    ksqlEngine.getMetaStore().deleteSource(topicName);
  }

  public void createStream(CreateStream createStream) {

    String streamName = createStream.getName().getSuffix().toUpperCase();
    if (ksqlEngine.getMetaStore().getSource(streamName) != null) {
      if (createStream.isNotExists()) {
        System.out.println("Stream already exists.");
      } else {
        throw new KSQLException("Stream already exists.");
      }
      return;
    }

    if (createStream.getElements().size() == 0) {
      throw new KSQLException("No column was specified.");
    }

    SchemaBuilder streamSchema = SchemaBuilder.struct();
    for (TableElement tableElement: createStream.getElements()) {
      streamSchema = streamSchema.field(tableElement.getName(), getKSQLType(tableElement.getType()));
    }

    if (createStream.getProperties().size() == 0) {
      throw new KSQLException("Create stream statement needs WITH clause.");
    }

    if (createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KSQLException("Topic for the stream should be set in WITH clause.");
    }

    String topicName = createStream.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) == null) {
      throw new KSQLException("Key field name for the stream should be set in WITH clause.");
    }

    String keyName = createStream.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString();
    keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);

    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KSQLException("THe corresponding topic is does not exist.");
    }

    KQLStream kqlStream = new KQLStream(streamName, streamSchema, streamSchema.field(keyName), ksqlEngine.getMetaStore().getTopic(topicName));

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(kqlStream);
  }

  public void createTable(CreateTable createTable) {

    String tableName = createTable.getName().getSuffix().toUpperCase();
    if (ksqlEngine.getMetaStore().getSource(tableName) != null) {
      if (createTable.isNotExists()) {
        System.out.println("Topic already exists.");
      } else {
        throw new KSQLException("Topic already exists.");
      }
      return;
    }

    if (createTable.getElements().size() == 0) {
      throw new KSQLException("No column was specified.");
    }

    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (TableElement tableElement: createTable.getElements()) {
      tableSchema = tableSchema.field(tableElement.getName(), getKSQLType(tableElement.getType()));
    }

    if (createTable.getProperties().size() == 0) {
      throw new KSQLException("Create table statement needs WITH clause.");
    }

    if (createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KSQLException("Topic (topic) for the table should be set in WITH clause.");
    }

    String topicName = createTable.getProperties().get(DDLConfig.TOPIC_NAME_PROPERTY).toString();
    topicName = enforceString(DDLConfig.TOPIC_NAME_PROPERTY, topicName);

    if (createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY) == null) {
      throw new KSQLException("State store (statestore) name for the table should be set in WITH clause.");
    }

    String stateStoreName = createTable.getProperties().get(DDLConfig.STATE_STORE_NAME_PROPERTY)
        .toString();
    stateStoreName = enforceString(DDLConfig.STATE_STORE_NAME_PROPERTY, stateStoreName);

    if (createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY) == null) {
      throw new KSQLException("Key(key) field name for the stream should be set in WITH clause.");
    }

    String keyName = createTable.getProperties().get(DDLConfig.KEY_NAME_PROPERTY).toString();
    keyName = enforceString(DDLConfig.KEY_NAME_PROPERTY, keyName);

    if (ksqlEngine.getMetaStore().getTopic(topicName) == null) {
      throw new KSQLException("The corresponding topic is does not exist.");
    }

    KQLTable kqlTable = new KQLTable(tableName, tableSchema, tableSchema.field(keyName),
                                     ksqlEngine.getMetaStore().getTopic(topicName), stateStoreName);


    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    ksqlEngine.getMetaStore().putSource(kqlTable);
  }

  //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
  private Schema getKSQLType(String sqlType) {
    if (sqlType.equalsIgnoreCase("BIGINT")) {
      return Schema.INT64_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("VARCHAR")) {
      return Schema.STRING_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("DOUBLE")) {
      return Schema.FLOAT64_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("INTEGER") || sqlType.equalsIgnoreCase("INT")) {
      return Schema.INT32_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("BOOELAN") || sqlType.equalsIgnoreCase("BOOL")) {
      return Schema.BOOLEAN_SCHEMA;
    }
    throw new KSQLException("Unsupported type: " + sqlType);
  }
  private String getAvroSchema(String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }
}
