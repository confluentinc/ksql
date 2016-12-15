package io.confluent.ksql.metastore;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.serde.json.KQLJsonTopicSerDe;
import io.confluent.ksql.util.KSQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MetastoreUtil {

  public StructuredDataSource createStructuredDataSource(MetaStore metaStore, JsonNode node)
      throws
                                                                                  IOException {

    KQLTopicSerDe topicSerDe;

    String name = node.get("name").asText().toUpperCase();
    String topicname = node.get("topic").asText();

    KafkaTopic kafkaTopic = (KafkaTopic) metaStore.getTopic(topicname);
    if (kafkaTopic == null) {
      throw new KSQLException("Unable to add the structured data source. The corresponding topic "
                              + "does not exist: "+topicname);
    }

    String type = node.get("type").asText();
    String keyFieldName = node.get("key").asText().toUpperCase();
    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);
    ArrayNode fields = (ArrayNode) node.get("fields");
    for (int i = 0; i < fields.size(); i++) {
      String fieldName = fields.get(i).get("name").textValue().toUpperCase();
      String fieldType;
      if (fields.get(i).get("type").isArray()) {
        fieldType = fields.get(i).get("type").get(0).textValue();
      } else {
        fieldType = fields.get(i).get("type").textValue();
      }

      dataSource.field(fieldName, getKSQLType(fieldType));
    }

    if (type.equalsIgnoreCase("stream")) {
      return new KQLStream(name, dataSource, dataSource.field(keyFieldName),
                           kafkaTopic);
    } else if (type.equalsIgnoreCase("table")) {
      // Use the changelog topic name as state store name.
      if (node.get("statestore") == null) {
        return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                            kafkaTopic, kafkaTopic.getName());
      }
      String stateStore = node.get("statestore").asText();
      return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                           kafkaTopic, stateStore);
    }
    throw new KSQLException("Type not supported.");
  }

  public KafkaTopic createKafkaTopicDataSource(JsonNode node) throws IOException {

    KQLTopicSerDe topicSerDe;
    String topicname = node.get("topicname").asText();
    String kafkaTopicName = node.get("kafkatopicname").asText();
    String serde = node.get("serde").asText();
    if (serde.equalsIgnoreCase("avro")) {
      if (node.get("avroschemafile") == null) {
        throw new KSQLException("For avro SerDe avro schema file path (avroschemafile) should be "
                                + "set in the schema.");
      }
      String schemaPath = node.get("avroschemafile").asText();
      String avroSchema = getAvroSchema(schemaPath);
      topicSerDe = new KQLAvroTopicSerDe(avroSchema);
    } else {
      topicSerDe = new KQLJsonTopicSerDe();
    }

    return new KafkaTopic(topicname, kafkaTopicName, topicSerDe);
  }

  private Schema getKSQLType(String sqlType) {
    if (sqlType.equalsIgnoreCase("long")) {
      return Schema.INT64_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("string")) {
      return Schema.STRING_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("double")) {
      return Schema.FLOAT64_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("int") || sqlType.equalsIgnoreCase("integer")) {
      return Schema.INT32_SCHEMA;
    } else if (sqlType.equalsIgnoreCase("bool") || sqlType.equalsIgnoreCase("boolean")) {
      return Schema.BOOLEAN_SCHEMA;
    }
    throw new KSQLException("Unsupported type: " + sqlType);
  }

  public MetaStore loadMetastoreFromJSONFile(String metastoreJsonFilePath) throws KSQLException {
    try {
      MetaStoreImpl metaStore = new MetaStoreImpl();
      byte[] jsonData = Files.readAllBytes(Paths.get(metastoreJsonFilePath));
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode root = objectMapper.readTree(jsonData);

      ArrayNode topicNodes = (ArrayNode) root.get("topics");
      for (JsonNode schemaNode : topicNodes) {
        KafkaTopic kafkaTopic = createKafkaTopicDataSource(schemaNode);
        metaStore.putTopic(kafkaTopic);
      }

      ArrayNode schemaNodes = (ArrayNode) root.get("schemas");
      for (JsonNode schemaNode : schemaNodes) {
        StructuredDataSource dataSource = createStructuredDataSource(metaStore, schemaNode);
        metaStore.putSource(dataSource);
      }
      return metaStore;
    } catch (FileNotFoundException fnf) {
      throw new KSQLException("Could not load the schema file from " + metastoreJsonFilePath, fnf);
    } catch (IOException ioex) {
      throw new KSQLException("Could not read schema from " + metastoreJsonFilePath, ioex);
    }
  }

  public static void main(String args[]) throws IOException {

//    new MetastoreUtil().loadMetastoreFromJSONFile("/Users/hojjat/userschema.json");
    new MetastoreUtil().loadMetastoreFromJSONFile("/Users/hojjat/kql_catalog.json");
    System.out.println("");

  }

  private String getAvroSchema(String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }
}
