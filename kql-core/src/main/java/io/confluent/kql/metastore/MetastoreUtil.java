/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.metastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.util.KQLException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class MetastoreUtil {

  private StructuredDataSource createStructuredDataSource(final MetaStore metaStore,
                                                         final JsonNode node)
      throws
      IOException {

    KQLTopicSerDe topicSerDe;

    String name = node.get("name").asText().toUpperCase();
    String topicname = node.get("topic").asText();

    KQLTopic kqlTopic = (KQLTopic) metaStore.getTopic(topicname);
    if (kqlTopic == null) {
      throw new KQLException("Unable to add the structured data source. The corresponding topic "
                             + "does not exist: " + topicname);
    }

    String type = node.get("type").asText();
    String keyFieldName = node.get("key").asText().toUpperCase();
    SchemaBuilder dataSourceBuilder = SchemaBuilder.struct().name(name);
    ArrayNode fields = (ArrayNode) node.get("fields");
    for (int i = 0; i < fields.size(); i++) {
      String fieldName = fields.get(i).get("name").textValue().toUpperCase();
      String fieldType;
      if (fields.get(i).get("type").isArray()) {
        fieldType = fields.get(i).get("type").get(0).textValue();
      } else {
        fieldType = fields.get(i).get("type").textValue();
      }

      dataSourceBuilder.field(fieldName, getKQLType(fieldType));
    }

    Schema dataSource = dataSourceBuilder.build();

    if (type.equalsIgnoreCase("stream")) {
      return new KQLStream(name, dataSource, dataSource.field(keyFieldName),
                           kqlTopic);
    } else if (type.equalsIgnoreCase("table")) {
      // Use the changelog topic name as state store name.
      if (node.get("statestore") == null) {
        return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                            kqlTopic, kqlTopic.getName());
      }
      String stateStore = node.get("statestore").asText();
      return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                          kqlTopic, stateStore);
    }
    throw new KQLException("Type not supported.");
  }

  private KQLTopic createKafkaTopicDataSource(final JsonNode node) throws IOException {

    KQLTopicSerDe topicSerDe;
    String topicname = node.get("topicname").asText();
    String kafkaTopicName = node.get("kafkatopicname").asText();
    String serde = node.get("serde").asText();
    if (serde.equalsIgnoreCase("avro")) {
      if (node.get("avroschemafile") == null) {
        throw new KQLException("For avro SerDe avro schema file path (avroschemafile) should be "
                               + "set in the schema.");
      }
      String schemaPath = node.get("avroschemafile").asText();
      String avroSchema = getAvroSchema(schemaPath);
      topicSerDe = new KQLAvroTopicSerDe(schemaPath, avroSchema);
    } else if (serde.equalsIgnoreCase("json")) {
      topicSerDe = new KQLJsonTopicSerDe();
    } else if (serde.equalsIgnoreCase("csv")) {
      topicSerDe = new KQLCsvTopicSerDe();
    } else {
      throw new KQLException("Topic serde is not supported.");
    }

    return new KQLTopic(topicname, kafkaTopicName, topicSerDe);
  }

  private Schema getKQLType(final String sqlType) {
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
    throw new KQLException("Unsupported type: " + sqlType);
  }

  private String getKQLTypeInJson(final Schema schemaType) {
    if (schemaType == Schema.INT64_SCHEMA) {
      return "long";
    } else if (schemaType == Schema.STRING_SCHEMA) {
      return "string";
    } else if (schemaType == Schema.FLOAT64_SCHEMA) {
      return "double";
    } else if (schemaType == Schema.INT64_SCHEMA) {
      return "integer";
    } else if (schemaType == Schema.BOOLEAN_SCHEMA) {
      return "boolean";
    }
    throw new KQLException("Unsupported type: " + schemaType);
  }

  public MetaStore loadMetaStoreFromJSONFile(final String metaStoreJsonFilePath)
      throws KQLException {

    try {
      MetaStoreImpl metaStore = new MetaStoreImpl();
      byte[] jsonData = Files.readAllBytes(Paths.get(metaStoreJsonFilePath));

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode root = objectMapper.readTree(jsonData);

      ArrayNode topicNodes = (ArrayNode) root.get("topics");
      for (JsonNode schemaNode : topicNodes) {
        KQLTopic kqlTopic = createKafkaTopicDataSource(schemaNode);
        metaStore.putTopic(kqlTopic);
      }

      ArrayNode schemaNodes = (ArrayNode) root.get("schemas");
      for (JsonNode schemaNode : schemaNodes) {
        StructuredDataSource dataSource = createStructuredDataSource(metaStore, schemaNode);
        metaStore.putSource(dataSource);
      }
      return metaStore;
    } catch (FileNotFoundException fnf) {
      throw new KQLException("Could not load the schema file from " + metaStoreJsonFilePath, fnf);
    } catch (IOException ioex) {
      throw new KQLException("Could not read schema from " + metaStoreJsonFilePath, ioex);
    }
  }

  private void addTopics(final StringBuilder stringBuilder, final Map<String, KQLTopic> topicMap) {
    stringBuilder.append("\"topics\" :[ \n");
    boolean isFist = true;
    for (KQLTopic kqlTopic : topicMap.values()) {
      if (!isFist) {
        stringBuilder.append("\t\t, \n");
      } else {
        isFist = false;
      }
      stringBuilder.append("\t\t{\n");
      stringBuilder.append("\t\t\t \"namespace\": \"kql-topics\", \n");
      stringBuilder.append("\t\t\t \"topicname\": \"" + kqlTopic.getTopicName() + "\", \n");
      stringBuilder
          .append("\t\t\t \"kafkatopicname\": \"" + kqlTopic.getKafkaTopicName() + "\", \n");
      stringBuilder.append("\t\t\t \"serde\": \"" + kqlTopic.getKqlTopicSerDe().getSerDe() + "\"");
      if (kqlTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlTopic.getKqlTopicSerDe();
        stringBuilder
            .append(",\n\t\t\t \"avroschemafile\": \"" + kqlAvroTopicSerDe.getSchemaFilePath() + "\"");
      }
      stringBuilder.append("\n\t\t}\n");

    }
    stringBuilder.append("\t\t]\n");
  }

  private void addSchemas(final StringBuilder stringBuilder, final Map<String, StructuredDataSource>
      dataSourceMap) {
    stringBuilder.append("\t\"schemas\" :[ \n");
    boolean isFirst = true;
    for (StructuredDataSource structuredDataSource : dataSourceMap.values()) {
      if (isFirst) {
        isFirst = false;
      } else {
        stringBuilder.append("\t\t, \n");
      }
      stringBuilder.append("\t\t{ \n");
      stringBuilder.append("\t\t\t \"namespace\": \"kql\", \n");
      if (structuredDataSource.dataSourceType == DataSource.DataSourceType.KSTREAM) {
        stringBuilder.append("\t\t\t \"type\": \"stream\", \n");
      } else if (structuredDataSource.dataSourceType == DataSource.DataSourceType.KTABLE) {
        stringBuilder.append("\t\t\t \"type\": \"table\", \n");
      } else {
        throw new KQLException("Incorrect data source type:" + structuredDataSource.dataSourceType);
      }

      stringBuilder.append("\t\t\t \"name\": \"" + structuredDataSource.getName() + "\", \n");
      stringBuilder
          .append("\t\t\t \"key\": \"" + structuredDataSource.getKeyField().name() + "\", \n");
      stringBuilder
          .append("\t\t\t \"topic\": \"" + structuredDataSource.getKqlTopic().getName() + "\", \n");
      if (structuredDataSource instanceof KQLTable) {
        KQLTable kqlTable = (KQLTable) structuredDataSource;
        stringBuilder.append("\t\t\t \"statestore\": \"" + kqlTable.getStateStoreName() + "\", \n");
      }
      stringBuilder.append("\t\t\t \"fields\": [\n");
      boolean isFirstField = true;
      for (Field field : structuredDataSource.getSchema().fields()) {
        if (isFirstField) {
          isFirstField = false;
        } else {
          stringBuilder.append(", \n");
        }
        stringBuilder.append("\t\t\t     {\"name\": \"" + field.name() + "\", \"type\": "
                             + "\"" + getKQLTypeInJson(field.schema()) + "\"} ");
      }
      stringBuilder.append("\t\t\t ]\n");
      stringBuilder.append("\t\t}\n");
    }
    stringBuilder.append("\t ]\n");
  }

  public void writeMetastoreToFile(String filePath, MetaStore metaStore) {
    StringBuilder stringBuilder = new StringBuilder("{ \n \"name\": \"kql_catalog\",\n ");

    addTopics(stringBuilder, metaStore.getAllKQLTopics());
    stringBuilder.append("\n\t, \n");
    addSchemas(stringBuilder, metaStore.getAllStructuredDataSources());
    stringBuilder.append("}");

    try {
      RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
      raf.writeBytes(stringBuilder.toString());
      raf.close();
    } catch (IOException e) {
      throw new KQLException(" Could not write the schema into the file.");
    }
  }


  public static final String DEFAULT_METASTORE_SCHEMA = "{\n"
                                                        + "\t\"name\": \"kql_catalog\",\n"
                                                        + "\t\"topics\":[],\n"
                                                        + "\t\"schemas\" :[]\n"
                                                        + "}";


  public static void main(String[] args) throws IOException {

//    new MetastoreUtil().loadMetaStoreFromJSONFile("/Users/hojjat/userschema.json");
    MetastoreUtil metastoreUtil = new MetastoreUtil();
//    MetaStore metaStore = metastoreUtil.loadMetaStoreFromJSONFile
//        ("/Users/hojjat/kql_catalog.json");
    MetaStore metaStore = metastoreUtil.loadMetaStoreFromJSONFile("/Users/hojjat/test_kql_catalog1.json");
    System.out.println("");
//    System.out.println(metastoreUtil.buildAvroSchema(metaStore.getAllStructuredDataSource().get
//        ("ORDERS")));
  }

  private String getAvroSchema(final String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }

  public void writeAvroSchemaFile(final String avroSchema, final String filePath) {

    try {
      RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
      randomAccessFile.writeBytes(avroSchema);
      randomAccessFile.close();
    } catch (IOException e) {
      throw new KQLException("Could not write result avro schema file: " + filePath);
    }
  }

  public String buildAvroSchema(final Schema schema, String name) {
    StringBuilder stringBuilder = new StringBuilder("{\n\t\"namespace\": \"kql\",\n");
    stringBuilder.append("\t\"name\": \"" + name + "\",\n");
    stringBuilder.append("\t\"type\": \"record\",\n");
    stringBuilder.append("\t\"fields\": [\n");
    boolean addCamma = false;
    for (Field field : schema.fields()) {
      if (addCamma) {
        stringBuilder.append(",\n");
      } else {
        addCamma = true;
      }
      stringBuilder
          .append("\t\t{\"name\": \"" + field.name() + "\", \"type\": \"" + getAvroTypeName(field
                                                                                                .schema()
                                                                                                .type())
                  + "\"}");
    }
    stringBuilder.append("\n\t]\n");
    stringBuilder.append("}");
    return stringBuilder.toString();
  }

  private String getAvroTypeName(final Schema.Type type) {
    if (type == Schema.Type.STRING) {
      return "string";
    } else if (type == Schema.Type.BOOLEAN) {
      return "boolean";
    } else if (type == Schema.Type.INT64) {
      return "long";
    } else if (type == Schema.Type.FLOAT64) {
      return "double";
    } else if (type == Schema.Type.INT32) {
      return "int";
    } else {
      throw new KQLException("Unsupported AVRO type: " + type.name());
    }
  }
}
