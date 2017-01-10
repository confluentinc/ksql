package io.confluent.ksql.metastore;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KQLJsonTopicSerDe;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.KSQLException;

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

  public StructuredDataSource createStructuredDataSource(MetaStore metaStore, JsonNode node)
      throws
                                                                                  IOException {

    KQLTopicSerDe topicSerDe;

    String name = node.get("name").asText().toUpperCase();
    String topicname = node.get("topic").asText();

    KQLTopic KQLTopic = (KQLTopic) metaStore.getTopic(topicname);
    if (KQLTopic == null) {
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
                           KQLTopic);
    } else if (type.equalsIgnoreCase("table")) {
      // Use the changelog topic name as state store name.
      if (node.get("statestore") == null) {
        return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                            KQLTopic, KQLTopic.getName());
      }
      String stateStore = node.get("statestore").asText();
      return new KQLTable(name, dataSource, dataSource.field(keyFieldName),
                          KQLTopic, stateStore);
    }
    throw new KSQLException("Type not supported.");
  }

  public KQLTopic createKafkaTopicDataSource(JsonNode node) throws IOException {

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
      topicSerDe = new KQLAvroTopicSerDe(schemaPath, avroSchema);
    } else if (serde.equalsIgnoreCase("json")) {
      topicSerDe = new KQLJsonTopicSerDe();
    } else if (serde.equalsIgnoreCase("csv")) {
      topicSerDe = new KQLCsvTopicSerDe();
    } else {
      throw new KSQLException("Topic serde is not supported.");
    }

    return new KQLTopic(topicname, kafkaTopicName, topicSerDe);
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

  private String getKSQLTypeInJson(Schema schemaType) {
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
    throw new KSQLException("Unsupported type: " + schemaType);
  }

  public MetaStore loadMetastoreFromJSONFile(String metastoreJsonFilePath) throws KSQLException {

    try {
      MetaStoreImpl metaStore = new MetaStoreImpl();
      byte[] jsonData;
      if (metastoreJsonFilePath.equalsIgnoreCase(KSQLConfig.DEFAULT_SCHEMA_FILE_PATH_CONFIG)) {
        jsonData = DEFAULT_METASTORE_SCHEMA.getBytes();
      } else {
        jsonData = Files.readAllBytes(Paths.get(metastoreJsonFilePath));
      }

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode root = objectMapper.readTree(jsonData);

      ArrayNode topicNodes = (ArrayNode) root.get("topics");
      for (JsonNode schemaNode : topicNodes) {
        KQLTopic KQLTopic = createKafkaTopicDataSource(schemaNode);
        metaStore.putTopic(KQLTopic);
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

  private void addTopics(StringBuilder stringBuilder, Map<String, KQLTopic> topicMap) {
    stringBuilder.append("\"topics\" :[ \n");
    boolean isFist = true;
    for (KQLTopic kqlTopic: topicMap.values()) {
      if (!isFist) {
        stringBuilder.append("\t\t, \n");
      } else {
        isFist = false;
      }
      stringBuilder.append("\t\t{\n");
      stringBuilder.append("\t\t\t \"namespace\": \"kql-topics\", \n");
      stringBuilder.append("\t\t\t \"topicname\": \""+kqlTopic.getTopicName()+"\", \n");
      stringBuilder.append("\t\t\t \"kafkatopicname\": \""+kqlTopic.getKafkaTopicName()+"\", \n");
      stringBuilder.append("\t\t\t \"serde\": \""+kqlTopic.getKqlTopicSerDe().getSerDe()+"\"");
      if (kqlTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlTopic.getKqlTopicSerDe();
        stringBuilder.append(",\n\t\t\t \"avroschemafile\": \""+kqlAvroTopicSerDe.getSchemaFilePath
            ()+"\"");
      }
      stringBuilder.append("\n\t\t}\n");

    }
    stringBuilder.append("\t\t]\n");
  }

  private void addSchemas(StringBuilder stringBuilder, Map<String, StructuredDataSource>
      dataSourceMap) {
    stringBuilder.append("\t\"schemas\" :[ \n");
    boolean isFirst = true;
    for (StructuredDataSource structuredDataSource:dataSourceMap.values()) {
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
        throw new KSQLException("Incorrect data source type:"+structuredDataSource.dataSourceType);
      }

      stringBuilder.append("\t\t\t \"name\": \""+structuredDataSource.getName()+"\", \n");
      stringBuilder.append("\t\t\t \"key\": \""+structuredDataSource.getKeyField().name()+"\", \n");
      stringBuilder.append("\t\t\t \"topic\": \""+structuredDataSource.getKQLTopic().getName()+"\", \n");
      if (structuredDataSource instanceof KQLTable) {
        KQLTable kqlTable = (KQLTable) structuredDataSource;
        stringBuilder.append("\t\t\t \"statestore\": \"users_statestore\", \n");
      }
      stringBuilder.append("\t\t\t \"fields\": [\n");
      boolean isFirstField = true;
      for (Field field:structuredDataSource.getSchema().fields()) {
        if (isFirstField) {
          isFirstField = false;
        } else {
          stringBuilder.append(", \n");
        }
        stringBuilder.append("\t\t\t     {\"name\": \""+field.name()+"\", \"type\": "
                             + "\""+getKSQLTypeInJson(field.schema())+"\"} ");
      }
      stringBuilder.append("\t\t\t ]\n");
      stringBuilder.append("\t\t}\n");
    }
    stringBuilder.append("\t ]\n");
  }

  public void writeMetastoreToFile(String filePath, MetaStoreImpl metaStore) {
    StringBuilder stringBuilder = new StringBuilder("{ \n \"name\": \"kql_catalog\",\n ");

    addTopics(stringBuilder, metaStore.getAllKafkaTopics());
    stringBuilder.append("\n\t, \n");
    addSchemas(stringBuilder, metaStore.getAllStructuredDataSource());
    stringBuilder.append("}");

    try {
      RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
      raf.writeBytes(stringBuilder.toString());
      raf.close();
    } catch (IOException e) {
      throw new KSQLException(" Could not write the schema into the file.");
    }
  }


  public static final String DEFAULT_METASTORE_SCHEMA = "{\n"
                                                        + "\t\"name\": \"kql_catalog\",\n"
                                                        + "\t\"topics\":[],\n"
                                                        + "\t\"schemas\" :[]\n"
                                                        + "}";


  public static void main(String args[]) throws IOException {

//    new MetastoreUtil().loadMetastoreFromJSONFile("/Users/hojjat/userschema.json");
    MetastoreUtil metastoreUtil = new MetastoreUtil();
//    MetaStore metaStore = metastoreUtil.loadMetastoreFromJSONFile
//        ("/Users/hojjat/kql_catalog.json");
    MetaStore metaStore = metastoreUtil.loadMetastoreFromJSONFile
        ("/Users/hojjat/test_kql_catalog1.json");
    System.out.println("");
//    System.out.println(metastoreUtil.buildAvroSchema(metaStore.getAllStructuredDataSource().get
//        ("ORDERS")));
  }

  private String getAvroSchema(String schemaFilePath) throws IOException {
    byte[] jsonData = Files.readAllBytes(Paths.get(schemaFilePath));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(jsonData);
    return root.toString();
  }

  public void writeAvroSchemaFile(String avroSchema, String filePath) {

    try {
      RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
      randomAccessFile.writeBytes(avroSchema);
      randomAccessFile.close();
    } catch (IOException e) {
      throw new KSQLException("Could not write result avro schema file: "+filePath);
    }
  }

  public String buildAvroSchema(Schema schema, String name) {
    StringBuilder stringBuilder = new StringBuilder("{\n\t\"namespace\": \"ksql\",\n");
    stringBuilder.append("\t\"name\": \""+name+"\",\n");
    stringBuilder.append("\t\"type\": \"record\",\n");
    stringBuilder.append("\t\"fields\": [\n");
    boolean addCamma = false;
    for (Field field:schema.fields()) {
      if (addCamma) {
        stringBuilder.append(",\n");
      } else {
        addCamma = true;
      }
      stringBuilder.append("\t\t{\"name\": \""+field.name()+"\", \"type\": \""+getAvroTypeName(field
                                                                                               .schema().type())
                           +"\"}");
    }
    stringBuilder.append("\n\t]\n");
    stringBuilder.append("}");
    return stringBuilder.toString();
  }

  private String getAvroTypeName(Schema.Type type) {
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
      throw new KSQLException("Unsupported AVRO type: "+type.name());
    }
  }
}
