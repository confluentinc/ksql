/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MetastoreUtil {

  private StructuredDataSource createStructuredDataSource(final MetaStore metaStore,
                                                          final JsonNode node)
      throws
      IOException {

    KsqlTopicSerDe topicSerDe;

    String name = node.get("name").asText();
    String topicname = node.get("topic").asText();

    KsqlTopic ksqlTopic = (KsqlTopic) metaStore.getTopic(topicname);
    if (ksqlTopic == null) {
      throw new KsqlException("Unable to add the structured data source. The corresponding topic "
          + "does not exist: " + topicname);
    }

    String type = node.get("type").asText().toUpperCase();
    String keyFieldName = node.get("key").asText();
    String timestampFieldName = node.get("timestamp").asText();
    ArrayNode fields = (ArrayNode) node.get("fields");
    Schema dataSource = buildDatasourceSchema(name, fields);

    if ("STREAM".equals(type)) {
      return new KsqlStream(name, dataSource, dataSource.field(keyFieldName),
                            (dataSource.field(timestampFieldName) != null)
                            ? dataSource.field(timestampFieldName) : null, ksqlTopic);
    } else if ("TABLE".equals(type)) {
      boolean isWindowed = false;
      if (node.get("iswindowed") != null) {
        isWindowed = node.get("iswindowed").asBoolean();
      }
      // Use the changelog topic name as state store name.
      if (node.get("statestore") == null) {
        return new KsqlTable(name, dataSource, dataSource.field(keyFieldName),
                             (dataSource.field(timestampFieldName) != null)
                             ? dataSource.field(timestampFieldName) : null,
                             ksqlTopic, ksqlTopic.getName(), isWindowed);
      }
      String stateStore = node.get("statestore").asText();
      return new KsqlTable(name, dataSource, dataSource.field(keyFieldName),
                           (dataSource.field(timestampFieldName) != null)
                           ? dataSource.field(timestampFieldName) : null,
          ksqlTopic, stateStore, isWindowed);
    }
    throw new KsqlException(String.format("Type not supported: '%s'", type));
  }

  private Schema buildDatasourceSchema(String name, ArrayNode fields) {
    SchemaBuilder dataSourceBuilder = SchemaBuilder.struct().name(name);
    for (int i = 0; i < fields.size(); i++) {
      String fieldName = fields.get(i).get("name").textValue();
      String fieldType;
      if (fields.get(i).get("type").isArray()) {
        fieldType = fields.get(i).get("type").get(0).textValue();
      } else {
        fieldType = fields.get(i).get("type").textValue();
      }

      dataSourceBuilder.field(fieldName, getKsqlType(fieldType));
    }

    return dataSourceBuilder.build();
  }

  private KsqlTopic createKafkaTopicDataSource(final JsonNode node) throws IOException {

    KsqlTopicSerDe topicSerDe;
    String topicname = node.get("topicname").asText();
    String kafkaTopicName = node.get("kafkatopicname").asText();
    String serde = node.get("serde").asText().toUpperCase();
    if ("AVRO".equals(serde)) {
      if (node.get(DdlConfig.AVRO_SCHEMA_FILE.toLowerCase()) == null) {
        throw new KsqlException("For avro SerDe avro schema file path (avroschemafile) should be "
            + "set in the schema.");
      }
      String schemaPath = node.get(DdlConfig.AVRO_SCHEMA_FILE.toLowerCase()).asText();
      String avroSchema = getAvroSchema(schemaPath);
      topicSerDe = new KsqlAvroTopicSerDe(avroSchema);
    } else if ("JSON".equals(serde)) {
      topicSerDe = new KsqlJsonTopicSerDe(null);
    } else if ("DELIMITED".equals(serde)) {
      topicSerDe = new KsqlDelimitedTopicSerDe();
    } else {
      throw new KsqlException("Topic serde is not supported.");
    }

    return new KsqlTopic(topicname, kafkaTopicName, topicSerDe);
  }

  private Schema getKsqlType(final String sqlType) {
    switch (sqlType.toUpperCase()) {
      case "STRING":
        return Schema.STRING_SCHEMA;
      case "BOOL":
        return Schema.BOOLEAN_SCHEMA;
      case "INT":
        return Schema.INT32_SCHEMA;
      case "LONG":
        return Schema.INT64_SCHEMA;
      case "DOUBLE":
        return Schema.FLOAT64_SCHEMA;
      default:
        throw new KsqlException("Unsupported type: " + sqlType);
    }
  }

  private String getKsqlTypeInJson(final Schema schemaType) {
    if (schemaType == Schema.INT64_SCHEMA) {
      return "LONG";
    } else if (schemaType == Schema.STRING_SCHEMA) {
      return "STRING";
    } else if (schemaType == Schema.FLOAT64_SCHEMA) {
      return "DOUBLE";
    } else if (schemaType == Schema.INT64_SCHEMA) {
      return "INTEGER";
    } else if (schemaType == Schema.BOOLEAN_SCHEMA) {
      return "BOOL";
    }
    throw new KsqlException("Unsupported type: " + schemaType);
  }

  public MetaStore loadMetaStoreFromJsonFile(final String metaStoreJsonFilePath)
      throws KsqlException {

    try {
      MetaStoreImpl metaStore = new MetaStoreImpl();
      byte[] jsonData = Files.readAllBytes(Paths.get(metaStoreJsonFilePath));

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode root = objectMapper.readTree(jsonData);

      ArrayNode topicNodes = (ArrayNode) root.get("topics");
      for (JsonNode schemaNode : topicNodes) {
        KsqlTopic ksqlTopic = createKafkaTopicDataSource(schemaNode);
        metaStore.putTopic(ksqlTopic);
      }

      ArrayNode schemaNodes = (ArrayNode) root.get("schemas");
      for (JsonNode schemaNode : schemaNodes) {
        StructuredDataSource dataSource = createStructuredDataSource(metaStore, schemaNode);
        metaStore.putSource(dataSource);
      }
      return metaStore;
    } catch (FileNotFoundException fnf) {
      throw new KsqlException("Could not load the schema file from " + metaStoreJsonFilePath, fnf);
    } catch (IOException ioex) {
      throw new KsqlException("Could not read schema from " + metaStoreJsonFilePath, ioex);
    }
  }

  private void addTopics(final StringBuilder stringBuilder, final Map<String, KsqlTopic> topicMap) {
    stringBuilder.append("\"topics\" :[ \n");
    boolean isFist = true;
    for (KsqlTopic ksqlTopic : topicMap.values()) {
      if (!isFist) {
        stringBuilder.append("\t\t, \n");
      } else {
        isFist = false;
      }
      stringBuilder.append("\t\t{\n");
      stringBuilder.append("\t\t\t \"namespace\": \"ksql-topics\", \n");
      stringBuilder.append("\t\t\t \"topicname\": \"" + ksqlTopic.getTopicName() + "\", \n");
      stringBuilder
          .append("\t\t\t \"kafkatopicname\": \"" + ksqlTopic.getKafkaTopicName() + "\", \n");
      stringBuilder.append("\t\t\t \"serde\": \"" + ksqlTopic.getKsqlTopicSerDe().getSerDe()
                           + "\"");
      if (ksqlTopic.getKsqlTopicSerDe() instanceof KsqlAvroTopicSerDe) {
        KsqlAvroTopicSerDe ksqlAvroTopicSerDe = (KsqlAvroTopicSerDe) ksqlTopic.getKsqlTopicSerDe();
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
      stringBuilder.append("\t\t\t \"namespace\": \"ksql\", \n");
      if (structuredDataSource.dataSourceType == DataSource.DataSourceType.KSTREAM) {
        stringBuilder.append("\t\t\t \"type\": \"STREAM\", \n");
      } else if (structuredDataSource.dataSourceType == DataSource.DataSourceType.KTABLE) {
        stringBuilder.append("\t\t\t \"type\": \"TABLE\", \n");
      } else {
        throw new KsqlException("Incorrect data source type:"
                                + structuredDataSource.dataSourceType);
      }

      stringBuilder.append("\t\t\t \"name\": \"" + structuredDataSource.getName() + "\", \n");
      stringBuilder
          .append("\t\t\t \"key\": \"" + structuredDataSource.getKeyField().name() + "\", \n");
      stringBuilder
          .append("\t\t\t \"timestamp\": \"null\", "
                  + "\n");
      stringBuilder
          .append("\t\t\t \"topic\": \"" + structuredDataSource.getKsqlTopic().getName()
                  + "\", \n");
      if (structuredDataSource instanceof KsqlTable) {
        KsqlTable ksqlTable = (KsqlTable) structuredDataSource;
        stringBuilder.append("\t\t\t \"statestore\": \"" + ksqlTable.getStateStoreName()
                             + "\", \n");
        stringBuilder.append("\t\t\t \"iswindowed\": \"" + ksqlTable.isWindowed() + "\", \n");
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
                             + "\"" + getKsqlTypeInJson(field.schema()) + "\"} ");
      }
      stringBuilder.append("\t\t\t ]\n");
      stringBuilder.append("\t\t}\n");
    }
    stringBuilder.append("\t ]\n");
  }

  public void writeMetastoreToFile(String filePath, MetaStore metaStore) {
    StringBuilder stringBuilder = new StringBuilder("{ \n \"name\": \"ksql_catalog\",\n ");

    addTopics(stringBuilder, metaStore.getAllKsqlTopics());
    stringBuilder.append("\n\t, \n");
    addSchemas(stringBuilder, metaStore.getAllStructuredDataSources());
    stringBuilder.append("}");

    try {
      RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
      raf.writeBytes(stringBuilder.toString());
      raf.close();
    } catch (IOException e) {
      throw new KsqlException(" Could not write the schema into the file.");
    }
  }


  public static final String DEFAULT_METASTORE_SCHEMA = "{\n"
      + "\t\"name\": \"ksql_catalog\",\n"
      + "\t\"topics\":[],\n"
      + "\t\"schemas\" :[]\n"
      + "}";

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
      throw new KsqlException("Could not write result avro schema file: " + filePath + ". "
                              + "Details: " + e.getMessage(), e);
    }
  }

  public String buildAvroSchema(final Schema schema, String name) {
    StringBuilder stringBuilder = new StringBuilder("{\n\t\"namespace\": \"ksql\",\n");
    stringBuilder.append("\t\"name\": \"" + name + "\",\n");
    stringBuilder.append("\t\"type\": \"record\",\n");
    stringBuilder.append("\t\"fields\": [\n");
    boolean addCamma = false;
    Set<String> fieldNameSet = new HashSet<>();
    for (Field field : schema.fields()) {
      if (addCamma) {
        stringBuilder.append(",\n");
      } else {
        addCamma = true;
      }
      String fieldName = field.name().replace(".", "_");
      while (fieldNameSet.contains(fieldName)) {
        fieldName = fieldName + "_";
      }
      fieldNameSet.add(fieldName);
      stringBuilder
          .append("\t\t{\"name\": \"" + fieldName + "\", \"type\": "
                  + getAvroTypeName(field.schema()) + "}");
    }
    stringBuilder.append("\n\t]\n");
    stringBuilder.append("}");
    return stringBuilder.toString();
  }

  private String getAvroTypeName(final Schema schema) {
    switch (schema.type()) {
      case STRING:
        return "\"string\"";
      case BOOLEAN:
        return "\"boolean\"";
      case INT32:
        return "\"int\"";
      case INT64:
        return "\"long\"";
      case FLOAT64:
        return "\"double\"";
      default:
        if (schema.type() == Schema.Type.ARRAY) {
          return "{\"type\": \"array\", \"items\": "
              + getAvroTypeName(schema.valueSchema()) + "}";
        } else if (schema.type() == Schema.Type.MAP) {
          return "{\"type\": \"map\", \"values\": "
              + getAvroTypeName(schema.valueSchema()) + "}";
        }
        throw new KsqlException("Unsupported AVRO type: " + schema.type().name());
    }
  }
}