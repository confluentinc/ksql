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
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class MetastoreUtil {

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

  private KsqlTopic createKafkaTopicDataSource(final JsonNode node) {

    KsqlTopicSerDe topicSerDe;
    String topicname = node.get("topicname").asText();
    String kafkaTopicName = node.get("kafkatopicname").asText();
    String serde = node.get("serde").asText().toUpperCase();
    if ("AVRO".equals(serde)) {
      topicSerDe = new KsqlAvroTopicSerDe();
    } else if ("JSON".equals(serde)) {
      topicSerDe = new KsqlJsonTopicSerDe();
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

  private String getKsqlTypeInJson(final Schema schema) {
    switch (schema.type()) {
      case INT64:
        return "LONG";
      case FLOAT64:
        return "DOUBLE";
      case STRING:
        return "STRING";
      case BOOLEAN:
        return "BOOL";
      default:
        throw new KsqlException("Unsupported type: " + schema.type());
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
      stringBuilder.append("\t\t{\n")
          .append("\t\t\t \"namespace\": \"ksql-topics\", \n")
          .append("\t\t\t \"topicname\": \"")
          .append(ksqlTopic.getTopicName())
          .append("\", \n")
          .append("\t\t\t \"kafkatopicname\": \"")
          .append(ksqlTopic.getKafkaTopicName())
          .append("\", \n")
          .append("\t\t\t \"serde\": \"")
          .append(ksqlTopic.getKsqlTopicSerDe().getSerDe())
          .append("\"")
          .append("\n\t\t}\n");
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

      stringBuilder.append("\t\t\t \"name\": \"")
          .append(structuredDataSource.getName())
          .append("\", \n")
          .append("\t\t\t \"key\": \"")
          .append(structuredDataSource.getKeyField().name())
          .append("\", \n")
          .append("\t\t\t \"timestamp\": \"null\", \n")
          .append("\t\t\t \"topic\": \"")
          .append(structuredDataSource.getKsqlTopic().getName())
          .append("\", \n");
      if (structuredDataSource instanceof KsqlTable) {
        KsqlTable ksqlTable = (KsqlTable) structuredDataSource;
        stringBuilder.append("\t\t\t \"statestore\": \"")
            .append(ksqlTable.getStateStoreName())
            .append("\", \n")
            .append("\t\t\t \"iswindowed\": \"")
            .append(ksqlTable.isWindowed())
            .append("\", \n");
      }
      stringBuilder.append("\t\t\t \"fields\": [\n");
      boolean isFirstField = true;
      for (Field field : structuredDataSource.getSchema().fields()) {
        if (isFirstField) {
          isFirstField = false;
        } else {
          stringBuilder.append(", \n");
        }
        stringBuilder.append("\t\t\t     {\"name\": \"")
            .append(field.name())
            .append("\", \"type\": ")
            .append("\"")
            .append(getKsqlTypeInJson(field.schema()))
            .append("\"} ");
      }
      stringBuilder.append("\t\t\t ]\n\t\t}\n");
    }
    stringBuilder.append("\t ]\n");
  }

  void writeMetastoreToFile(String filePath, MetaStore metaStore) {
    StringBuilder stringBuilder = new StringBuilder("{ \n \"name\": \"ksql_catalog\",\n ");

    addTopics(stringBuilder, metaStore.getAllKsqlTopics());
    stringBuilder.append("\n\t, \n");
    addSchemas(stringBuilder, metaStore.getAllStructuredDataSources());
    stringBuilder.append("}");

    try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
      raf.writeBytes(stringBuilder.toString());
      raf.close();
    } catch (IOException e) {
      throw new KsqlException(" Could not write the schema into the file.");
    }
  }
}