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

package io.confluent.ksql.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
import io.confluent.ksql.serde.delimited.KsqlDelimitedDeserializer;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerializer;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;


public class SerDeUtil {

  public static Serde<GenericRow> getGenericRowJsonSerde(Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", GenericRow.class);

    final Serializer<GenericRow> genericRowSerializer = new KsqlJsonSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlJsonDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);

  }

  private static Serde<GenericRow> getGenericRowDelimitedSerde(final Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();

    final Serializer<GenericRow> genericRowSerializer = new KsqlDelimitedSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlDelimitedDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getGenericRowAvroSerde(final Schema schema, final KsqlConfig ksqlConfig) {
    Map<String, Object> serdeProps = new HashMap<>();
    String avroSchemaString = new MetastoreUtil().buildAvroSchema(schema, DdlConfig.AVRO_SCHEMA);
    serdeProps.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchemaString);

    SchemaRegistryClient schemaRegistryClient;
    if (ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
        .equalsIgnoreCase(SchemaRegistryClientFactory.schemaRegistryUrl)) {
      schemaRegistryClient = SchemaRegistryClientFactory.getSchemaRegistryClient();
    } else {
      schemaRegistryClient = new CachedSchemaRegistryClient(ksqlConfig.getString(KsqlConfig
                                                                                     .SCHEMA_REGISTRY_URL_PROPERTY), 1000);
    }
    final Serializer<GenericRow> genericRowSerializer = new KsqlGenericRowAvroSerializer(schema,
                                                                                         schemaRegistryClient, ksqlConfig);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer =
        new KsqlGenericRowAvroDeserializer(schema, schemaRegistryClient);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KsqlTopicSerDe topicSerDe, Schema schema,
                                              KsqlConfig ksqlConfig) {
    if (topicSerDe instanceof KsqlAvroTopicSerDe) {
      return SerDeUtil.getGenericRowAvroSerde(schema, ksqlConfig);
    } else if (topicSerDe instanceof KsqlJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJsonSerde(schema);
    } else if (topicSerDe instanceof KsqlDelimitedTopicSerDe) {
      return SerDeUtil.getGenericRowDelimitedSerde(schema);
    } else {
      throw new KsqlException("Unknown topic serde.");
    }
  }

  public static synchronized Schema getSchemaFromAvro(String avroSchemaString) {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);

    SchemaBuilder inferredSchema = SchemaBuilder.struct().name(avroSchema.getName());
    for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
      inferredSchema.field(avroField.name(), getKSQLSchemaForAvroSchema(avroField.schema()));
    }

    return inferredSchema.build();
  }

  public static Schema getKSQLSchemaForAvroSchema(org.apache.avro.Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
        return Schema.INT16_SCHEMA;
      case LONG:
        return Schema.FLOAT64_SCHEMA;
      case DOUBLE:
      case FLOAT:
        return Schema.FLOAT64_SCHEMA;
      case BOOLEAN:
        return Schema.BOOLEAN_SCHEMA;
      case STRING:
        return Schema.STRING_SCHEMA;
      case ARRAY:
        return SchemaBuilder.array(getKSQLSchemaForAvroSchema(avroSchema.getElementType()));
      case MAP:
        return SchemaBuilder.map(Schema.STRING_SCHEMA,
                                 getKSQLSchemaForAvroSchema(avroSchema.getValueType()));
      default:
        throw new KsqlException(String.format("Cannot find correct type for avro type: %s",
                                              avroSchema.getFullName()));
    }
  }

}
