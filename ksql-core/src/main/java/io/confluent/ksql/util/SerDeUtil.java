/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.physical.GenericRow;
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

  public static Serde<GenericRow> getGenericRowAvroSerde(final Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    String avroSchemaString = new MetastoreUtil().buildAvroSchema(schema, DdlConfig.AVRO_SCHEMA);
    serdeProps.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchemaString);

    final Serializer<GenericRow> genericRowSerializer = new KsqlGenericRowAvroSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer =
        new KsqlGenericRowAvroDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KsqlTopicSerDe topicSerDe, Schema schema) {
    if (topicSerDe instanceof KsqlAvroTopicSerDe) {
      KsqlAvroTopicSerDe avroTopicSerDe = (KsqlAvroTopicSerDe) topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(schema);
    } else if (topicSerDe instanceof KsqlJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJsonSerde(schema);
    } else if (topicSerDe instanceof KsqlDelimitedTopicSerDe) {
      return SerDeUtil.getGenericRowDelimitedSerde(schema);
    } else {
      throw new KsqlException("Unknown topic serde.");
    }
  }

}
