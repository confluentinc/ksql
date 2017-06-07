/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
import io.confluent.ksql.serde.csv.KsqlCsvDeserializer;
import io.confluent.ksql.serde.csv.KsqlCsvSerializer;
import io.confluent.ksql.serde.csv.KsqlCsvTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonPojoDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonPojoSerializer;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;


public class SerDeUtil {

  public static Serde<GenericRow> getGenericRowJSONSerde(Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", GenericRow.class);

    final Serializer<GenericRow> genericRowSerializer = new KsqlJsonPojoSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlJsonPojoDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);

  }

  private static Serde<GenericRow> getGenericRowCsvSerde() {
    Map<String, Object> serdeProps = new HashMap<>();

    final Serializer<GenericRow> genericRowSerializer = new KsqlCsvSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlCsvDeserializer();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getGenericRowAvroSerde(final Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    String avroSchemaString = new MetastoreUtil().buildAvroSchema(schema, "AvroSchema");
    serdeProps.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchemaString);

    final Serializer<GenericRow> genericRowSerializer = new KsqlGenericRowAvroSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlGenericRowAvroDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KsqlTopicSerDe topicSerDe, Schema schema) {
    if (topicSerDe instanceof KsqlAvroTopicSerDe) {
      KsqlAvroTopicSerDe avroTopicSerDe = (KsqlAvroTopicSerDe) topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(schema);
    } else if (topicSerDe instanceof KsqlJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJSONSerde(schema);
    } else if (topicSerDe instanceof KsqlCsvTopicSerDe) {
      return SerDeUtil.getGenericRowCsvSerde();
    } else {
      throw new KsqlException("Unknown topic serde.");
    }
  }

}
