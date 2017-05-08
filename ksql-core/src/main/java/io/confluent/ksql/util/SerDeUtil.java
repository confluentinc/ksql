/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KQLGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KQLGenericRowAvroSerializer;
import io.confluent.ksql.serde.csv.KQLCsvDeserializer;
import io.confluent.ksql.serde.csv.KQLCsvSerializer;
import io.confluent.ksql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KQLJsonPOJODeserializer;
import io.confluent.ksql.serde.json.KQLJsonPOJOSerializer;
import io.confluent.ksql.serde.json.KQLJsonTopicSerDe;
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

    final Serializer<GenericRow> genericRowSerializer = new KQLJsonPOJOSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLJsonPOJODeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);

  }

  private static Serde<GenericRow> getGenericRowCsvSerde() {
    Map<String, Object> serdeProps = new HashMap<>();

    final Serializer<GenericRow> genericRowSerializer = new KQLCsvSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLCsvDeserializer();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getGenericRowAvroSerde(final Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    String avroSchemaString = new MetastoreUtil().buildAvroSchema(schema, "AvroSchema");
    serdeProps.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchemaString);

    final Serializer<GenericRow> genericRowSerializer = new KQLGenericRowAvroSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLGenericRowAvroDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KQLTopicSerDe topicSerDe, Schema schema) {
    if (topicSerDe instanceof KQLAvroTopicSerDe) {
      KQLAvroTopicSerDe avroTopicSerDe = (KQLAvroTopicSerDe) topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(schema);
    } else if (topicSerDe instanceof KQLJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJSONSerde(schema);
    } else if (topicSerDe instanceof KQLCsvTopicSerDe) {
      return SerDeUtil.getGenericRowCsvSerde();
    } else {
      throw new KQLException("Unknown topic serde.");
    }
  }

}
