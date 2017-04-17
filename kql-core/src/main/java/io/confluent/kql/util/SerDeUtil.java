/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.serde.avro.KQLGenericRowAvroDeserializer;
import io.confluent.kql.serde.avro.KQLGenericRowAvroSerializer;
import io.confluent.kql.serde.csv.KQLCsvDeserializer;
import io.confluent.kql.serde.csv.KQLCsvSerializer;
import io.confluent.kql.serde.csv.KQLCsvTopicSerDe;
import io.confluent.kql.serde.json.KQLJsonPOJODeserializer;
import io.confluent.kql.serde.json.KQLJsonPOJOSerializer;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;

/**
 * Created by hojjat on 12/9/16.
 */
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

  public static Serde<GenericRow> getGenericRowAvroSerde(final String schemaStr) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);

    final Serializer<GenericRow> genericRowSerializer = new KQLGenericRowAvroSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLGenericRowAvroDeserializer();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KQLTopicSerDe topicSerDe, Schema schema) {
    if (topicSerDe instanceof KQLAvroTopicSerDe) {
      KQLAvroTopicSerDe avroTopicSerDe = (KQLAvroTopicSerDe) topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(avroTopicSerDe.getSchemaString());
    } else if (topicSerDe instanceof KQLJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJSONSerde(schema);
    } else if (topicSerDe instanceof KQLCsvTopicSerDe) {
      return SerDeUtil.getGenericRowCsvSerde();
    } else {
      throw new KQLException("Unknown topic serde.");
    }
  }

}
