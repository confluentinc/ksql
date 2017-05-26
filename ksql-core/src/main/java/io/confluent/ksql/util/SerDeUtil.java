/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KSQLGenericRowAvroSerializer;
import io.confluent.ksql.serde.csv.KSQLCsvDeserializer;
import io.confluent.ksql.serde.csv.KSQLCsvSerializer;
import io.confluent.ksql.serde.csv.KSQLCsvTopicSerDe;
import io.confluent.ksql.serde.json.KSQLJsonPOJODeserializer;
import io.confluent.ksql.serde.json.KSQLJsonPOJOSerializer;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
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

    final Serializer<GenericRow> genericRowSerializer = new KSQLJsonPOJOSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KSQLJsonPOJODeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);

  }

  private static Serde<GenericRow> getGenericRowCsvSerde() {
    Map<String, Object> serdeProps = new HashMap<>();

    final Serializer<GenericRow> genericRowSerializer = new KSQLCsvSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KSQLCsvDeserializer();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getGenericRowAvroSerde(final Schema schema) {
    Map<String, Object> serdeProps = new HashMap<>();
    String avroSchemaString = new MetastoreUtil().buildAvroSchema(schema, "AvroSchema");
    serdeProps.put(KSQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchemaString);

    final Serializer<GenericRow> genericRowSerializer = new KSQLGenericRowAvroSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KSQLGenericRowAvroDeserializer(schema);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(final KSQLTopicSerDe topicSerDe, Schema schema) {
    if (topicSerDe instanceof KSQLAvroTopicSerDe) {
      KSQLAvroTopicSerDe avroTopicSerDe = (KSQLAvroTopicSerDe) topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(schema);
    } else if (topicSerDe instanceof KSQLJsonTopicSerDe) {
      return SerDeUtil.getGenericRowJSONSerde(schema);
    } else if (topicSerDe instanceof KSQLCsvTopicSerDe) {
      return SerDeUtil.getGenericRowCsvSerde();
    } else {
      throw new KSQLException("Unknown topic serde.");
    }
  }

}
