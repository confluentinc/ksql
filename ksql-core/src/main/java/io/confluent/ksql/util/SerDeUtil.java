package io.confluent.ksql.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KQLGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KQLGenericRowAvroSerializer;
import io.confluent.ksql.serde.json.KQLJsonPOJODeserializer;
import io.confluent.ksql.serde.json.KQLJsonPOJOSerializer;

/**
 * Created by hojjat on 12/9/16.
 */
public class SerDeUtil {

  public static Serde<GenericRow> getGenericRowJSONSerde() {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", GenericRow.class);

    final Serializer<GenericRow> genericRowSerializer = new KQLJsonPOJOSerializer<>();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLJsonPOJODeserializer<>();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);

  }

  public static Serde<GenericRow> getGenericRowAvroSerde(String schemaStr) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put(KSQLConfig.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);

    final Serializer<GenericRow> genericRowSerializer = new KQLGenericRowAvroSerializer();
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KQLGenericRowAvroDeserializer();
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer,  genericRowDeserializer);
  }

  public static Serde<GenericRow> getRowSerDe(KQLTopicSerDe topicSerDe) {
    if (topicSerDe instanceof KQLAvroTopicSerDe) {
      KQLAvroTopicSerDe avroTopicSerDe = (KQLAvroTopicSerDe)topicSerDe;
      return SerDeUtil.getGenericRowAvroSerde(avroTopicSerDe.getSchemaString());
    } else {
      return SerDeUtil.getGenericRowJSONSerde();
    }
  }

}
