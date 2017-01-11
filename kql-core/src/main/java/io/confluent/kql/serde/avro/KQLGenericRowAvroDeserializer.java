package io.confluent.kql.serde.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.KQLConfig;

public class KQLGenericRowAvroDeserializer implements Deserializer<GenericRow> {

  String rowSchema;
  Schema.Parser parser;
  Schema schema;
  GenericDatumReader<GenericRecord> reader;

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    rowSchema = (String) map.get(KQLConfig.AVRO_SERDE_SCHEMA_CONFIG);
    if (rowSchema == null) {
      throw new SerializationException("Avro schema is not set for the deserializer.");
    }
    parser = new Schema.Parser();
    schema = parser.parse(rowSchema);
    reader = new GenericDatumReader<>(schema);
  }

  @Override
  public GenericRow deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    GenericRow genericRow = null;
    GenericRecord genericRecord = null;
    try {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      genericRecord = reader.read(genericRecord, decoder);
      List<Schema.Field> fields = genericRecord.getSchema().getFields();
      List columns = new ArrayList();
      for (Schema.Field field: fields) {
        columns.add(genericRecord.get(field.name()));
      }
      genericRow = new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
    return genericRow;
  }

  @Override
  public void close() {

  }
}
