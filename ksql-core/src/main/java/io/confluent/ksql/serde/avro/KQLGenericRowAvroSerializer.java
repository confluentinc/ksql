package io.confluent.ksql.serde.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.KSQLConfig;

public class KQLGenericRowAvroSerializer implements Serializer<GenericRow> {

  String rowSchema;
  Schema.Parser parser;
  Schema schema;
  GenericDatumWriter<GenericRecord> writer;
  ByteArrayOutputStream output;
  Encoder encoder;
  List<Schema.Field> fields;

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    rowSchema = (String) map.get(KSQLConfig.AVRO_SERDE_SCHEMA_CONFIG);
    if (rowSchema == null) {
      throw new SerializationException("Avro schema is not set for the serializer.");
    }
    parser = new Schema.Parser();
    schema = parser.parse(rowSchema);
    fields = schema.getFields();
    writer = new GenericDatumWriter<>(schema);
  }

  @Override
  public byte[] serialize(String topic, GenericRow genericRow) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      avroRecord.put(fields.get(i).name(), genericRow.columns.get(i));
    }

    try {
      output = new ByteArrayOutputStream();
      encoder = EncoderFactory.get().binaryEncoder(output, null);
      writer.write(avroRecord, encoder);
      encoder.flush();
      output.flush();
    } catch (IOException e) {
      throw new SerializationException("Error serializing AVRO message", e);
    }
    return output.toByteArray();
  }

  @Override
  public void close() {

  }

  private GenericRecord getGenericRecord(Schema schema, GenericRow genericRow) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("ordertime", genericRow.columns.get(0));
    avroRecord.put("orderid", genericRow.columns.get(1));
    avroRecord.put("itemid", genericRow.columns.get(2));
    avroRecord.put("orderunits", genericRow.columns.get(3));
    return avroRecord;
  }
}
