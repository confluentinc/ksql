/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KsqlGenericRowAvroDeserializer implements Deserializer<GenericRow> {

  private final org.apache.kafka.connect.data.Schema schema;

  String rowSchema;
  Schema.Parser parser;
  Schema avroSchema;
  GenericDatumReader<GenericRecord> reader;

  public KsqlGenericRowAvroDeserializer(org.apache.kafka.connect.data.Schema schema) {
    this.schema = schema;
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
    rowSchema = (String) map.get(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG);
    if (rowSchema == null) {
      throw new SerializationException("Avro schema is not set for the deserializer.");
    }
    parser = new Schema.Parser();
    avroSchema = parser.parse(rowSchema);
    reader = new GenericDatumReader<>(avroSchema);
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    GenericRow genericRow = null;
    GenericRecord genericRecord = null;
    try {
      Decoder decoder = DecoderFactory.get().binaryDecoder((bytes[0] == 0)?
                                                           removeSchemaRegistryMetaBytes(bytes):
                                                           bytes, null);
      genericRecord = reader.read(genericRecord, decoder);
      List<Schema.Field> fields = genericRecord.getSchema().getFields();
      List columns = new ArrayList();
      for (Schema.Field field : fields) {
        columns.add(enforceFieldType(field.schema(), genericRecord.get(field.name())));
      }
      genericRow = new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
    return genericRow;
  }

  private byte[] removeSchemaRegistryMetaBytes(final byte[] data) {
    byte[] avroBytes = new byte[data.length - 5];
    for (int i = 5; i < data.length; i++) {
      avroBytes[i-5] = data[i];
    }
    return avroBytes;
  }

  private Object enforceFieldType(Schema fieldSchema, Object value) {

    switch (fieldSchema.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case DOUBLE:
      case STRING:
      case MAP:
        return value;
      case ARRAY:
        GenericData.Array genericArray = (GenericData.Array) value;
        Class elementClass = getJavaTypeForAvroType(fieldSchema.getElementType());
        Object[] arrayField =
            (Object[]) java.lang.reflect.Array.newInstance(elementClass, genericArray.size());
        for (int i = 0; i < genericArray.size(); i++) {
          Object obj = enforceFieldType(fieldSchema.getElementType(), genericArray.get(i));
          arrayField[i] = obj;
        }
        return arrayField;
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.getType());

    }
  }

  private static Class getJavaTypeForAvroType(final Schema schema) {
    switch (schema.getType()) {
      case STRING:
        return String.class;
      case BOOLEAN:
        return Boolean.class;
      case INT:
        return Integer.class;
      case LONG:
        return Long.class;
      case DOUBLE:
        return Double.class;
      case ARRAY:
        Class elementClass = getJavaTypeForAvroType(schema.getElementType());
        return java.lang.reflect.Array.newInstance(elementClass, 0).getClass();
      case MAP:
        return (new HashMap<>()).getClass();
      default:
        throw new KsqlException("Type is not supported: " + schema.getType());
    }
  }


  @Override
  public void close() {

  }
}
