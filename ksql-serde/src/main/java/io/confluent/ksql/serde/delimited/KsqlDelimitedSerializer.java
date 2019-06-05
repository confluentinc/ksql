/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import io.confluent.ksql.util.KsqlException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;


public class KsqlDelimitedSerializer implements Serializer<Object> {

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public byte[] serialize(final String topic, final Object data) {
    if (data == null) {
      return null;
    }

    try {
      if (!(data instanceof Struct)) {
        throw new SerializationException("DELIMITED does not support anonymous fields");
      }

      final StringWriter stringWriter = new StringWriter();
      final CSVPrinter csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT);
      csvPrinter.printRecord(() -> new FieldIterator((Struct)data));
      final String result = stringWriter.toString();
      return result.substring(0, result.length() - 2).getBytes(StandardCharsets.UTF_8);
    } catch (final Exception e) {
      throw new SerializationException("Error serializing CSV message", e);
    }
  }

  @Override
  public void close() {
  }

  private static class FieldIterator implements Iterator<Object> {

    private final Struct data;
    private final Iterator<Field> fieldIt;

    FieldIterator(final Struct data) {
      this.data = Objects.requireNonNull(data, "data");
      this.fieldIt = data.schema().fields().iterator();
    }

    @Override
    public boolean hasNext() {
      return fieldIt.hasNext();
    }

    @Override
    public Object next() {
      final Field field = fieldIt.next();
      throwOnUnsupportedType(field.schema());
      return data.get(field);
    }

    private static void throwOnUnsupportedType(final Schema schema) {
      switch (schema.type()) {
        case ARRAY:
        case MAP:
        case STRUCT:
          throw new KsqlException("DELIMITED does not support type: " + schema.type());

        default:
      }
    }
  }
}
