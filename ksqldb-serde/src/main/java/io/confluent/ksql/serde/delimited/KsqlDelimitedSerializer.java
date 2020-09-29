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

import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;


class KsqlDelimitedSerializer implements Serializer<List<?>> {

  private final PersistenceSchema schema;
  private final CSVFormat csvFormat;

  KsqlDelimitedSerializer(
      final PersistenceSchema schema,
      final CSVFormat csvFormat
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.csvFormat = Objects.requireNonNull(csvFormat, "csvFormat");
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public byte[] serialize(final String topic, final List<?> data) {
    if (data == null) {
      return null;
    }

    try {
      final StringWriter stringWriter = new StringWriter();
      final CSVPrinter csvPrinter = new CSVPrinter(stringWriter, csvFormat);
      csvPrinter.printRecord(() -> new FieldIterator(data, schema));
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

    private final Iterator<?> dataIt;
    private final Iterator<SimpleColumn> columnIt;

    FieldIterator(final List<?> data, final PersistenceSchema schema) {
      this.dataIt = data.iterator();
      this.columnIt = schema.columns().iterator();
    }

    @Override
    public boolean hasNext() {
      return columnIt.hasNext();
    }

    @Override
    public Object next() {
      final Object value = dataIt.next();
      final SimpleColumn column = columnIt.next();

      return column.type().baseType().equals(SqlBaseType.DECIMAL)
          ? handleDecimal((BigDecimal) value)
          : value;
    }

    private static String handleDecimal(final BigDecimal value) {
      // Avoid scientific notation for now:
      return value.toPlainString();
    }
  }
}
