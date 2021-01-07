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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

class KsqlDelimitedDeserializer implements Deserializer<List<?>> {

  private interface Parser {

    Object parse(String value);
  }

  private interface ParserFactory {

    Parser build(SqlType type);
  }

  private static final Map<SqlBaseType, ParserFactory> PARSERS = ImmutableMap
      .<SqlBaseType, ParserFactory>builder()
      .put(SqlBaseType.BOOLEAN, t -> Boolean::parseBoolean)
      .put(SqlBaseType.INTEGER, t -> Integer::parseInt)
      .put(SqlBaseType.BIGINT, t -> Long::parseLong)
      .put(SqlBaseType.DOUBLE, t -> Double::parseDouble)
      .put(SqlBaseType.STRING, t -> v -> v)
      .put(SqlBaseType.DECIMAL, KsqlDelimitedDeserializer::decimalParser)
      .put(SqlBaseType.TIMESTAMP,KsqlDelimitedDeserializer::timestampParser)
      .build();

  private final CSVFormat csvFormat;
  private final List<Parser> parsers;

  KsqlDelimitedDeserializer(
      final PersistenceSchema schema,
      final CSVFormat csvFormat
  ) {
    this.csvFormat = Objects.requireNonNull(csvFormat, "csvFormat");
    this.parsers = buildParsers(schema);
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public List<?> deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      final String recordCsvString = new String(bytes, StandardCharsets.UTF_8);
      final List<CSVRecord> csvRecords = CSVParser.parse(recordCsvString, csvFormat)
          .getRecords();

      if (csvRecords.isEmpty()) {
        throw new SerializationException("No fields in record");
      }

      final CSVRecord csvRecord = csvRecords.get(0);
      if (csvRecord == null || csvRecord.size() == 0) {
        throw new SerializationException("No fields in record.");
      }

      SerdeUtils.throwOnColumnCountMismatch(parsers.size(), csvRecord.size(), false, topic);

      final List<Object> values = new ArrayList<>(parsers.size());
      final Iterator<Parser> pIt = parsers.iterator();

      for (int i = 0; i < csvRecord.size(); i++) {
        final String value = csvRecord.get(i);
        final Parser parser = pIt.next();

        final Object parsed = value == null || value.isEmpty()
            ? null
            : parser.parse(value);

        values.add(parsed);
      }

      return values;
    } catch (final Exception e) {
      throw new SerializationException("Error deserializing delimited", e);
    }
  }

  @Override
  public void close() {
  }

  private static Parser decimalParser(final SqlType sqlType) {
    final SqlDecimal decimalType = (SqlDecimal) sqlType;
    return v -> DecimalUtil.ensureFit(new BigDecimal(v), decimalType);
  }

  private static Parser timestampParser(final SqlType sqlType) {
    return v -> new Timestamp(Long.parseLong(v));
  }

  private static List<Parser> buildParsers(final PersistenceSchema schema) {
    final List<Parser> parsers = new ArrayList<>(schema.columns().size());
    for (final SimpleColumn column : schema.columns()) {
      final SqlBaseType baseType = column.type().baseType();
      final ParserFactory parserFactory = PARSERS.get(baseType);
      if (parserFactory == null) {
        throw new KsqlException("The '" + FormatFactory.DELIMITED.name()
            + "' format does not support type '" + baseType + "', column: " + column.name());
      }

      parsers.add(parserFactory.build(column.type()));
    }
    return parsers;
  }
}
