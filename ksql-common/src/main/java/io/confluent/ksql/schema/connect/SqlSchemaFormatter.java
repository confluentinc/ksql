/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.connect;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

/**
 * Format the schema as SQL.
 */
public class SqlSchemaFormatter implements SchemaFormatter {

  private static final String MAP_START = "MAP<";
  private static final String ARRAY_START = "ARRAY<";
  private static final String STRUCT_START = "STRUCT<";
  private static final String STRUCTURED_END = ">";
  private static final String NOT_NULL_SUFFIX = " NOT NULL";

  public enum Option {
    /**
     * Append {@code NOT NULL} for non-optional schemas.
     */
    APPEND_NOT_NULL,

    /**
     * If the schema is a {@code STRUCT} list the columns in the form
     * {@code col0 type, col1 type, ...}.
     *
     * <p>The default form would be {@code STRUCT<col0 type, col1 type, ...>}.
     */
    AS_COLUMN_LIST
  }

  public static final SqlSchemaFormatter DEFAULT = new SqlSchemaFormatter();
  public static final SqlSchemaFormatter STRICT = new SqlSchemaFormatter(Option.APPEND_NOT_NULL);

  private final Set<Option> options;
  private final Set<String> reservedWords;

  public SqlSchemaFormatter(final Option... options) {
    this(ImmutableSet.of(), options);
  }

  public SqlSchemaFormatter(final Set<String> reservedWords, final Option... options) {
    this.options = options.length == 0
        ? EnumSet.noneOf(Option.class)
        : EnumSet.of(options[0], options);
    this.reservedWords = Objects.requireNonNull(reservedWords, "reservedWords");
  }

  @Override
  public String format(final Schema schema) {
    final String converted = SchemaWalker.visit(schema, new Converter()) + typePostFix(schema);

    return options.contains(Option.AS_COLUMN_LIST)
        ? stripTopLevelStruct(converted)
        : converted;
  }

  private String quoteIfReserved(final String value) {
    return reservedWords.contains(value) ? "`" + value + "`" : value;
  }

  private String typePostFix(final Schema schema) {
    if (options.contains(Option.APPEND_NOT_NULL) && !schema.isOptional()) {
      return NOT_NULL_SUFFIX;
    }

    return "";
  }

  private static String stripTopLevelStruct(final String toStrip) {
    if (!toStrip.startsWith(STRUCT_START)) {
      return toStrip;
    }

    final String suffixStripped = toStrip.endsWith(NOT_NULL_SUFFIX)
        ? toStrip.substring(0, toStrip.length() - NOT_NULL_SUFFIX.length())
        : toStrip;

    return suffixStripped
        .substring(STRUCT_START.length(), suffixStripped.length() - STRUCTURED_END.length());
  }

  private final class Converter implements SchemaWalker.Visitor<String, String> {

    public String visitSchema(final Schema schema) {
      throw new KsqlException("Invalid type in schema: " + schema);
    }

    public String visitBoolean(final Schema schema) {
      return "BOOLEAN";
    }

    public String visitInt32(final Schema schema) {
      return "INT";
    }

    public String visitInt64(final Schema schema) {
      return "BIGINT";
    }

    public String visitFloat64(final Schema schema) {
      return "DOUBLE";
    }

    public String visitString(final Schema schema) {
      return "VARCHAR";
    }

    @Override
    public String visitBytes(final Schema schema) {
      DecimalUtil.requireDecimal(schema);
      return "DECIMAL("
          + DecimalUtil.precision(schema) + ", "
          + DecimalUtil.scale(schema) + ")";
    }

    public String visitArray(final Schema schema, final String element) {
      return ARRAY_START
          + element + typePostFix(schema.valueSchema())
          + STRUCTURED_END;
    }

    public String visitMap(final Schema schema, final String key, final String value) {
      return MAP_START
          + key + typePostFix(schema.keySchema()) + ", "
          + value + typePostFix(schema.valueSchema())
          + STRUCTURED_END;
    }

    public String visitStruct(final Schema schema, final List<? extends String> fields) {
      return fields.stream()
          .collect(Collectors.joining(", ", STRUCT_START, STRUCTURED_END));
    }

    public String visitField(final Field field, final String type) {
      final Schema schema = field.schema();
      final String typePostFix = typePostFix(schema);

      return quoteIfReserved(field.name()) + " " + type + typePostFix;
    }
  }
}
