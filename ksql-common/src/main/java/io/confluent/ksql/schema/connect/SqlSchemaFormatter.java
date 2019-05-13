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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Format the schema as SQL.
 */
public class SqlSchemaFormatter implements SchemaFormatter {

  private static final Map<Type, Formatter> SCHEMA_TYPE_TO_SQL_TYPE =
      ImmutableMap.<Schema.Type, Formatter>builder()
          .put(Schema.Type.INT32, (s, o, tl) -> "INT")
          .put(Schema.Type.INT64, (s, o, tl) -> "BIGINT")
          .put(Schema.Type.FLOAT32, (s, o, tl) -> "DOUBLE")
          .put(Schema.Type.FLOAT64, (s, o, tl) -> "DOUBLE")
          .put(Schema.Type.BOOLEAN, (s, o, tl) -> "BOOLEAN")
          .put(Schema.Type.STRING, (s, o, tl) -> "VARCHAR")
          .put(Schema.Type.ARRAY, SqlSchemaFormatter::formatArray)
          .put(Schema.Type.MAP, SqlSchemaFormatter::formatMap)
          .put(Schema.Type.STRUCT, SqlSchemaFormatter::formatStruct)
          .build();

  public enum Option {
    /**
     * Append {@code NOT NULL} for non-optional schemas.
     */
    APPEND_NOT_NULL,

    /**
     * If the schema is a {@code STRUCT} list the columns in the form {@code [col0 type, ...]}.
     *
     * <p>The default form would be {@code STRUCT<col0 type, ...>}.
     */
    AS_COLUMN_LIST
  }

  public static final SqlSchemaFormatter DEFAULT = new SqlSchemaFormatter();
  public static final SqlSchemaFormatter STRICT = new SqlSchemaFormatter(Option.APPEND_NOT_NULL);

  private final Set<Option> options;

  public SqlSchemaFormatter(final Option... options) {
    this.options = options.length == 0
        ? EnumSet.noneOf(Option.class)
        : EnumSet.of(options[0], options);
  }

  @Override
  public String format(final Schema schema) {
    return formatSchema(schema, options, true);
  }

  private static String formatSchema(
      final Schema schema,
      final Set<Option> options,
      final boolean topLevel
  ) {
    final String type = formatSchemaType(schema, options, topLevel);
    if (!options.contains(Option.APPEND_NOT_NULL) || schema.isOptional()) {
      return type;
    }
    return type + " NOT NULL";
  }

  private static String formatSchemaType(
      final Schema schema,
      final Set<Option> options,
      final boolean topLevel
  ) {
    final Formatter formatter = SCHEMA_TYPE_TO_SQL_TYPE.get(schema.type());
    if (formatter == null) {
      throw new KsqlException("Invalid type in schema: " + schema.toString());
    }

    return formatter.format(schema, options, topLevel);
  }

  private static String formatArray(
      final Schema s,
      final Set<Option> options,
      final boolean ignored
  ) {
    return "ARRAY<"
        + formatSchema(s.valueSchema(), options, false)
        + ">";
  }

  private static String formatMap(
      final Schema s,
      final Set<Option> options,
      final boolean ignored
  ) {
    return "MAP<"
        + formatSchema(s.keySchema(), options, false) + ", "
        + formatSchema(s.valueSchema(), options, false)
        + ">";
  }

  private static String formatStruct(
      final Schema schema,
      final Set<Option> options,
      final boolean topLevel
  ) {
    final String prefix;
    final String postFix;
    if (topLevel && options.contains(Option.AS_COLUMN_LIST)) {
      prefix = "[";
      postFix = "]";
    } else {
      prefix = "STRUCT<";
      postFix = ">";
    }

    return schema.fields().stream()
        .map(field -> field.name() + " " + formatSchema(field.schema(), options, false))
        .collect(Collectors.joining(", ", prefix, postFix));
  }

  private interface Formatter {

    String format(Schema schema, Set<Option> options, boolean topLevel);
  }
}
