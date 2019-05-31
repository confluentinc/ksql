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
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Format the schema as SQL.
 */
public class SqlSchemaFormatter implements SchemaFormatter {

  private static final Map<Type, Formatter> SCHEMA_TYPE_TO_SQL_TYPE =
      ImmutableMap.<Schema.Type, Formatter>builder()
          .put(Schema.Type.INT32, ignored -> "INT")
          .put(Schema.Type.INT64, ignored -> "BIGINT")
          .put(Schema.Type.FLOAT32, ignored -> "DOUBLE")
          .put(Schema.Type.FLOAT64, ignored -> "DOUBLE")
          .put(Schema.Type.BOOLEAN, ignored -> "BOOLEAN")
          .put(Schema.Type.STRING, ignored -> "VARCHAR")
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
    return formatSchema(new Context(schema, options, true, reservedWords));
  }

  private static String formatSchema(final Context context) {
    final String type = formatSchemaType(context);
    if (!context.options.contains(Option.APPEND_NOT_NULL) || context.schema.isOptional()) {
      return type;
    }
    return type + " NOT NULL";
  }

  private static String formatSchemaType(final Context context) {
    final Formatter formatter = SCHEMA_TYPE_TO_SQL_TYPE.get(context.schema.type());
    if (formatter == null) {
      throw new KsqlException("Invalid type in schema: " + context.schema.toString());
    }

    return formatter.apply(context);
  }

  private static String formatArray(final Context context) {
    return "ARRAY<"
        + formatSchema(context.withSchema(context.schema.valueSchema()))
        + ">";
  }

  private static String formatMap(final Context context) {
    return "MAP<"
        + formatSchema(context.withSchema(context.schema.keySchema())) + ", "
        + formatSchema(context.withSchema(context.schema.valueSchema()))
        + ">";
  }

  private static String formatStruct(final Context context) {
    final String prefix;
    final String postFix;
    if (context.topLevel && context.options.contains(Option.AS_COLUMN_LIST)) {
      prefix = "";
      postFix = "";
    } else {
      prefix = "STRUCT<";
      postFix = ">";
    }

    return context.schema.fields().stream()
        .map(field ->
            quote(field.name(), context.reservedWords)
                + " " + formatSchema(context.withSchema(field.schema())))
        .collect(Collectors.joining(", ", prefix, postFix));
  }

  private static String quote(final String value, final Set<String> reservedWords) {
    return reservedWords.contains(value) ? "`" + value + "`" : value;
  }

  private static final class Context {
    final Schema schema;
    final Set<Option> options;
    final boolean topLevel;
    final Set<String> reservedWords;

    private Context(
        final Schema schema,
        final Set<Option> options,
        final boolean topLevel,
        final Set<String> reservedWords
    ) {
      this.schema = schema;
      this.options = options;
      this.topLevel = topLevel;
      this.reservedWords = reservedWords;
    }

    Context withSchema(final Schema newSchema) {
      // after changing the schema, it is no longer a top level schema
      return new Context(newSchema, options, false, reservedWords);
    }
  }

  private interface Formatter extends Function<Context, String> {
  }
}
