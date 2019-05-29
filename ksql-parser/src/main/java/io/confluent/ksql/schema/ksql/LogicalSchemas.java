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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Util class for converting to and from KSQL's logical schema.
 *
 * <p>KSQL has four main type systems:
 *
 * <ul>
 *   <li><b>Physical:</b> - the type system of the physical bytes strored in Kafka.
 *   This may be JSON, Avro, Delimited, etc. Some formats have associated schemas, e.g. Avro.</li>
 *   <li><b>Standardized Physical:</b> - the serde layer in KSQL defines the standardized schema of
 *   the physical data using the Connect {@code Schema} type, e.g. an Avro schema will be converted
 *   into a standardised Connect schema.</li>
 *   <li><b>Logical:</b> - the SQL types converted into the corresponding {@code Schema} type.</li>
 *   <li><b>SQL:</b> - the SQL type system, e.g. INTEGER, BIGINT, ARRAY&lt;something&gt;, etc</li>
 * </ul>
 *
 * <p>It can be confusing as both the Logical and Standardized Physical layers make use of the
 * Connect {@code Schema} type. However, the <i>Logical Schema</i> only holds types that can be
 * directly converted to <i>SQL</i> types. Where as the <i>Physical Schema</i> is the
 * <i>actual</i> schema of the Data in Kafka, which may contain types KSQL does not support,
 * or only support once coerced to other types.
 */
public final class LogicalSchemas {

  public static final Schema BOOLEAN = Schema.OPTIONAL_BOOLEAN_SCHEMA;
  public static final Schema INTEGER = Schema.OPTIONAL_INT32_SCHEMA;
  public static final Schema BIGINT = Schema.OPTIONAL_INT64_SCHEMA;
  public static final Schema DOUBLE = Schema.OPTIONAL_FLOAT64_SCHEMA;
  public static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;

  private static final LogicalToSqlTypeConverter TO_SQL_CONVERTER = new ToSqlTypeConverter();
  private static final SqlTypeToLogicalConverter FROM_SQL_CONVERTER = new FromSqlTypeConverter();

  private LogicalSchemas() {
  }

  public interface LogicalToSqlTypeConverter {
    /**
     * Convert the supplied logical KSQL {@code schema} to its corresponding SQL type.
     *
     * @param schema the logical KSQL schema.
     * @return the sql type.
     */
    Type toSqlType(Schema schema);
  }

  public interface SqlTypeToLogicalConverter {

    /**
     * @see #fromSqlType(Type, String, String)
     */
    Schema fromSqlType(Type sqlType);

    /**
     * Convert the supplied primitive {@code sqlType} to its corresponding logical KSQL schema.
     *
     * @param sqlType the sql type to convert
     * @param name    the name for the schema
     * @param doc     the doc for the schema
     * @return the logical schema.
     */
    Schema fromSqlType(Type sqlType, String name, String doc);
  }

  public static LogicalToSqlTypeConverter toSqlTypeConverter() {
    return TO_SQL_CONVERTER;
  }

  public static SqlTypeToLogicalConverter fromSqlTypeConverter() {
    return FROM_SQL_CONVERTER;
  }

  private static final class ToSqlTypeConverter implements LogicalToSqlTypeConverter {

    private static final Map<Schema.Type, Function<Schema, Type>> LOGICAL_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, Type>>builder()
        .put(Schema.Type.INT32, s -> PrimitiveType.of(Type.SqlType.INTEGER))
        .put(Schema.Type.INT64, s -> PrimitiveType.of(Type.SqlType.BIGINT))
        .put(Schema.Type.FLOAT32, s -> PrimitiveType.of(Type.SqlType.DOUBLE))
        .put(Schema.Type.FLOAT64, s -> PrimitiveType.of(Type.SqlType.DOUBLE))
        .put(Schema.Type.BOOLEAN, s -> PrimitiveType.of(Type.SqlType.BOOLEAN))
        .put(Schema.Type.STRING, s -> PrimitiveType.of(Type.SqlType.STRING))
        .put(Schema.Type.ARRAY, ToSqlTypeConverter::toSqlArray)
        .put(Schema.Type.MAP, ToSqlTypeConverter::toSqlMap)
        .put(Schema.Type.STRUCT, ToSqlTypeConverter::toSqlStruct)
        .build();

    @Override
    public Type toSqlType(final Schema schema) {
      return sqlType(schema);
    }

    private static Type sqlType(final Schema schema) {
      final Function<Schema, Type> handler = LOGICAL_TO_SQL.get(schema.type());
      if (handler == null) {
        throw new KsqlException("Unexpected logical type: " + schema);
      }

      return handler.apply(schema);
    }

    private static Array toSqlArray(final Schema schema) {
      return io.confluent.ksql.parser.tree.Array.of(sqlType(schema.valueSchema()));
    }

    private static Type toSqlMap(final Schema schema) {
      if (schema.keySchema().type() != Schema.Type.STRING) {
        throw new KsqlException("Unsupported map key type: " + schema.keySchema());
      }
      return io.confluent.ksql.parser.tree.Map.of(sqlType(schema.valueSchema()));
    }

    private static Struct toSqlStruct(final Schema schema) {
      final Struct.Builder builder = Struct.builder();

      schema.schema().fields()
          .forEach(field -> builder.addField(field.name(), sqlType(field.schema())));

      return builder.build();
    }
  }

  private static final class FromSqlTypeConverter implements SqlTypeToLogicalConverter {

    private static final Map<Type.SqlType, Function<Type, SchemaBuilder>> SQL_TO_LOGICAL =
        ImmutableMap.<Type.SqlType, Function<Type, SchemaBuilder>>builder()
        .put(Type.SqlType.STRING, t -> SchemaBuilder.string().optional())
        .put(Type.SqlType.BOOLEAN, t -> SchemaBuilder.bool().optional())
        .put(Type.SqlType.INTEGER, t -> SchemaBuilder.int32().optional())
        .put(Type.SqlType.BIGINT, t -> SchemaBuilder.int64().optional())
        .put(Type.SqlType.DOUBLE, t -> SchemaBuilder.float64().optional())
        .put(Type.SqlType.ARRAY, t -> FromSqlTypeConverter.fromSqlArray((Array) t))
        .put(Type.SqlType.MAP, t -> FromSqlTypeConverter
            .fromSqlMap((io.confluent.ksql.parser.tree.Map) t))
        .put(Type.SqlType.STRUCT, t -> FromSqlTypeConverter.fromSqlStruct((Struct) t))
        .build();

    @Override
    public Schema fromSqlType(final Type sqlType) {
      return logicalType(sqlType).build();
    }

    @Override
    public Schema fromSqlType(final Type type, final String name, final String doc) {
      return logicalType(type).name(name).doc(doc).build();
    }

    private static SchemaBuilder logicalType(final Type sqlType) {
      final Function<Type, SchemaBuilder> handler = SQL_TO_LOGICAL.get(sqlType.getSqlType());
      if (handler == null) {
        throw new KsqlException("Unexpected sql type: " + sqlType);
      }

      return handler.apply(sqlType);
    }

    private static SchemaBuilder fromSqlArray(final Array sqlType) {
      return SchemaBuilder
          .array(logicalType(sqlType.getItemType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlMap(final io.confluent.ksql.parser.tree.Map sqlType) {
      return SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, logicalType(sqlType.getValueType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlStruct(final Struct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.getFields()
          .forEach(field -> builder.field(field.getName(), logicalType(field.getType()).build()));

      return builder
          .optional();
    }
  }
}
