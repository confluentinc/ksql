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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Util class for converting between KSQL's different type systems.
 *
 * <p>KSQL the following main type systems / schema types:
 *
 * <ul>
 *   <li>
 *     <b>{@link SqlType}:</b>
 *     - the SQL type system, e.g. INTEGER, BIGINT, ARRAY&lt;something&gt;, etc
 *   </li>
 *   <li>
 *     <b>{@link LogicalSchema}:</b>
 *     - the set of named fields of some {@link SqlType} that represent the schema of one row in a
 *     KSQL stream or table.
 *   </li>
 *   <li>
 *     <b>PhysicalSchema</b>
 *     - the schema of how all the row's parts, e.g. key, value, are serialized to/from Kafka.
 *   </li>
 *   <li>
 *     <b>{@link PersistenceSchema}:</b>
 *     - the schema of how one part of the row, e.g. key, value, is serialized to/from Kafka.
 *   </li>
 *   <li>
 *     <b>Connect's {@link Schema}:</b>
 *  *     - used internally in the serde classes and else where to hold schema information.
 *   </li>
 * </ul>
 */
public final class SchemaConverters {

  private static final ConnectToSqlTypeConverter CONNECT_TO_SQL_CONVERTER =
      new ConnectToSqlConverter();

  private static final SqlToConnectTypeConverter SQL_TO_CONNECT_CONVERTER =
      new ConnectFromSqlConverter();

  private static final JavaToSqlTypeConverter JAVA_TO_SQL_CONVERTER =
      new JavaToSqlConverter();

  private static final SqlToJavaTypeConverter SQL_TO_JAVA_CONVERTER =
      new SqlToJavaConverter();

  private SchemaConverters() {
  }

  public interface ConnectToSqlTypeConverter {
    /**
     * Convert the supplied Connect {@code schema} to its corresponding SQL type.
     *
     * @param schema the Connect schema.
     * @return the sql type.
     */
    SqlType toSqlType(Schema schema);
  }

  public interface SqlToConnectTypeConverter {

    /**
     * @see #toConnectSchema(SqlType, String, String)
     */
    Schema toConnectSchema(SqlType sqlType);

    /**
     * Convert the supplied primitive {@code sqlType} to its corresponding Connect KSQL schema.
     *
     * @param sqlType the sql type to convert
     * @param name    the name for the schema
     * @param doc     the doc for the schema
     * @return the Connect schema.
     */
    Schema toConnectSchema(SqlType sqlType, String name, String doc);
  }

  public interface JavaToSqlTypeConverter {

    /**
     * Convert the supplied {@code javaType} to its corresponding SQL type.
     *
     * <p/>Structured types are not supported
     *
     * @param javaType the java type to convert.
     * @return the sql type.
     */
    SqlBaseType toSqlType(Class<?> javaType);
  }

  public interface SqlToJavaTypeConverter {

    /**
     * Convert the supplied {@code sqlBaseType} to its corresponding Java type.
     *
     * @param sqlBaseType the SQL type to convert.
     * @return the java type.
     */
    Class<?> toJavaType(SqlBaseType sqlBaseType);
  }

  public static ConnectToSqlTypeConverter connectToSqlConverter() {
    return CONNECT_TO_SQL_CONVERTER;
  }

  public static SqlToConnectTypeConverter sqlToConnectConverter() {
    return SQL_TO_CONNECT_CONVERTER;
  }

  public static JavaToSqlTypeConverter javaToSqlConverter() {
    return JAVA_TO_SQL_CONVERTER;
  }

  public static SqlToJavaTypeConverter sqlToJavaConverter() {
    return SQL_TO_JAVA_CONVERTER;
  }

  private static final class ConnectToSqlConverter implements ConnectToSqlTypeConverter {

    private static final Map<Schema.Type, Function<Schema, SqlType>> CONNECT_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, SqlType>>builder()
        .put(Schema.Type.INT32, s -> SqlTypes.INTEGER)
        .put(Schema.Type.INT64, s -> SqlTypes.BIGINT)
        .put(Schema.Type.FLOAT64, s -> SqlTypes.DOUBLE)
        .put(Schema.Type.BOOLEAN, s -> SqlTypes.BOOLEAN)
        .put(Schema.Type.STRING, s -> SqlTypes.STRING)
        .put(Schema.Type.ARRAY, ConnectToSqlConverter::toSqlArray)
        .put(Schema.Type.MAP, ConnectToSqlConverter::toSqlMap)
        .put(Schema.Type.STRUCT, ConnectToSqlConverter::toSqlStruct)
        .put(Schema.Type.BYTES, ConnectToSqlConverter::handleBytes)
        .build();

    @Override
    public SqlType toSqlType(final Schema schema) {
      return sqlType(schema);
    }

    private static SqlType sqlType(final Schema schema) {
      final Function<Schema, SqlType> handler = CONNECT_TO_SQL.get(schema.type());
      if (handler == null) {
        throw new KsqlException("Unexpected schema type: " + schema);
      }

      return handler.apply(schema);
    }

    private static SqlDecimal handleBytes(final Schema schema) {
      DecimalUtil.requireDecimal(schema);
      return SqlDecimal.of(DecimalUtil.precision(schema), DecimalUtil.scale(schema));
    }

    private static SqlArray toSqlArray(final Schema schema) {
      return SqlArray.of(sqlType(schema.valueSchema()));
    }

    private static SqlMap toSqlMap(final Schema schema) {
      if (schema.keySchema().type() != Schema.Type.STRING) {
        throw new KsqlException("Unsupported map key type: " + schema.keySchema());
      }
      return SqlMap.of(sqlType(schema.valueSchema()));
    }

    private static SqlStruct toSqlStruct(final Schema schema) {
      final SqlStruct.Builder builder = SqlStruct.builder();

      schema.schema().fields()
          .forEach(field -> builder.field(field.name(), sqlType(field.schema())));

      return builder.build();
    }
  }

  private static final class ConnectFromSqlConverter implements SqlToConnectTypeConverter {

    private static final Map<SqlBaseType, Function<SqlType, SchemaBuilder>> SQL_TO_CONNECT =
        ImmutableMap.<SqlBaseType, Function<SqlType, SchemaBuilder>>builder()
            .put(SqlBaseType.STRING, t -> SchemaBuilder.string().optional())
            .put(SqlBaseType.BOOLEAN, t -> SchemaBuilder.bool().optional())
            .put(SqlBaseType.INTEGER, t -> SchemaBuilder.int32().optional())
            .put(SqlBaseType.BIGINT, t -> SchemaBuilder.int64().optional())
            .put(SqlBaseType.DOUBLE, t -> SchemaBuilder.float64().optional())
            .put(SqlBaseType.DECIMAL, t -> ConnectFromSqlConverter.fromSqlDecimal((SqlDecimal) t))
            .put(SqlBaseType.ARRAY, t -> ConnectFromSqlConverter.fromSqlArray((SqlArray) t))
            .put(SqlBaseType.MAP, t -> ConnectFromSqlConverter.fromSqlMap((SqlMap) t))
            .put(SqlBaseType.STRUCT, t -> ConnectFromSqlConverter.fromSqlStruct((SqlStruct) t))
        .build();

    @Override
    public Schema toConnectSchema(final SqlType type) {
      return connectType(type).build();
    }

    @Override
    public Schema toConnectSchema(final SqlType type, final String name, final String doc) {
      return connectType(type).name(name).doc(doc).build();
    }

    private static SchemaBuilder connectType(final SqlType sqlType) {
      final Function<SqlType, SchemaBuilder> handler = SQL_TO_CONNECT.get(sqlType.baseType());
      if (handler == null) {
        throw new KsqlException("Unexpected sql type: " + sqlType);
      }

      return handler.apply(sqlType);
    }

    private static SchemaBuilder fromSqlDecimal(final SqlDecimal sqlDecimal) {
      return DecimalUtil.builder(sqlDecimal.getPrecision(), sqlDecimal.getScale());
    }

    private static SchemaBuilder fromSqlArray(final SqlArray sqlArray) {
      return SchemaBuilder
          .array(connectType(sqlArray.getItemType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlMap(final SqlMap sqlMap) {
      return SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, connectType(sqlMap.getValueType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlStruct(final SqlStruct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.getFields()
          .forEach(field -> builder.field(field.fullName(), connectType(field.type()).build()));

      return builder
          .optional();
    }
  }

  private static class JavaToSqlConverter implements JavaToSqlTypeConverter {

    private static final BiMap<Class<?>, SqlBaseType> JAVA_TO_SQL
        = ImmutableBiMap.<Class<?>, SqlBaseType>builder()
        .put(Boolean.class, SqlBaseType.BOOLEAN)
        .put(Integer.class, SqlBaseType.INTEGER)
        .put(Long.class, SqlBaseType.BIGINT)
        .put(Double.class, SqlBaseType.DOUBLE)
        .put(String.class, SqlBaseType.STRING)
        .put(BigDecimal.class, SqlBaseType.DECIMAL)
        .put(List.class, SqlBaseType.ARRAY)
        .put(Map.class, SqlBaseType.MAP)
        .put(Struct.class, SqlBaseType.STRUCT)
        .build();

    @Override
    public SqlBaseType toSqlType(final Class<?> javaType) {
      final SqlBaseType sqlType = JAVA_TO_SQL.get(javaType);
      if (sqlType == null) {
        throw new KsqlException("Unexpected java type: " + javaType);
      }

      return sqlType;
    }
  }

  private static class SqlToJavaConverter implements SqlToJavaTypeConverter {

    private static final BiMap<SqlBaseType, Class<?>> SQL_TO_JAVA =
        JavaToSqlConverter.JAVA_TO_SQL.inverse();

    @Override
    public Class<?> toJavaType(final SqlBaseType sqlBaseType) {
      final Class<?> javaType = SQL_TO_JAVA.get(sqlBaseType);
      if (javaType == null) {
        throw new KsqlException("Unexpected sql type: " + sqlBaseType);
      }

      return javaType;
    }
  }
}
