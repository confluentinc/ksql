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
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

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

  private static final SqlToFunctionConverter SQL_TO_FUNCTION_CONVERTER = new SqlToFunction();

  private static final FunctionToSqlConverter FUNCTION_TO_SQL_CONVERTER = new FunctionToSql();

  private static final FunctionToSqlBase FUNCTION_TO_BASE_CONVERTER = new FunctionToSqlBase();

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

  public interface ConnectToJavaTypeConverter {
    /**
     * Convert the supplied Connect {@code schema} to its corresponding Java type.
     *
     * @param schema the Connect schema.
     * @return the java type.
     */
    Class<?> toJavaType(Schema schema);
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

    default Class<?> toJavaType(final SqlType sqlType) {
      return toJavaType(sqlType.baseType());
    }
  }

  public interface SqlToFunctionConverter {
    /**
     * Convert the supplied {@code SqlType} to its corresponding Function type.
     */
    ParamType toFunctionType(SqlType sqlType);
  }

  public interface FunctionToSqlConverter {
    SqlType toSqlType(ParamType paramType);
  }

  public interface FunctionToSqlBaseConverter {
    SqlBaseType toBaseType(ParamType paramType);
  }

  public static ConnectToSqlTypeConverter connectToSqlConverter() {
    return CONNECT_TO_SQL_CONVERTER;
  }

  public static SqlToConnectTypeConverter sqlToConnectConverter() {
    return SQL_TO_CONNECT_CONVERTER;
  }

  public static ConnectToJavaTypeConverter connectToJavaTypeConverter() {
    return schema -> SchemaConverters.sqlToJavaConverter().toJavaType(
        SchemaConverters.connectToSqlConverter().toSqlType(schema)
    );
  }

  public static JavaToSqlTypeConverter javaToSqlConverter() {
    return JAVA_TO_SQL_CONVERTER;
  }

  public static SqlToJavaTypeConverter sqlToJavaConverter() {
    return SQL_TO_JAVA_CONVERTER;
  }

  public static SqlToFunctionConverter sqlToFunctionConverter() {
    return SQL_TO_FUNCTION_CONVERTER;
  }

  public static FunctionToSqlConverter functionToSqlConverter() {
    return FUNCTION_TO_SQL_CONVERTER;
  }

  public static FunctionToSqlBaseConverter functionToSqlBaseConverter() {
    return FUNCTION_TO_BASE_CONVERTER;
  }

  private static final class ConnectToSqlConverter implements ConnectToSqlTypeConverter {

    private static final Map<Schema.Type, Function<Schema, SqlType>> CONNECT_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, SqlType>>builder()
        .put(Schema.Type.INT32, ConnectToSqlConverter::toIntegerType)
        .put(Schema.Type.INT64, s ->
            Timestamp.LOGICAL_NAME.equals(s.name()) ? SqlTypes.TIMESTAMP : SqlTypes.BIGINT)
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

    private static SqlType handleBytes(final Schema schema) {
      if (DecimalUtil.isDecimal(schema)) {
        return SqlDecimal.of(DecimalUtil.precision(schema), DecimalUtil.scale(schema));
      } else {
        return SqlTypes.BYTES;
      }
    }

    private static SqlArray toSqlArray(final Schema schema) {
      return SqlArray.of(sqlType(schema.valueSchema()));
    }

    private static SqlMap toSqlMap(final Schema schema) {
      return SqlMap.of(sqlType(schema.keySchema()), sqlType(schema.valueSchema()));
    }

    private static SqlStruct toSqlStruct(final Schema schema) {
      final SqlStruct.Builder builder = SqlStruct.builder();

      schema.schema().fields()
          .forEach(field -> builder.field(field.name(), sqlType(field.schema())));

      return builder.build();
    }

    private static SqlPrimitiveType toIntegerType(final Schema schema) {
      if (Time.LOGICAL_NAME.equals(schema.name())) {
        return SqlTypes.TIME;
      } else if (Date.LOGICAL_NAME.equals(schema.name())) {
        return SqlTypes.DATE;
      } else {
        return SqlTypes.INTEGER;
      }
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
            .put(SqlBaseType.TIME, t -> Time.builder().optional())
            .put(SqlBaseType.DATE, t -> Date.builder().optional())
            .put(SqlBaseType.TIMESTAMP, t -> Timestamp.builder().optional())
            .put(SqlBaseType.BYTES, t -> SchemaBuilder.bytes().optional())
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
          .map(
              connectType(sqlMap.getKeyType()).build(),
              connectType(sqlMap.getValueType()).build()
          )
          .optional();
    }

    private static SchemaBuilder fromSqlStruct(final SqlStruct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.fields()
          .forEach(field -> builder.field(field.name(), connectType(field.type()).build()));

      final Optional<SqlStruct.UnionType> unionType = struct.unionType();

      if (unionType.isPresent()) {
        switch (unionType.get()) {
          case ONE_OF_TYPE:
            builder.name(JsonSchemaData.JSON_TYPE_ONE_OF);
            break;
          case GENERALIZED_TYPE:
            builder.parameter(JsonSchemaData.GENERALIZED_TYPE_UNION, "0");
            break;
          default:
            throw new IllegalStateException("Unexpected value: " + struct.unionType());
        }
      }
      return builder.optional();
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
        .put(java.sql.Time.class, SqlBaseType.TIME)
        .put(java.sql.Date.class, SqlBaseType.DATE)
        .put(java.sql.Timestamp.class, SqlBaseType.TIMESTAMP)
        .put(ByteBuffer.class, SqlBaseType.BYTES)
        .build();

    @Override
    public SqlBaseType toSqlType(final Class<?> javaType) {
      return JAVA_TO_SQL.entrySet().stream()
          .filter(e -> e.getKey().isAssignableFrom(javaType))
          .map(Entry::getValue)
          .findAny()
          .orElseThrow(() -> new KsqlException("Unexpected java type: " + javaType));
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

  private static class FunctionToSql implements FunctionToSqlConverter {

    private static final BiMap<ParamType, SqlType> FUNCTION_TO_SQL =
        ImmutableBiMap.<ParamType, SqlType>builder()
            .put(ParamTypes.STRING, SqlTypes.STRING)
            .put(ParamTypes.BOOLEAN, SqlTypes.BOOLEAN)
            .put(ParamTypes.INTEGER, SqlTypes.INTEGER)
            .put(ParamTypes.LONG, SqlTypes.BIGINT)
            .put(ParamTypes.DOUBLE, SqlTypes.DOUBLE)
            .put(ParamTypes.TIME, SqlTypes.TIME)
            .put(ParamTypes.DATE, SqlTypes.DATE)
            .put(ParamTypes.TIMESTAMP, SqlTypes.TIMESTAMP)
            .put(ParamTypes.BYTES, SqlTypes.BYTES)
            .build();

    @Override
    public SqlType toSqlType(final ParamType paramType) {
      final SqlType sqlType = FUNCTION_TO_SQL.get(paramType);
      if (sqlType != null) {
        return sqlType;
      }

      if (paramType instanceof MapType) {
        final MapType mapType = (MapType) paramType;
        final SqlType keyType = toSqlType(mapType.key());
        final SqlType valueType = toSqlType(mapType.value());
        return SqlTypes.map(keyType, valueType);
      }

      if (paramType instanceof ArrayType) {
        return SqlTypes.array(toSqlType(((ArrayType) paramType).element()));
      }

      if (paramType instanceof StructType) {
        final Builder struct = SqlTypes.struct();
        ((StructType) paramType).getSchema()
            .forEach((name, type) -> struct.field(name, toSqlType(type)));
        return struct.build();
      }

      throw new KsqlException("Cannot convert param type to sql type: " + paramType);
    }
  }

  private static class FunctionToSqlBase implements FunctionToSqlBaseConverter {

    private static final BiMap<ParamType, SqlBaseType> FUNCTION_TO_BASE =
        ImmutableBiMap.<ParamType, SqlBaseType>builder()
            .put(ParamTypes.STRING, SqlBaseType.STRING)
            .put(ParamTypes.BOOLEAN, SqlBaseType.BOOLEAN)
            .put(ParamTypes.INTEGER, SqlBaseType.INTEGER)
            .put(ParamTypes.LONG, SqlBaseType.BIGINT)
            .put(ParamTypes.DOUBLE, SqlBaseType.DOUBLE)
            .put(ParamTypes.DECIMAL, SqlBaseType.DECIMAL)
            .put(ParamTypes.TIME, SqlBaseType.TIME)
            .put(ParamTypes.DATE, SqlBaseType.DATE)
            .put(ParamTypes.TIMESTAMP, SqlBaseType.TIMESTAMP)
            .put(ParamTypes.BYTES, SqlBaseType.BYTES)
            .build();

    @Override
    public SqlBaseType toBaseType(final ParamType paramType) {
      final SqlBaseType sqlType = FUNCTION_TO_BASE.get(paramType);
      if (sqlType != null) {
        return sqlType;
      }

      if (paramType instanceof MapType) {
        return SqlBaseType.MAP;
      }

      if (paramType instanceof ArrayType) {
        return SqlBaseType.ARRAY;
      }

      if (paramType instanceof StructType) {
        return SqlBaseType.STRUCT;
      }

      throw new KsqlException("Cannot convert param type to sql type: " + paramType);
    }
  }

  private static class SqlToFunction implements SqlToFunctionConverter {

    @Override
    public ParamType toFunctionType(final SqlType sqlType) {
      final ParamType paramType = FunctionToSql.FUNCTION_TO_SQL.inverse().get(sqlType);
      if (paramType != null) {
        return paramType;
      }

      if (sqlType.baseType() == SqlBaseType.DECIMAL) {
        return ParamTypes.DECIMAL;
      }

      if (sqlType.baseType() == SqlBaseType.ARRAY) {
        return ArrayType.of(toFunctionType(((SqlArray) sqlType).getItemType()));
      }

      if (sqlType.baseType() == SqlBaseType.MAP) {
        final SqlMap sqlMap = (SqlMap) sqlType;
        return MapType.of(
            toFunctionType(sqlMap.getKeyType()),
            toFunctionType(sqlMap.getValueType())
        );
      }

      if (sqlType.baseType() == SqlBaseType.STRUCT) {
        final StructType.Builder builder = StructType.builder();
        for (final Field field : ((SqlStruct) sqlType).fields()) {
          builder.field(field.name(), toFunctionType(field.type()));
        }
        return builder.build();
      }

      throw new KsqlException("Cannot convert sql type to param type: " + sqlType);
    }
  }

}
