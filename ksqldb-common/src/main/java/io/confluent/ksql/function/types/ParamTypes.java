/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.types;

import static io.confluent.ksql.schema.ksql.SchemaConverters.functionToSqlBaseConverter;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Map.Entry;
import java.util.Optional;

public final class ParamTypes {

  private ParamTypes() {
  }

  public static final BooleanType BOOLEAN = BooleanType.INSTANCE;
  public static final IntegerType INTEGER = IntegerType.INSTANCE;
  public static final DoubleType DOUBLE = DoubleType.INSTANCE;
  public static final StringType STRING = StringType.INSTANCE;
  public static final LongType LONG = LongType.INSTANCE;
  public static final ParamType DECIMAL = DecimalType.INSTANCE;
  public static final TimeType TIME = TimeType.INSTANCE;
  public static final DateType DATE = DateType.INSTANCE;
  public static final TimestampType TIMESTAMP = TimestampType.INSTANCE;
  public static final IntervalUnitType INTERVALUNIT = IntervalUnitType.INSTANCE;
  public static final BytesType BYTES = BytesType.INSTANCE;
  public static final AnyType ANY = AnyType.INSTANCE;

  public static boolean areCompatible(final SqlArgument actual, final ParamType declared) {
    return areCompatible(actual, declared, false);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  // CHECKSTYLE_RULES.OFF: NPathComplexity
  public static boolean areCompatible(
      final SqlArgument argument,
      final ParamType declared,
      final boolean allowCast
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // CHECKSTYLE_RULES.ON: NPathComplexity
    if (declared instanceof AnyType) {
      return true;
    }

    final Optional<SqlLambda> sqlLambdaOptional = argument.getSqlLambda();

    if (sqlLambdaOptional.isPresent() && declared instanceof LambdaType) {
      final LambdaType declaredLambda = (LambdaType) declared;
      final SqlLambda sqlLambda = sqlLambdaOptional.get();
      if (sqlLambda instanceof SqlLambdaResolved) {
        final SqlLambdaResolved sqlLambdaResolved = (SqlLambdaResolved) sqlLambda;
        if (sqlLambdaResolved.getInputType().size()
            != declaredLambda.inputTypes().size()) {
          return false;
        }
        int i = 0;
        for (final ParamType paramType : declaredLambda.inputTypes()) {
          if (!areCompatible(
              SqlArgument.of(sqlLambdaResolved.getInputType().get(i)),
              paramType,
              allowCast)
          ) {
            return false;
          }
          i++;
        }
        return areCompatible(
            SqlArgument.of(sqlLambdaResolved.getReturnType()),
            declaredLambda.returnType(),
            allowCast);
      } else {
        return sqlLambda.getNumInputs() == declaredLambda.inputTypes().size();
      }
    }

    if (argument.getSqlIntervalUnit().isPresent() && declared instanceof IntervalUnitType) {
      return true;
    } else if (argument.getSqlIntervalUnit().isPresent() || declared instanceof IntervalUnitType) {
      return false;
    }

    final SqlType argumentSqlType = argument.getSqlTypeOrThrow();
    if (argumentSqlType.baseType() == SqlBaseType.ARRAY && declared instanceof ArrayType) {
      return areCompatible(
          SqlArgument.of(((SqlArray) argumentSqlType).getItemType()),
          ((ArrayType) declared).element(),
          allowCast);
    }

    if (argumentSqlType.baseType() == SqlBaseType.MAP && declared instanceof MapType) {
      final SqlMap sqlType = (SqlMap) argumentSqlType;
      final MapType mapType = (MapType) declared;
      return areCompatible(SqlArgument.of(sqlType.getKeyType()), mapType.key(), allowCast)
          && areCompatible(
          SqlArgument.of(sqlType.getValueType()),
          mapType.value(),
          allowCast
      );
    }

    if (argumentSqlType.baseType() == SqlBaseType.STRUCT && declared instanceof StructType) {
      return isStructCompatible(argumentSqlType, declared);
    }

    return isPrimitiveMatch(argumentSqlType, declared, allowCast);
  }

  private static boolean isStructCompatible(final SqlType actual, final ParamType declared) {
    final SqlStruct actualStruct = (SqlStruct) actual;

    // consider a struct that is empty to match any other struct
    if (actualStruct.fields().isEmpty() || ((StructType) declared).getSchema().isEmpty()) {
      return true;
    }

    for (final Entry<String, ParamType> entry : ((StructType) declared).getSchema().entrySet()) {
      final String k = entry.getKey();
      final Optional<Field> field = actualStruct.field(k);
      // intentionally do not allow implicit casting within structs
      if (!field.isPresent()
          || !areCompatible(SqlArgument.of(field.get().type()), entry.getValue(), false)) {
        return false;
      }
    }
    return actualStruct.fields().size() == ((StructType) declared).getSchema().size();
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static boolean isPrimitiveMatch(
      final SqlType actual,
      final ParamType declared,
      final boolean allowCast
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    final SqlBaseType base = actual.baseType();
    return base == SqlBaseType.STRING   && declared instanceof StringType
        || base == SqlBaseType.INTEGER  && declared instanceof IntegerType
        || base == SqlBaseType.BIGINT   && declared instanceof LongType
        || base == SqlBaseType.BOOLEAN  && declared instanceof BooleanType
        || base == SqlBaseType.DOUBLE   && declared instanceof DoubleType
        || base == SqlBaseType.DECIMAL  && declared instanceof DecimalType
        || base == SqlBaseType.TIME  && declared instanceof TimeType
        || base == SqlBaseType.DATE  && declared instanceof DateType
        || base == SqlBaseType.TIMESTAMP  && declared instanceof TimestampType
        || base == SqlBaseType.BYTES  && declared instanceof BytesType
        || allowCast && base.canImplicitlyCast(functionToSqlBaseConverter().toBaseType(declared));
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
  }
}
