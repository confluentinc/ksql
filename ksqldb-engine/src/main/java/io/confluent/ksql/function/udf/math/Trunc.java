/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@UdfDescription(
        name = "trunc",
        category = FunctionCategory.MATHEMATICAL,
        description = Trunc.DESCRIPTION,
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Trunc {

  static final String DESCRIPTION =
          "Truncates a value to the number of decimal places as specified by scale to the right "
                  + "of the decimal point. If scale is negative then value is truncated to the "
                  + "right of the decimal point.";

  @Udf
  public Long trunc(@UdfParameter final Long val) {
    return val;
  }

  @Udf
  public Long trunc(@UdfParameter final Integer val) {
    return val == null ? null : val.longValue();
  }

  @Udf
  public Long trunc(@UdfParameter final Double val) {
    return val == null ? null : val.longValue();
  }

  @Udf
  public Double trunc(@UdfParameter final Double val, @UdfParameter final Integer decimalPlaces) {
    return (val == null || decimalPlaces == null)
            ? null
            : roundBigDecimal(BigDecimal.valueOf(val), decimalPlaces).doubleValue();
  }

  @Udf(schemaProvider = "provideDecimalSchema")
  public BigDecimal trunc(@UdfParameter final BigDecimal val) {
    if (val == null) {
      return null;
    }
    return roundBigDecimal(val, 0);
  }

  @Udf(schemaProvider = "provideDecimalSchemaWithDecimalPlaces")
  public BigDecimal trunc(
          @UdfParameter final BigDecimal val,
          @UdfParameter final Integer decimalPlaces
  ) {
    return (val == null || decimalPlaces == null) ? null : roundBigDecimal(val, decimalPlaces)
            // Must maintain source scale for now. See https://github.com/confluentinc/ksql/issues/6235.
            .setScale(val.scale(), RoundingMode.UNNECESSARY);
  }

  @SuppressWarnings("unused") // Invoked via reflection
  @UdfSchemaProvider
  public static SqlType provideDecimalSchemaWithDecimalPlaces(final List<SqlArgument> params) {
    final SqlType s0 = params.get(0).getSqlTypeOrThrow();
    if (s0.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException("The schema provider method for round expects a BigDecimal parameter"
              + "type as first parameter.");
    }
    final SqlType s1 = params.get(1).getSqlTypeOrThrow();
    if (s1.baseType() != SqlBaseType.INTEGER) {
      throw new KsqlException("The schema provider method for round expects an Integer parameter"
              + "type as second parameter.");
    }

    // While the user requested a certain number of decimal places, this can't be used to change
    // the scale of the return type. See https://github.com/confluentinc/ksql/issues/6235.
    return s0;
  }

  @SuppressWarnings("unused") // Invoked via reflection
  @UdfSchemaProvider
  public static SqlType provideDecimalSchema(final List<SqlArgument> params) {
    final SqlType s0 = params.get(0).getSqlTypeOrThrow();
    if (s0.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException("The schema provider method for round expects a BigDecimal parameter"
              + "type as a parameter.");
    }
    final SqlDecimal param = (SqlDecimal)s0;
    return SqlDecimal.of(param.getPrecision() - param.getScale(), 0);
  }

  private static BigDecimal roundBigDecimal(
          final BigDecimal val,
          final int decimalPlaces
  ) {
    return val.setScale(decimalPlaces, RoundingMode.DOWN);
  }
}
