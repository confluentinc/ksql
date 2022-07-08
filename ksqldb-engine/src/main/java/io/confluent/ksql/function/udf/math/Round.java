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

/*
The rounding behaviour implemented here follows that of java.lang.Math.round() - we do that
in order to provide compatibility with the previous ROUND() implementation which used
Math.round(). The BigDecimal HALF_UP rounding behaviour is a bit saner and would be a better
choice if we were starting from scratch.

It's an implementation of rounding "half up". This means we round to the nearest integer value and
in the case we are equidistant from the two nearest integers we round UP to the nearest integer.
This means:

ROUND(1.1) -> 1
ROUND(1.5) -> 2
ROUND(1.9) -> 2

ROUND(-1.1) -> -1
ROUND(-1.5) -> -1    Note this is not -2! We round up and up is in a positive direction.
ROUND(-1.9) -> -2

Unfortunately there is an inconsistency in the way that java.lang.Math and
BigDecimal work with respect to rounding:

Math.round(1.5) --> 2
Math.round(-1.5) -- -1

But:

new BigDecimal("1.5").setScale(2, RoundingMode.HALF_UP) --> 2
new BigDecimal("-1.5").setScale(2, RoundingMode.HALF_UP) --> -2

There isn't any BigDecimal rounding mode which captures the java.lang.Math behaviour so
we need to use different rounding modes on BigDecimal depending on whether the value
is +ve or -ve to get consistent behaviour.
*/
@SuppressWarnings("MethodMayBeStatic")
@UdfDescription(
    name = "Round",
    category = FunctionCategory.MATHEMATICAL,
    description = Round.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Round {

  static final String DESCRIPTION =
      "Round a value to the number of decimal places as specified by scale to the right of the "
        + "decimal point. If scale is negative then value is rounded to the right of the decimal "
        + "point. Numbers equidistant to the nearest value are rounded up (in the positive"
        + " direction). If the number of decimal places is not provided it defaults to zero.";

  @Udf
  public Long round(@UdfParameter final long val) {
    return val;
  }

  @Udf
  public Long round(@UdfParameter final int val) {
    return (long)val;
  }

  @Udf
  public Long round(@UdfParameter final Double val) {
    return val == null ? null : Math.round(val);
  }

  @Udf
  public Double round(@UdfParameter final Double val, @UdfParameter final Integer decimalPlaces) {
    return (val == null || decimalPlaces == null)
        ? null
        : roundBigDecimal(BigDecimal.valueOf(val), decimalPlaces).doubleValue();
  }

  @Udf(schemaProvider = "provideDecimalSchema")
  public BigDecimal round(@UdfParameter final BigDecimal val) {
    if (val == null) {
      return null;
    }
    return roundBigDecimal(val, 0);
  }

  @Udf(schemaProvider = "provideDecimalSchemaWithDecimalPlaces")
  public BigDecimal round(
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
    final RoundingMode roundingMode = val.compareTo(BigDecimal.ZERO) > 0
        ? RoundingMode.HALF_UP : RoundingMode.HALF_DOWN;
    return val.setScale(decimalPlaces, roundingMode);
  }
}