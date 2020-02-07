/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;

import java.math.BigDecimal;
import java.util.List;

@SuppressWarnings("WeakerAccess") // Invoked via reflection
@UdfDescription(
    name = "mod",
    description = Mod.DESCRIPTION
)
public class Mod {

  static final String DESCRIPTION = "Returns the remainder of a number divided by another number.";

  @Udf(description = "Returns the remainder from a division of an INT value.")
  public Integer mod(
      @UdfParameter("The dividend value.") final Integer dividend,
      @UdfParameter("The divisor value.") final Integer divisor
  ) {
    return (dividend == null || divisor == null) ? null : dividend % divisor;
  }

  @Udf(description = "Returns the remainder from a division of an LONG value.")
  public Long mod(
      @UdfParameter("The dividend value.") final Long dividend,
      @UdfParameter("The divisor value.") final Long divisor
  ) {
    return (dividend == null || divisor == null) ? null : dividend % divisor;
  }

  @Udf(description = "Returns the remainder from a division of an DOUBLE value.")
  public Double mod(
      @UdfParameter("The dividend value.") final Double dividend,
      @UdfParameter("The divisor value.") final Double divisor
  ) {
    return (dividend == null || divisor == null) ? null : dividend % divisor;
  }

  @Udf(
      description = "Returns the remainder from a division of an DECIMAL value.",
      schemaProvider = "modDecimalProvider"
  )
  public BigDecimal mod(
      @UdfParameter("The dividend value.") final BigDecimal dividend,
      @UdfParameter("The divisor value.") final BigDecimal divisor
  ) {
    return (dividend == null || divisor == null) ? null : dividend.remainder(divisor);
  }

  @UdfSchemaProvider
  public SqlType modDecimalProvider(final List<SqlType> params) {
    final SqlType s = params.get(0);
    if (s.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException("The schema provider method for Mod expects a BigDecimal parameter"
          + "type");
    }
    return s;
  }
}
