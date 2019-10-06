/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.List;

@UdfDescription(name = "Ceil", description = Ceil.DESCRIPTION)
public class Ceil {

  static final String DESCRIPTION = "Returns the smallest value that is greater than or equal "
      + "to the argument and is equal to a mathematical integer. The behavior is the same as "
      + "math.ceil in Java. Specifically, if the argument is already equal to a mathematical "
      + "integer, then the result is the same as the argument. If the argument is NaN or an "
      + "infinity or positive zero or negative zero, then the result is the same as the argument. "
      + "If the argument value is less than zero but greater than -1.0, then the result is "
      + "negative zero.";

  @Udf
  public Double ceil(@UdfParameter final Double val) {
    return val == null ? null : Math.ceil(val);
  }

  @Udf(schemaProvider = "provideDecimalSchema")
  public BigDecimal ceil(@UdfParameter final BigDecimal val) {
    return val == null ? null : val.setScale(0, BigDecimal.ROUND_CEILING);
  }

  @UdfSchemaProvider
  public SqlType provideDecimalSchema(final List<SqlType> params) {
    final SqlType s0 = params.get(0);
    if (s0.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException("The schema provider method for round expects a BigDecimal parameter"
                                  + "type as a parameter.");
    }
    final SqlDecimal param = (SqlDecimal)s0;
    return SqlDecimal.of(param.getPrecision(), 0);
  }
}
