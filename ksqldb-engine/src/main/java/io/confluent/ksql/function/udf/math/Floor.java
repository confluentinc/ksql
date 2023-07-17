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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@UdfDescription(
    name = "Floor",
    category = FunctionCategory.MATHEMATICAL,
    description = Floor.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Floor {

  static final String DESCRIPTION = "Returns the largest integer less than or equal to the "
      + "specified numeric expression.";


  @Udf
  public Integer floor(@UdfParameter final Integer val) {
    return val;
  }

  @Udf
  public Long floor(@UdfParameter final Long val) {
    return val;
  }

  @Udf
  public Double floor(@UdfParameter final Double val) {
    return (val == null) ? null : Math.floor(val);
  }

  @Udf(schemaProvider = "floorDecimalProvider")
  public BigDecimal floor(@UdfParameter final BigDecimal val) {
    return val == null
        ? null
        : val.setScale(0, RoundingMode.FLOOR).setScale(val.scale(), RoundingMode.UNNECESSARY);
  }

  @UdfSchemaProvider
  public SqlType floorDecimalProvider(final List<SqlArgument> params) {
    final SqlType s = params.get(0).getSqlTypeOrThrow();
    if (s.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException("The schema provider method for Floor expects a BigDecimal parameter"
          + "type");
    }
    return s;
  }

}
