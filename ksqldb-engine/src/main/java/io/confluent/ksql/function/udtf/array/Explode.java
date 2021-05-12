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

package io.confluent.ksql.function.udtf.array;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the 'explode' table function. This table function takes an array of values and
 * explodes it into zero or more rows, one for each value in the array.
 */
@UdtfDescription(
    name = "explode",
    category = FunctionCategory.TABLE,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description =
        "Explodes an array. This function outputs one value for each element of the array."
)
public class Explode {

  @Udtf
  public <T> List<T> explode(final List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

  @Udtf(schemaProvider = "provideSchema")
  public List<BigDecimal> explodeBigDecimal(final List<BigDecimal> input) {
    return explode(input);
  }

  @UdfSchemaProvider
  public SqlType provideSchema(final List<SqlArgument> params) {
    final SqlType argType = params.get(0).getSqlTypeOrThrow();
    if (!(argType instanceof SqlArray)) {
      throw new KsqlException("explode should be provided with an ARRAY");
    }
    return ((SqlArray) argType).getItemType();
  }
}
