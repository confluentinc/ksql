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

import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

@UdtfDescription(name = "explode", author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "explodes an array")
public class Explode {

  @Udtf
  public List<Long> explodeLong(final List<Long> input) {
    return explode(input);
  }

  @Udtf
  public List<Integer> explodeInt(final List<Integer> input) {
    return explode(input);
  }

  @Udtf
  public List<Double> explodeDouble(final List<Double> input) {
    return explode(input);
  }

  @Udtf
  public List<Boolean> explodeBoolean(final List<Boolean> input) {
    return explode(input);
  }

  @Udtf
  public List<String> explodeString(final List<String> input) {
    return explode(input);
  }

  @Udtf(schemaProvider = "provideDecimalSchema")
  public List<BigDecimal> explodeBigDecimal(final List<BigDecimal> input) {
    return explode(input);
  }

  private <T> List<T> explode(final List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

  @UdfSchemaProvider
  public SqlType provideDecimalSchema(final List<SqlType> params) {
    final SqlType s0 = params.get(0);
    if (s0.baseType() != SqlBaseType.DECIMAL) {
      throw new KsqlException(
          "The schema provider method for explode expects a BigDecimal parameter"
              + "type as a parameter.");
    }
    return s0;
  }

}
