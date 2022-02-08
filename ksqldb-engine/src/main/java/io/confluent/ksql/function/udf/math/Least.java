/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.Streams;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@UdfDescription(
    name = "least",
    category = FunctionCategory.MATHEMATICAL,
    description = "Returns the lowest non-null value among a variable number of comparable columns",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Least {

  @Udf
  public Integer least(@UdfParameter final Integer val, @UdfParameter final Integer... vals) {

    return (vals == null) ? null : Stream.concat(Stream.of(val), Arrays.stream(vals))
        .filter(Objects::nonNull)
        .min(Integer::compareTo)
        .orElse(null);
  }

  @Udf
  public Long least(@UdfParameter final Long val, @UdfParameter final Long... vals) {

    return (vals == null) ? null : Stream.concat(Stream.of(val), Arrays.stream(vals))
        .filter(Objects::nonNull)
        .min(Long::compareTo)
        .orElse(null);
  }

  @Udf
  public Double least(@UdfParameter final Double val, @UdfParameter final Double... vals) {

    return (vals == null) ? null : Streams.concat(Stream.of(val), Arrays.stream(vals))
        .filter(Objects::nonNull)
        .min(Double::compareTo)
        .orElse(null);
  }

  @Udf
  public String least(@UdfParameter final String val, @UdfParameter final String... vals) {

    return (vals == null) ? null : Streams.concat(Stream.of(val), Arrays.stream(vals))
        .filter(Objects::nonNull)
        .min(String::compareTo)
        .orElse(null);
  }

  @Udf(schemaProvider = "leastDecimalProvider")
  public BigDecimal least(@UdfParameter final BigDecimal val,
      @UdfParameter final BigDecimal... vals) {

    return (vals == null) ? null : Streams.concat(Stream.of(val), Arrays.stream(vals))
        .filter(Objects::nonNull)
        .min(Comparator.naturalOrder())
        .orElse(null);
  }

  @UdfSchemaProvider
  public SqlType leastDecimalProvider(final List<SqlArgument> params) {

    return params.stream()
        .filter(s -> s.getSqlType().isPresent())
        .map(SqlArgument::getSqlTypeOrThrow)
        .reduce(DecimalUtil::widen)
        .orElse(null);
  }

}