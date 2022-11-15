/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf.array;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

@SuppressWarnings("MethodMayBeStatic") // UDF methods can not be static.
@UdfDescription(
    name = "ARRAY_JOIN",
    category = FunctionCategory.ARRAY,
    description = "joins the array elements into a flat string representation",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class ArrayJoin {

  private static final String DEFAULT_DELIMITER = ",";
  private static final Set<Class> KSQL_PRIMITIVES = ImmutableSet.of(
      Boolean.class,Integer.class,Long.class,Double.class,BigDecimal.class,String.class
  );

  @Udf
  public <T> String join(
      @UdfParameter(description = "the array to join using the default delimiter '"
          + DEFAULT_DELIMITER + "'") final List<T> array
  ) {
    return join(array, DEFAULT_DELIMITER);
  }

  @Udf
  public <T> String join(
      @UdfParameter(description = "the array to join using the specified delimiter")
      final List<T> array,
      @UdfParameter(description = "the string to be used as element delimiter")
      final String delimiter
  ) {

    if (array == null) {
      return null;
    }

    final StringJoiner sj = new StringJoiner(delimiter == null ? "" : delimiter);
    array.forEach(e -> processElement(e, sj));
    return sj.toString();

  }

  @SuppressWarnings("unchecked")
  private static <T> void processElement(final T element, final StringJoiner joiner) {

    if (element == null || KSQL_PRIMITIVES.contains(element.getClass())) {
      handlePrimitiveType(element, joiner);
    } else {
      throw new KsqlFunctionException("error: hit element of type "
          + element.getClass().getTypeName() + " which is currently not supported");
    }

  }

  private static void handlePrimitiveType(final Object element, final StringJoiner joiner) {
    joiner.add(element != null ? element.toString() : null);
  }

}
