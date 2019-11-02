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

package io.confluent.ksql.function.udf.array;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * This UDF constructs an array containing an array of INTs or BIGINTs in the specified range
 */
@UdfDescription(name = "RANGE", description = "Construct an array of a range of values")
public class Range {

  @Udf
  public List<Integer> rangeInt(
      @UdfParameter final int startInclusive, @UdfParameter final int endExclusive
  ) {
    return IntStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList());
  }

  @Udf
  public List<Long> rangeLong(
      @UdfParameter final long startInclusive, @UdfParameter final long endExclusive
  ) {
    return LongStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList());
  }

}