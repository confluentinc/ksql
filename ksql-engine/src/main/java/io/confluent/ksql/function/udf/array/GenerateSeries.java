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

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This UDF constructs an array containing an array of INTs or BIGINTs in the specified range
 */
@UdfDescription(name = "GENERATE_SERIES", description = "Construct an array of a range of values")
public class GenerateSeries {

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int startInclusive,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int endinclusive
  ) {
    return generateSeriesInt(startInclusive, endinclusive, 1);
  }

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int startInclusive,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int endinclusive,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    if (endinclusive <= startInclusive) {
      return Collections.emptyList();
    }
    final List<Integer> result = new ArrayList<>((endinclusive - startInclusive + 1) / step);
    for (int i = startInclusive; i <= endinclusive; i += step) {
      result.add(i);
    }
    return result;
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long startInclusive,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final long endinclusive
  ) {
    return generateSeriesLong(startInclusive, endinclusive, 1);
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long startInclusive,
      @UdfParameter
          (description = "Marks the end of the series (inclusive)") final long endinclusive,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    if (endinclusive < startInclusive) {
      return Collections.emptyList();
    }
    final List<Long> result = new ArrayList<>((int) (endinclusive - startInclusive + 1) / step);
    for (long i = startInclusive; i <= endinclusive; i += step) {
      result.add(i);
    }
    return result;
  }

  private void checkStep(final int step) {
    if (step < 1) {
      throw new KsqlFunctionException("GENERATE_SERIES step must be > 0");
    }
  }

}