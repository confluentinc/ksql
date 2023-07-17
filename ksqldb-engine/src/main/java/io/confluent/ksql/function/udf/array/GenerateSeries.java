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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;

/**
 * This UDF constructs an array containing an array of INTs or BIGINTs in the specified range
 */
@UdfDescription(
    name = "GENERATE_SERIES",
    category = FunctionCategory.ARRAY,
    description = "Construct an array of a range of values",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class GenerateSeries {

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int end
  ) {
    return generateSeriesInt(start, end, end - start > 0 ? 1 : -1);
  }

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int end,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    final int diff = end - start;
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KsqlFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final int size = 1 + diff / step;
    final List<Integer> result = new ArrayList<>(size);
    int pos = 0;
    int val = start;
    while (pos++ < size) {
      result.add(val);
      val += step;
    }
    return result;
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final long end
  ) {
    return generateSeriesLong(start, end, end - start > 0 ? 1 : -1);
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long start,
      @UdfParameter
          (description = "Marks the end of the series (inclusive)") final long end,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    final long diff = end - start;
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KsqlFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final int size = 1 + (int) (diff / step);
    final List<Long> result = new ArrayList<>(size);
    int pos = 0;
    long val = start;
    while (pos++ < size) {
      result.add(val);
      val += step;
    }
    return result;
  }

  private void checkStep(final int step) {
    if (step == 0) {
      throw new KsqlFunctionException("GENERATE_SERIES step cannot be zero");
    }
  }

}