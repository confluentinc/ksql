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

  /**
   * Upper bound on the number of elements GENERATE_SERIES will produce. Without this cap, a
   * caller can request a series whose size is computed via overflow-prone int arithmetic and
   * the function either throws IllegalArgumentException on new ArrayList(neg) or attempts to
   * allocate multiple GB of contiguous memory and OOMs the server.
   */
  static final int MAX_SERIES_SIZE = 1_000_000;

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
    // Compute diff in long to defeat the int-overflow that previously made (end - start) wrap
    // negative for extreme input pairs like (Integer.MIN_VALUE, Integer.MAX_VALUE).
    final long diff = (long) end - (long) start;
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KsqlFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final long size = 1L + diff / step;
    final int boundedSize = checkAndCastSize(size);
    final List<Integer> result = new ArrayList<>(boundedSize);
    int pos = 0;
    int val = start;
    while (pos++ < boundedSize) {
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
    // Detect long-arithmetic overflow on (end - start); the previous unchecked subtraction
    // could wrap for inputs at opposite ends of the long range.
    final long diff;
    try {
      diff = Math.subtractExact(end, start);
    } catch (final ArithmeticException e) {
      throw new KsqlFunctionException(
          "GENERATE_SERIES range overflow: " + start + " to " + end);
    }
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KsqlFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final long size = 1L + diff / step;
    final int boundedSize = checkAndCastSize(size);
    final List<Long> result = new ArrayList<>(boundedSize);
    int pos = 0;
    long val = start;
    while (pos++ < boundedSize) {
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

  private static int checkAndCastSize(final long size) {
    if (size < 0 || size > MAX_SERIES_SIZE) {
      throw new KsqlFunctionException(
          "GENERATE_SERIES size (" + size + ") is negative or exceeds the maximum of "
              + MAX_SERIES_SIZE + " elements.");
    }
    return (int) size;
  }

}