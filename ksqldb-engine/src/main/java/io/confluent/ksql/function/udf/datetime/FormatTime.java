/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Time;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "format_time",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a TIME value into the string representation of the time"
        + " in the given format."
)
public class FormatTime {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts a TIME value into the"
      + " string representation of the time in the given format."
      + " The format pattern should be in the format expected"
      + " by java.time.format.DateTimeFormatter")
  public String formatTime(
      @UdfParameter(
          description = "TIME value.") final Time time,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    if (time == null || formatPattern == null) {
      return null;
    }
    try {
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(time.getTime())).format(formatter);
    } catch (ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to format time "
          + LocalTime.ofNanoOfDay(time.getTime() * 1000000)
          + " with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}