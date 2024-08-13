/*
 * Copyright 2020 Confluent Inc.
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
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "parse_time",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a time in the given format"
        + " into a TIME value."
)
public class ParseTime {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(pattern -> new DateTimeFormatterBuilder()
              .parseCaseInsensitive()
              .appendPattern(pattern)
              .toFormatter()));

  @Udf(description = "Converts a string representation of a time in the given format"
      + " into the TIME value.")
  public Time parseTime(
      @UdfParameter(
          description = "The string representation of a time.") final String formattedTime,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    if (formattedTime == null | formatPattern == null) {
      return null;
    }
    try {
      final TemporalAccessor ta = formatters.get(formatPattern).parse(formattedTime);
      final Optional<ChronoField> dateField = Arrays.stream(ChronoField.values())
          .filter(ChronoField::isDateBased)
          .filter(ta::isSupported)
          .findFirst();

      if (dateField.isPresent()) {
        throw new KsqlFunctionException("Time format contains date field.");
      }

      return new Time(TimeUnit.NANOSECONDS.toMillis(LocalTime.from(ta).toNanoOfDay()));
    } catch (ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse time '" + formattedTime
          + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}