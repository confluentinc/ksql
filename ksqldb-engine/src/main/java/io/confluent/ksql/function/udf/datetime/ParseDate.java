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
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "parse_date",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into a DATE value. The format pattern should be in the format expected by"
        + " java.time.format.DateTimeFormatter"
)
public class ParseDate {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into a DATE value.")
  public Date parseDate(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedDate,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    try {

      final TemporalAccessor ta = formatters.get(formatPattern).parse(formattedDate);
      final Optional<ChronoField> timeField = Arrays.stream(ChronoField.values())
          .filter(field -> field.isTimeBased())
          .filter(field -> ta.isSupported(field))
          .findFirst();

      if (timeField.isPresent()) {
        throw new KsqlFunctionException("Date format contains time field.");
      }

      return new Date(
          TimeUnit.DAYS.toMillis(LocalDate.from(ta).toEpochDay()));
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse date '" + formattedDate
          + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}