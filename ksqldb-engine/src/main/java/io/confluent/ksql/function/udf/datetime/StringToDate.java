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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.concurrent.ExecutionException;

@UdfDescription(
    name = "stringtodate",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into the number of days since 1970-01-01 00:00:00 UTC/GMT."
        + " The system default time zone is used when no time zone is explicitly provided."
        + " Note this is the format Kafka Connect uses to represent dates with no time component."
        + " The format pattern should be in the format expected by"
        + " java.time.format.DateTimeFormatter"
)
public class StringToDate {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(pattern -> new DateTimeFormatterBuilder()
              .parseCaseInsensitive()
              .appendPattern(pattern)
              .toFormatter()));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into the number of days since 1970-01-01 00:00:00 UTC/GMT.")
  public int stringToDate(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedDate,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    // NB: We do not perform a null here preferring to throw an exception as
    // there is no sentinel value for a "null" Date.
    try {
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return ((int)LocalDate.parse(formattedDate, formatter).toEpochDay());
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse date '" + formattedDate
          + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

}