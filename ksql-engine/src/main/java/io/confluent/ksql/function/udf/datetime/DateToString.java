/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function.udf.datetime;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

@UdfDescription(name = "datetostring", author = "Confluent",
    description = "Converts an integer representing days since epoch to a date string"
        + " using the given format pattern. Note this is the format Kafka Connect uses"
        + " to represent dates with no time component.  The format pattern should be"
        + " in the format expected by java.time.format.DateTimeFormatter")
public class DateToString {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts an integer representing days since epoch to a string"
      + " using the given format pattern. The format pattern should be in the format"
      + " expected by java.time.format.DateTimeFormatter")
  public String dateToString(
      @UdfParameter(value = "epochDays",
          description = "The Epoch Day to convert,"
              + " based on the epoch 1970-01-01") final int epochDays,
      @UdfParameter(value = "formatPattern",
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    try {
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return LocalDate.ofEpochDay(epochDays).format(formatter);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to format date " + epochDays
          + " with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

}
