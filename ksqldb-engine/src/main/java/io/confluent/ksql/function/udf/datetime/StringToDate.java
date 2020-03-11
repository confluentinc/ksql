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
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

@UdfDescription(name = "stringtodate", author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date into an integer representing"
        + " days since epoch using the given format pattern."
        + " Note this is the format Kafka Connect uses to represent dates with no time component."
        + " The format pattern should be in the format expected by"
        + " java.time.format.DateTimeFormatter")
public class StringToDate {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts formattedDate, a string representation of a date into"
      + " an integer representing days since epoch using the given formatPattern.")
  public int stringToDate(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedDate,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
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