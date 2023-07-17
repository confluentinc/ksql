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
import io.confluent.ksql.util.timestamp.StringToTimestampParser;
import java.time.ZoneId;
import java.util.concurrent.ExecutionException;

@UdfDescription(
    name = "stringtotimestamp",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into the number of milliseconds since 1970-01-01 00:00:00 UTC/GMT."
        + " The system default time zone is used when no time zone is explicitly provided."
)
public class StringToTimestamp {

  private final LoadingCache<String, StringToTimestampParser> parsers =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(StringToTimestampParser::new));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into the number of milliseconds since 1970-01-01 00:00:00 UTC/GMT."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'."
      + " The system default time zone is used when no time zone is explicitly provided.")
  public long stringToTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    // NB: We do not perform a null here preferring to throw an exception as
    // there is no sentinel value for a "null" Date.
    try {
      final StringToTimestampParser timestampParser = parsers.get(formatPattern);
      return timestampParser.parse(formattedTimestamp);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse timestamp '" + formattedTimestamp
           + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

  @Udf(description = "Converts a string representation of a date at the given time zone"
      + " into the number of milliseconds since 1970-01-01 00:00:00 UTC/GMT."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'.")
  public long stringToTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern,
      @UdfParameter(
          description =  " timeZone is a java.util.TimeZone ID format, for example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String timeZone) {
    // NB: We do not perform a null here preferring to throw an exception as
    // there is no sentinel value for a "null" Date.
    try {
      final StringToTimestampParser timestampParser = parsers.get(formatPattern);
      final ZoneId zoneId = ZoneId.of(timeZone);
      return timestampParser.parse(formattedTimestamp, zoneId);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse timestamp '" + formattedTimestamp
          + "' at timezone '" + timeZone + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

}
