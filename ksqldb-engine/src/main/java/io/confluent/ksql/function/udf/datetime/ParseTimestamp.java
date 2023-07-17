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
import io.confluent.ksql.util.timestamp.StringToTimestampParser;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.ExecutionException;

@UdfDescription(
    name = "parse_timestamp",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into a TIMESTAMP value."
)
public class ParseTimestamp {

  private final LoadingCache<String, StringToTimestampParser> parsers =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(StringToTimestampParser::new));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into the TIMESTAMP value."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'.")
  public Timestamp parseTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    return parseTimestamp(formattedTimestamp, formatPattern, ZoneId.of("GMT").getId());
  }

  @Udf(description = "Converts a string representation of a date at the given time zone"
      + " in the given format into the TIMESTAMP value."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'.")
  public Timestamp parseTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern,
      @UdfParameter(
          description =  " timeZone is a java.util.TimeZone ID format, for example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String timeZone) {
    if (formattedTimestamp == null || formatPattern == null || timeZone == null) {
      return null;
    }
    try {
      final StringToTimestampParser timestampParser = parsers.get(formatPattern);
      final ZoneId zoneId = ZoneId.of(timeZone);
      return timestampParser.parseToTimestamp(formattedTimestamp, zoneId);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to parse timestamp '" + formattedTimestamp
          + "' at timezone '" + timeZone + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}