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
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

@UdfDescription(
    name = "timestamptostring",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a number of milliseconds since 1970-01-01 00:00:00 UTC/GMT into"
        + " the string representation of the timestamp in the given format."
        + " The system default time zone is used when no time zone is explicitly provided."
)
public class TimestampToString {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts a number of milliseconds since 1970-01-01 00:00:00 UTC/GMT into the"
      + " string representation of the timestamp in the given format. Single quotes in the"
      + " timestamp format can be escaped with '', for example: 'yyyy-MM-dd''T''HH:mm:ssX'."
      + " The system default time zone is used when no time zone is explicitly provided."
      + " The format pattern should be in the format expected"
      + " by java.time.format.DateTimeFormatter")
  public String timestampToString(
      @UdfParameter(
          description = "Milliseconds since"
              + " January 1, 1970, 00:00:00 UTC/GMT.") final long epochMilli,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    if (formatPattern == null) {
      return null;
    }
    try {
      final Timestamp timestamp = new Timestamp(epochMilli);
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return timestamp.toInstant()
          .atZone(ZoneId.systemDefault())
          .format(formatter);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to format timestamp " + epochMilli
          + " with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }

  }

  @Udf(description = "Converts a number of milliseconds since 1970-01-01 00:00:00 UTC/GMT into the"
      + " string representation of the timestamp in the given format. Single quotes in the"
      + " timestamp format can be escaped with '', for example: 'yyyy-MM-dd''T''HH:mm:ssX'")
  public String timestampToString(
      @UdfParameter(
          description = "Milliseconds since"
              + " January 1, 1970, 00:00:00 UTC/GMT.") final long epochMilli,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern,
      @UdfParameter(
          description =  " timeZone is a java.util.TimeZone ID format, for example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String timeZone) {
    if (formatPattern == null || timeZone == null) {
      return null;
    }
    try {
      final Timestamp timestamp = new Timestamp(epochMilli);
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      final ZoneId zoneId = ZoneId.of(timeZone);
      return timestamp.toInstant()
          .atZone(zoneId)
          .format(formatter);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to format timestamp " + epochMilli
          + " at timeZone '" + timeZone + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }

  }

}
