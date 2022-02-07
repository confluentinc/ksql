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
import java.text.ParseException;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.FastDateFormat;

@UdfDescription(
    name = "parse_date",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into a DATE value. The format pattern should be in the format expected by"
        + " java.text.SimpleDateFormat"
)
public class ParseDate {

  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

  private final LoadingCache<String, FastDateFormat> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(pattern ->
              FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("GMT"))));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into a DATE value.")
  public Date parseDate(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedDate,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.text.SimpleDateFormat.") final String formatPattern) {
    if (formattedDate == null || formatPattern == null) {
      return null;
    }
    try {
      final long time = formatters.get(formatPattern).parse(formattedDate).getTime();
      if (time % MILLIS_IN_DAY != 0) {
        throw new KsqlFunctionException("Date format contains time field.");
      }
      return new Date(time);
    } catch (final ExecutionException | RuntimeException | ParseException e) {
      throw new KsqlFunctionException("Failed to parse date '" + formattedDate
          + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}