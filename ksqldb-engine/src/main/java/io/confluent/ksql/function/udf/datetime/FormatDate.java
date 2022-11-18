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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "format_date",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a DATE value to a string"
        + " using the given format pattern. The format pattern should be"
        + " in the format expected by java.time.format.DateTimeFormatter."
)
public class FormatDate {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts the number of days since 1970-01-01 00:00:00 UTC/GMT to a date "
      + "string using the given format pattern. The format pattern should be in the format"
      + " expected by java.time.format.DateTimeFormatter")
  public String formatDate(
      @UdfParameter(
          description = "The date to convert") final Date date,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    if (date == null || formatPattern == null) {
      return null;
    }
    try {
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return LocalDate.ofEpochDay(TimeUnit.MILLISECONDS.toDays(date.getTime())).format(formatter);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to format date " + date
          + " with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }
}
