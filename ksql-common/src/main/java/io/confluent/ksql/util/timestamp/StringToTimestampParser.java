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

package io.confluent.ksql.util.timestamp;

import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class StringToTimestampParser {
  private final DateTimeFormatter formatter;

  public StringToTimestampParser(final String pattern) {
    formatter = new DateTimeFormatterBuilder()
        .appendPattern(pattern)
        .parseDefaulting(ChronoField.YEAR_OF_ERA, 1970)
        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .toFormatter();
  }

  public long parse(final String text) {
    return parse(text, ZoneId.systemDefault());
  }

  public long parse(final String text, final ZoneId zoneId) {
    TemporalAccessor parsed = formatter.parseBest(
        text,
        ZonedDateTime::from,
        LocalDateTime::from);

    if (parsed == null) {
      throw new KsqlException("text value: "
          + text
          +  "cannot be parsed into a timestamp");
    }

    if (parsed instanceof LocalDateTime) {
      parsed = ((LocalDateTime) parsed).atZone(zoneId);
    }

    final LocalDateTime dateTime = ((ZonedDateTime) parsed)
        .withZoneSameInstant(ZoneId.systemDefault())
        .toLocalDateTime();
    return Timestamp.valueOf(dateTime).getTime();
  }

}
