/**
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
 **/

package io.confluent.ksql.util.timestamp;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

import io.confluent.ksql.util.KsqlException;

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
    TemporalAccessor parsed = formatter.parseBest(
        text,
        ZonedDateTime::from,
        LocalDateTime::from);

    if (parsed == null) {
      throw new KsqlException("text value: "
          + text
          +  "cannot be parsed into a timestamp");
    }

    if (parsed instanceof ZonedDateTime) {
      parsed = ((ZonedDateTime) parsed)
          .withZoneSameInstant(ZoneId.systemDefault())
          .toLocalDateTime();
    }

    final LocalDateTime dateTime = (LocalDateTime) parsed;
    return Timestamp.valueOf(dateTime).getTime();
  }

}
