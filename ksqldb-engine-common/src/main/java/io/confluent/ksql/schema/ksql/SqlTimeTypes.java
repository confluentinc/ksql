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

package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;

/**
 * Helpers for working with SQL time types.
 */
public final class SqlTimeTypes {

  private static PartialStringToTimestampParser PARSER = new PartialStringToTimestampParser();

  private static final String TIME_HELP_MESSAGE = System.lineSeparator()
      + "Required format is: \"" + KsqlConstants.TIME_PATTERN + "\", "
      + "for example: '01:34:20.123' or '01:34:20'. "
      + "Partials are also supported, for example '01:34'";
  private static final String DATE_HELP_MESSAGE = System.lineSeparator()
      + "Required format is: \"" + KsqlConstants.DATE_PATTERN + "\", "
      + "for example '2020-05-26'. "
      + "Partials are also supported, for example '2020-05'";

  private SqlTimeTypes() {
  }

  /**
   * Parse a SQL timestamp from a string.
   *
   * @param str the string to parse.
   * @return the Timestamp value.
   */
  public static Timestamp parseTimestamp(final String str) {
    return PARSER.parseToTimestamp(str);
  }

  public static String formatTimestamp(final Timestamp timestamp) {
    return DateTimeFormatter
        .ofPattern(KsqlConstants.DATE_TIME_PATTERN)
        .withZone(ZoneId.of("Z"))
        .format(timestamp.toInstant());
  }

  public static Time parseTime(final String str) {
    try {
      return new Time(LocalTime.parse(str).toNanoOfDay() / 1000000);
    } catch (DateTimeParseException e) {
      throw new KsqlException("Failed to parse time '" + str
          + "': " + e.getMessage()
          + TIME_HELP_MESSAGE,
          e
      );
    }
  }

  public static String formatTime(final Time time) {
    return LocalTime.ofSecondOfDay(time.getTime() / 1000).toString();
  }

  public static Date parseDate(final String str) {
    try {
      return new Date(TimeUnit.DAYS.toMillis(
          LocalDate.parse(PartialStringToTimestampParser.completeDate(str))
          .toEpochDay()));
    } catch (DateTimeParseException e) {
      throw new KsqlException("Failed to parse date '" + str
          + "': " + e.getMessage()
          + DATE_HELP_MESSAGE,
          e
      );
    }
  }

  public static String formatDate(final Date date) {
    return LocalDate.ofEpochDay(TimeUnit.MILLISECONDS.toDays(date.getTime())).toString();
  }

  public static Date timestampToDate(final Timestamp timestamp) {
    final long epochDay = timestamp.toInstant().atZone(ZoneId.of("Z")).toLocalDate().toEpochDay();
    return new Date(TimeUnit.DAYS.toMillis(epochDay));
  }

  public static Time timestampToTime(final Timestamp timestamp) {
    final long nanoOfDay = timestamp.toInstant().atZone(ZoneId.of("Z")).toLocalTime().toNanoOfDay();
    return new Time(TimeUnit.NANOSECONDS.toMillis(nanoOfDay));
  }
}
