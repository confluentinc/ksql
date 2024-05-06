/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.ksql.util.KsqlConstants.TIME_PATTERN;

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.time.ZoneId;

/**
 * A parser that can handle partially complete date-times.
 *
 * <p>A hack around the fact we do not as yet have a DATETIME type.
 */
public class PartialStringToTimestampParser {

  private static final String HELP_MESSAGE = System.lineSeparator()
      + "Required format is: \"" + KsqlConstants.DATE_TIME_PATTERN + "\", "
      + "with an optional numeric 4-digit timezone, for example: "
      + "'2020-05-26T23:59:58.000' or with tz: '2020-05-26T23:59:58.000+0200'. "
      + "A trailing 'Z' instead of tz indicates the zero UTC offset. "
      + "Partials are also supported, for example \"2020-05-26\"";

  private static final StringToTimestampParser PARSER =
      new StringToTimestampParser(KsqlConstants.DATE_TIME_PATTERN);

  public Timestamp parseToTimestamp(final String text) {
    return new Timestamp(parse(text));
  }

  @SuppressWarnings("MethodMayBeStatic") // Non-static to support DI.
  public long parse(final String text) {

    final String date;
    final String time;
    final String timezone;

    if (text.contains("T")) {
      date = text.substring(0, text.indexOf('T'));
      final String withTimezone = text.substring(text.indexOf('T') + 1);
      timezone = getTimezone(withTimezone);
      time = completeTime(withTimezone.substring(0, withTimezone.length() - timezone.length())
              .replaceAll("Z$",""));
    } else {
      date = completeDate(text);
      time = completeTime("");
      timezone = "";
    }

    try {
      final ZoneId zoneId = parseTimezone(timezone);
      return PARSER.parse(date + "T" + time, zoneId);
    } catch (final RuntimeException e) {
      throw new KsqlException("Failed to parse timestamp '" + text
          + "': " + e.getMessage()
          + HELP_MESSAGE,
          e
      );
    }
  }

  private static String getTimezone(final String time) {
    if (time.contains("+")) {
      return time.substring(time.indexOf('+'));
    }

    if (time.contains("-")) {
      return time.substring(time.indexOf('-'));
    }

    return "";
  }

  private static ZoneId parseTimezone(final String timezone) {
    if (timezone.trim().isEmpty()) {
      return ZoneId.of("+0000");
    }

    try {
      return ZoneId.of(timezone);
    } catch (final Exception e) {
      throw new KsqlException("Failed to parse timezone '" + timezone
          + "': " + e.getMessage(),
          e
      );
    }
  }

  public static String completeDate(final String date) {
    final String[] parts = date.split("-");
    if (parts.length == 1) {
      return date + "-01-01";
    }

    if (parts.length == 2) {
      return date + "-01";
    }

    // It is either a complete date or an incorrectly formatted one.
    // In the latter case, we can pass the incorrectly formed string
    // to the timestamp parser which will deal with the error handling.
    return date;
  }

  private static String completeTime(final String time) {
    if (time.length() >= TIME_PATTERN.length()) {
      return time;
    }

    return time + TIME_PATTERN
        .substring(time.length())
        .replaceAll("[a-zA-Z]", "0");
  }
}
