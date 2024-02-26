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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;

public class StringToTimestampParser {

  private static final Function<ZoneId, ZonedDateTime> DEFAULT_ZONED_DATE_TIME =
      zid -> ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, zid);
  private static final long LEAP_DAY_OF_THE_YEAR = 366;

  private final DateTimeFormatter formatter;

  public StringToTimestampParser(final String pattern) {
    formatter = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern(pattern)
        .toFormatter(Locale.ROOT);
  }

  /**
   * @param text    the textual representation of the timestamp
   * @param zoneId  the zoneId to use, if none present in {@code text}
   *
   * @return the millis since epoch that {@code text} represents
   */
  public Timestamp parseToTimestamp(final String text, final ZoneId zoneId) {
    return Timestamp.from(parseZoned(text, zoneId).toInstant());
  }

  /**
   * Parse with a default time zone of {@code ZoneId#systemDefault}
   *
   * @see #parse(String, ZoneId)
   */
  public long parse(final String text) {
    return parse(text, ZoneId.systemDefault());
  }

  /**
   * @param text    the textual representation of the timestamp
   * @param zoneId  the zoneId to use, if none present in {@code text}
   *
   * @return the millis since epoch that {@code text} represents
   */
  public long parse(final String text, final ZoneId zoneId) {
    return parseZoned(text, zoneId).toInstant().toEpochMilli();
  }

  @VisibleForTesting
  ZonedDateTime parseZoned(final String text, final ZoneId zoneId) {
    final TemporalAccessor parsed = formatter.parse(text);
    final ZoneId parsedZone = parsed.query(TemporalQueries.zone());

    ZonedDateTime resolved = DEFAULT_ZONED_DATE_TIME.apply(
        ObjectUtils.defaultIfNull(parsedZone, zoneId));

    for (final TemporalField override : ChronoField.values()) {
      if (parsed.isSupported(override)) {
        if (!resolved.isSupported(override)) {
          throw new KsqlException(
              "Unsupported temporal field in timestamp: " + text + " (" + override + ")");
        }

        final long value = parsed.getLong(override);
        if (override == ChronoField.DAY_OF_YEAR && value == LEAP_DAY_OF_THE_YEAR) {
          if (!parsed.isSupported(ChronoField.YEAR)) {
            throw new KsqlException("Leap day cannot be parsed without supplying the year field");
          }
          // eagerly override year, to avoid mismatch with epoch year, which is not a leap year
          resolved = resolved.withYear(parsed.get(ChronoField.YEAR));
        }
        resolved = resolved.with(override, value);
      }
    }

    return resolved;
  }

}
