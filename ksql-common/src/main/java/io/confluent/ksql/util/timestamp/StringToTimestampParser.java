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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Locale;
import java.util.function.Function;

public class StringToTimestampParser {

  private static final Function<ZoneId, ZonedDateTime> DEFAULT_ZONED_DATE_TIME =
      zid -> ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, zid);

  private final DateTimeFormatter formatter;

  public StringToTimestampParser(final String pattern) {
    formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
  }

  public long parse(final String text) {
    return parse(text, ZoneId.systemDefault());
  }

  public long parse(final String text, final ZoneId zoneId) {
    final TemporalAccessor parsed = formatter.parse(text);
    ZonedDateTime resolved = DEFAULT_ZONED_DATE_TIME.apply(zoneId);

    for (final TemporalField override : ChronoField.values()) {
      if (parsed.isSupported(override)) {
        if (!resolved.isSupported(override)) {
          throw new KsqlException(
              "Unsupported temporal field in timestamp: " + text + " (" + override + ")");
        }
        resolved = resolved.with(override, parsed.getLong(override));
      }
    }

    return Timestamp.valueOf(
        resolved.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime()).getTime();
  }

}
