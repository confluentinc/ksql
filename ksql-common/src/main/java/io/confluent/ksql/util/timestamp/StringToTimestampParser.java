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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalUnit;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
public class StringToTimestampParser {

  private static final Logger LOG = LoggerFactory.getLogger(StringToTimestampParser.class);

  private static final Multimap<TemporalUnit, ChronoField> CHRONO_UNIT_TO_FIELDS;

  static {
    ImmutableMultimap.Builder<TemporalUnit, ChronoField> unitBuilder = ImmutableMultimap.builder();
    for (ChronoField field : ChronoField.values())  {
      unitBuilder.put(field.getBaseUnit(), field);
    }
    CHRONO_UNIT_TO_FIELDS = unitBuilder.build();
  }

  private final DateTimeFormatter formatter;

  public StringToTimestampParser(final String pattern) {
    final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);

    // Java APIs don't let you access which TemporalFields are supported directly
    // from a DateTimeFormatter. Instead, we format LocalDateTime.now() using the
    // pattern and parse it again into a TemporalAccessor - which allows us sneak
    // a look into which temporal units are supported. Using parseUnresolved means
    // that it won't expand the format into all supported inferred types (see
    // documentation)
    final String sample = ofPattern.format(LocalDateTime.now());
    final TemporalAccessor now = ofPattern.parseUnresolved(sample, new ParsePosition(0));
    final DateTimeFormatterBuilder formatBuilder = new DateTimeFormatterBuilder()
        .appendPattern(pattern);

    // We need to fill in defaults for any chrono field that is not supported that
    // has a base unit greater than or equal to ChronoUnit.HOURS. Some fields can
    // span multiple time units - for example, DAY_OF_YEAR spans (DAY, YEAR] which
    // makes it exclusive with MONTH_OF_YEAR (which spans (MONTH, YEAR)). Constructing
    // a range of what the format supports allows us to fill in the blanks
    final RangeSet<ChronoUnit> supportedRange = TreeRangeSet.create();
    for (ChronoField chronoField : ChronoField.values()) {
      if (now.isSupported(chronoField)) {
        // we chose closedOpen because we do not want to set defaults for the base limit
        // but do want to set it for the range. For example, DAY_OF_YEAR should preclude
        // us from setting defaults for DAY or MONTH, but we still set default for YEAR
        supportedRange.add(Range.closedOpen(
            (ChronoUnit) chronoField.getBaseUnit(),
            (ChronoUnit) chronoField.getRangeUnit()));
      }
    }

    LOG.debug("Supported range for pattern {} is: {}", pattern, supportedRange);
    System.out.println("Supported range: " + supportedRange);

    setDefault(formatBuilder, now, ChronoUnit.YEARS, ChronoField.YEAR_OF_ERA, 1970, supportedRange);
    setDefault(formatBuilder, now, ChronoUnit.MONTHS, ChronoField.MONTH_OF_YEAR, 1, supportedRange);
    setDefault(formatBuilder, now, ChronoUnit.DAYS, ChronoField.DAY_OF_MONTH, 1, supportedRange);
    setDefault(formatBuilder, now, ChronoUnit.HOURS, ChronoField.HOUR_OF_DAY, 0, supportedRange);

    formatter = formatBuilder.toFormatter();
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

  private void setDefault(
      final DateTimeFormatterBuilder formatterBuilder,
      final TemporalAccessor sample,
      final ChronoUnit chronoUnit,
      final ChronoField defaultField,
      final int defaultValue,
      final RangeSet<ChronoUnit> supportedRange) {
    if (supportedRange == null || !supportedRange.contains(chronoUnit)) {
      // if it is not within the supported range, we can safely use
      // the default field
      formatterBuilder.parseDefaulting(defaultField, defaultValue);
    } else {
      final Optional<ChronoField> defaultingField = CHRONO_UNIT_TO_FIELDS.get(chronoUnit)
          .stream()
          .filter(sample::isSupported)
          .findFirst();

      defaultingField.ifPresent(
          chronoField -> formatterBuilder.parseDefaulting(chronoField, defaultValue));
    }
  }

}
