/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class StringToTimestamp implements Kudf {

  private DateTimeFormatter threadSafeFormatter;

  @Override
  public Object evaluate(final Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("StringToTimestamp udf should have two input argument:"
                                      + " date value and format.");
    }

    try {
      ensureInitialized(args);

      TemporalAccessor parsed = threadSafeFormatter.parseBest(
          args[0].toString(), ZonedDateTime::from, LocalDateTime::from);

      if (parsed == null) {
        throw new KsqlFunctionException("Value could not be parsed");
      }

      if (parsed instanceof ZonedDateTime) {
        parsed = ((ZonedDateTime) parsed)
            .withZoneSameInstant(ZoneId.systemDefault())
            .toLocalDateTime();
      }

      final LocalDateTime dateTime = (LocalDateTime) parsed;
      return Timestamp.valueOf(dateTime).getTime();
    } catch (final Exception e) {
      throw new KsqlFunctionException("Exception running StringToTimestamp(" + args[0] + ", "
                                      + args[1] + ") : " + e.getMessage(), e);
    }
  }

  private void ensureInitialized(final Object[] args) {
    if (threadSafeFormatter == null) {
      threadSafeFormatter = new DateTimeFormatterBuilder()
          .appendPattern(args[1].toString())
          .parseDefaulting(ChronoField.YEAR_OF_ERA, 1970)
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .toFormatter();
    }
  }
}
