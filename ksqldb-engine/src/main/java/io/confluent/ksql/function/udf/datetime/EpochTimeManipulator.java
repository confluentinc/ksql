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

package io.confluent.ksql.function.udf.datetime;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UdfDescription(name = "epochTimeOfTimeZone",
    category = FunctionCategory.DATE_TIME,
    author = "retarus GmbH",
    description = "Offsets BIGINT millisecond timestamp value to specific timezone.")
public class EpochTimeManipulator {
  private static final Logger LOG = LoggerFactory.getLogger(EpochTimeManipulator.class);

  @Udf(description = "Offsets the input BIGINT millisecond timestamp value (at UTC) to the same "
      + "epoch \"in a different timezone\". "
      + "Milliseconds are stripped and only seconds used for conversion. "
      + "The conversion is daylight savings aware. Because epochs don't have timezones, the result "
      + "is \"wrong\" in UTC terms. This can be used to force KSQL window operations for full days "
      + "to match a day in a given timezone and not be aligned to the epoch as they "
      + "are in the standard. Example: select rowtime, TIMESTAMPTOSTRING(rowtime, " 
      + "'yyyy-MM-dd''T''HH:mm:ss.SSSX') utc_str,  EPOCHTIMEOFTIMEZONE(rowtime, " 
      + "'Europe/Berlin') berlin, TIMESTAMPTOSTRING(EPOCHTIMEOFTIMEZONE(rowtime, " 
      + "'Europe/Berlin'), 'yyyy-MM-dd''T''HH:mm:ss.SSSX') berlin_string from " 
      + "<my stream> emit changes;")
  public long epochTimeOfTimeZone(
      @UdfParameter(value = "epochTimeInMillis",
          description = "Date time in millis") final long epochTimeInMillis,
      @UdfParameter(value = "timeZoneName",
          description = "Target time zone as timezone "
          + "string, e.g. \"Europe/Berlin\"") final String timeZoneName) {

    long result = epochTimeInMillis;

    if (epochTimeInMillis < 0) {
      result = 0L;
    }

    if (null != timeZoneName && !timeZoneName.trim().isEmpty()) {
      try {
        final LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochTimeInMillis / 1000, 
            0, ZoneOffset.UTC);
        final ZonedDateTime zonedDateTime = dateTime.atZone(ZoneId.of(timeZoneName.trim()));
        final long offsetMillis = zonedDateTime.getOffset().getTotalSeconds() * 1000;

        return epochTimeInMillis + offsetMillis;
      } catch (DateTimeException e) {
        LOG.warn("TZ parsing error. Returning 0. Was: millis: " + epochTimeInMillis 
            + " / TZ Name: " + timeZoneName);
        return result;
      }
    }

    return result;
  }
}
