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

package io.confluent.ksql.function.udf.datetime;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.TimeZone;

@UdfDescription(
    name = "convert_tz",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Converts a TIMESTAMP value from one timezone to another."
)
public class ConvertTz {
  @Udf(description = "Converts a TIMESTAMP value from one timezone to another")
  public Timestamp convertTz(
      @UdfParameter(
          description = "The TIMESTAMP value.") final Timestamp timestamp,
      @UdfParameter(
          description =  "The fromTimeZone in java.util.TimeZone ID format. For example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String fromTimeZone,
      @UdfParameter(
          description =  "The toTimeZone in java.util.TimeZone ID format. For example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String toTimeZone
  ) {
    if (timestamp == null || fromTimeZone == null || toTimeZone == null) {
      return null;
    }
    try {
      final long offset = TimeZone.getTimeZone(ZoneId.of(toTimeZone)).getOffset(timestamp.getTime())
          - TimeZone.getTimeZone(ZoneId.of(fromTimeZone)).getOffset(timestamp.getTime());
      return new Timestamp(timestamp.getTime() + offset);
    } catch (DateTimeException e) {
      throw new KsqlFunctionException("Invalid time zone: " + e.getMessage());
    }
  }
}
