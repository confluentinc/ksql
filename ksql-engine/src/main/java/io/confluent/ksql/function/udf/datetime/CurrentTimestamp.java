/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.datetime;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.sql.Timestamp;
import java.time.ZoneId;

@UdfDescription(name = "currenttimestamp",
    description = "Gets the current timestamp represented in millisecond as BIGINT.")
public class CurrentTimestamp {

  @Udf(description = "Gets a BIGINT millisecond from the current timestamp.")
  public long currentTimestamp() {
    return new Timestamp(System.currentTimeMillis()).getTime();
  }

  @Udf(description = "Gets a BIGINT millisecond from the current timestamp"
      + " in the given time zone.")
  public long currentTimestamp(
      @UdfParameter(value = "timeZone",
          description =  "timeZone is a java.util.TimeZone ID format, for example: \"UTC\","
              + " \"America/Los_Angeles\", \"PDT\", \"Europe/London\"") final String timeZone) {
    try {
      final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      final ZoneId zoneId = ZoneId.of(timeZone);
      timestamp.toLocalDateTime().atZone(zoneId);
      return timestamp.getTime();
    } catch (final RuntimeException e) {
      throw new KsqlFunctionException("Failed to get the current timestamp': "
          + "' at timezone '" + timeZone + "': " + e.getMessage(), e);
    }
  }

}
