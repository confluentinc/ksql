/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "timestampadd",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Adds a duration to a TIMESTAMP value."
)
public class TimestampAdd {

  @Udf(description = "Adds a duration to a timestamp")
  public Timestamp timestampAdd(
      @UdfParameter(description = "A unit of time, for example DAY or HOUR") final TimeUnit unit,
      @UdfParameter(description = "An integer number of intervals to add") final int interval,
      @UdfParameter(description = "A TIMESTAMP value.") final Timestamp timestamp
  ) {
    return new Timestamp(timestamp.getTime() + unit.toMillis(interval));
  }
}
