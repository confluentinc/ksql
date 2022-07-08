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
import java.sql.Date;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "datesub",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Subtracts a duration from a DATE value."
)
public class DateSub {

  @Udf(description = "Subtracts a duration from a date")
  public Date dateSub(
      @UdfParameter(description = "A unit of time, for example DAY") final TimeUnit unit,
      @UdfParameter(
          description = "An integer number of intervals to subtract") final Integer interval,
      @UdfParameter(description = "A DATE value.") final Date date
  ) {
    if (unit == null || interval == null || date == null) {
      return null;
    }
    final long epochDayResult =
        TimeUnit.MILLISECONDS.toDays(date.getTime() - unit.toMillis(interval));
    return new Date(TimeUnit.DAYS.toMillis(epochDayResult));
  }
}
