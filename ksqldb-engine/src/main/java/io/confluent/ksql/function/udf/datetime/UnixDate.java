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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Date;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "unix_date",
    category = FunctionCategory.DATE_TIME,
    description = "Returns the current number of days for the system since "
        + "1970-01-01 00:00:00 UTC/GMT.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class UnixDate {

  @Udf(description = "Returns the current number of days for the system since "
      + "1970-01-01 00:00:00 UTC/GMT.")
  public int unixDate() {
    return ((int) LocalDate.now().toEpochDay());
  }

  @Udf(description = "Returns the current number of days since "
      + "1970-01-01 00:00:00 UTC/GMT represented by the given date.")
  public Integer unixDate(final Date date) {
    if (date == null) {
      return null;
    }
    return (int) TimeUnit.MILLISECONDS.toDays(date.getTime());
  }

}
