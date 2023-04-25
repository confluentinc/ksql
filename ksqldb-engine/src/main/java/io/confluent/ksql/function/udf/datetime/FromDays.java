/*
 * Copyright 2021 Confluent Inc.
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
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "from_days",
    category = FunctionCategory.DATE_TIME,
    description = "Converts a number of days since epoch to a DATE value.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class FromDays {

  @Udf(description = "Converts a number of days since epoch to a DATE value.")
  public Date fromDays(final int days) {
    return new Date(TimeUnit.DAYS.toMillis(days));
  }

}
