/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Timestamp;

@UdfDescription(
    name = "from_unixtime",
    category = FunctionCategory.DATE_TIME,
    description = "Converts the number of milliseconds since 1970-01-01 00:00:00 UTC/GMT into "
        + "a TIMESTAMP value.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class FromUnixTime {
  @Udf(description = "Converts the number of milliseconds since 1970-01-01 00:00:00 UTC/GMT into "
      + "a TIMESTAMP value.")
  public Timestamp fromUnixTime(
      @UdfParameter(
          description = "Milliseconds since"
              + " January 1, 1970, 00:00:00 GMT.") final Long epochMilli
  ) {
    if (epochMilli == null) {
      return null;
    }
    return new Timestamp(epochMilli);
  }
}
