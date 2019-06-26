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

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.time.LocalDate;

@UdfDescription(name = "unix_date",
    description = "Gets an integer representing days since epoch.")
public class UnixDate {

  @Udf(description = "Gets an integer representing days since epoch.")
  public int unixDate() {
    return ((int) LocalDate.now().toEpochDay());
  }

}
