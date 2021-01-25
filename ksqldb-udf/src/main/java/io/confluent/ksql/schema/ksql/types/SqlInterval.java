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

package io.confluent.ksql.schema.ksql.types;

import io.confluent.ksql.schema.utils.FormatOptions;

public final class SqlInterval extends SqlType {

  public static SqlInterval of() {
    return new SqlInterval();
  }

  private SqlInterval() {
    super(SqlBaseType.BIGINT);
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return toString();
  }

  @Override
  public String toString() {
    return "INTERVAL";
  }
}
