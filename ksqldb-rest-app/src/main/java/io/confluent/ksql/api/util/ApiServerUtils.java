/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.util;

import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.util.QueryMask;

public final class ApiServerUtils {

  private ApiServerUtils() {
  }

  public static void setMaskedSqlIfNeeded(final KsqlRequest request) {
    try {
      request.getMaskedKsql();
    } catch (final Exception e) {
      ApiServerUtils.setMaskedSql(request);
    }
  }

  public static void setMaskedSql(final KsqlRequest request) {
    request.setMaskedKsql(QueryMask.getMaskedStatement(request.getUnmaskedKsql()));
  }
}
