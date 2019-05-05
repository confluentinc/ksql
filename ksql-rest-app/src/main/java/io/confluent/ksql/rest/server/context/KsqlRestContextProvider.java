/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.context;

import java.util.Collections;

public final class  KsqlRestContextProvider {
  private static final ThreadLocal<KsqlRestContext> restContextThreadLocal = new ThreadLocal<>();

  private static final KsqlRestContext defaultRestContext = new KsqlRestContext(
      Collections.emptyMap()
  );

  private KsqlRestContextProvider() {
  }

  public static void setRestContextThreadLocal(final KsqlRestContext ksqlRestContext) {
    restContextThreadLocal.set(ksqlRestContext);
  }

  public static KsqlRestContext getRestContextThreadLocal() {
    final KsqlRestContext ksqlRestContext = restContextThreadLocal.get();
    if (ksqlRestContext != null) {
      return ksqlRestContext;
    }

    return defaultRestContext;
  }
}