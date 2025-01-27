/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.extensions;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.KsqlConfig;

public class KsqlResourceContextImpl implements KsqlResourceContext {

  private final KsqlConfig ksqlConfig;
  private final KsqlRestConfig ksqlRestConfig;

  public KsqlResourceContextImpl(KsqlConfig ksqlConfig, KsqlRestConfig restConfig) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlRestConfig = restConfig;
  }

  @Override
  public KsqlConfig ksqlConfig() {
    return ksqlConfig;
  }

  @Override
  public KsqlRestConfig ksqlRestConfig() {
    return ksqlRestConfig;
  }
}
