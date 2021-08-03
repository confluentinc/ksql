/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.rest.entity.KsqlEntity;
import java.util.Optional;

public final class StatementExecutorResponse {

  private final boolean isHandled;
  private final Optional<KsqlEntity> entity;

  private StatementExecutorResponse(final boolean isHandled, final Optional<KsqlEntity> entity) {
    this.isHandled = isHandled;
    this.entity = entity;
  }

  public static StatementExecutorResponse handled(final Optional<KsqlEntity> ksqlEntity) {
    // return what the custom executor returned
    return new StatementExecutorResponse(true, ksqlEntity);
  }

  public static StatementExecutorResponse notHandled() {
    // default back to the distributor
    return new StatementExecutorResponse(false, Optional.empty());
  }

  public Optional<KsqlEntity> getEntity() {
    return entity;
  }

  public boolean isHandled() {
    return isHandled;
  }
}
