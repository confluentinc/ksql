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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ExecuteStatementResult;
import java.util.Objects;
import java.util.Optional;

public class ExecuteStatementResultImpl implements ExecuteStatementResult {

  private final Optional<String> queryId;

  ExecuteStatementResultImpl(final Optional<String> queryId) {
    this.queryId = Objects.requireNonNull(queryId);
  }

  @Override
  public Optional<String> queryId() {
    return queryId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecuteStatementResultImpl that = (ExecuteStatementResultImpl) o;
    return queryId.equals(that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId);
  }

  @Override
  public String toString() {
    return "ExecuteStatementResult{"
        + "queryId=" + queryId
        + '}';
  }
}
