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

package io.confluent.ksql.api.server;

import java.util.Objects;
import java.util.UUID;

/**
 * Handle to a query that is passed to the client on query creation and can subsequently be used to
 * close a query. Uses UUID.randomUUID() which internally uses SecureRandom - this makes the id
 * cryptographically secure. This is important as we don't want random users guessing query IDs and
 * closing other peoples queries.
 */
public final class QueryID {

  private final String id;

  public QueryID() {
    this.id = UUID.randomUUID().toString();
  }

  public QueryID(final String id) {
    this.id = Objects.requireNonNull(id);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryID queryID = (QueryID) o;
    return Objects.equals(id, queryID.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}
