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

package io.confluent.ksql.util;

import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * This represents a unique store in the system and the basis for getting lag information from
 * KafkaStreams, exposed by the QueryMetadata.
 */
public final class LagInfoKey {
  private static final String SEPARATOR = "$";

  private final String queryApplicationId;
  private final String stateStoreName;

  public static LagInfoKey of(final String queryApplicationId, final String stateStoreName) {
    return new LagInfoKey(queryApplicationId, stateStoreName);
  }

  public static LagInfoKey of(final String serializedKey) {
    return new LagInfoKey(serializedKey);
  }

  private LagInfoKey(final String queryApplicationId, final String stateStoreName) {
    this.queryApplicationId = queryApplicationId;
    this.stateStoreName = stateStoreName;
  }

  private LagInfoKey(final String serializedKey) {
    final String [] parts = serializedKey.split("\\" + SEPARATOR);
    Preconditions.checkArgument(parts.length == 2);
    this.queryApplicationId = Objects.requireNonNull(parts[0]);
    this.stateStoreName = Objects.requireNonNull(parts[1]);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LagInfoKey that = (LagInfoKey) o;
    return Objects.equals(queryApplicationId, that.queryApplicationId)
        && Objects.equals(stateStoreName, that.stateStoreName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryApplicationId, stateStoreName);
  }

  @Override
  public String toString() {
    return queryApplicationId + SEPARATOR + stateStoreName;
  }

}
