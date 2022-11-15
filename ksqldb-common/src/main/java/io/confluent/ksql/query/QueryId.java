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

package io.confluent.ksql.query;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * A query id.
 *
 * <p>For backwards compatibility reasons query ids must preserve their case, as their text
 * representation is used, among other things, for internal topic naming.
 *
 * <p>However, two ids with the same text, with different case, should compare equal. This is needed
 * so that look ups against query ids are not case-sensitive.
 */
@Immutable
public class QueryId {

  private final String id;
  private final String cachedUpperCase;

  @JsonCreator
  public QueryId(final String id) {
    this.id = requireNonNull(id, "id");
    this.cachedUpperCase = id.toUpperCase();
  }

  @JsonValue
  public String toString() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryId)) {
      return false;
    }
    final QueryId queryId = (QueryId) o;
    return Objects.equals(cachedUpperCase, queryId.cachedUpperCase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cachedUpperCase);
  }
}
