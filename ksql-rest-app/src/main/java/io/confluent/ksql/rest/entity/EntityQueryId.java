/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.confluent.ksql.query.QueryId;

import java.util.Objects;

public class EntityQueryId {
  private final String id;

  public EntityQueryId(QueryId queryId) {
    this.id = queryId.getId();
  }

  @JsonCreator
  public EntityQueryId(final String id) {
    this.id = id;
  }

  @JsonValue
  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof EntityQueryId
        && Objects.equals(((EntityQueryId) o).id, id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
}
