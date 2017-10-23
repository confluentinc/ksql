/**
 * Copyright 2017 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonSubTypes({})
public class KsqlRequest {
  private final String ksql;
  private final Map<String, Object> streamsProperties;

  @JsonCreator
  public KsqlRequest(
      @JsonProperty("ksql") String ksql,
      @JsonProperty("streamsProperties") Map<String, Object> streamsProperties
  ) {
    this.ksql = ksql;
    this.streamsProperties = Optional.ofNullable(streamsProperties).orElse(Collections.emptyMap());
  }

  public String getKsql() {
    return ksql;
  }

  public Map<String, Object> getStreamsProperties() {
    return streamsProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlRequest)) {
      return false;
    }
    KsqlRequest that = (KsqlRequest) o;
    return Objects.equals(getKsql(), that.getKsql())
        && Objects.equals(getStreamsProperties(), that.getStreamsProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKsql(), getStreamsProperties());
  }
}
