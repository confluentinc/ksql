/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
