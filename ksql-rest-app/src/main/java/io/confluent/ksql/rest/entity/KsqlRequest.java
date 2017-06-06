/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KsqlRequest {
  private final String ksql;

  @JsonCreator
  public KsqlRequest(@JsonProperty("ksql") String ksql) {
    this.ksql = ksql;
  }

  public String getKsql() {
    return ksql;
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
    return Objects.equals(getKsql(), that.getKsql());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKsql());
  }
}
