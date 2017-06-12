/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName("KSQL Server Info")
public class ServerInfo {
  private final String version;

  @JsonCreator
  public ServerInfo(@JsonProperty("version") String version) {
    this.version = version;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ServerInfo)) {
      return false;
    }
    ServerInfo serverInfo1 = (ServerInfo) o;
    return Objects.equals(getVersion(), serverInfo1.getVersion());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getVersion());
  }
}
