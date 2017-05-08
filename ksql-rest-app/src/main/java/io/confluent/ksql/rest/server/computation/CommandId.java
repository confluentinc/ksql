/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class CommandId {

  public enum Type {
    TOPIC,
    STREAM,
    TABLE,
    TERMINATE
  }

  private final Type type;
  private final String entity;

  public CommandId(Type type, String entity) {
    this.type = type;
    this.entity = entity;
  }

  @JsonCreator
  public CommandId(@JsonProperty("type") String type, @JsonProperty("entity") String entity) {
    this(Type.valueOf(type.toUpperCase()), entity);
  }

  public Type getType() {
    return type;
  }

  public String getEntity() {
    return entity;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CommandId)) {
      return false;
    }

    CommandId that = (CommandId) o;

    return Objects.equals(this.type, that.type) && Objects.equals(this.entity, that.entity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, entity);
  }

  @Override
  public String toString() {
    return String.format("%s/%s", type.toString().toLowerCase(), entity);
  }

}
