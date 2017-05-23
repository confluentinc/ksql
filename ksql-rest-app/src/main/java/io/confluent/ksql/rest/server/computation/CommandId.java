/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

public class CommandId {
  private final Type type;
  private final String entity;

  public enum Type {
    TOPIC,
    STREAM,
    TABLE,
    TERMINATE
  }

  public CommandId(Type type, String entity) {
    this.type = type;
    this.entity = entity;
  }

  public CommandId(String type, String entity) {
    this(Type.valueOf(type.toUpperCase()), entity);
  }

  @JsonCreator
  public static CommandId fromString(String fromString) {
    String[] splitOnSlash = fromString.split("/", 2);
    if (splitOnSlash.length != 2) {
      throw new IllegalArgumentException("Expected a string of the form <type>/<entity>");
    }
    return new CommandId(splitOnSlash[0], splitOnSlash[1]);
  }

  public Type getType() {
    return type;
  }

  public String getEntity() {
    return entity;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.format("%s/%s", type.toString().toLowerCase(), entity);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandId)) {
      return false;
    }
    CommandId commandId = (CommandId) o;
    return getType() == commandId.getType() &&
        Objects.equals(getEntity(), commandId.getEntity());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getEntity());
  }
}
