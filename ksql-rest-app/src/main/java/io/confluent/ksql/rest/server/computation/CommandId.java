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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

@JsonSubTypes({})
public class CommandId {

  private final Type type;
  private final String entity;
  private final Action action;

  public enum Type {
    TOPIC,
    STREAM,
    TABLE,
    TERMINATE
  }

  public enum Action {
    CREATE,
    DROP,
    EXECUTE
  }

  public CommandId(
      final Type type,
      final String entity,
      final Action action
  ) {
    this.type = type;
    this.entity = entity;
    this.action = action;
  }

  public CommandId(
      final String type,
      final String entity,
      final String action
  ) {
    this(Type.valueOf(type.toUpperCase()), entity, Action.valueOf(action.toUpperCase()));
  }

  @JsonCreator
  public static CommandId fromString(String fromString) {
    String[] splitOnSlash = fromString.split("/", 3);
    if (splitOnSlash.length != 3) {
      throw new IllegalArgumentException("Expected a string of the form <type>/<entity>/<action>");
    }
    return new CommandId(splitOnSlash[0], splitOnSlash[1], splitOnSlash[2]);
  }

  public Type getType() {
    return type;
  }

  public String getEntity() {
    return entity;
  }

  public Action getAction() {
    return action;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.format(
        "%s/%s/%s",
        type.toString().toLowerCase(),
        entity,
        action.toString().toLowerCase()
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommandId commandId = (CommandId) o;
    return type == commandId.type
        && Objects.equals(entity, commandId.entity)
        && action == commandId.action;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, entity, action);
  }
}
