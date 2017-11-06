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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeName("commandStatuses")
@JsonTypeInfo(
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({})
public class CommandStatuses extends HashMap<CommandId, CommandStatus.Status> {

  @JsonCreator
  public CommandStatuses(Map<CommandId, CommandStatus.Status> statuses) {
    super(statuses);
  }

  public static CommandStatuses fromFullStatuses(Map<CommandId, CommandStatus> fullStatuses) {
    Map<CommandId, CommandStatus.Status> statuses = fullStatuses.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getStatus())
    );
    return new CommandStatuses(statuses);
  }
}
