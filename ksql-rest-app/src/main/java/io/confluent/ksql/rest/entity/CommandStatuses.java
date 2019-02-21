/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.rest.server.computation.CommandId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("commandStatuses")
@JsonTypeInfo(
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({})
public class CommandStatuses extends HashMap<CommandId, CommandStatus.Status> {

  @JsonCreator
  public CommandStatuses(final Map<CommandId, CommandStatus.Status> statuses) {
    super(statuses);
  }

  public static CommandStatuses fromFullStatuses(final Map<CommandId, CommandStatus> fullStatuses) {
    final Map<CommandId, CommandStatus.Status> statuses = fullStatuses.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getStatus())
    );
    return new CommandStatuses(statuses);
  }
}
