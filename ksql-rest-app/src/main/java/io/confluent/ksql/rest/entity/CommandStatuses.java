package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeName("command_statuses")
@JsonTypeInfo(
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    use = JsonTypeInfo.Id.NAME
)
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
