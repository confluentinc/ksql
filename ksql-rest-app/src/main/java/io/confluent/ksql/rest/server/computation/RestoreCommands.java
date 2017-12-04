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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.query.QueryId;

class RestoreCommands {
  private final Map<CommandId, Command> toRestore = new LinkedHashMap<>();
  private final Map<QueryId, CommandId> allTerminatedQueries = new HashMap<>();
  private final List<CommandId> allCommandIds = new ArrayList<>();

  void addCommand(final CommandId key, final Command value) {
    if (key.getType() == CommandId.Type.TERMINATE) {
      allTerminatedQueries.put(new QueryId(key.getEntity()), key);
    } else if (!toRestore.containsKey(key)){
      toRestore.put(key, value);
    }
    allCommandIds.add(key);
  }

  boolean remove(final CommandId commandId) {
    if (toRestore.remove(commandId) != null) {
      allCommandIds.remove(commandId);
      return true;
    }
    return false;
  }

  interface ForEach {
    void apply(final CommandId commandId,
               final Command command,
               final Map<QueryId, CommandId> terminatedQueries);
  }

  void forEach(final ForEach action) {
    toRestore.forEach((commandId, command) -> {
      final int commandIdIdx = allCommandIds.indexOf(commandId);
      final Map<QueryId, CommandId> terminatedAfter = new HashMap<>();
      allTerminatedQueries.entrySet().stream()
          .filter(entry -> allCommandIds.indexOf(entry.getValue()) > commandIdIdx)
          .forEach(queryIdCommandIdEntry ->
              terminatedAfter.put(queryIdCommandIdEntry.getKey(), queryIdCommandIdEntry.getValue()));
      action.apply(commandId, command, terminatedAfter);
    });
  }

  // Visible for testing
  Map<QueryId, CommandId> terminatedQueries() {
    return Collections.unmodifiableMap(allTerminatedQueries);
  }
}
