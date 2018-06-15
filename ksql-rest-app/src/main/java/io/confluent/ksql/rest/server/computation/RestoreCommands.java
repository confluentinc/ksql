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
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.Pair;

class RestoreCommands {

  private final Map<Pair<Integer, CommandId>, Command> toRestore = new LinkedHashMap<>();
  private final Map<QueryId, CommandId> allTerminatedQueries = new HashMap<>();
  private final Map<String, CommandId> dropped = new HashMap<>();
  private final List<CommandId> allCommandIds = new ArrayList<>();

  void addCommand(final CommandId key, final Command value) {
    if (key.getType() == CommandId.Type.TERMINATE) {
      allTerminatedQueries.put(new QueryId(key.getEntity()), key);
      if (allCommandIds.contains(key)) {
        allCommandIds.remove(key);
      }
    } else {
      toRestore.put(new Pair<>(allCommandIds.size(), key), value);
      if (key.getAction() == CommandId.Action.DROP) {
        dropped.put(key.getEntity(), key);
      }
    }
    allCommandIds.add(key);
  }

  interface ForEach {

    void apply(
        final CommandId commandId,
        final Command command,
        final Map<QueryId, CommandId> terminatedQueries,
        final boolean dropped
    );
  }

  void forEach(final ForEach action) {
    toRestore.forEach((commandIdIndexPair, command) -> {
      final Map<QueryId, CommandId> terminatedAfter = new HashMap<>();
      allTerminatedQueries.entrySet().stream()
          .filter(entry -> allCommandIds.indexOf(entry.getValue()) > commandIdIndexPair.left)
          .forEach(queryIdCommandIdEntry ->
                       terminatedAfter.put(
                           queryIdCommandIdEntry.getKey(),
                           queryIdCommandIdEntry.getValue()
                       ));
      final Set<String> droppedEntities = this.dropped.entrySet().stream()
          .filter(entry -> allCommandIds.indexOf(entry.getValue()) > commandIdIndexPair.left)
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
      action.apply(
          commandIdIndexPair.right,
          command,
          terminatedAfter,
          droppedEntities.contains(commandIdIndexPair.right.getEntity())
      );
    });
  }

  // Visible for testing
  Map<QueryId, CommandId> terminatedQueries() {
    return Collections.unmodifiableMap(allTerminatedQueries);
  }
}
