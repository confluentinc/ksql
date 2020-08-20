/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Util for compacting the restore commands
 */
public final class RestoreCommandsCompactor {

  private RestoreCommandsCompactor() {
  }

  /**
   * Compact the list of commands to restore.
   *
   * <p>Finds any command's whose queries plans that are later terminated. Any such commands have
   * their query plan and associated terminate command removed.
   *
   * <p>This compaction stops unnecessary creation of Streams topologies on a server restart.
   * Building such topologies is relatively slow and best avoided.
   *
   * @param restoreCommands the list of commands to compact.
   * @return the compacted list of commands.
   */
  static List<QueuedCommand> compact(final List<QueuedCommand> restoreCommands) {
    final List<QueuedCommand> compacted = new LinkedList<>(restoreCommands);

    final Set<QueuedCommand> terminatedQueries =
        findTerminatedQueriesAndRemoveTerminateCommands(compacted);

    removeQueryPlansOfTerminated(compacted, terminatedQueries);

    return compacted;
  }

  private static Set<QueuedCommand> findTerminatedQueriesAndRemoveTerminateCommands(
      final List<QueuedCommand> commands
  ) {
    final Map<QueryId, QueuedCommand> queries = new HashMap<>();
    final Set<QueuedCommand> terminatedQueries = Collections.newSetFromMap(new IdentityHashMap<>());

    final Iterator<QueuedCommand> it = commands.iterator();
    while (it.hasNext()) {
      final QueuedCommand cmd = it.next();

      // Find known queries:
      final Command command =
          cmd.getAndDeserializeCommand(InternalTopicSerdes.deserializer(Command.class));
      if (command.getPlan().isPresent()
          && command.getPlan().get().getQueryPlan().isPresent()
      ) {
        final QueryId queryId =
            command.getPlan().get().getQueryPlan().get().getQueryId();
        queries.putIfAbsent(queryId, cmd);
      }

      // Find TERMINATE's that match known queries:
      if (cmd.getAndDeserializeCommandId().getType() == Type.TERMINATE) {
        final QueryId queryId = new QueryId(cmd.getAndDeserializeCommandId().getEntity());
        final QueuedCommand terminated = queries.remove(queryId);
        if (terminated != null) {
          terminatedQueries.add(terminated);
          it.remove();
        }
      }
    }

    return terminatedQueries;
  }

  private static void removeQueryPlansOfTerminated(final List<QueuedCommand> compacted,
      final Set<QueuedCommand> terminatedQueries
  ) {
    final ListIterator<QueuedCommand> it = compacted.listIterator();
    while (it.hasNext()) {
      final QueuedCommand cmd = it.next();
      if (!terminatedQueries.remove(cmd)) {
        continue;
      }

      final Optional<QueuedCommand> replacement = buildNewCmdWithoutQuery(cmd);
      if (replacement.isPresent()) {
        it.set(replacement.get());
      } else {
        it.remove();
      }
    }
  }

  private static Optional<QueuedCommand> buildNewCmdWithoutQuery(final QueuedCommand cmd) {
    final Command command =
        cmd.getAndDeserializeCommand(InternalTopicSerdes.deserializer(Command.class));
    if (!command.getPlan().isPresent() || !command.getPlan().get().getDdlCommand().isPresent()) {
      // No DDL command, so no command at all if we remove the query plan. (Likely INSERT INTO cmd).
      return Optional.empty();
    }

    final Command newCommand = new Command(
        command.getStatement(),
        Optional.of(command.getOverwriteProperties()),
        Optional.of(command.getOriginalProperties()),
        command.getPlan().map(KsqlPlan::withoutQuery),
        command.getVersion()
    );

    return Optional.of(new QueuedCommand(
        InternalTopicSerdes.serializer().serialize("", cmd.getAndDeserializeCommandId()),
        InternalTopicSerdes.serializer().serialize("", newCommand),
        cmd.getStatus(),
        cmd.getOffset()
    ));
  }
}
