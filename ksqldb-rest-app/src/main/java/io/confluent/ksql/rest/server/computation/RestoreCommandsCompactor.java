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
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Util for compacting the restore commands
 */
public final class RestoreCommandsCompactor {

  private RestoreCommandsCompactor() {
  }

  /**
   * Compact the list of commands to restore. A command should be compacted if it
   * either (1) has been terminated or (2) has a later command with the same {@code QueryId}
   * (which may happen if a {@code CREATE OR REPLACE} is issued).
   *
   * <p>This compaction stops unnecessary creation of Streams topologies on a server restart.
   * Building such topologies is relatively slow and best avoided.
   *
   * @param restoreCommands the list of commands to compact.
   * @return the compacted list of commands.
   */
  static List<QueuedCommand> compact(final List<QueuedCommand> restoreCommands) {
    final Map<QueryId, CompactedNode> latestNodeWithId = new HashMap<>();
    final Map<SourceName, QueryId> latestCreateAsWithId = new HashMap<>();
    CompactedNode current = null;

    for (final QueuedCommand cmd : restoreCommands) {
      // Whenever a new command is processed, we check if a previous command with
      // the same queryID exists - in which case, we mark that command as "shouldSkip"
      // and it will not be included in the output
      current = CompactedNode.maybeAppend(current, cmd, latestNodeWithId, latestCreateAsWithId);
    }

    final List<QueuedCommand> compacted = new LinkedList<>();
    while (current != null) {
      // traverse backwards and add each next node to the start of the list
      compact(current).ifPresent(cmd -> compacted.add(0, cmd));
      current = current.prev;
    }

    return compacted;
  }

  private static final class CompactedNode {

    final CompactedNode prev;
    final QueuedCommand queued;
    final Command command;

    boolean shouldSkip = false;

    public static CompactedNode maybeAppend(
        final CompactedNode prev,
        final QueuedCommand queued,
        final Map<QueryId, CompactedNode> latestNodeWithId,
        final Map<SourceName, QueryId> latestCreateAsWithId
    ) {
      final Command command = queued.getAndDeserializeCommand(
          InternalTopicSerdes.deserializer(Command.class)
      );

      final Optional<KsqlPlan> plan = command.getPlan();
      final Optional<DdlCommand> ddlCommand = plan.flatMap(p -> p.getDdlCommand());

      if (queued.getAndDeserializeCommandId().getType() == Type.TERMINATE) {
        final QueryId queryId = new QueryId(queued.getAndDeserializeCommandId().getEntity());
        if (queryId.toString().equals(TerminateQuery.ALL_QUERIES)) {
          latestNodeWithId.values().forEach(node -> node.shouldSkip = true);
        } else {
          markShouldSkip(queryId, latestNodeWithId);
        }

        // terminate commands don't get added to the list of commands to execute
        // because we "execute" them in this class by removing query plans from
        // terminated queries
        return prev;
      } else if (!plan.isPresent() || !plan.get().getQueryPlan().isPresent()) {
        // drop sources may have a query linked by create_as commands
        // we mark this query as "shouldSkip" now that drop commands terminate the query too
        ddlCommand.ifPresent(ddl ->
            getDropSourceName(ddl).ifPresent(sourceName -> {
              final QueryId queryId = latestCreateAsWithId.get(sourceName);
              if (queryId != null) {
                markShouldSkip(queryId, latestNodeWithId);
              }
            }));

        // DDL
        return new CompactedNode(prev, queued, command);
      }

      final CompactedNode node = new CompactedNode(prev, queued, command);

      final QueryId queryId = plan.get().getQueryPlan().get().getQueryId();
      markShouldSkip(queryId, latestNodeWithId);
      latestNodeWithId.put(queryId, node);

      // keep track of the latest query ID for the new CREATE_AS source
      ddlCommand.ifPresent(ddl ->
          getCreateSourceName(ddl).ifPresent(sourceName ->
              latestCreateAsWithId.put(sourceName, queryId)));

      return node;
    }

    private static Optional<SourceName> getCreateSourceName(final DdlCommand ddlCommand) {
      if (ddlCommand instanceof CreateSourceCommand) {
        return Optional.of(((CreateSourceCommand)ddlCommand).getSourceName());
      }

      return Optional.empty();
    }

    private static Optional<SourceName> getDropSourceName(final DdlCommand ddlCommand) {
      if (ddlCommand instanceof DropSourceCommand) {
        return Optional.of(((DropSourceCommand)ddlCommand).getSourceName());
      }

      return Optional.empty();
    }

    private static void markShouldSkip(
        final QueryId queryId,
        final Map<QueryId, CompactedNode> latestNodeWithId
    ) {
      final CompactedNode prevWithID = latestNodeWithId.get(queryId);
      if (prevWithID != null) {
        prevWithID.shouldSkip = true;
      }
    }

    private CompactedNode(
        @Nullable final CompactedNode prev,
        final QueuedCommand queued,
        final Command command
    ) {
      this.prev = prev;
      this.queued = queued;
      this.command = command;
    }
  }

  private static Optional<QueuedCommand> compact(final CompactedNode node) {
    final Command command = node.command;
    if (!node.shouldSkip) {
      return Optional.of(node.queued);
    }

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
        InternalTopicSerdes.serializer().serialize("", node.queued.getAndDeserializeCommandId()),
        InternalTopicSerdes.serializer().serialize("", newCommand),
        node.queued.getStatus(),
        node.queued.getOffset()
    ));
  }
}
