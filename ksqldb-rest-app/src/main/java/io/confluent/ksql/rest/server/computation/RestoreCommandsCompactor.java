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
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util for compacting the restore commands
 */
public final class RestoreCommandsCompactor {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreCommandsCompactor.class);
  private static final Serializer<Object> serializer = InternalTopicSerdes.serializer();

  private final Map<QueryId, CompactedNode> latestNodeWithId = new HashMap<>();
  private final Map<SourceName, QueryId> latestCreateAsWithId = new HashMap<>();

  // This is a temporary list that helps to detect invalid CREATE_AS statements with
  // IF NOT EXISTS in the restore process.
  // Know bug https://github.com/confluentinc/ksql/issues/8173
  private final Set<SourceName> createAsIfNotExistsBugDetection = new HashSet<>();

  private CompactedNode current = null;

  /**
   * Compact the list of commands to restore. A command should be compacted if it
   * either (1) has been terminated or (2) has a later command with the same {@code QueryId}
   * (which may happen if a {@code CREATE OR REPLACE} is issued).
   *
   * <p>This compaction stops unnecessary creation of Streams topologies on a server restart.
   * Building such topologies is relatively slow and best avoided.
   *
   * @param restoreCommand the command to compact.
   */
  public void apply(final QueuedCommand restoreCommand) {
    // Whenever a new command is processed, we check if a previous command with
    // the same queryID exists - in which case, we mark that command as "shouldSkip"
    // and it will not be included in the output
    current = CompactedNode.maybeAppend(current, restoreCommand,
        latestNodeWithId, latestCreateAsWithId, createAsIfNotExistsBugDetection);
  }

  public List<QueuedCommand> getList() {
    final List<QueuedCommand> compacted = new LinkedList<>();
    while (current != null) {
      // traverse backwards and add each next node to the start of the list
      getList(current).ifPresent(cmd -> compacted.add(0, cmd));
      current = current.prev;
    }
    return compacted;
  }

  public void compact() {
    while (current != null && current.shouldSkip) {
      current = current.prev;
    }
    if (current == null) {
      return;
    }

    CompactedNode next = current;
    CompactedNode temp = next.prev;
    while (temp != null) {
      // traverse backwards and add each next node to the start of the list
      if (temp.shouldSkip) {
        next.prev = temp.prev;
      } else {
        next = temp;
      }
      temp = temp.prev;
    }
  }

  private static final class CompactedNode {

    CompactedNode prev;
    final QueuedCommand queued;
    final Command command;

    boolean shouldSkip = false;

    public static CompactedNode maybeAppend(
        final CompactedNode prev,
        final QueuedCommand queued,
        final Map<QueryId, CompactedNode> latestNodeWithId,
        final Map<SourceName, QueryId> latestCreateAsWithId,
        final Set<SourceName> createAsIfNotExistsBugDetection
    ) {
      final Command command = queued.getAndDeserializeCommand(
          InternalTopicSerdes.deserializer(Command.class)
      );

      final Optional<KsqlPlan> plan = command.getPlan();
      final Optional<DdlCommand> ddlCommand = plan.flatMap(KsqlPlan::getDdlCommand);

      CommandId commandId = queued.getAndDeserializeCommandId();
      if (commandId.getType() == Type.TERMINATE) {
        final QueryId queryId = new QueryId(commandId.getEntity());
        if (TerminateQuery.ALL_QUERIES.equals(queryId.toString())) {
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

                // If a DROP statement was found, then we can safely ignore the previous
                // CREATE_AS with IF NOT EXISTS statement (if exists in the set).
                createAsIfNotExistsBugDetection.remove(sourceName);
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
          getCreateSourceName(ddl).ifPresent(sourceName -> {
            // Only CREATE statements are executed at this point
            final CreateSourceCommand createCommand = (CreateSourceCommand) ddl;
            if (!createCommand.isOrReplace() && isCreateIfNotExists(command)) {
              // This condition is hit only for create statements with queries. If the CREATE_AS
              // does not have OR REPLACE clause, but has an IF NOT EXISTS clause, then we are
              // hitting a known bug that wrote IF NOT EXISTS statements to the command topic.
              // See https://github.com/confluentinc/ksql/issues/8173
              if (createAsIfNotExistsBugDetection.contains(sourceName)) {
                QueryLogger.warn(
                    "A known bug is found while restoring the command topic. The restoring "
                    + "process will continue, but the query of the affected stream or table won't "
                    + "be executed until https://github.com/confluentinc/ksql/issues/8173 "
                    + "is fixed.", command.getStatement());
              }
            }

            createAsIfNotExistsBugDetection.add(sourceName);
            latestCreateAsWithId.put(sourceName, queryId);
          }));

      return node;
    }

    private static boolean isCreateIfNotExists(final Command command) {
      final String statement = command.getStatement().toUpperCase();
      return statement.startsWith("CREATE STREAM IF NOT EXISTS")
          || statement.startsWith("CREATE TABLE IF NOT EXISTS");
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

  private static Optional<QueuedCommand> getList(final CompactedNode node) {
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
        serializer.serialize("", node.queued.getAndDeserializeCommandId()),
        serializer.serialize("", newCommand),
        node.queued.getStatus(),
        node.queued.getOffset()
    ));
  }
}
