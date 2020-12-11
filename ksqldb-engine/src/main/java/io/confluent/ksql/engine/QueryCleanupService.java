/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.services.ServiceContext;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code QueryCleanupService} helps cleanup external resources from queries
 * out of the main line of query execution. This ensures that tasks that might
 * take a long time don't happen on the CLI feedback path (such as cleaning up
 * consumer groups).
 *
 * <p>NOTE: this cleanup service is intended to be used across threads and across
 * real/sandboxed engines.</p>
 */
@SuppressWarnings("UnstableApiUsage")
class QueryCleanupService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(QueryCleanupService.class);
  private static final Runnable SHUTDOWN_SENTINEL = () -> { };

  private final BlockingQueue<Runnable> cleanupTasks;

  QueryCleanupService() {
    cleanupTasks = new LinkedBlockingDeque<>();
  }

  @Override
  protected void run() {
    try {
      while (true) {
        final Runnable task = cleanupTasks.take();
        if (task == SHUTDOWN_SENTINEL) {
          return;
        }

        task.run();
      }
    } catch (final InterruptedException e) {
      // gracefully exit if this method was interrupted and reset
      // the interrupt flag
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void triggerShutdown() {
    cleanupTasks.add(SHUTDOWN_SENTINEL);
  }

  public Set<String> pendingApplicationIds() {
    return cleanupTasks.stream()
        .filter(QueryCleanupTask.class::isInstance)
        .map(QueryCleanupTask.class::cast)
        .map(t -> t.appId).collect(ImmutableSet.toImmutableSet());
  }

  public boolean isEmpty() {
    return cleanupTasks.isEmpty();
  }

  public void addCleanupTask(final QueryCleanupTask task) {
    cleanupTasks.add(task);
  }

  static class QueryCleanupTask implements Runnable {
    private final String appId;
    private final boolean isTransient;
    private final ServiceContext serviceContext;

    QueryCleanupTask(
        final ServiceContext serviceContext,
        final String appId,
        final boolean isTransient
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.appId = Objects.requireNonNull(appId, "appId");
      this.isTransient = isTransient;
    }

    public String getAppId() {
      return appId;
    }

    @Override
    public void run() {
      tryRun(
          () -> SchemaRegistryUtil.cleanupInternalTopicSchemas(
              appId,
              serviceContext.getSchemaRegistryClient(),
              isTransient),
          "internal topic schemas"
      );

      tryRun(() -> serviceContext.getTopicClient().deleteInternalTopics(appId), "internal topics");
      tryRun(
          () -> serviceContext
              .getConsumerGroupClient()
              .deleteConsumerGroups(ImmutableSet.of(appId)),
          "internal consumer groups");
    }

    private void tryRun(final Runnable runnable, final String resource) {
      try {
        runnable.run();
      } catch (final Exception e) {
        LOG.warn("Failed to cleanup {} for {}", resource, appId, e);
      }
    }
  }

}
