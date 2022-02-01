/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.spun.util.io.FileUtils;
import io.confluent.ksql.services.ServiceContext;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientQueryCleanupService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(TransientQueryCleanupService.class);

  private final BlockingQueue<Callable<Boolean>> cleanupTasks;
  private final Retryer<Boolean> retryer;

  public TransientQueryCleanupService() {
    cleanupTasks = new LinkedBlockingDeque<>();

    retryer = RetryerBuilder.<Boolean>newBuilder()
            .retryIfResult(aBoolean -> Objects.equals(aBoolean, false))
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(5))
            .build();
  }

  @Override
  protected void run() {
    LOG.info("Starting cleanup for transient topics: {}; size: {}", cleanupTasks, cleanupTasks.size());
    try {
      while (!cleanupTasks.isEmpty()) {
        final Callable<Boolean> task = cleanupTasks.take();

        try {
          retryer.call(task);
        } catch (RetryException | ExecutionException e) {
          cleanupTasks.add(task);
        }
      }
    } catch (final InterruptedException e) {
      // gracefully exit if this method was interrupted and reset
      // the interrupt flag
      Thread.currentThread().interrupt();
    }
  }

  public void addCleanupTask(final Callable<Boolean> task) {
    LOG.info("Adding cleanup task");
    cleanupTasks.add(task);
    LOG.info("size after adding: {}", cleanupTasks.size());
  }

  public static class TransientQueryTopicCleanupTask implements Callable<Boolean> {

    private final ServiceContext serviceContext;
    private final String appId;

    public TransientQueryTopicCleanupTask(
            final ServiceContext serviceContext,
            final String appId
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.appId = Objects.requireNonNull(appId, "appId");
    }

    @Override
    public Boolean call() {
      try {
        LOG.info("Deleting topics for transient query: {}", appId);
        serviceContext.getTopicClient().deleteInternalTopics(appId);
      } catch (final Exception e) {
        LOG.warn("Failed to cleanup topics for transient query: {}", appId, e);
        return false;
      }

      final Set<String> orphanedTopics = serviceContext
              .getTopicClient()
              .listTopicNames()
              .stream()
              .filter(topicName -> topicName.startsWith(appId))
              .collect(Collectors.toSet());

      if (orphanedTopics.isEmpty()) {
        return true;
      }

      LOG.warn("Failed to cleanup topics for transient query: {}\n"
                      + "The following topics seem to have leaked: {}",
              appId,
              orphanedTopics);

      return true;
    }
  }

  public static class TransientQueryStateCleanupTask implements Callable<Boolean> {
    private final ServiceContext serviceContext;
    private final String appId;
    private final String pathName;

    public TransientQueryStateCleanupTask(
            final ServiceContext serviceContext,
            final String appId,
            final String stateDir
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.appId = Objects.requireNonNull(appId, "appId");
      this.pathName = stateDir + "/" + appId;
    }

    @Override
    public Boolean call() {
      final Path pathName = Paths.get(this.pathName);
      final File directory = new File(String.valueOf(pathName.normalize()));
      try {
        if (directory.exists()) {
          LOG.warn("Deleting local state store for non-existing query {}. "
                          + "This is not expected and was likely due to a "
                          + "race condition when the query was dropped before.",
                  appId);
          FileUtils.deleteDirectory(directory);
        } else {
          return true;
        }
      } catch (Exception e) {
        LOG.error("Error cleaning up state directory {}\n. {}", pathName, e);
        return false;
      }
      if (directory.exists()) {
        LOG.warn("Failed to delete local state store for non-existing query: {}",
                appId);
        return false;
      } else {
        return true;
      }
    }
  }
}
