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

package io.confluent.ksql.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.engine.TransientQueryCleanupService;
import io.confluent.ksql.engine.TransientQueryCleanupService.TransientQueryStateCleanupTask;
import io.confluent.ksql.engine.TransientQueryCleanupService.TransientQueryTopicCleanupTask;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientQueryCleanupListener implements QueryEventListener {
  private static final Logger log = LoggerFactory.getLogger(TransientQueryCleanupListener.class);
  private final KsqlConfig ksqlConfig;
  private final TransientQueryCleanupService cleanupService;
  private final ServiceContext serviceContext;
  private final String stateDir;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public TransientQueryCleanupListener(
        final TransientQueryCleanupService cleanupService,
        final ServiceContext serviceContext,
        final KsqlConfig ksqlConfig) {
    this.cleanupService = cleanupService;
    this.serviceContext = serviceContext;
    this.ksqlConfig = ksqlConfig;
    this.stateDir = ksqlConfig.getKsqlStreamConfigProps()
            .getOrDefault(
                    StreamsConfig.STATE_DIR_CONFIG,
                    StreamsConfig.configDef()
                            .defaultValues()
                            .get(StreamsConfig.STATE_DIR_CONFIG))
            .toString();
  }

  @Override
  public void onClose(
          final QueryMetadata query
  ) {
    if (query instanceof TransientQueryMetadata) {
      final String applicationId = query.getQueryApplicationId();
      if (query.hasEverBeenStarted()) {
        log.info("Cleaning up after query {}", applicationId);

        final TransientQueryTopicCleanupTask topicTask = new TransientQueryTopicCleanupTask(
                serviceContext,
                applicationId
        );

        final TransientQueryStateCleanupTask stateTask = new TransientQueryStateCleanupTask(
                applicationId,
                stateDir
        );

        cleanupService.addTopicCleanupTask(topicTask);
        cleanupService.addStateCleanupTask(stateTask);
      } else {
        log.info("Skipping cleanup for query {} since it was never started", applicationId);
      }
    }
  }
}
