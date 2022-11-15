/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlEngine;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeakedResourcesMetrics implements Runnable {
  private static final Logger LOGGER
          = LoggerFactory.getLogger(LeakedResourcesMetrics.class);

  private final KsqlEngine engine;
  private final MetricsReporter reporter;
  private final Map<String, String> customTags;
  private final Supplier<Instant> time;

  public LeakedResourcesMetrics(
          final KsqlEngine ksqlEngine,
          final JmxDataPointsReporter jmxDataPointsReporter,
          final Map<String, String> customTags
  ) {
    this(Instant::now, ksqlEngine, jmxDataPointsReporter, customTags);
  }

  LeakedResourcesMetrics(
          final Supplier<Instant> time,
          final KsqlEngine ksqlEngine,
          final JmxDataPointsReporter jmxDataPointsReporter,
          final Map<String, String> customTags) {
    this.time = Objects.requireNonNull(time, "time");
    this.engine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.reporter = Objects.requireNonNull(jmxDataPointsReporter, "jmxDataPointsReporter");
    this.customTags = Objects.requireNonNull(customTags, "customTags");
  }

  @Override
  public void run() {
    final Instant now = time.get();

    try {
      final int numLeakedTopics = engine.reportNumberOfLeakedTopics();
      final int numLeakedStateDirs = engine.reportNumberOfLeakedStateDirs();
      final int numLeakedTopicsAfterCleanup = engine.reportNumLeakedTopicsAfterCleanup();
      final int numLeakedStateDirsAfterCleanup = engine.reportNumLeakedStateDirsAfterCleanup();
      reportLeakedResources(
              now,
              numLeakedTopics,
              numLeakedStateDirs,
              numLeakedTopicsAfterCleanup,
              numLeakedStateDirsAfterCleanup);
    } catch (final RuntimeException e) {
      LOGGER.error("Error collecting leaked resources metrics", e);
      throw e;
    }
  }

  private void reportLeakedResources(
          final Instant now,
          final int numLeakedTopics,
          final int numLeakedStateDirs,
          final int numLeakedTopicsAfterCleanup,
          final int numLeakedStateDirsAfterCleanup) {
    reportNumLeakedTopics(now, numLeakedTopics);
    reportNumLeakedStateDirs(now, numLeakedStateDirs);
    reportNumLeakedTopicsAfterCleanup(now, numLeakedTopicsAfterCleanup);
    reportNumLeakedStateDirsAfterCleanup(now, numLeakedStateDirsAfterCleanup);
  }

  private void reportNumLeakedTopics(final Instant now, final int numLeakedTopics) {
    LOGGER.info("Reporting number of leaked topics: {}", numLeakedTopics);

    reporter.report(
            ImmutableList.of(
                    new MetricsReporter.DataPoint(
                            now,
                            "leaked-topics",
                            numLeakedTopics,
                            customTags
                    )
            )
    );
  }

  private void reportNumLeakedStateDirs(final Instant now, final int numLeakedStateDirs) {
    LOGGER.info("Reporting number of leaked state files: {}", numLeakedStateDirs);

    reporter.report(
            ImmutableList.of(
                    new MetricsReporter.DataPoint(
                            now,
                            "leaked-state-dirs",
                            numLeakedStateDirs,
                            customTags
                    )
            )
    );
  }

  private void reportNumLeakedTopicsAfterCleanup(
          final Instant now,
          final int numLeakedTopicsAfterCleanup) {
    LOGGER.info(
            "Reporting number of leaked topics after cleanup: {}",
            numLeakedTopicsAfterCleanup);

    reporter.report(
            ImmutableList.of(
                    new MetricsReporter.DataPoint(
                            now,
                            "leaked-topics-after-cleanup",
                            numLeakedTopicsAfterCleanup,
                            customTags
                    )
            )
    );
  }

  private void reportNumLeakedStateDirsAfterCleanup(
          final Instant now,
          final int numLeakedStateDirsAfterCleanup) {
    LOGGER.info(
            "Reporting number of leaked state directories after cleanup: {}",
            numLeakedStateDirsAfterCleanup);

    reporter.report(
            ImmutableList.of(
                    new MetricsReporter.DataPoint(
                            now,
                            "leaked-state-dirs-after-cleanup",
                            numLeakedStateDirsAfterCleanup,
                            customTags
                    )
            )
    );
  }
}
