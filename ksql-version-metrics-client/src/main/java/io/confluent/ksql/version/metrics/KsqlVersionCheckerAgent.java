/*
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
 */

package io.confluent.ksql.version.metrics;

import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.PhoneHomeConfig;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlVersionCheckerAgent implements VersionCheckerAgent {

  // 24 hours
  private static final long MAX_INTERVAL = TimeUnit.DAYS.toMillis(1);

  private KsqlVersionChecker ksqlVersionChecker;

  private boolean enableSettlingTime;

  private long requestTime;
  private final Supplier<Boolean> engineActiveQueryStatusSupplier;

  private static final Logger log = LoggerFactory.getLogger(KsqlVersionCheckerAgent.class);

  public KsqlVersionCheckerAgent(final Supplier<Boolean> engineActiveQueryStatusSupplier) {
    this(engineActiveQueryStatusSupplier, true);
  }

  private KsqlVersionCheckerAgent(
      final Supplier<Boolean> engineActiveQueryStatusSupplier,
      final boolean enableSettlingTime) {
    Objects.requireNonNull(
        engineActiveQueryStatusSupplier,
        " engineActiveQueryStatusSupplier cannot be null.");
    this.enableSettlingTime = enableSettlingTime;
    this.engineActiveQueryStatusSupplier = engineActiveQueryStatusSupplier;
  }

  @Override
  public void start(final KsqlModuleType moduleType, final Properties ksqlProperties) {
    final BaseSupportConfig ksqlVersionCheckerConfig =
        new PhoneHomeConfig(ksqlProperties, "ksql");

    if (!ksqlVersionCheckerConfig.isProactiveSupportEnabled()) {
      log.warn(legalDisclaimerProactiveSupportDisabled());
      return;
    }

    try {
      final Runtime serverRuntime = Runtime.getRuntime();

      ksqlVersionChecker =
          new KsqlVersionChecker(
                  "KsqlVersionCheckerAgent",
                  true,
                  ksqlVersionCheckerConfig,
                  serverRuntime,
                  moduleType,
                  enableSettlingTime,
                  engineActiveQueryStatusSupplier
                  );
      ksqlVersionChecker.init();
      ksqlVersionChecker.setUncaughtExceptionHandler((t, e)
          -> log.error("Uncaught exception in thread '{}':", t.getName(), e));
      ksqlVersionChecker.start();
      final long reportIntervalMs = ksqlVersionCheckerConfig.getReportIntervalMs();
      final long reportIntervalHours = reportIntervalMs / (60 * 60 * 1000);
      // We log at WARN level to increase the visibility of this information.
      log.warn(legalDisclaimerProactiveSupportEnabled(reportIntervalHours));

    } catch (final Exception e) {
      // We catch any exceptions to prevent collateral damage to the more important broker
      // threads that are running in the same JVM.
      log.error("Failed to start KsqlVersionCheckerAgent: {}", e.getMessage());
    }

  }

  private static String legalDisclaimerProactiveSupportEnabled(final long reportIntervalHours) {
    return "Please note that the version check feature of KSQL is enabled.  "
        + "With this enabled, this instance is configured to collect and report "
        + "anonymously the version information to Confluent, Inc. "
        + "(\"Confluent\") or its parent, subsidiaries, affiliates or service providers every "
        + reportIntervalHours
        + "hours.  This Metadata may be transferred to any country in which Confluent maintains "
        + "facilities.  For a more in depth discussion of how Confluent processes "
        + "such information, please read our Privacy Policy located at "
        + "http://www.confluent.io/privacy. "
        + "By proceeding with `"
        + BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=true`, "
        + "you agree to all such collection, transfer and use of Version information "
        + "by Confluent. You can turn the version check  feature off by setting `"
        + BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=false` in the "
        + "KSQL configuration and restarting the KSQL.  See the Confluent Platform "
        + "documentation for further information.";
  }

  private static String legalDisclaimerProactiveSupportDisabled() {
    return "The version check feature of KSQL  is disabled.";
  }

  @Override
  public void updateLastRequestTime() {
    this.requestTime = System.currentTimeMillis();
  }

  @Override
  public Boolean get() {
    return (System.currentTimeMillis() - this.requestTime) < MAX_INTERVAL
        || engineActiveQueryStatusSupplier.get();
  }
}
