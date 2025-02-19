/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.version.metrics;

import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.PhoneHomeConfig;
import java.time.Clock;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KsqlVersionCheckerAgent implements VersionCheckerAgent {

  private static final long MAX_INTERVAL = TimeUnit.DAYS.toMillis(1);


  private final boolean enableSettlingTime;

  private final Clock clock;
  private final VersionCheckerFactory versionCheckerFactory;

  private volatile long requestTime;
  private final Supplier<Boolean> activeQuerySupplier;

  private static final Logger log = LogManager.getLogger(KsqlVersionCheckerAgent.class);

  /**
   *
   * @param activeQuerySupplier supplier that indicates if there are any active persistent queries
   */
  public KsqlVersionCheckerAgent(final Supplier<Boolean> activeQuerySupplier) {
    this(activeQuerySupplier, true, Clock.systemDefaultZone(), KsqlVersionChecker::new);
  }

  KsqlVersionCheckerAgent(
      final Supplier<Boolean> activeQuerySupplier,
      final boolean enableSettlingTime,
      final Clock clock,
      final VersionCheckerFactory versionCheckerFactory) {
    this.enableSettlingTime = enableSettlingTime;
    this.activeQuerySupplier = Objects.requireNonNull(activeQuerySupplier, "activeQuerySupplier");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.versionCheckerFactory =
        Objects.requireNonNull(versionCheckerFactory, "versionCheckerFactory");
  }

  @Override
  public void start(
      final KsqlModuleType moduleType,
      final Properties ksqlProperties) {
    final BaseSupportConfig ksqlVersionCheckerConfig =
        new PhoneHomeConfig(ksqlProperties, "ksql");

    if (!ksqlVersionCheckerConfig.isProactiveSupportEnabled()) {
      log.warn(legalDisclaimerProactiveSupportDisabled());
      return;
    }
    try {
      final KsqlVersionChecker ksqlVersionChecker = versionCheckerFactory.create(
          ksqlVersionCheckerConfig,
          moduleType,
          enableSettlingTime,
          this::isActive
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

  @FunctionalInterface
  interface VersionCheckerFactory {
    KsqlVersionChecker create(
        BaseSupportConfig ksqlVersionCheckerConfig,
        KsqlModuleType moduleType,
        boolean enableSettingTime,
        Supplier<Boolean> activenessStatusSupplier
    );
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
    this.requestTime = clock.millis();
  }

  private boolean hasRecentRequests() {
    return (clock.millis() - this.requestTime) < MAX_INTERVAL;
  }

  private boolean isActive() {
    return hasRecentRequests() || activeQuerySupplier.get();
  }
}
