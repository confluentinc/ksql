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

package io.confluent.ksql.support.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import io.confluent.ksql.support.metrics.collector.KsqlModuleType;

public class KsqlSupportMetricsAgent {

  private static final Logger log = LoggerFactory.getLogger(KsqlSupportMetricsAgent.class);

  public static void initialize(KsqlModuleType moduleType, Properties ksqlProperties){
    KsqlSupportConfig ksqlSupportConfig = new KsqlSupportConfig(ksqlProperties);
    if(ksqlSupportConfig.isProactiveSupportEnabled()) {
      try {
        Runtime serverRuntime = Runtime.getRuntime();

        MetricsReporter metricsReporter =
            new MetricsReporter(ksqlSupportConfig, serverRuntime, moduleType);
        metricsReporter.init();
        Thread metricsThread = newThread("KsqlSupportMetricsAgent", metricsReporter);
        long reportIntervalMs = ksqlSupportConfig.getReportIntervalMs();
        long reportIntervalHours = reportIntervalMs / (60 * 60 * 1000);
        metricsThread.start();
        // We log at WARN level to increase the visibility of this information.
        log.warn(legalDisclaimerProactiveSupportEnabled(reportIntervalHours));

      } catch (Exception e) {
        // We catch any exceptions to prevent collateral damage to the more important broker
        // threads that are running in the same JVM.
        log.error("Failed to start Proactive Support Metrics agent: {}", e.getMessage());
      }
    } else {
      log.warn(legalDisclaimerProactiveSupportDisabled());
    }
  }


  private static Thread newThread(String name, Runnable runnable) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(true);
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        log.error("Uncaught exception in thread '{}':", t.getName(), e);
      }
    });
    return thread;
  }

  private static String legalDisclaimerProactiveSupportEnabled(long reportIntervalHours) {
    return "Please note that the support metrics collection feature (\"Metrics\") of Proactive Support is enabled.  " +
           "With Metrics enabled, this broker is configured to collect and report certain broker and " +
           "cluster metadata (\"Metadata\") about your use of the Confluent Platform (including " +
           "without limitation, your remote internet protocol address) to Confluent, Inc. " +
           "(\"Confluent\") or its parent, subsidiaries, affiliates or service providers every " +
           reportIntervalHours +
           "hours.  This Metadata may be transferred to any country in which Confluent maintains " +
           "facilities.  For a more in depth discussion of how Confluent processes such information, " +
           "please read our Privacy Policy located at http://www.confluent.io/privacy. " +
           "By proceeding with `" + KsqlSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=true`, " +
           "you agree to all such collection, transfer, storage and use of Metadata by Confluent.  " +
           "You can turn the Metrics feature off by setting `" +
           KsqlSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG + "=false` in the broker " +
           "configuration and restarting the broker.  See the Confluent Platform documentation for " +
           "further information.";
  }

  private static String legalDisclaimerProactiveSupportDisabled() {
    return "The support metrics collection feature (\"Metrics\") of Proactive Support is disabled.";
  }
}
