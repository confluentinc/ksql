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

package io.confluent.ksql.logging.processing;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import org.apache.kafka.common.metrics.Sensor;

public class MeteredProcessingLogger implements ProcessingLogger {
  private final ProcessingLogger logger;
  private final Sensor errorSensor;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public MeteredProcessingLogger(
      final ProcessingLogger logger,
      final Sensor errorSensor
  ) {
    if (errorSensor == null) {
      throw new RuntimeException("Expected sensor to be passed into metered processing logger");
    }
    this.logger = Objects.requireNonNull(logger, "logger");
    this.errorSensor = errorSensor;
  }

  @Override
  public void error(final ErrorMessage msg) {
    errorSensor.record();
    logger.error(msg);
  }
}
