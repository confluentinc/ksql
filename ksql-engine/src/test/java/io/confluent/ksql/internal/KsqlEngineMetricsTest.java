/**
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
 **/
package io.confluent.ksql.internal;


import org.apache.kafka.common.metrics.Metrics;
import org.easymock.EasyMock;
import org.junit.Test;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metrics.MetricCollectors;

import static org.junit.Assert.assertTrue;

public class KsqlEngineMetricsTest {

  @Test
  public void shouldRemoveAllSensorsOnClose() {
    KsqlEngine ksqlEngine = EasyMock.niceMock(KsqlEngine.class);
    KsqlEngineMetrics engineMetrics = new KsqlEngineMetrics("testGroup", ksqlEngine);
    assertTrue(engineMetrics.registeredSensors().size() > 0);

    engineMetrics.close();

    Metrics metrics = MetricCollectors.getMetrics();
    engineMetrics.registeredSensors().forEach(sensor -> {
      assertTrue(metrics.getSensor(sensor.name()) == null);
    });

  }
}
