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

package io.confluent.ksql.version.metrics.collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.EnumSet;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Version;

@RunWith(Parameterized.class)
public class BasicCollectorTest {

  @Parameterized.Parameters
  public static Collection<KsqlModuleType> data() {
    return EnumSet.allOf(KsqlModuleType.class);
  }

  @Parameterized.Parameter
  public KsqlModuleType moduleType;

  @Test
  public void testGetCollector(){
    BasicCollector basicCollector = new BasicCollector(moduleType);
    KsqlVersionMetrics metricsRecord = (KsqlVersionMetrics) basicCollector.collectMetrics();
    Assert.assertNotNull("Metrics record shouldn't be null ", metricsRecord);
    Assert.assertNotNull("Timestamp must not be null " , metricsRecord.getTimestamp());
    Assert.assertNotNull("Version must not be null",metricsRecord.getConfluentPlatformVersion());
    Assert.assertNotNull("ComponentType must not be null",metricsRecord.getKsqlComponentType());
    Assert.assertEquals(Version.getVersion(),metricsRecord.getConfluentPlatformVersion() );
    Assert.assertEquals(moduleType.name(),metricsRecord.getKsqlComponentType() );
  }

}
