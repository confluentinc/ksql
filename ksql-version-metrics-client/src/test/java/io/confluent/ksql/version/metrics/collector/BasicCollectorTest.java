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

import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.EnumSet;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;

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

    TimeUtils timeUtils = EasyMock.mock(TimeUtils.class);
    EasyMock.expect(timeUtils.nowInUnixTime()).andReturn(System.currentTimeMillis()).anyTimes();
    EasyMock.replay(timeUtils);
    BasicCollector basicCollector = new BasicCollector(moduleType, timeUtils);

    KsqlVersionMetrics expectedMetrics = new KsqlVersionMetrics();
    expectedMetrics.setTimestamp(timeUtils.nowInUnixTime());
    expectedMetrics.setConfluentPlatformVersion(Version.getVersion());
    expectedMetrics.setKsqlComponentType(moduleType.name());

    Assert.assertThat(basicCollector.collectMetrics(), CoreMatchers.equalTo(expectedMetrics));
  }

}
