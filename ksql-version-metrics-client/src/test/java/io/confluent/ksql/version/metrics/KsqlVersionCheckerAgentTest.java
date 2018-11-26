/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent.VersionCheckerFactory;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;
import java.time.Clock;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlVersionCheckerAgentTest {

  @Mock
  private Clock clock;
  @Mock
  private KsqlVersionChecker ksqlVersionChecker;
  @Mock
  private Properties properties;
  @Mock
  private Supplier<Boolean> activeQuerySupplier;
  @Mock
  private VersionCheckerFactory versionCheckerFactory;

  private KsqlVersionCheckerAgent ksqlVersionCheckerAgent;

  @Before
  public void setup() {
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any()))
        .thenReturn(ksqlVersionChecker);
    ksqlVersionCheckerAgent = new KsqlVersionCheckerAgent(
        activeQuerySupplier,
        true,
        clock,
        versionCheckerFactory);
  }

  @Test
  public void shouldStartTheAgentCorrectly() throws Exception {

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    final InOrder inOrder = Mockito.inOrder(ksqlVersionChecker);
    inOrder.verify(ksqlVersionChecker).init();
    inOrder.verify(ksqlVersionChecker).setUncaughtExceptionHandler(any());
    inOrder.verify(ksqlVersionChecker).start();
  }

  @Test(expected = Exception.class)
  public void shouldFailIfVersionCheckerFails() throws Exception {
    // Given:
    doThrow(new Exception("FOO")).when(ksqlVersionChecker).start();

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);
  }


  @Test
  public void shouldUpdateLastRequestTimeCorrectly() throws Exception {
    // When:
    ksqlVersionCheckerAgent.updateLastRequestTime();

    // Then:
    verify(clock).millis();
  }

  @Test
  public void shouldCreateKsqlVersionCheckerWithCorrectConfigArg() throws Exception {
    // Given:
    final ArgumentCaptor<BaseSupportConfig> baseSupportConfigArgumentCaptor = ArgumentCaptor
        .forClass(BaseSupportConfig.class);
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any()))
        .thenReturn(ksqlVersionChecker);
    final Properties properties = new Properties();
    properties.put("foo", "bar");

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory)
        .create(baseSupportConfigArgumentCaptor.capture(), any(), anyBoolean(), any());
    assertThat(baseSupportConfigArgumentCaptor.getValue().getProperties().getProperty("foo"),
        equalTo("bar"));
  }

  @Test
  public void shouldCreateKsqlVersionCheckerWithCorrectKsqlModuleType() throws Exception {
    // Given:
    final ArgumentCaptor<KsqlModuleType> ksqlModuleTypeArgumentCaptor = ArgumentCaptor
        .forClass(KsqlModuleType.class);
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any()))
        .thenReturn(ksqlVersionChecker);
    final Properties properties = new Properties();
    properties.put("foo", "bar");

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory)
        .create(any(), ksqlModuleTypeArgumentCaptor.capture(), anyBoolean(), any());
    assertThat(ksqlModuleTypeArgumentCaptor.getValue(), equalTo(KsqlModuleType.SERVER));
  }

  @Test
  public void shouldCreateKsqlVersionCheckerWithCorrectActivenessStatusSupplier() throws Exception {
    // Given:
    final ArgumentCaptor<Supplier> activenessStatusSupplierArgumentCaptor = ArgumentCaptor
        .forClass(Supplier.class);
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any()))
        .thenReturn(ksqlVersionChecker);
    when(activeQuerySupplier.get()).thenReturn(true);

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory)
        .create(any(), any(), anyBoolean(), activenessStatusSupplierArgumentCaptor.capture());
    assertThat(activenessStatusSupplierArgumentCaptor.getValue().get(), equalTo(true));
  }

  @Test
  public void shouldReturnTrueIfThereIsActiveQueriesAndRecentActivity() throws Exception {
    testActivenessStatusSupplier(true,
        System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(1) - 10000), true);
  }

  @Test
  public void shouldReturnTrueIfThereIsActiveQueries() throws Exception {
    testActivenessStatusSupplier(true,
        System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(1) + 10), true);
  }

  @Test
  public void shouldReturnTrueIfNoActivityRecently() throws Exception {
    testActivenessStatusSupplier(false,
        System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(1) - 10000), true);
  }

  @Test
  public void shouldReturnFalseIfNoActivityRecently() throws Exception {
    testActivenessStatusSupplier(false,
        System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(1) + 10), false);
  }

  private void testActivenessStatusSupplier(final boolean activeQuerySupplierValue,
      final long lastUseTimeInterval, final boolean result) {
    // Given:
    final ArgumentCaptor<Supplier> activenessStatusSupplierArgumentCaptor = ArgumentCaptor
        .forClass(Supplier.class);
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any()))
        .thenReturn(ksqlVersionChecker);
    when(activeQuerySupplier.get()).thenReturn(activeQuerySupplierValue);
    when(clock.millis()).thenReturn(lastUseTimeInterval);

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);
    ksqlVersionCheckerAgent.updateLastRequestTime();

    // Then:
    verify(versionCheckerFactory)
        .create(any(), any(), anyBoolean(), activenessStatusSupplierArgumentCaptor.capture());
    assertThat(activenessStatusSupplierArgumentCaptor.getValue().get(), equalTo(result));
  }

}