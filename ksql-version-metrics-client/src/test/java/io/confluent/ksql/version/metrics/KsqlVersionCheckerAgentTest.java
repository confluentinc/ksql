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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
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
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlVersionCheckerAgentTest {

  private static final long NOW = System.currentTimeMillis();
  private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);

  @Mock
  private Clock clock;
  @Mock
  private KsqlVersionChecker ksqlVersionChecker;

  private Properties properties;
  @Mock
  private Supplier<Boolean> activeQuerySupplier;
  @Mock
  private VersionCheckerFactory versionCheckerFactory;
  @Captor
  private ArgumentCaptor<BaseSupportConfig> configCaptor;
  @Captor
  private ArgumentCaptor<Supplier<Boolean>> activenessCaptor;

  private KsqlVersionCheckerAgent ksqlVersionCheckerAgent;

  @Before
  public void setup() {
    properties = new Properties();
    properties.put("foo", "bar");
    when(versionCheckerFactory.create(any(), any(), anyBoolean(), any())).thenReturn(ksqlVersionChecker);
    ksqlVersionCheckerAgent = new KsqlVersionCheckerAgent(
        activeQuerySupplier,
        true,
        clock,
        versionCheckerFactory);
  }

  @Test
  public void shouldStartTheAgentCorrectly() {

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    final InOrder inOrder = Mockito.inOrder(ksqlVersionChecker);
    inOrder.verify(ksqlVersionChecker).init();
    inOrder.verify(ksqlVersionChecker).setUncaughtExceptionHandler(any());
    inOrder.verify(ksqlVersionChecker).start();
  }

  @Test (expected = Exception.class)
  public void shouldFailIfVersionCheckerFails() {
    // Given:
    doThrow(new Exception("FOO")).when(ksqlVersionChecker).start();

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);
  }

  @Test
  public void shouldCreateKsqlVersionCheckerWithCorrectConfigArg() {
    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory).create(configCaptor.capture(), any(), anyBoolean(), any());
    assertThat(configCaptor.getValue().getProperties().getProperty("foo"), equalTo("bar"));
  }

  @Test
  public void shouldCreateKsqlVersionCheckerWithCorrectKsqlModuleType() {

    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory).create(any(), eq(KsqlModuleType.SERVER), anyBoolean(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCreateKsqlVersionCheckerWithCorrectActivenessStatusSupplier() {
    // When:
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);

    // Then:
    verify(versionCheckerFactory).create(any(), any(), anyBoolean(), activenessCaptor.capture());
    assertThat(activenessCaptor.getValue().get(), equalTo(true));
  }

  @Test
  public void shouldReportActiveIfThereAreActiveQueries() {
    // Given:
    final Supplier<Boolean> activenessCheck = getActivenessCheck();

    when(activeQuerySupplier.get()).thenReturn(true);

    // Then:
    assertThat(activenessCheck.get(), is(true));
  }

  @Test
  public void shouldReportActiveIfThereAreRecentRequests() {
    // Given:
    final Supplier<Boolean> activenessCheck = getActivenessCheck();

    givenLastRequest(NOW - ONE_DAY + 1); // <-- just recent enough to count.

    // Then:
    assertThat(activenessCheck.get(), is(true));
  }

  @Test
  public void shouldReportInactiveIfThereAreNoActiveQueriesAndNoRequests() {
    // Given:
    final Supplier<Boolean> activenessCheck = getActivenessCheck();

    when(activeQuerySupplier.get()).thenReturn(false);

    // Then:
    assertThat(activenessCheck.get(), is(false));
  }

  @Test
  public void shouldReportInActiveIfThereAreOnlyOldRequestsAndNoActiveQueries() {
    // Given:
    final Supplier<Boolean> activenessCheck = getActivenessCheck();

    when(activeQuerySupplier.get()).thenReturn(false);

    givenLastRequest(NOW - ONE_DAY - 1); // <-- just old enough to not count.

    // Then:
    assertThat(activenessCheck.get(), is(false));
  }

  private Supplier<Boolean> getActivenessCheck() {
    when(clock.millis()).thenReturn(NOW);
    ksqlVersionCheckerAgent.start(KsqlModuleType.SERVER, properties);
    verify(versionCheckerFactory).create(any(), any(), anyBoolean(), activenessCaptor.capture());
    return activenessCaptor.getValue();
  }

  private void givenLastRequest(final long at) {
    when(clock.millis()).thenReturn(at);
    ksqlVersionCheckerAgent.updateLastRequestTime();
    when(clock.millis()).thenReturn(NOW);
  }

}