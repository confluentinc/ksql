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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import java.time.Clock;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlVersionCheckerAgentTest {

  @Mock
  private Clock clock;
  @Mock
  private KsqlVersionChecker ksqlVersionChecker;
  @Mock
  private Properties properties;

  private KsqlVersionCheckerAgent ksqlVersionCheckerAgent;

  @Before
  public void setup() {
    ksqlVersionCheckerAgent = new KsqlVersionCheckerAgent(() -> true, true, clock);
  }

  @Test
  public void shouldStartTheAgentCorrectly() throws Exception {
    // When:
    ksqlVersionCheckerAgent.start(ksqlVersionChecker, KsqlModuleType.SERVER, properties);

    // Then:
    verify(ksqlVersionChecker).init();
    verify(ksqlVersionChecker).setUncaughtExceptionHandler(any());
    verify(ksqlVersionChecker).start();
  }

  @Test (expected = Exception.class)
  public void shouldSFailIfVersionCheckerFails() throws Exception {
    // Given:
    doThrow(new Exception("FOO")).when(ksqlVersionChecker).start();

    // When:
    ksqlVersionCheckerAgent.start(ksqlVersionChecker, KsqlModuleType.SERVER, properties);
  }


  @Test
  public void shouldUpdateLastRequestTimeCorrectly() throws Exception {
    // When:
    ksqlVersionCheckerAgent.updateLastRequestTime();

    // Then:
    verify(clock).millis();
  }

}