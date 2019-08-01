/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlConnectTest {

  @Mock
  private ConnectPollingService pollingService;
  @Mock
  private ConnectConfigService configService;

  private KsqlConnect ksqlConnect;

  @Before
  public void setUp() {
    ksqlConnect = new KsqlConnect(pollingService, configService);

    when(pollingService.startAsync()).thenReturn(pollingService);
    when(pollingService.stopAsync()).thenReturn(pollingService);

    when(configService.startAsync()).thenReturn(configService);
    when(configService.stopAsync()).thenReturn(configService);
  }

  @Test
  public void shouldStartBothSubServicesAsynchronously() {
    // When:
    ksqlConnect.startAsync();

    // Then:
    verify(pollingService).startAsync();
    verify(configService).startAsync();

    verifyNoMoreInteractions(pollingService, configService);
  }

  @Test
  public void shouldStopBothSubServicesSynchronously() {
    // When:
    ksqlConnect.close();

    // Then:
    verify(configService).stopAsync();
    verify(pollingService).stopAsync();
    verify(configService).awaitTerminated();
    verify(pollingService).awaitTerminated();
  }

}