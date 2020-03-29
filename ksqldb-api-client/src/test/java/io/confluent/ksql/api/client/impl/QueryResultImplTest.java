/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import static org.junit.Assert.assertThrows;

import io.confluent.ksql.api.client.Row;
import io.vertx.core.Context;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;

@RunWith(MockitoJUnitRunner.class)
public class QueryResultImplTest {

  @Mock
  private Context context;
  @Mock
  private Subscriber<Row> subscriber;

  private QueryResultImpl queryResult;

  @Before
  public void setUp() {
    queryResult = new QueryResultImpl(context, "queryId", Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void shouldNotSubscribeIfPolling() {
    // Given
    queryResult.poll(1, TimeUnit.NANOSECONDS);

    // When / Then
    assertThrows(IllegalStateException.class, () -> queryResult.subscribe(subscriber));
  }

  @Test
  public void shouldNotPollIfSubscribed() {
    // Given
    queryResult.subscribe(subscriber);

    // When / Then
    assertThrows(IllegalStateException.class, () -> queryResult.poll());
  }
}