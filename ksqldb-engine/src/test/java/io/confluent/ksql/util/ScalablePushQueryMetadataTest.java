/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.execution.scalablepush.PushQueryQueuePopulator;
import io.confluent.ksql.execution.scalablepush.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushQueryMetadataTest {

  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private TransientQueryQueue transientQueryQueue;
  @Mock
  private PushQueryQueuePopulator populator;
  @Mock
  private PushConnectionsHandle handle;
  @Captor
  private ArgumentCaptor<Consumer<Throwable>> errorCallbackCaptor;
  @Mock
  private Consumer<Throwable> errorCallback;
  @Mock
  private Optional<ScalablePushQueryMetrics> metrics;

  private ScalablePushQueryMetadata query;

  @Before
  public void setUp()  {
    query = new ScalablePushQueryMetadata(
        logicalSchema,
        new QueryId("queryid"),
        transientQueryQueue,
        metrics,
        ResultType.STREAM,
        populator,
        () -> { },
        null,
        null,
        null
    );
  }

  @Test
  public void shouldStart_exception() {
    // Given:
    when(populator.run()).thenReturn(CompletableFuture.completedFuture(null)
        .thenApply(v -> {
          throw new RuntimeException("Error!");
        }));
    query.onException(errorCallback);

    // When:
    query.start();

    // Then:
    verify(errorCallback).accept(any(RuntimeException.class));
  }

  @Test
  public void shouldStart_handleException() {
    // Given:
    when(populator.run()).thenReturn(CompletableFuture.completedFuture(handle));
    doNothing().when(handle).onException(errorCallbackCaptor.capture());
    query.onException(errorCallback);

    // When:
    query.start();
    errorCallbackCaptor.getValue().accept(new RuntimeException("Error!"));

    // Then:
    verify(errorCallback).accept(any(RuntimeException.class));
  }


  @Test
  public void shouldStop_closeHandler() {
    // Given:
    when(populator.run()).thenReturn(CompletableFuture.completedFuture(handle));
    doNothing().when(handle).onException(errorCallbackCaptor.capture());
    query.onException(errorCallback);

    // When:
    query.start();
    query.close();

    // Then:
    verify(errorCallback, never()).accept(any());
    verify(handle).close();
  }
}
