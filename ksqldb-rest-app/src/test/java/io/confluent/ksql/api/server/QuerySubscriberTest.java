/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_TOO_MANY_REQUESTS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.util.KsqlRateLimitException;
import io.vertx.core.Context;
import io.vertx.core.http.HttpServerResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QuerySubscriberTest {

  @Mock
  private Context context;
  @Mock
  private HttpServerResponse response;
  @Mock
  private QueryStreamResponseWriter writer;

  private AtomicBoolean serverCompleted;
  private QuerySubscriber subscriber;

  @Before
  public void setUp() {
    serverCompleted = new AtomicBoolean(false);
    subscriber = new QuerySubscriber(context, response, writer, () -> false, serverCompleted);
  }

  @Test
  public void shouldMarkServerCompletedOnSuccess() {
    when(writer.writeCompletionMessage()).thenReturn(writer);

    subscriber.handleComplete();

    assertThat(serverCompleted.get(), is(true));
  }

  @Test
  public void shouldMarkServerCompletedOnError() {
    when(writer.writeError(org.mockito.ArgumentMatchers.any())).thenReturn(writer);

    subscriber.handleError(new RuntimeException("boom"));

    // Without this the close handler cannot tell a failed query from a client that
    // walked away, and the failure is counted as a client disconnect.
    assertThat(serverCompleted.get(), is(true));
  }

  @Test
  public void shouldCodeRateLimitErrorAsRetriable() {
    final ArgumentCaptor<KsqlErrorMessage> captor =
        ArgumentCaptor.forClass(KsqlErrorMessage.class);
    when(writer.writeError(captor.capture())).thenReturn(writer);

    // A router-queue rejection reaches the client in-stream (headers already sent), so it
    // cannot be a 429 status; the frame must still be coded retriable so a client can tell.
    subscriber.handleError(new KsqlRateLimitException("the router queue is full"));

    assertThat(captor.getValue().getErrorCode(), is(ERROR_CODE_TOO_MANY_REQUESTS));
  }

  @Test
  public void shouldCodeOtherErrorsAsServerError() {
    final ArgumentCaptor<KsqlErrorMessage> captor =
        ArgumentCaptor.forClass(KsqlErrorMessage.class);
    when(writer.writeError(captor.capture())).thenReturn(writer);

    subscriber.handleError(new RuntimeException("boom"));

    assertThat(captor.getValue().getErrorCode(), is(ERROR_CODE_SERVER_ERROR));
  }
}
