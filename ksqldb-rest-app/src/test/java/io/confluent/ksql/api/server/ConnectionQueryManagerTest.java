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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionQueryManagerTest {

  @Mock
  private Context context;
  @Mock
  private Server server;
  @Mock
  private HttpServerRequest request;
  @Mock
  private HttpConnection connection;
  @Mock
  private QueryPublisher publisher1;
  @Mock
  private QueryPublisher publisher2;
  @Mock
  private QueryPublisher publisher3;

  @Before
  public void setUp() {
    when(request.connection()).thenReturn(connection);
    when(publisher1.queryId()).thenReturn(new QueryId("q1"));
    when(publisher2.queryId()).thenReturn(new QueryId("q2"));
    when(publisher3.queryId()).thenReturn(new QueryId("q3"));
  }

  @Test
  public void shouldCloseAllQueriesOnConnectionCloseWithoutCME() {
    // Given: a connection with three registered push queries, where each
    // PushQueryHolder.close() invokes the closeHandler that removes the query
    // from the same underlying set. Iterating the live set would throw CME on
    // the second query and leak the rest.
    try (MockedStatic<VertxUtils> mocked = mockStatic(VertxUtils.class)) {
      mocked.when(() -> VertxUtils.checkContext(any())).then(inv -> null);

      final ConnectionQueryManager manager = new ConnectionQueryManager(context, server);
      manager.createApiQuery(publisher1, request);
      manager.createApiQuery(publisher2, request);
      manager.createApiQuery(publisher3, request);

      final ArgumentCaptor<Handler<Void>> closeHandler =
          ArgumentCaptor.forClass(Handler.class);
      verify(connection).closeHandler(closeHandler.capture());

      // When: the Vert.x connection-close handler fires.
      closeHandler.getValue().handle(null);

      // Then: all three publishers are closed (no CME thrown on the second).
      verify(publisher1).close();
      verify(publisher2).close();
      verify(publisher3).close();
      verify(server).removeQueryConnection(connection);
    }
  }
}
