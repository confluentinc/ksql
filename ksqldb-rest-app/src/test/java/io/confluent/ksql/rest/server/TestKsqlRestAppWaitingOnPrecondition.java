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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.server.services.TestRestServiceContextFactory.InternalSimpleKsqlClientFactory;
import io.confluent.ksql.services.ServiceContext;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * A {@link TestKsqlRestApp} for testing behavior of a server stuck waiting on a precondition.
 *
 * The server is not started automatically. Rather, {@code startAndWaitForPrecondition()} should
 * be called by the test suite. A {@code CountDownLatch} provided in the constructor counts down
 * when a precondition check is called, in order to finish configuring the app once the server
 * is waiting for preconditions.
 */
public class TestKsqlRestAppWaitingOnPrecondition extends TestKsqlRestApp {

  private CountDownLatch latch;

  TestKsqlRestAppWaitingOnPrecondition(
      final Supplier<String> bootstrapServers,
      final Map<String, Object> additionalProps,
      final Supplier<ServiceContext> serviceContext,
      final Optional<BasicCredentials> credentials,
      final CountDownLatch latch,
      final InternalSimpleKsqlClientFactory internalSimpleKsqlClientFactory
  ) {
    super(bootstrapServers, additionalProps, serviceContext, credentials,
        internalSimpleKsqlClientFactory);
    this.latch = latch;
  }

  @Override
  protected void before() {
    initialize();
  }

  public void startAndWaitForPrecondition() {
    try {
      new Thread(() -> {
        try {
          ksqlRestApplication.startAsync();
        } catch (Exception e) {
          throw new RuntimeException("Error starting server", e);
        }
      }).start();
      latch.await();
    } catch (final Exception var2) {
      throw new RuntimeException("Failed to start Ksql rest server", var2);
    }

    listeners.addAll(ksqlRestApplication.getListeners());
    ksqlEngine = ksqlRestApplication.getEngine();
  }
}
