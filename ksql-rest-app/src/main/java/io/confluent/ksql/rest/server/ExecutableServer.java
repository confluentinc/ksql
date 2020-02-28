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

package io.confluent.ksql.rest.server;

import io.confluent.rest.ApplicationServer;
import io.confluent.rest.RestConfig;
import java.util.List;

/**
 * An {@code Executable} that wraps a {@link ApplicationServer} and delegates
 * the lifecycle methods.
 *
 * @param <T> the type of the rest server config
 */
public class ExecutableServer<T extends RestConfig> implements Executable {

  private final ApplicationServer<T> server;
  private final List<ExecutableApplication<T>> apps;

  public ExecutableServer(
      final ApplicationServer<T> server,
      final List<ExecutableApplication<T>> apps
  ) {
    this.server = server;
    this.apps = apps;
  }

  @Override
  public void startAsync() throws Exception {
    apps.forEach(server::registerApplication);
    server.start();

    for (final ExecutableApplication<T> app : apps) {
      app.startAsync();
    }
  }

  @Override
  public void triggerShutdown() throws Exception {
    for (final ExecutableApplication<T> app : apps) {
      app.triggerShutdown();
    }

    server.stop();
  }

  @Override
  public void awaitTerminated() throws InterruptedException {
    for (final ExecutableApplication<T> app : apps) {
      app.awaitTerminated();
    }

    server.join();
  }
}
