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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.util.KsqlConfig;
import java.io.Closeable;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A simple wrapper around {@link ConnectPollingService} and {@link ConnectConfigService}
 * to make lifecycle management a little easier.
 */
public class KsqlConnect implements Closeable {

  private final ConnectPollingService connectPollingService;
  private final ConnectConfigService configService;
  private final boolean enabled;

  public KsqlConnect(
      final KsqlExecutionContext executionContext,
      final KsqlConfig ksqlConfig,
      final Consumer<CreateSource> sourceCallback
  ) {
    connectPollingService = new ConnectPollingService(executionContext, sourceCallback);
    configService = new ConnectConfigService(ksqlConfig, connectPollingService);
    enabled = !ksqlConfig.getBoolean(KsqlConfig.CONNECT_POLLING_DISABLE_PROPERTY);
  }

  @VisibleForTesting
  KsqlConnect(
      final ConnectPollingService connectPollingService,
      final ConnectConfigService connectConfigService
  ) {
    this.connectPollingService = Objects
        .requireNonNull(connectPollingService, "connectPollingService");
    this.configService = Objects
        .requireNonNull(connectConfigService, "connectConfigService");
    enabled = true;
  }

  /**
   * Asynchronously starts the KSQL-Connect integration components - does not
   * wait for them to startup before returning.
   */
  public void startAsync() {
    if (enabled) {
      connectPollingService.startAsync();
      configService.startAsync();
    }
  }

  @Override
  public void close() {
    if (enabled) {
      configService.stopAsync().awaitTerminated();
      connectPollingService.stopAsync().awaitTerminated();
    }
  }
}
