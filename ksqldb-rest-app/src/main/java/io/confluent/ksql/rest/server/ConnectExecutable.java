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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.BindException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Connect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectExecutable implements Executable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectExecutable.class);

  private final ConnectDistributed connectDistributed;
  private final Map<String, String> workerProps;
  private Connect connect;
  private final CountDownLatch terminateLatch = new CountDownLatch(1);

  public static ConnectExecutable of(final String configFile) throws IOException {
    final Map<String, String> workerProps = !configFile.isEmpty()
        ? Utils.propsToStringMap(Utils.loadProps(configFile))
        : Collections.emptyMap();

    return new ConnectExecutable(workerProps);
  }

  @VisibleForTesting
  ConnectExecutable(final Map<String, String> workerProps) {
    this.workerProps = Objects.requireNonNull(workerProps, "workerProps");
    connectDistributed = new ConnectDistributed();
  }

  @Override
  public void startAsync() {
    try {
      connect = connectDistributed.startConnect(workerProps);
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    } catch (final ConnectException e) {
      if (e.getCause() instanceof IOException && e.getCause().getCause() instanceof BindException) {
        LOG.warn("Cannot start a local connect instance because connect is running locally!", e);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void shutdown() {
    if (connect != null) {
      connect.stop();
    }
  }

  @Override
  public void notifyTerminated() {
    terminateLatch.countDown();
  }

  @Override
  public void awaitTerminated() throws InterruptedException {
    terminateLatch.await();
  }
}
