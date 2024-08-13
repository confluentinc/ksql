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

package io.confluent.ksql.api.server;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This class give a resource the opportunity to register a set of particular callbacks based upon
 * arbitrary criteria. Once the response is complete, the callback is invoked.
 */
public class MetricsCallbackHolder {

  private AtomicReference<MetricsCallback> callbackRef = new AtomicReference<>(null);

  public MetricsCallbackHolder() {
  }

  public void setCallback(final MetricsCallback callback) {
    this.callbackRef.set(callback);
  }

  public void reportMetrics(final int statusCode, final long requestBytes, final long responseBytes,
      final long startTimeNanos) {
    final MetricsCallback callback = callbackRef.get();
    if (callback != null) {
      callback.reportMetricsOnCompletion(statusCode, requestBytes, responseBytes, startTimeNanos);
    }
  }
}
