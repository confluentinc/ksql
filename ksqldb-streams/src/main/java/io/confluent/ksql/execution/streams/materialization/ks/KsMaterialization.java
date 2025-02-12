/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterialization;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedTable;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedWindowedTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Optional;

/**
 * Kafka Streams impl of {@link Materialization}.
 */
public final class KsMaterialization implements StreamsMaterialization {

  private final Optional<WindowInfo> windowInfo;
  private final KsStateStore stateStore;
  private final Locator locator;

  KsMaterialization(
      final Optional<WindowInfo> windowInfo,
      final Locator locator,
      final KsStateStore stateStore
  ) {
    this.windowInfo = requireNonNull(windowInfo, "windowInfo");
    this.stateStore = requireNonNull(stateStore, "stateStore");
    this.locator = requireNonNull(locator, "locator");
  }

  @Override
  public LogicalSchema schema() {
    return stateStore.schema();
  }

  @Override
  public Locator locator() {
    return locator;
  }

  @Override
  public Optional<WindowType> windowType() {
    return windowInfo.map(WindowInfo::getType);
  }

  @Override
  public StreamsMaterializedTable nonWindowed() {
    if (windowInfo.isPresent()) {
      throw new UnsupportedOperationException("Table has windowed key");
    }
    return new KsMaterializedTable(stateStore);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent") // Enforced by type
  @Override
  public StreamsMaterializedWindowedTable windowed() {
    if (!windowInfo.isPresent()) {
      throw new UnsupportedOperationException("Table has non-windowed key");
    }

    final WindowInfo wndInfo = windowInfo.get();
    final WindowType wndType = wndInfo.getType();
    switch (wndType) {
      case SESSION:
        return new KsMaterializedSessionTable(stateStore,
            SessionStoreCacheBypass::fetch, SessionStoreCacheBypass::fetchRange);

      case HOPPING:
      case TUMBLING:
        return new KsMaterializedWindowTable(stateStore, wndInfo.getSize().get(),
            WindowStoreCacheBypass::fetch,
            WindowStoreCacheBypass::fetchAll,
            WindowStoreCacheBypass::fetchRange);

      default:
        throw new UnsupportedOperationException("Unknown window type: " + wndInfo);
    }
  }
}
