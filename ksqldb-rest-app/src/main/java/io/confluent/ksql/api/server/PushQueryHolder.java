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

package io.confluent.ksql.api.server;

import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.rest.entity.PushQueryId;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Holder for a push query running on the server.
 *
 * <p>Uses UUID.randomUUID() for the id which internally uses SecureRandom - this makes the id
 * cryptographically secure. This is important as we don't want random users guessing query IDs and
 * closing other peoples queries.
 */
public class PushQueryHolder {

  private final Server server;
  private final PushQueryId id;
  private final QueryPublisher queryPublisher;
  private final Consumer<PushQueryHolder> closeHandler;

  PushQueryHolder(final Server server,
      final QueryPublisher queryPublisher,
      final Consumer<PushQueryHolder> closeHandler) {
    this.server = Objects.requireNonNull(server);
    this.queryPublisher = Objects.requireNonNull(queryPublisher);
    this.closeHandler = Objects.requireNonNull(closeHandler);
    this.id = new PushQueryId(queryPublisher.queryId().toString());
  }

  public void close() {
    server.removeQuery(id);
    queryPublisher.close();
    closeHandler.accept(this);
  }

  public PushQueryId getId() {
    return id;
  }
}
