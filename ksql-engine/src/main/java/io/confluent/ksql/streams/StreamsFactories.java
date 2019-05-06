/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.streams;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;

public class StreamsFactories {
  private final GroupedFactory groupedFactory;
  private final JoinedFactory joinedFactory;
  private final MaterializedFactory materializedFactory;

  public static StreamsFactories create(final KsqlConfig ksqlConfig) {
    Objects.requireNonNull(ksqlConfig);
    return new StreamsFactories(
        GroupedFactory.create(ksqlConfig),
        JoinedFactory.create(ksqlConfig),
        MaterializedFactory.create(ksqlConfig)
    );
  }

  public StreamsFactories(
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory,
      final MaterializedFactory materializedFactory) {
    this.groupedFactory = Objects.requireNonNull(groupedFactory);
    this.joinedFactory = Objects.requireNonNull(joinedFactory);
    this.materializedFactory = Objects.requireNonNull(materializedFactory);
  }

  public GroupedFactory getGroupedFactory() {
    return groupedFactory;
  }

  public JoinedFactory getJoinedFactory() {
    return joinedFactory;
  }

  public MaterializedFactory getMaterializedFactory() {
    return materializedFactory;
  }
}
