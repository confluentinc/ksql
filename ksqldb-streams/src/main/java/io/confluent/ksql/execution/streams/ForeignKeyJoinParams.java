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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;

public class ForeignKeyJoinParams<KRightT> {
  private final KsqlKeyExtractor<KRightT> keyExtractor;
  private final KsqlValueJoiner joiner;
  private final LogicalSchema schema;

  ForeignKeyJoinParams(final KsqlKeyExtractor<KRightT> keyExtractor,
                       final KsqlValueJoiner joiner,
                       final LogicalSchema schema) {
    this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
    this.joiner = Objects.requireNonNull(joiner, "joiner");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KsqlKeyExtractor<KRightT> getKeyExtractor() {
    return keyExtractor;
  }

  public KsqlValueJoiner getJoiner() {
    return joiner;
  }
}
