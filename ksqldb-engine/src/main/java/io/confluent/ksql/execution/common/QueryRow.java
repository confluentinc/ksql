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

package io.confluent.ksql.execution.common;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.Optional;

/**
 * Represents a row being processed by any type of query -- namely pull or scalable push.
 */
public interface QueryRow {
  LogicalSchema schema();

  long rowTime();

  GenericKey key();

  Optional<Window> window();

  GenericRow value();

  Optional<PushOffsetRange> getOffsetRange();
}
