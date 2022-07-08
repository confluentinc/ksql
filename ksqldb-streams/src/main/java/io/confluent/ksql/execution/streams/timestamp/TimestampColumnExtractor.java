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

package io.confluent.ksql.execution.streams.timestamp;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import java.sql.Timestamp;

public class TimestampColumnExtractor implements KsqlTimestampExtractor {
  private final ColumnExtractor extractor;

  TimestampColumnExtractor(final ColumnExtractor extractor) {
    this.extractor = requireNonNull(extractor, "extractor");
  }

  @Override
  public long extract(final Object key, final GenericRow value) {
    final Timestamp ts = (Timestamp) extractor.extract(key, value);
    return ts.getTime();
  }
}
