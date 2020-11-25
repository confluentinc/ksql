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

package io.confluent.ksql.execution.function;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Merger;

public interface UdafAggregator<K> extends Aggregator<K, GenericRow, GenericRow> {

  Merger<GenericKey, GenericRow> getMerger();

  /**
   * @return a transformer to map the intermediate schema the output schema
   */
  KsqlTransformer<K, GenericRow> getResultMapper();
}
