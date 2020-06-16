/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.query;

import org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * An extension of {@link StateListener} which can also react
 * to classified uncaught errors, represented by {@link QueryError}.
 */
public interface ErrorStateListener extends StateListener {

  /**
   * This method will be called whenever the underlying application
   * throws an uncaught exception.
   *
   * @param error the error that occurred
   */
  void onError(QueryError error);

}
