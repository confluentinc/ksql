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

package io.confluent.ksql.api.client;

import org.reactivestreams.Publisher;

/**
 * An acknowledgment from the ksqlDB server that a row has been successfully inserted into a
 * ksqlDB stream. See {@link Client#streamInserts(String, Publisher)} for details.
 */
public interface InsertAck {

  /**
   * Returns the corresponding sequence number for this acknowledgment. Sequence numbers start at
   * zero for each new {@link Client#streamInserts} request.
   *
   * @return the sequence number
   */
  long seqNum();

}
