/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.streams.KafkaStreams;

final class KsqlQueryStateUtil {

  private KsqlQueryStateUtil() {
    //Do nothing!
  }

  static double getQueryStatNumber(final KafkaStreams.State state) {
    switch (state) {
      case CREATED:
        return 0.0;
      case REBALANCING:
        return 1.0;
      case RUNNING:
        return 2.0;
      case PENDING_SHUTDOWN:
        return 3.0;
      case NOT_RUNNING:
        return 4.0;
      case ERROR:
        return 5.0;
      default:
        throw new KsqlException("Invalid query state: " + state);
    }
  }

}
