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

public final class KsqlQueryStateUtil {

  private KsqlQueryStateUtil() {
    //Do nothing!
  }

  public static String getQueryStatString(final KafkaStreams.State state) {
    switch (state) {
      case CREATED:
        return "CREATED";
      case REBALANCING:
        return "REBALANCING";
      case RUNNING:
        return "RUNNING";
      case PENDING_SHUTDOWN:
        return "PENDING_SHUTDOWN";
      case NOT_RUNNING:
        return "NOT_RUNNING";
      case ERROR:
        return "ERROR";
      default:
        throw new KsqlException("Invalid query state: " + state);
    }
  }

}
