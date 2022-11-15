/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import java.util.Map.Entry;
import org.apache.kafka.streams.query.Position;

public final class ConsistencyUtil {

  private ConsistencyUtil() {
  }

  public static void updateFromPosition(
      final ConsistencyOffsetVector vector,
      final Position position
  ) {
    for (String topic: position.getTopics()) {
      for (Entry<Integer, Long> entry : position.getPartitionPositions(topic).entrySet()) {
        vector.update(topic, entry.getKey(), entry.getValue());
      }
    }
  }
}
