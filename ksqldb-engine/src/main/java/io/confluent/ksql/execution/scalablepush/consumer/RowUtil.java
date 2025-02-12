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

package io.confluent.ksql.execution.scalablepush.consumer;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Windowed;

public final class RowUtil {

  private RowUtil() { }

  /**
   * Takes a raw key object read from the topic and returns the appropriate row, depending on
   * whether it's windowed or not.
   * @param key The key object
   * @param value The value
   * @param timestamp The timestamp of the row
   * @param windowed If the data is known to have a windowed key
   * @param logicalSchema The logical schema of the data
   * @return The appropriate row object containing the data
   */
  @SuppressWarnings("unchecked")
  public static QueryRow createRow(
      final Object key,
      final GenericRow value,
      final long timestamp,
      final boolean windowed,
      final LogicalSchema logicalSchema
  ) {
    if (!windowed) {
      final GenericKey keyCopy = GenericKey.fromList(
          key != null ? ((GenericKey) key).values() : Collections.emptyList());
      final GenericRow valueCopy = GenericRow.fromList(value.values());
      return QueryRowImpl.of(logicalSchema, keyCopy, Optional.empty(), valueCopy, timestamp);
    } else {
      final Windowed<GenericKey> windowedKey = (Windowed<GenericKey>) key;
      final GenericKey keyCopy = GenericKey.fromList(windowedKey.key().values());
      final GenericRow valueCopy = GenericRow.fromList(value.values());
      return QueryRowImpl.of(logicalSchema, keyCopy, Optional.of(Window.of(
          windowedKey.window().startTime(),
          windowedKey.window().endTime()
      )), valueCopy, timestamp);
    }
  }
}
