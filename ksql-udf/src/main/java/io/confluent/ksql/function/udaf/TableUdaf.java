/*
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
 */

package io.confluent.ksql.function.udaf;

/**
 * {@code TableUdaf} is an {@link Udaf} that can be used to
 * aggregate KSQL tables.
 * @param <V> value type
 * @param <A> aggregate type
 */
public interface TableUdaf<V, A> extends Udaf<V, A> {
  /**
   * Called to update a value when a record with the same key
   * has been updated. When called this function should
   * reverse the operation that was performed during {@link Udaf#aggregate(Object, Object)}
   * @param valueToUndo   value that has been changed
   * @param aggregateValue the current aggregate
   * @return new aggregate
   */
  A undo(final V valueToUndo, final A aggregateValue);
}
