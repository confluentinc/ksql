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

package io.confluent.ksql.function.udaf.placeholder;

import io.confluent.ksql.function.udaf.TableUdaf;

/**
 * A no-op {@link TableUdaf} that is used as a placeholder for some later hardcoded computation.
 */
public final class PlaceholderTableUdaf implements TableUdaf<Long, Long> {

  public static final PlaceholderTableUdaf INSTANCE = new PlaceholderTableUdaf();

  private PlaceholderTableUdaf(){
  }

  @Override
  public Long undo(final Long valueToUndo, final Long aggregateValue) {
    return null;
  }

  @Override
  public Long initialize() {
    return null;
  }

  @Override
  public Long aggregate(final Long value, final Long aggregate) {
    return null;
  }

  @Override
  public Long merge(final Long aggOne, final Long aggTwo) {
    return null;
  }
}
