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

package io.confluent.ksql.function.udaf.window;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

/**
 * A placeholder KUDAF for extracting window start times.
 *
 * <p>The KUDAF itself does nothing.
 *
 * @see WindowSelectMapper
 */
@UdafDescription(name = "WindowStartTime", author = "Confluent",
    description = "Returns the window start time, in milliseconds, for the given record. "
        + "If the given record is not part of a window the function will return NULL.")
public final class WindowStartTimeKudaf {

  private WindowStartTimeKudaf() {
  }

  static String getFunctionName() {
    return "WindowStartTime"; // Todo(ac): Test that ensures this matches annotation.
  }

  @UdafFactory(description = "Extracts the window start time")
  public static TableUdaf<Long, Long> createWindowStart() {
    return new TableUdaf<Long, Long>() {
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
    };
  }
}
