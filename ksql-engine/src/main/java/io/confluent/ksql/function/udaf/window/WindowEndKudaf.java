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
import io.confluent.ksql.function.udaf.placeholder.PlaceholderTableUdaf;

/**
 * A placeholder KUDAF for extracting window end times.
 *
 * <p>The KUDAF itself does nothing. It's just a placeholder.
 *
 * @see WindowSelectMapper
 */
@SuppressWarnings("WeakerAccess") // Invoked via reflection.
@UdafDescription(name = "WindowEnd", author = "Confluent",
    description = "Returns the window end time, in milliseconds, for the given record. "
        + "If the given record is not part of a window the function will return NULL.")
public final class WindowEndKudaf {

  private WindowEndKudaf() {
  }

  static String getFunctionName() {
    return "WindowEnd";
  }

  @UdafFactory(description = "Extracts the window end time")
  public static TableUdaf<Long, Long> createWindowEnd() {
    return PlaceholderTableUdaf.INSTANCE;
  }
}
