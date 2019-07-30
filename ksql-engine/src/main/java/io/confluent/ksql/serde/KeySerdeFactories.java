/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde;

import io.confluent.ksql.model.WindowType;
import java.time.Duration;
import java.util.Optional;

public interface KeySerdeFactories {

  /**
   * Create {@link SerdeFactory}.
   *
   * @param windowType optional type of the window
   * @param windowSize window size required by some window types.
   */
  <K> SerdeFactory<K> create(Optional<WindowType> windowType, Optional<Duration> windowSize);
}
