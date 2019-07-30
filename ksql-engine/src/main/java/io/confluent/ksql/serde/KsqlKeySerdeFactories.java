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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public class KsqlKeySerdeFactories implements KeySerdeFactories {

  @Override
  public <K> SerdeFactory<K> create(
      final Optional<WindowType> windowType,
      final Optional<Duration> windowSize
  ) {
    return createSerdeFactory(windowType, windowSize);
  }

  @SuppressWarnings("unchecked")
  public static <K> SerdeFactory<K> createSerdeFactory(
      final Optional<WindowType> windowType,
      final Optional<Duration> windowSize
  ) {
    if (!windowType.isPresent()) {
      return (SerdeFactory) Serdes::String;
    }

    if (windowType.get() == WindowType.SESSION) {
      return () -> (Serde) WindowedSerdes.sessionWindowedSerdeFrom(String.class);
    }

    if (!windowSize.isPresent()) {
      throw new IllegalArgumentException("size is required");
    }

    final long windowSizeMs = windowSize.get().toMillis();

    return () -> (Serde) WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSizeMs);
  }
}
