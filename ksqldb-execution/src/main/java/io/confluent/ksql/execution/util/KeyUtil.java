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

package io.confluent.ksql.execution.util;

import io.confluent.ksql.GenericKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Windowed;

public final class KeyUtil {

  private KeyUtil() {
  }

  @SuppressWarnings("unchecked")
  public static List<Object> asList(final Object key) {
    final Optional<Windowed<Object>> windowed = key instanceof Windowed
        ? Optional.of((Windowed<Object>) key)
        : Optional.empty();

    final Object naturalKey = windowed
        .map(Windowed::key)
        .orElse(key);

    if (naturalKey != null && !(naturalKey instanceof GenericKey)) {
      throw new IllegalArgumentException("Non generic key: " + key);
    }

    final Optional<GenericKey> genericKey = Optional.ofNullable((GenericKey) naturalKey);

    final List<Object> data = new ArrayList<>(
        genericKey.map(GenericKey::size).orElse(0)
            + (windowed.isPresent() ? 2 : 0)
    );

    genericKey.ifPresent(k -> data.addAll(k.values()));

    windowed
        .map(Windowed::window)
        .ifPresent(wnd -> {
          data.add(wnd.start());
          data.add(wnd.end());
        });

    return data;
  }
}
