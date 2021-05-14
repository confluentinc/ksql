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

package io.confluent.ksql.test.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Util class for creating immutable collections
 */
public final class ImmutableCollections {

  private ImmutableCollections() {
  }

  public static <T> ImmutableList<T> immutableCopyOf(
      final Iterable<? extends T> source
  ) {
    return source == null
        ? ImmutableList.of()
        : ImmutableList.copyOf(source);
  }

  public static <K, V> ImmutableMap<K, V> immutableCopyOf(
      final Map<? extends K, ? extends V> source
  ) {
    return source == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(source);
  }
}
