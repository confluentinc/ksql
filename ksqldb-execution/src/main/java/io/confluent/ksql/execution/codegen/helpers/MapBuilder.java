/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to construct maps using the builder pattern. Note that we cannot use {@link
 * com.google.common.collect.ImmutableMap} because it does not accept null values.
 */
public class MapBuilder {

  private final HashMap<Object, Object> map;

  public MapBuilder(final int size) {
    map = new HashMap<>(size);
  }

  public MapBuilder put(final Object key, final Object value) {
    map.put(key, value);
    return this;
  }

  public Map<Object, Object> build() {
    return Collections.unmodifiableMap(map);
  }
}