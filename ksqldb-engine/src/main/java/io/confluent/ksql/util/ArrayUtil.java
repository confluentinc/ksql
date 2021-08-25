/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import java.util.Arrays;
import java.util.Objects;

public final class ArrayUtil {
  private ArrayUtil() {
  }

  public static <T> int getNullIndex(final T[] array) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        return i;
      }
    }
    return -1;
  }

  public static <T> boolean containsValue(final T value, final T[] array) {
    return Arrays.stream(array).anyMatch(o -> Objects.equals(o, value));
  }
}
