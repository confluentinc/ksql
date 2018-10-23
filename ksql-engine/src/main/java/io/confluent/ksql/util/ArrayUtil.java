/*
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import java.util.Arrays;
import java.util.Objects;

public final class ArrayUtil {
  private ArrayUtil(){}

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
