/**
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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;

public class ArrayUtil {

  public static <T> int getNullIndex(T[] array) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        return i;
      }
    }
    return -1;
  }

  public static <T> T[] getNoNullArray(Class<T> clazz, T[] array) {
    int nullIndex = getNullIndex(array);
    if (nullIndex == -1) {
      return array;
    }
    T[] noNullArray = (T[]) Array.newInstance(clazz, nullIndex);
    for (int i = 0; i < noNullArray.length; i++) {
      noNullArray[i] = array[i];
    }
    return noNullArray;
  }

  public static <T> T[] padWithNull(Class<T> clazz, T[] array, int finalLength) {
    if (array.length >= finalLength) {
      return array;
    }
    T[] paddedArray = (T[]) Array.newInstance(clazz, finalLength);
    for(int i = 0; i < array.length; i++) {
      paddedArray[i] = array[i];
    }
    for (int i = array.length; i < finalLength; i++) {
      paddedArray[i] = null;
    }
    return paddedArray;
  }

  public static <T> boolean containsValue(T value, T[] array) {
    return Arrays.stream(array).anyMatch(o -> Objects.equals(o, value));
  }
}
