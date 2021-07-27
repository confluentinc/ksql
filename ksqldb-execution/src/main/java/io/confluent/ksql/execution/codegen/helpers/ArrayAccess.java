/*
 * Copyright 2019 Confluent Inc.
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

import java.util.List;

/**
 * Used by reflection in the SqlToJavaVisitor to resolve
 * ArrayAccess operations in a 1-indexed fashion.
 */
public final class ArrayAccess {

  private ArrayAccess() {

  }

  /**
   * @param list  the input list
   * @param index the index, base-1 or negative (n from the end)
   * @return the {@code index}-th item in {@code list}
   */
  public static <T> T arrayAccess(final List<T> list, final int index) {
    // subtract by 1 because SQL standard uses 1-based indexing; since
    // SQL standard does not support negative (end-based) indexing, we
    // will use -1 to represent the last element
    final int trueIndex = index >= 0 ? index - 1 : list.size() + index;
    if (trueIndex >= list.size() || trueIndex < 0) {
      return null;
    }

    return list.get(trueIndex);
  }

}
