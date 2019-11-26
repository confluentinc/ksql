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

package io.confluent.ksql.function.udtf;

import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@UdtfDescription(name = "cube_explode", author = KsqlConstants.CONFLUENT_AUTHOR,
    description =
        "Takes as argument a list of columns and outputs all possible combinations of them. "
            + "It produces 2^d new rows where d is the number of columns given as parameter. "
            + "Duplicate entries for columns with null value are skipped.")
public class Cube {

  @Udtf
  public <T> List<List<T>> cube(final List<T> columns) {
    if (columns == null) {
      return Collections.emptyList();
    }
    return createAllCombinations(columns);
  }


  private <T> List<List<T>>  createAllCombinations(List<T> columns) {

    int combinations = 1 << columns.size();
    // skipBitmask is a binary number representing the input. A set bit means that the input is not
    // null in that index. This bitmask is used to skip duplicates when the input already contains
    // a null field as that field should be "flipped" and should not count towards the actual
    // combinations.
    int skipBitmask = 0;
    for (int i = 0; i < columns.size(); i++) {
      int shift = columns.size() - 1 - i;
      skipBitmask |=  columns.get(i) == null ? 0 << shift : 1 << shift;
    }

    List<List<T>> result = new ArrayList<>(combinations);
    // bitmask is a binary number where a set bit represents that the value at that index of input
    // should be included - start with an empty bitmask (necessary for correctness)
    for (int bitmask = 0; bitmask <= combinations - 1; bitmask++) {
      // if the logical end result is greater or equal than the bitmask, we can safely skip because
      // we know we saw before a value that was smaller than the bitmask. This holds because we
      // start from an empty bitmask hence, for every index we first encounter a zero (smaller)
      // and then a one (greater or equal).
      if ((bitmask & skipBitmask) < bitmask) {
        continue;
      }
      List<T> row = new ArrayList<>(columns.size());
      for (int i = 0; i < columns.size(); i++) {
        row.add(0, (bitmask & (1 << i)) == 0 ? null : columns.get(columns.size() -1 -i));
      }
      result.add(row);
    }
    return result;
  }
}
