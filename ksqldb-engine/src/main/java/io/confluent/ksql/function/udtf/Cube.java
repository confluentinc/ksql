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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@UdtfDescription(
    name = "cube_explode",
    category = FunctionCategory.TABLE,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description =
        "Takes as argument a list of columns and outputs all possible combinations of them. "
            + "It produces 2^d new rows where d is the number of columns given as parameter. "
            + "Duplicate entries for columns with null value are skipped."
)
public class Cube {

  @Udtf
  public <T> List<List<T>> cube(final List<T> columns) {
    if (columns == null) {
      return Collections.emptyList();
    }
    return createAllCombinations(columns);
  }


  private <T> List<List<T>>  createAllCombinations(final List<T> columns) {

    final int combinations = 1 << columns.size();
    // when a column value is null there is only a single possibility for the output
    // value [null] instead of two [null, original]. in order to avoid creating duplicate
    // rows, we use nullMask: a binary number with set bits at non-null column
    // indices (see comment below on usage)
    int nullMask = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i) != null) {
        nullMask |= 1 << (columns.size() - 1 - i);
      }
    }

    final List<List<T>> result = new ArrayList<>(combinations);
    // bitmask is a binary number where a set bit represents that the value at that index of input
    // should be included - (e.g. the bitmask 5 (101) represents that cols[2] and cols[0]
    // should be set while cols[1] should be null).
    // Start with an empty bitmask (necessary for correctness)
    for (int bitMask = 0; bitMask <= combinations - 1; bitMask++) {
      // canonicalBitMask represents which indices in the output
      // row will be null after taking into consideration which values
      // in columns were originally null
      final int canonicalBitMask = bitMask & nullMask;

      if (canonicalBitMask != bitMask) {
        // if the canonicalBitMask is not the same as bitMask, then this row is a logical
        // duplicate of another row, and we should not emit it
        continue;
      }
      final List<T> row = new ArrayList<>(columns.size());
      for (int i = 0; i < columns.size(); i++) {
        row.add(0, (bitMask & (1 << i)) == 0 ? null : columns.get(columns.size() - 1 - i));
      }
      result.add(row);
    }
    return result;
  }
}
