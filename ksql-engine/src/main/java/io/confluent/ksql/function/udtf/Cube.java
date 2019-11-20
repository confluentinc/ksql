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

    List<List<T>> result = new ArrayList<>();
    createAllCombinations(columns, 0, new ArrayList<>(), result);

    return result;
  }


  private <T> void createAllCombinations(List<T> columns, int pos, List<T> current,
                                         List<List<T>> result) {
    if (pos == columns.size()) {
      result.add(new ArrayList<>(current));
      return;
    }
    current.add(columns.get(pos));
    createAllCombinations(columns, pos + 1, current, result);

    Object col = current.remove(pos);
    if (col != null) {
      current.add(null);
      createAllCombinations(columns, pos + 1, current, result);
      current.remove(pos);
    }
  }
}
