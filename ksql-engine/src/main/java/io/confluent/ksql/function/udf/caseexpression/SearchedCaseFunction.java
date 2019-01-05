/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.caseexpression;

import io.confluent.ksql.util.KsqlException;
import java.util.List;

public final class SearchedCaseFunction {

  private SearchedCaseFunction() {

  }

  public static <T> T searchedCaseFunction(
      final List<Boolean> whenList,
      final List<T> thenList,
      final T defaultValue) {
    if (whenList.size() != thenList.size()) {
      throw new KsqlException("Invalid arguments."
          + " When list and Then list should have the same size."
          + " When list size is " + whenList.size() + ", thenList size is " + thenList.size());
    }
    for (int i = 0; i < whenList.size(); i++) {
      if (whenList.get(i)) {
        return thenList.get(i);
      }
    }
    return defaultValue;
  }

}
