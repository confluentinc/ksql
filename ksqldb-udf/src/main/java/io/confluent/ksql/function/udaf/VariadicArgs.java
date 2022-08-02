/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class VariadicArgs<T> implements Iterable<T> {
  private final List<T> values;

  public VariadicArgs(final List<T> values) {
    this.values = Collections.unmodifiableList(new ArrayList<>(values));
  }

  public T get(final int index) {
    if (index < 0 || index >= values.size()) {
      throw new IndexOutOfBoundsException(
              String.format(
                      "Attempted to access variadic argument at index %s when only %s "
                      + "arguments are available",
                      index,
                      values.size()
              )
      );
    }

    return values.get(index);
  }

  public int size() {
    return values.size();
  }

  public Stream<T> stream() {
    return values.stream();
  }

  @Override
  public Iterator<T> iterator() {
    return values.iterator();
  }
}
