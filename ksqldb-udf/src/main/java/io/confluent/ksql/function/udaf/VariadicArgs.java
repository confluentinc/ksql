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

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class VariadicArgs<T> implements Iterable<T> {
  private final ImmutableList<T> values;

  public VariadicArgs(final List<T> values) {
    this.values = ImmutableList.copyOf(values);
  }

  public T get(final int index) {
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
