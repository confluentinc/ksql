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

package io.confluent.ksql;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TestResult implements Iterable<List<String>>{

  final List<List<String>> data;

  public TestResult() {
    this.data = Collections.emptyList();
  }

  public TestResult(final Object... fields) {
    this.data = ImmutableList.of(
        ImmutableList.copyOf(
            Arrays.stream(fields)
                .map(String::valueOf)
                .collect(Collectors.toList())
        )
    );
  }

  private TestResult(List<List<String>> data) {
    this.data = ImmutableList.copyOf(data);
  }

  public static class Builder {
    private final List<List<String>> data = new ArrayList<>();

    public Builder() {
    }

    TestResult.Builder addRow(final GenericRow row) {
      data.add(
          ImmutableList.copyOf(
              row.getColumns().stream()
                  .map(String::valueOf)
                  .collect(Collectors.toList())
          )
      );
      return this;
    }

    TestResult.Builder addRow(final Object... fields) {
      data.add(
          ImmutableList.copyOf(
              Arrays.stream(fields)
                  .map(String::valueOf)
                  .collect(Collectors.toList())
          )
      );
      return this;
    }

    TestResult.Builder addRows(final List<List<String>> rows) {
      rows.forEach(
          r -> data.add(ImmutableList.copyOf(r))
      );
      return this;
    }

    public TestResult build() {
      return new TestResult(data);
    }
  }

  public List<List<String>> rows() {
    return data;
  }

  @Override
  public Iterator<List<String>> iterator() {
    return data.iterator();
  }

  @Override
  public Spliterator<List<String>> spliterator() {
    return data.spliterator();
  }

  @Override
  public void forEach(final Consumer<? super List<String>> action) {
    data.forEach(action);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final TestResult that = (TestResult) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }
}
