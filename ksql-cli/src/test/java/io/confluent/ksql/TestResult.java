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

package io.confluent.ksql;

import io.confluent.ksql.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public abstract class TestResult {

  private static final String LINE_SEPARATOR = ", ";

  Collection<List<String>> data;
  private boolean sealed = false;

  protected TestResult() {}

  protected TestResult(final TestResult model) {
    sealed = model.sealed;
  }

  static class OrderedResult extends TestResult {
    private OrderedResult() {
      data = new ArrayList<>();
    }

    private OrderedResult(final OrderedResult model) {
      super(model);
      data = new ArrayList<>();
      data.addAll(model.data);
    }

    private OrderedResult(final String singleRow) {
      this();
      if (singleRow.length() > 0) {
        data.add(Arrays.asList(singleRow.split(LINE_SEPARATOR)));
      }
      seal();
    }

    private OrderedResult(final List<List<String>> rows) {
      this();
      data.addAll(rows);
    }

    @Override
    public String toString() {
      return data.toString();
    }

    @Override
    public OrderedResult copy() {
      return new OrderedResult(this);
    }
  }

  static class UnorderedResult extends TestResult {
    private UnorderedResult() {
      data = new HashSet<>();
    }

    private UnorderedResult(final UnorderedResult model) {
      super(model);
      data = new HashSet<>();
      data.addAll(model.data);
    }

    private UnorderedResult(final Map<String, Object> map) {
      this();
      for (final Map.Entry<String, Object> kv : map.entrySet()) {
        data.add(Arrays.asList(kv.getKey(), String.valueOf(kv.getValue())));
      }
      seal();
    }

    @Override
    public String toString() {
      // for convenience, we show content ordered by first column (key) alphabetically
      final TreeMap<String, Object> map = new TreeMap<>();
      for (final List<String> entry: data) {
        map.put(entry.get(0), entry);
      }
      return map.values().toString();
    }

    @Override
    public UnorderedResult copy() {
      return new UnorderedResult(this);
    }
  }

  static UnorderedResult build(final Map<String, Object> map) {
    return new UnorderedResult(map);
  }

  static OrderedResult build(final String singleRow) {
    return new OrderedResult(singleRow);
  }

  static OrderedResult build(final Object... cols) {
    return new OrderedResult(StringUtil.join(", ", Arrays.asList(cols)));
  }

  static OrderedResult build(final List<List<String>> rows) {
    return new OrderedResult(rows);
  }

  static OrderedResult build() { return new OrderedResult(); }

  static TestResult init(final boolean requireOrder) {
    return requireOrder ? new OrderedResult() : new UnorderedResult();
  }

  public abstract TestResult copy();

  void addRow(final GenericRow row) {
    if (sealed) {
      throw new RuntimeException("TestResult already sealed, cannot add more rows to it.");
    }

    final List<String> newRow = new ArrayList<>();
    for (final Object column : row.getColumns()) {
      newRow.add(String.valueOf(column));
    }

    data.add(newRow);
  }

  void addRows(final List<List<String>> rows) {
    if (sealed) {
      throw new RuntimeException("TestResult already sealed, cannot add more rows to it.");
    }

    data.addAll(rows);
  }

  void seal() {
    this.sealed = true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final TestResult that = (TestResult) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }
}
