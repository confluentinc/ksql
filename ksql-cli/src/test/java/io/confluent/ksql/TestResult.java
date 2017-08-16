/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import io.confluent.ksql.physical.GenericRow;
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

  static class OrderedResult extends TestResult {
    private OrderedResult() {
      data = new ArrayList<>();
    }

    private OrderedResult(String singleRow) {
      this();
      if (singleRow.length() > 0) {
        data.add(Arrays.asList(singleRow.split(LINE_SEPARATOR)));
      }
      seal();
    }

    @Override
    public String toString() {
      return data.toString();
    }
  }

  static class UnorderedResult extends TestResult {
    private UnorderedResult() {
      data = new HashSet<>();
    }

    private UnorderedResult(Map<String, Object> map) {
      this();
      for (Map.Entry<String, Object> kv : map.entrySet()) {
        data.add(Arrays.asList(kv.getKey(), String.valueOf(kv.getValue())));
      }
      seal();
    }

    @Override
    public String toString() {
      // for convenience, we show content ordered by first column (key) alphabetically
      TreeMap<String, Object> map = new TreeMap<>();
      for (List<String> entry: data) {
        map.put(entry.get(0), entry);
      }
      return map.values().toString();
    }
  }

  static UnorderedResult build(Map<String, Object> map) {
    return new UnorderedResult(map);
  }

  static OrderedResult build(String singleRow) {
    return new OrderedResult(singleRow);
  }

  static OrderedResult build(Object... cols) {
    return new OrderedResult(StringUtil.join(", ", Arrays.asList(cols)));
  }

  static TestResult init(boolean requireOrder) {
    return requireOrder ? new OrderedResult() : new UnorderedResult();
  }

  void addRow(GenericRow row) {
    if (sealed) {
      throw new RuntimeException("TestResult already sealed, cannot add more rows to it.");
    }

    List<String> newRow = new ArrayList<>();
    for (Object column : row.getColumns()) {
      newRow.add(String.valueOf(column));
    }

    data.add(newRow);
  }

  void addRows(List<List<String>> rows) {
    if (sealed) {
      throw new RuntimeException("TestResult already sealed, cannot add more rows to it.");
    }

    data.addAll(rows);
  }

  void seal() {
    this.sealed = true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TestResult that = (TestResult) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data);
  }
}
