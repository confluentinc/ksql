/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class EntriesTest {

  private static final int ENTRIES = 20;

  private Entries entriesUdf = new Entries();

  @Test
  public void shouldComputeIntEntries() {
    Map<String, Integer> map = createMap(i -> i);
    shouldComputeEntries(map, () -> entriesUdf.entriesInt(map, false));
  }

  @Test
  public void shouldComputeBigIntEntries() {
    Map<String, Long> map = createMap(Long::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesBigInt(map, false));
  }

  @Test
  public void shouldComputeDoubleEntries() {
    Map<String, Double> map = createMap(Double::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesDouble(map, false));
  }

  @Test
  public void shouldComputeBooleanEntries() {
    Map<String, Boolean> map = createMap(i -> i % 2 == 0);
    shouldComputeEntries(map, () -> entriesUdf.entriesBoolean(map, false));
  }

  @Test
  public void shouldComputeStringEntries() {
    Map<String, String> map = createMap(String::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesString(map, false));
  }

  @Test
  public void shouldComputeIntEntriesSorted() {
    Map<String, Integer> map = createMap(i -> i);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesInt(map, true));
  }

  @Test
  public void shouldComputeBigIntEntriesSorted() {
    Map<String, Long> map = createMap(Long::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesBigInt(map, true));
  }

  @Test
  public void shouldComputeDoubleEntriesSorted() {
    Map<String, Double> map = createMap(Double::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesDouble(map, true));
  }

  @Test
  public void shouldComputeBooleanEntriesSorted() {
    Map<String, Boolean> map = createMap(i -> i % 2 == 0);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesBoolean(map, true));
  }

  @Test
  public void shouldComputeStringEntriesSorted() {
    Map<String, String> map = createMap(String::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesString(map, true));
  }

  private <T> void shouldComputeEntries(
      Map<String, T> map, Supplier<List<Struct>> supplier
  ) {
    List<Struct> out = supplier.get();
    assertThat(out, hasSize(map.size()));
    for (int i = 0; i < out.size(); i++) {
      Struct struct = out.get(i);
      T val = map.get(struct.getString("K"));
      assertThat(val == null, is(false));
      assertThat(val, is(struct.get("V")));
    }
  }

  private <T> void shouldComputeEntriesSorted(Map<String, T> map, Supplier<List<Struct>> supplier) {
    List<Struct> out = supplier.get();
    List<Map.Entry<String, T>> entries = new ArrayList<>(map.entrySet());
    entries.sort(Comparator.comparing(Entry::getKey));
    assertThat(out.size(), is(entries.size()));
    for (int i = 0; i < entries.size(); i++) {
      Struct struct = out.get(i);
      Map.Entry<String, T> entry = entries.get(i);
      assertThat(struct.get("K"), is(entry.getKey()));
      assertThat(struct.get("V"), is(entry.getValue()));
    }
  }

  private <T> Map<String, T> createMap(Function<Integer, T> valueSupplier) {
    Map<String, T> map = new HashMap<>();
    for (int i = 0; i < ENTRIES; i++) {
      map.put(UUID.randomUUID().toString(), valueSupplier.apply(i));
    }
    return map;
  }

}
