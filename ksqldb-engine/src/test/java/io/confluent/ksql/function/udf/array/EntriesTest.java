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

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class EntriesTest {

  private static final int ENTRIES = 20;

  private final Entries entriesUdf = new Entries();

  @Test
  public void shouldComputeIntEntries() {
    final Map<String, Integer> map = createMap(i -> i);
    shouldComputeEntries(map, () -> entriesUdf.entriesInt(map, false));
  }

  @Test
  public void shouldComputeBigIntEntries() {
    final Map<String, Long> map = createMap(Long::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesBigInt(map, false));
  }

  @Test
  public void shouldComputeDoubleEntries() {
    final Map<String, Double> map = createMap(Double::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesDouble(map, false));
  }

  @Test
  public void shouldComputeBooleanEntries() {
    final Map<String, Boolean> map = createMap(i -> i % 2 == 0);
    shouldComputeEntries(map, () -> entriesUdf.entriesBoolean(map, false));
  }

  @Test
  public void shouldComputeStringEntries() {
    final Map<String, String> map = createMap(String::valueOf);
    shouldComputeEntries(map, () -> entriesUdf.entriesString(map, false));
  }

  @Test
  public void shouldComputeStructEntries() {
    final Map<String, Struct> map = createMapOfStructs();
    shouldComputeEntries(map, () -> entriesUdf.entriesStruct(map, false));
  }

  @Test
  public void shouldComputeIntEntriesSorted() {
    final Map<String, Integer> map = createMap(i -> i);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesInt(map, true));
  }

  @Test
  public void shouldComputeBigIntEntriesSorted() {
    final Map<String, Long> map = createMap(Long::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesBigInt(map, true));
  }

  @Test
  public void shouldComputeDoubleEntriesSorted() {
    final Map<String, Double> map = createMap(Double::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesDouble(map, true));
  }

  @Test
  public void shouldComputeBooleanEntriesSorted() {
    final Map<String, Boolean> map = createMap(i -> i % 2 == 0);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesBoolean(map, true));
  }

  @Test
  public void shouldComputeStringEntriesSorted() {
    final Map<String, String> map = createMap(String::valueOf);
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesString(map, true));
  }

  @Test
  public void shouldComputeStructEntriesSorted() {
    final Map<String, Struct> map = createMapOfStructs();
    shouldComputeEntriesSorted(map, () -> entriesUdf.entriesStruct(map, true));
  }

  @Test
  public void shouldReturnNullListForNullMapInt() {
    assertNull(entriesUdf.entriesInt(null, false));
  }

  @Test
  public void shouldReturnNullListForNullMapBigInt() {
    assertNull(entriesUdf.entriesBigInt(null, false));
  }

  @Test
  public void shouldReturnNullListForNullMapDouble() {
    assertNull(entriesUdf.entriesDouble(null, false));
  }

  @Test
  public void shouldReturnNullListForNullMapBoolean() {
    assertNull(entriesUdf.entriesBoolean(null, false));
  }

  @Test
  public void shouldReturnNullListForNullMapString() {
    assertNull(entriesUdf.entriesString(null, false));
  }

  @Test
  public void shouldReturnNullListForNullMapStruct() {
    assertNull(entriesUdf.entriesStruct(null, false));
  }

  private <T> void shouldComputeEntries(final Map<String, T> map, final Supplier<List<Struct>> supplier) {
    final List<Struct> out = supplier.get();
    assertThat(out, hasSize(map.size()));
    for (final Struct struct : out) {
      final T val = map.get(struct.getString("K"));
      assertThat(val == null, is(false));
      assertThat(val, is(struct.get("V")));
    }
  }

  private <T> void shouldComputeEntriesSorted(final Map<String, T> map, final Supplier<List<Struct>> supplier) {
    final List<Struct> out = supplier.get();
    final List<Map.Entry<String, T>> entries = new ArrayList<>(map.entrySet());
    entries.sort(Entry.comparingByKey());
    assertThat(out.size(), is(entries.size()));
    for (int i = 0; i < entries.size(); i++) {
      final Struct struct = out.get(i);
      final Map.Entry<String, T> entry = entries.get(i);
      assertThat(struct.get("K"), is(entry.getKey()));
      assertThat(struct.get("V"), is(entry.getValue()));
    }
  }

  private <T> Map<String, T> createMap(final Function<Integer, T> valueSupplier) {
    final Map<String, T> map = new HashMap<>();
    for (int i = 0; i < ENTRIES; i++) {
      map.put(UUID.randomUUID().toString(), valueSupplier.apply(i));
    }
    return map;
  }

  private Map<String, Struct> createMapOfStructs() {
    final Map<String, Struct> map = new HashMap<>();
    final ConnectSchema schema = new ConnectSchema(Schema.Type.STRUCT).schema();
    for (int i = 0; i < ENTRIES; i++) {
      final Struct struct = new Struct(schema);
      map.put(UUID.randomUUID().toString(), struct);
    }
    return map;
  }
}
