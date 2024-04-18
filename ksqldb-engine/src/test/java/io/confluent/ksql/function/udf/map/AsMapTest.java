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

package io.confluent.ksql.function.udf.map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class AsMapTest {

  @Test
  public void shouldCreateMap() {
    // Given:
    final List<String> keys = Lists.newArrayList("1", "2");
    final List<String> values = Lists.newArrayList("a", "b");

    // When:
    final Map<String, String> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map, hasEntry("1", "a"));
    assertThat(map, hasEntry("2", "b"));
  }

  @Test
  public void shouldCreateMapWithIntegerValues() {
    // Given:
    final List<String> keys = Lists.newArrayList("1", "2");
    final List<Integer> values = Lists.newArrayList(1, 2);

    // When:
    final Map<String, Integer> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map, hasEntry("1", 1));
    assertThat(map, hasEntry("2", 2));
  }

  @Test
  public void shouldIgnoreMismatchingLengthKeysAndValues() {
    // Given:
    final List<String> keys = Lists.newArrayList("1", "2");
    final List<Integer> values = Lists.newArrayList(1, 2, 3);

    // When:
    final Map<String, Integer> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map.size(), is(2));
    assertThat(map, hasEntry("1", 1));
    assertThat(map, hasEntry("2", 2));
  }

  @Test
  public void shouldHandleNullKey() {
    // Given:
    final List<String> keys = Lists.newArrayList("1", null);
    final List<Integer> values = Lists.newArrayList(1, 2);

    // When:
    final Map<String, Integer> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map, hasEntry("1", 1));
    assertThat(map, hasEntry(null, 2));
  }

  @Test
  public void shouldReturnNullForNullKeys() {
    // Given:
    final List<String> keys = null;
    final List<Integer> values = Lists.newArrayList(1, 2);

    // When:
    final Map<String, Integer> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map,is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullValues() {
    // Given:
    final List<String> keys = Lists.newArrayList("1", "2");
    final List<Integer> values = null;

    // When:
    final Map<String, Integer> map = new AsMap().asMap(keys, values);

    // Then:
    assertThat(map,is(nullValue()));
  }
}