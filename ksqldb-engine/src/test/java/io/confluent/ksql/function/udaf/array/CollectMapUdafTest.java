/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udaf.array;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class CollectMapUdafTest {

  @Test
  public void shouldCollectMaps() {
    final Udaf<Map<String, String>, Map<String, String>, Map<String, String>> udaf =
        CollectMapUdaf.createCollecMapT();

    final Map<String, String> map1 = new HashMap<String, String>() {{ put("key1", "value1"); }};
    final Map<String, String> map2 = new HashMap<String, String>() {{ put("key2", "value2"); }};
    final Map<String, String> map3 = new HashMap<String, String>() {{ put("key1", "value3"); }};

    final Map<String, String> init = udaf.initialize();

    final Map<String, String> agg1 = udaf.aggregate(map1, init);
    final Map<String, String> agg2 = udaf.aggregate(map2, agg1);
    assertThat(agg2.values(), hasSize(2));

    // Show that updates can happen.
    final Map<String, String> agg3 = udaf.aggregate(map3, agg2);
    assertEquals(agg3.get("key1"), "value3");
  }

  @Test
  public void shouldRespectSizeLimit() {
    /*
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListT();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, 10));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i < 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }
    assertThat(runningList, hasSize(10));
    assertThat(runningList, hasItem(1));
    assertThat(runningList, hasItem(10));
    assertThat(runningList, not(hasItem(11)));

    */
  }

  @Test
  public void shouldRespectSizeLimitString() {
    /*
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListT();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, "10"));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i < 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }
    assertThat(runningList, hasSize(10));
    assertThat(runningList, hasItem(1));
    assertThat(runningList, hasItem(10));
    assertThat(runningList, not(hasItem(11)));

    */
  }

  @Test
  public void shouldIgnoreNegativeLimit() {
    /* JNH
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListT();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, -10));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i <= 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }

    assertThat(runningList, hasSize(25));

    */
  }

}
