/*
 * Copyright 2021 Confluent Inc.
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.List;
import org.apache.kafka.common.Configurable;
import org.junit.Test;

public class CollectListUdafTest {

  @Test
  public void shouldCollectInts() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    final Integer[] values = new Integer[] {3, 4, 5, 3};
    List<Integer> runningList = udaf.initialize();
    for (final Integer i : values) {
      runningList = udaf.aggregate(i, runningList);
    }
    assertThat(runningList, contains(3, 4, 5, 3));
  }

  @Test
  public void shouldMergeIntLists() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();

    List<Integer> lhs = udaf.initialize();
    final Integer[] lhsValues = new Integer[] {1, 2, null, 3};
    for (final Integer i : lhsValues) {
      lhs = udaf.aggregate(i, lhs);
    }
    assertThat(lhs, contains(1, 2, null, 3));

    List<Integer> rhs = udaf.initialize();
    final Integer[] rhsValues = new Integer[] {2, null, 3, 4, 5, 6};
    for (final Integer i : rhsValues) {
      rhs = udaf.aggregate(i, rhs);
    }
    assertThat(rhs, contains(2, null, 3, 4, 5, 6));

    final List<Integer> merged = udaf.merge(lhs, rhs);
    assertThat(merged, contains(1, 2, null, 3, 2, null, 3, 4, 5, 6));
  }

  @Test
  public void shouldRespectSizeLimit() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, 10));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i < 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }
    assertThat(runningList, hasSize(10));
    assertThat(runningList, hasItem(1));
    assertThat(runningList, hasItem(10));
    assertThat(runningList, not(hasItem(11)));
  }

  @Test
  public void shouldRespectSizeLimitString() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, "10"));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i < 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }
    assertThat(runningList, hasSize(10));
    assertThat(runningList, hasItem(1));
    assertThat(runningList, hasItem(10));
    assertThat(runningList, not(hasItem(11)));
  }

  @Test
  public void shouldIgnoreNegativeLimit() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, -10));

    List<Integer> runningList = udaf.initialize();
    for (int i = 1; i <= 25; i++) {
      runningList = udaf.aggregate(i, runningList);
    }

    assertThat(runningList, hasSize(25));
  }

  @Test
  public void shouldUndo() {
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    final Integer[] values = new Integer[] {3, 4, 5, 3};
    List<Integer> runningList = udaf.initialize();
    for (final Integer i : values) {
      runningList = udaf.aggregate(i, runningList);
    }
    runningList = udaf.undo(4, runningList);
    assertThat(runningList, contains(3, 5, 3));
    runningList = udaf.undo(3, runningList);
    assertThat(runningList, contains(3, 5));
  }

  @Test
  public void shouldUndoAfterHittingLimit() {
    final int limit = 10;
    final TableUdaf<Integer, List<Integer>, List<Integer>> udaf = CollectListUdaf.createCollectListInt();
    ((Configurable) udaf).configure(ImmutableMap.of(CollectListUdaf.LIMIT_CONFIG, limit));
    List<Integer> runningList = udaf.initialize();
    for (int i = 0; i < limit ; i++) {
      runningList = udaf.aggregate(i, runningList);
    }
    runningList = udaf.aggregate(limit  + 1, runningList);
    assertThat(limit  + 1, not(isIn(runningList)));
    runningList = udaf.undo(limit  + 1, runningList);
    assertThat(limit  + 1, not(isIn(runningList)));
  }

}
