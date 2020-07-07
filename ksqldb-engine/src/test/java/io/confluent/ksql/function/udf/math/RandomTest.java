/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.math;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class RandomTest {

  private Random udf;

  @Before
  public void setUp() {
    udf = new Random();
  }

  @Test
  public void shouldReturnDistinctValueEachInvocation() {
    int capacity = 1000;
    final Set<Double> outputs = new HashSet<>(capacity);
    for (int i = 0; i < capacity; i++) {
      outputs.add(udf.random());
    }
    assertThat(outputs, hasSize(capacity));
    assertThat(outputs, everyItem(greaterThanOrEqualTo(0.0)));
    assertThat(outputs, everyItem(lessThan(1.0)));
  }

}