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

package io.confluent.support.metrics.utils;

import static org.junit.Assert.assertTrue;

import io.confluent.support.metrics.utils.Jitter;
import java.io.IOException;
import org.junit.Test;

public class JitterTest {
  @Test
  public void testAddOnePercentJitter() throws IOException {
    // Given
    long[] baseArray = {-2232, -1, 0, 99, 100, 101, 1000, 10000, 38742};

    // When/Then
    for (long base : baseArray) {
      long val = Jitter.addOnePercentJitter(base);
      assertTrue(base <= val);
      assertTrue(val <= base + Math.abs(base) / 100);
    }
  }
}
