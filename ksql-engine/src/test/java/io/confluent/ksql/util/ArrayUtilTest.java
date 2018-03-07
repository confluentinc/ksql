/*
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

package io.confluent.ksql.util;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class ArrayUtilTest {

  @Test
  public void shouldGetCorrectNullIndex() {
    Double[] doubles1 = new Double[]{10.0, null, null};
    Double[] doubles2 = new Double[]{null, null, null};
    Double[] doubles3 = new Double[]{10.0, 9.0, 8.0};

    assertThat(ArrayUtil.getNullIndex(doubles1), equalTo(1));
    assertThat(ArrayUtil.getNullIndex(doubles2), equalTo(0));
    assertThat(ArrayUtil.getNullIndex(doubles3), equalTo(-1));
  }

  @Test
  public void shouldCheckArrayItemsCorrectly() {
    Double[] doubles = new Double[]{10.0, null, null};
    Long[] longs = new Long[]{10L, 35L, 70L, null};
    Integer[] integers = new Integer[]{10, 35, 70, null};
    String[] strings = new String[]{"Hello", "hi", "bye", null};

    Assert.assertTrue(ArrayUtil.containsValue(10.0, doubles));
    Assert.assertTrue(ArrayUtil.containsValue(35L, longs));
    Assert.assertTrue(ArrayUtil.containsValue(70, integers));
    Assert.assertTrue(ArrayUtil.containsValue("hi", strings));
  }
}
