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

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class StringTopkDistinctKudafTest {

  String[] valueArray;
  private final TopkDistinctKudaf<String> stringTopkDistinctKudaf
          = new TopkDistinctKudaf<>(0, 3, Schema.STRING_SCHEMA, String.class);

  @Before
  public void setup() {
    valueArray = new String[]{"10", "30", "45", "10", "50", "60", "20", "60", "80", "35", "25",
                              "60", "80"};

  }

  @Test
  public void shouldAggregateTopK() {
    String[] currentVal = new String[]{null, null, null};
    for (String d: valueArray) {
      currentVal = stringTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new String[]{"80", "60", "50"}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    String[] currentVal = new String[]{null, null, null};
    currentVal = stringTopkDistinctKudaf.aggregate("80", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new String[]{"80", null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    String[] array1 = new String[]{"50", "45", "25"};
    String[] array2 = new String[]{"60", "50", "48"};

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new String[]{"60", "50", "48"}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    String[] array1 = new String[]{"50", "45", null};
    String[] array2 = new String[]{"60", null, null};

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new String[]{"60", "50", "45"}));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    String[] array1 = new String[]{"50", "45", null};
    String[] array2 = new String[]{"60", "50", null};

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new String[]{"60", "50", "45"}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    String[] array1 = new String[]{"60", null, null};
    String[] array2 = new String[]{"60", null, null};

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new String[]{"60", null, null}));
  }
}
