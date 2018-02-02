///**
// * Copyright 2017 Confluent Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// **/
//
//package io.confluent.ksql.function.udaf.topk;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.junit.Assert.assertThat;
//
//public class StringTopkKudafTest {
//
//  String[] valueArray;
//  @Before
//  public void setup() {
//    valueArray = new String[]{"10", "ab", "cde", "efg", "aa", "32", "why", "How are you", "Test",
//                              "123", "432"};
//
//  }
//
//  @Test
//  public void shouldAggregateTopK() {
//    TopkKudaf<String> stringTopkKudaf = new TopkKudaf(0, 3, String.class);
//    String[] currentVal = new String[]{null, null, null};
//    for (String s: valueArray) {
//      currentVal = stringTopkKudaf.aggregate(s, currentVal);
//    }
//
//    assertThat("Invalid results.", currentVal, equalTo(new String[]{"why", "efg", "cde"}));
//  }
//
//  @Test
//  public void shouldAggregateTopKWithLessThanKValues() {
//    TopkKudaf<String> stringTopkKudaf = new TopkKudaf(0, 3, String.class);
//    String[] currentVal = new String[]{null, null, null};
//    currentVal = stringTopkKudaf.aggregate("why", currentVal);
//
//    assertThat("Invalid results.", currentVal, equalTo(new String[]{"why", null, null}));
//  }
//
//  @Test
//  public void shouldMergeTopK() {
//    TopkKudaf<String> stringTopkKudaf = new TopkKudaf(0, 3, String.class);
//    String[] array1 = new String[]{"123", "Hello", "paper"};
//    String[] array2 = new String[]{"Hi", "456", "Zzz"};
//
//    assertThat("Invalid results.", stringTopkKudaf.getMerger().apply("key", array1, array2), equalTo(
//        new String[]{"paper", "Zzz", "Hi"}));
//  }
//
//  @Test
//  public void shouldMergeTopKWithNulls() {
//    TopkKudaf<String> stringTopkKudaf = new TopkKudaf(0, 3, String.class);
//    String[] array1 = new String[]{"50", "45", null};
//    String[] array2 = new String[]{"60", null, null};
//
//    assertThat("Invalid results.", stringTopkKudaf.getMerger().apply("key", array1, array2), equalTo(
//        new String[]{"60", "50", "45"}));
//  }
//
//  @Test
//  public void shouldMergeTopKWithMoreNulls() {
//    TopkKudaf<String> stringTopkKudaf = new TopkKudaf(0, 3, String.class);
//    String[] array1 = new String[]{"50", null, null};
//    String[] array2 = new String[]{"60", null, null};
//
//    assertThat("Invalid results.", stringTopkKudaf.getMerger().apply("key", array1, array2), equalTo(
//        new String[]{"60", "50", null}));
//  }
//}
