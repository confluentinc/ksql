/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.codegen.helpers;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ArrayGetTest {

  private final List<String> sampleList = ImmutableList.of("one", "two", "three");

  @Test
  public void shouldGetCorrectItemForLegacy() {
    final Object item = ArrayGet.getItem(sampleList, 1, true);
    assertThat(item, instanceOf(String.class));
    assertThat((String) item, equalTo("two"));
  }

  @Test
  public void shouldGetCorrectItem() {
    final Object item = ArrayGet.getItem(sampleList, 1, false);
    assertThat(item, instanceOf(String.class));
    assertThat((String) item, equalTo("one"));
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void shouldFailForOutOfBound() {
    final Object item = ArrayGet.getItem(sampleList, 5, false);
    Assert.fail();
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void shouldFailForNegativeIndex() {
    final Object item = ArrayGet.getItem(sampleList, -1, false);
    Assert.fail();
  }

}
