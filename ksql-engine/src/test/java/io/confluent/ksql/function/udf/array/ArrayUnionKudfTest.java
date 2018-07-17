/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.function.udf.array;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import java.util.Arrays;
import java.util.List;

public class ArrayUnionKudfTest {
  private final ArrayUnionKudf udf = new ArrayUnionKudf();

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldUnionArraysOfLikeType() {
    final List result = udf.union(Arrays.asList("foo", " ", "bar"), Arrays.asList("baz"));
    assertThat(result, is(Arrays.asList("foo", " ", "bar", "baz")));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.union(null, null);
    assertThat(result, is(nullValue()));
  }

}
