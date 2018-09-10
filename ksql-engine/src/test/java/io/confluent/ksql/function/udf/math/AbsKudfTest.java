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
 */

package io.confluent.ksql.function.udf.math;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udf.KudfTester;
import org.junit.Before;
import org.junit.Test;

public class AbsKudfTest {

  private AbsKudf udf;

  @Before
  public void setUp() {
    udf = new AbsKudf();
  }

  @Test
  public void shouldBeWellBehavedUdf() {
    new KudfTester(AbsKudf::new)
        .withArgumentTypes(Number.class)
        .test();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.evaluate((Object)null), is(nullValue()));
  }

  @Test
  public void shouldAbs() {
    assertThat(udf.evaluate(-1.234), is(1.234));
    assertThat(udf.evaluate(5567), is(5567.0));
  }
}