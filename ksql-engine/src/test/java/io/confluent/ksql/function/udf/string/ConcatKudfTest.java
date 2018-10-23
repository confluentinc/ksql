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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udf.KudfTester;
import org.junit.Before;
import org.junit.Test;

public class ConcatKudfTest {

  private ConcatKudf udf;

  @Before
  public void setUp() {
    udf = new ConcatKudf();
  }

  @Test
  public void shouldBeWellBehavedUdf() {
    new KudfTester(ConcatKudf::new)
        .withArgumentTypes(Object.class, Object.class)
        .withUnboundedMaxArgCount()
        .test();
  }

  @Test
  public void shouldConcatStrings() {
    assertThat(udf.evaluate("Hello", " Mum"), is("Hello Mum"));
  }

  @Test
  public void shouldConcatNonStrings() {
    assertThat(udf.evaluate(1.345, 34), is("1.34534"));
  }

  @Test
  public void shouldConcatIgnoringNulls() {
    assertThat(
        udf.evaluate(null, "this ", null, "should ", null, "work!", null),
        is("this should work!"));
  }

  @Test
  public void shouldReturnEmptyStringIfAllArgsNull() {
    assertThat(udf.evaluate(null, null), is(""));
  }
}