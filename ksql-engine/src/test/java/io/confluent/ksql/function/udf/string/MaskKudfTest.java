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

import org.junit.Before;
import org.junit.Test;

import java.util.stream.IntStream;

import io.confluent.ksql.function.KsqlFunctionException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class MaskKudfTest {
  private final String JSON_DOC = "{"
                                  + "\"thing1\":{\"thing2\":\"hello\"},"
                                  + "\"array\":[101,102]"
                                  + "}";

  private MaskKudf udf;

  @Before
  public void setUp() {
    udf = new MaskKudf();
  }

  @Test
  public void shouldApplyAllDefaultTypeMasks() {
    final String result = udf.mask("AbCd#$123xy Z");
    assertThat(result, is("XxXx--nnnxx-X"));
  }

  @Test
  public void shouldApplyAllExplicitTypeMasks() {
    final String result = udf.mask("AbCd#$123xy Z", 'Q', 'q', '9', '@');
    assertThat(result, is("QqQq@@999qq@Q"));
  }

//  @Test(expected = KsqlFunctionException.class)
//  public void shouldThrowIfTooFewParameters() {
//    udf.evaluate(JSON_DOC);
//  }
//
//  @Test(expected = KsqlFunctionException.class)
//  public void shouldThrowIfTooManyParameters() {
//    udf.evaluate(JSON_DOC, "$.thing1", "extra");
//  }
//
//  @Test(expected = KsqlFunctionException.class)
//  public void shouldThrowOnInvalidJsonDoc() {
//    udf.evaluate("this is NOT a JSON doc", "$.thing1");
//  }
//
//  @Test
//  public void shouldBeThreadSafe() {
//    IntStream.range(0, 10_000)
//        .parallel()
//        .forEach(idx -> shouldExtractJsonField());
//  }
}