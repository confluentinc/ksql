/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udf.json;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class IsJsonStringTest {
  private IsJsonString udf;

  @Before
  public void setUp() {
    udf = new IsJsonString();
  }

  @Test
  public void shouldInterpretNumber() {
    assertTrue(udf.check("1"));
  }

  @Test
  public void shouldInterpretString() {
    assertTrue(udf.check("\"abc\""));
  }

  @Test
  public void shouldInterpretNullString() {
    assertTrue(udf.check("null"));
  }

  @Test
  public void shouldInterpretArray() {
    assertTrue(udf.check("[1, 2, 3]"));
  }

  @Test
  public void shouldInterpretObject() {
    assertTrue(udf.check("{\"1\": 2}"));
  }

  @Test
  public void shouldNotInterpretUnquotedString() {
    assertFalse(udf.check("abc"));
  }

  @Test
  public void shouldNotInterpretEmptyString() {
    assertFalse(udf.check(""));
  }

  @Test
  public void shouldNotInterpretNull() {
    assertFalse(udf.check(null));
  }

  @Test
  public void shouldNotInterpretStringWithSyntaxErrors() {
    assertFalse(udf.check("{1:2]"));
  }
}