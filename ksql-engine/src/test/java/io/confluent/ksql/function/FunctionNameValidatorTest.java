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

package io.confluent.ksql.function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.parser.SqlBaseParser;
import org.antlr.v4.runtime.Vocabulary;
import org.junit.Test;

public class FunctionNameValidatorTest {

  private final FunctionNameValidator validator = new FunctionNameValidator();

  @Test
  public void shouldNotAllowJavaReservedWords() {
    // not exhaustive..
    assertFalse(validator.test("enum"));
    assertFalse(validator.test("static"));
    assertFalse(validator.test("final"));
    assertFalse(validator.test("do"));
    assertFalse(validator.test("while"));
    assertFalse(validator.test("double"));
    assertFalse(validator.test("float"));
    assertFalse(validator.test("private"));
    assertFalse(validator.test("public"));
    assertFalse(validator.test("goto"));
    assertFalse(validator.test("default"));
  }

  @Test
  public void shouldNotAllowKsqlReservedWordsExceptSubstringAndConcat() {
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int maxTokenType = vocabulary.getMaxTokenType();
    for(int i = 0; i < maxTokenType; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        if (symbolicName.equalsIgnoreCase("substring")
            || symbolicName.equalsIgnoreCase("concat")) {
          assertTrue(validator.test(symbolicName));
        } else {
          assertFalse(validator.test(symbolicName));
        }
      }
    }
  }

  @Test
  public void shouldNotAllowInvalidJavaIdentifiers() {
    assertFalse(validator.test("@foo"));
    assertFalse(validator.test("1foo"));
    assertFalse(validator.test("^foo"));
    assertFalse(validator.test("&foo"));
    assertFalse(validator.test("%foo"));
    assertFalse(validator.test("+foo"));
    assertFalse(validator.test("-foo"));
    assertFalse(validator.test("#foo"));
    assertFalse(validator.test("f1@%$"));
  }

  @Test
  public void shouldAllowValidJavaIdentifiers() {
    assertTrue(validator.test("foo"));
    assertTrue(validator.test("$foo"));
    assertTrue(validator.test("f1_b"));
    assertTrue(validator.test("__blah"));
  }
}
