/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.parser.SqlBaseParser;
import org.antlr.v4.runtime.Vocabulary;
import org.junit.Test;

public class ParserKeywordValidatorUtilTest {

  private final ParserKeywordValidatorUtil validator = new ParserKeywordValidatorUtil();

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
  public void shouldNotAllowKsqlReservedWordsExceptSubstringAndConcatAndReplace() {
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int maxTokenType = vocabulary.getMaxTokenType();
    for(int i = 0; i < maxTokenType; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        if (symbolicName.equalsIgnoreCase("substring")
            || symbolicName.equalsIgnoreCase("concat")
            || symbolicName.equalsIgnoreCase("replace")) {
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
