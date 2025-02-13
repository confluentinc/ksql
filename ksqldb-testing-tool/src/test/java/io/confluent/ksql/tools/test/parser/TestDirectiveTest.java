/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.test.parser;

import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.tools.test.parser.TestDirective.Type;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.Token;
import org.hamcrest.Matchers;
import org.junit.Test;

public class TestDirectiveTest {

  private static final NodeLocation LOC = new NodeLocation(1, 1);

  @Test
  public void shouldParseKnownDirectives() {
    // Given:
    final Token tok = new CommonToken(0, "--@test: bar");

    // When:
    final TestDirective directive = DirectiveParser.parse(tok);

    // Then:
    assertThat(directive, Matchers.is(new TestDirective(Type.TEST, "bar", LOC)));
  }

  @Test
  public void shouldParseKnownDirectivesAtLocation() {
    // Given:
    final CommonToken tok = new CommonToken(0, "--@test: bar");
    tok.setLine(1);
    tok.setCharPositionInLine(10);

    // When:
    final TestDirective directive = DirectiveParser.parse(tok);

    // Then:
    assertThat(directive, Matchers.is(new TestDirective(Type.TEST, "bar", LOC)));
  }

  @Test
  public void shouldParseKnownDirectivesCaseInsensitive() {
    // Given:
    final Token tok = new CommonToken(0, "--@teST: bar");

    // When:
    final TestDirective directive = DirectiveParser.parse(tok);

    // Then:
    assertThat(directive, Matchers.is(new TestDirective(Type.TEST, "bar", LOC)));
  }

  @Test
  public void shouldParseUnknownDirectives() {
    // Given:
    final Token tok = new CommonToken(0, "--@foo: bar");

    // When:
    final TestDirective directive = DirectiveParser.parse(tok);

    // Then:
    assertThat(directive, Matchers.is(new TestDirective(Type.UNKNOWN, "bar", LOC)));
  }

}
