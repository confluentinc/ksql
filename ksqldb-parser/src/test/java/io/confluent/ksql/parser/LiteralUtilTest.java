/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class LiteralUtilTest {

  @Test
  public void shouldConvertBooleanToBoolean() {
    assertThat(LiteralUtil.toBoolean(new BooleanLiteral("true"), "bob"), is(true));
    assertThat(LiteralUtil.toBoolean(new BooleanLiteral("false"), "bob"), is(false));
  }

  @Test
  public void shouldConvertStringContainingBooleanToBoolean() {
    assertThat(LiteralUtil.toBoolean(new StringLiteral("tRuE"), "bob"), is(true));
    assertThat(LiteralUtil.toBoolean(new StringLiteral("faLSe"), "bob"), is(false));
  }

  @Test
  public void shouldThrowConvertingOtherLiteralTypesToBoolean() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> LiteralUtil.toBoolean(new LongLiteral(10), "bob")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Property 'bob' is not a boolean value"));
  }
}