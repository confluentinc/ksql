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

package io.confluent.ksql.parser.tree;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SingleColumnTest {

  private static final Optional<NodeLocation> A_LOCATION = Optional.empty();
  private static final Expression AN_EXPRESSION = new StringLiteral("foo");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowIfAliasIsSystemColumnName() {
    // Expect:
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage("is a reserved token for implicit column.");

    // When:
    new SingleColumn(A_LOCATION, AN_EXPRESSION, Optional.of(SchemaUtil.WINDOWSTART_NAME));
  }
}