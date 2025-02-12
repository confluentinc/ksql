/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;

import org.mockito.junit.MockitoRule;

public class AssertSchemaTest {

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final Optional<Integer> SOME_ID = Optional.of(25);
  private static final Optional<String> SOME_SUBJECT = Optional.of("subject");
  private static final WindowTimeClause SOME_TIMEOUT = new WindowTimeClause(5, TimeUnit.SECONDS);

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), SOME_SUBJECT, SOME_ID, Optional.of(SOME_TIMEOUT), true),
            new AssertSchema(Optional.of(new NodeLocation(1, 1)), SOME_SUBJECT, SOME_ID, Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), Optional.empty(), SOME_ID, Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), Optional.of("another subject"), SOME_ID, Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), SOME_SUBJECT, Optional.empty(), Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), SOME_SUBJECT, Optional.of(33), Optional.of(SOME_TIMEOUT), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), SOME_SUBJECT, SOME_ID, Optional.empty(), true))
        .addEqualityGroup(
            new AssertSchema(Optional.empty(), SOME_SUBJECT, SOME_ID, Optional.of(SOME_TIMEOUT), false))
        .testEquals();
  }

}
