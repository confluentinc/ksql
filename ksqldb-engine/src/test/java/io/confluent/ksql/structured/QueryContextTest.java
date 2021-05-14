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

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.context.QueryContext;
import java.io.IOException;
import org.junit.Test;

public class QueryContextTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker().push("node");
  private final QueryContext queryContext = contextStacker.getQueryContext();

  private static void assertQueryContext(
      final QueryContext queryContext,
      final String ...context) {
    if (context.length > 0) {
      assertThat(queryContext.getContext(), contains(context));
    } else {
      assertThat(queryContext.getContext(), empty());
    }
  }

  @Test
  public void shouldGenerateNewContextOnPush() {
    // When:
    final QueryContext.Stacker childContextStacker = contextStacker.push("child");
    final QueryContext childContext = childContextStacker.getQueryContext();
    final QueryContext grandchildContext = childContextStacker.push("grandchild").getQueryContext();

    // Then:
    assertThat(ImmutableSet.of(queryContext, childContext, grandchildContext), hasSize(3));
    assertQueryContext(queryContext, "node");
    assertQueryContext(childContext, "node", "child");
    assertQueryContext(grandchildContext, "node", "child", "grandchild");
  }

  @Test
  public void shouldSerializeCorrectly() throws IOException {
    // Given:
    final QueryContext context = contextStacker.push("child").getQueryContext();

    // When:
    final String serialized = MAPPER.writeValueAsString(context);

    // Then:
    assertThat(serialized, is("\"node/child\""));
  }

  @Test
  public void shouldDeserializeCorrectly() throws IOException {
    // When:
    final QueryContext deserialized = MAPPER.readValue("\"node/child\"", QueryContext.class);

    // Then:
    final QueryContext expected = contextStacker.push("child").getQueryContext();
    assertThat(deserialized, is(expected));
  }
}