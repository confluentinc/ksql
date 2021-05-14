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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.query.QueryId;
import org.junit.Test;

public class QueryLoggerUtilTest {
  private final QueryId queryId = new QueryId("queryid");
  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker();

  @Test
  public void shouldBuildCorrectName() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(
        queryId,
        contextStacker.push("biz", "baz").getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid.biz.baz"));
  }

  @Test
  public void shouldBuildCorrectNameWhenNoSubhierarchy() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(queryId, contextStacker.getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid"));
  }
}
