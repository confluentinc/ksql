/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.structured.QueryContext;
import org.junit.Test;

public class QueryLoggerUtilTest {
  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker(new QueryId("queryid"));

  @Test
  public void shouldBuildCorrectName() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(
        contextStacker.push("biz", "baz").getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid.biz.baz"));
  }

  @Test
  public void shouldBuildCorrectNameWhenNoSubhierarchy() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(contextStacker.getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid"));
  }
}
