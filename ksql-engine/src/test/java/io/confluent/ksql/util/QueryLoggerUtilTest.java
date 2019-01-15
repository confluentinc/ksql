/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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

import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import org.junit.Test;

public class QueryLoggerUtilTest {
  private final QueryId queryId = new QueryId("queryid");
  private final PlanNodeId nodeId = new PlanNodeId("nodeid");

  @Test
  public void shouldBuildCorrectName() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(queryId, nodeId, "biz", "baz");

    // Then:
    assertThat(name, equalTo("queryid.nodeid.biz.baz"));
  }

  @Test
  public void shouldBuildCorrectNameWhenNoSubhierarchy() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(queryId, nodeId);

    // Then:
    assertThat(name, equalTo("queryid.nodeid"));
  }
}
