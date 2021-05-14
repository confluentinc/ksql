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

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class QueryIdTest {

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(new QueryId("matching"), new QueryId("MaTcHiNg"))
        .addEqualityGroup(new QueryId("different"))
        .testEquals();
  }

  @Test
  public void shouldBeCaseInsensitiveOnCommparison() {
    // When:
    final QueryId id = new QueryId("Mixed-Case-Id");

    // Then:
    assertThat(id, is(new QueryId("MIXED-CASE-ID")));
  }

  @Test
  public void shouldPreserveCase() {
    // When:
    final QueryId id = new QueryId("Mixed-Case-Id");

    // Then:
    assertThat(id.toString(), is("Mixed-Case-Id"));
  }
}
