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

package io.confluent.ksql.api.client.impl;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import java.util.Optional;
import org.junit.Test;

public class QueryInfoImplTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PERSISTENT, "id", "sql", Optional.of("sink"), Optional.of("sink_topic")),
            new QueryInfoImpl(QueryType.PERSISTENT, "id", "sql", Optional.of("sink"), Optional.of("sink_topic"))
        )
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PUSH, "id", "sql", Optional.empty(), Optional.empty()),
            new QueryInfoImpl(QueryType.PUSH, "id", "sql", Optional.empty(), Optional.empty())
        )
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PERSISTENT, "other_id", "sql", Optional.of("sink"), Optional.of("sink_topic"))
        )
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PERSISTENT, "id", "other_sql", Optional.of("sink"), Optional.of("sink_topic"))
        )
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PERSISTENT, "id", "sql", Optional.of("other_sink"), Optional.of("sink_topic"))
        )
        .addEqualityGroup(
            new QueryInfoImpl(QueryType.PERSISTENT, "id", "sql", Optional.of("sink"), Optional.of("other_sink_topic"))
        )
        .testEquals();
  }

}