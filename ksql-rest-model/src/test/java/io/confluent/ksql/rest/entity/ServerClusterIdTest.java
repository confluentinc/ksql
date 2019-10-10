/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class ServerClusterIdTest {
  @Test
  public void shouldReturnServerClusterId() {
    // Given:
    final ServerClusterId serverClusterId = ServerClusterId.of("kafka1", "ksql1");

    // When:
    final String id = serverClusterId.getId();
    final Map<String, Object> scope = serverClusterId.getScope();

    // Then:
    assertThat(id, is(""));
    assertThat(
        scope,
        equalTo(ImmutableMap.of(
            "path", Collections.emptyList(),
            "clusters", ImmutableMap.of(
                "kafka-cluster", "kafka1",
                "ksql-cluster", "ksql1")
        ))
    );
  }
}
