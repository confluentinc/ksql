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

package io.confluent.ksql.parser.properties.with;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.properties.with.InsertIntoConfigs;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class InsertIntoPropertiesTest {
  @Test
  public void shouldReturnOptionalEmptyForMissingProps() {
    // When:
    final InsertIntoProperties properties = InsertIntoProperties.from(Collections.emptyMap());

    // Then:
    assertThat(properties.getQueryId(), is(Optional.empty()));
  }

  @Test
  public void shoulSetQueryId() {
    // When:
    final InsertIntoProperties properties = InsertIntoProperties.from(
        ImmutableMap.<String, Literal>builder()
            .put(InsertIntoConfigs.QUERY_ID_PROPERTY, new StringLiteral("my_insert_query_id"))
            .build());

    // Then:
    assertThat(properties.getQueryId(), is(Optional.of("MY_INSERT_QUERY_ID")));
  }
}
