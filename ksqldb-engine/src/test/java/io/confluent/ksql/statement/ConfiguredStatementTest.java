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

package io.confluent.ksql.statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredStatementTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());
  @Mock
  private PreparedStatement<? extends Statement> prepared;

  @Test
  public void shouldTakeDefensiveCopyOfProperties() {
    // Given:
    final Map<String, Object> props = new HashMap<>();
    props.put("this", "that");

    final ConfiguredStatement<? extends Statement> statement = ConfiguredStatement
        .of(prepared, props, CONFIG);

    // When:
    props.put("other", "thing");

    // Then:
    assertThat(statement.getOverrides(), is(ImmutableMap.of("this", "that")));
  }

  @Test
  public void shouldReturnImmutableProperties() {
    // Given:
    final ConfiguredStatement<? extends Statement> statement = ConfiguredStatement
        .of(prepared, new HashMap<>(), CONFIG);

    // Then:
    assertThat(statement.getOverrides(), is(instanceOf(ImmutableMap.class)));
  }
}