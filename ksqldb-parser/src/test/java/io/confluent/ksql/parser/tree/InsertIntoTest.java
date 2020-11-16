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

package io.confluent.ksql.parser.tree;

import static io.confluent.ksql.properties.with.InsertIntoConfigs.QUERY_ID_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.properties.with.InsertIntoProperties;
import java.util.Optional;
import org.junit.Test;

public class InsertIntoTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final Query SOME_QUERY = mock(Query.class);
  private static final InsertIntoProperties NO_PROPS = InsertIntoProperties.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new InsertInto(SOME_NAME, SOME_QUERY),
            new InsertInto(SOME_NAME, SOME_QUERY),
            new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, NO_PROPS),
            new InsertInto(Optional.of(OTHER_LOCATION), SOME_NAME, SOME_QUERY, NO_PROPS)
        )
        .addEqualityGroup(
            new InsertInto(SourceName.of("jim"), SOME_QUERY)
        )
        .addEqualityGroup(
            new InsertInto(SOME_NAME, mock(Query.class))
        )
        .addEqualityGroup(
            new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY,
                InsertIntoProperties.from(
                    ImmutableMap.of(QUERY_ID_PROPERTY, new StringLiteral("insert1")))),
            new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY,
                InsertIntoProperties.from(
                    ImmutableMap.of(QUERY_ID_PROPERTY, new StringLiteral("insert1"))))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnOptionalQueryId() {
    // When:
    final InsertInto insertInto = new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY,
        InsertIntoProperties.from(
            ImmutableMap.of(QUERY_ID_PROPERTY, new StringLiteral("my_id"))));

    // Then:
    assertThat(insertInto.getQueryId(), is(Optional.of("MY_ID")));
  }
}