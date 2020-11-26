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

package io.confluent.ksql.naming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlException;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class CaseSensitiveSourceTopicNamingStrategyTest {

  private CaseSensitiveSourceTopicNamingStrategy strategy;

  @Before
  public void setUp() {
    strategy = new CaseSensitiveSourceTopicNamingStrategy();
  }

  @Test
  public void shouldThrowOnNoMatch() {
    // Given:
    final SourceName sourceName = SourceName.of("Bob");
    final Set<String> topicNames = ImmutableSet.of("bob");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> strategy.resolveExistingTopic(sourceName, topicNames)
    );

    // Then:
    assertThat(e.getMessage(), is("No existing topic named `Bob` (case-sensitive)"
        + System.lineSeparator()
        + "You can specify an explicit existing topic name by setting 'KAFKA_TOPIC' in the WITH clause. "
        + "Alternatively, if you intended to create a new topic, set 'PARTITIONS' in the WITH clause."));
  }

  @Test
  public void shouldReturnSingleMatch() {
    // Given:
    final SourceName sourceName = SourceName.of("Bob");
    final Set<String> topicNames = ImmutableSet.of("bob", "Bob", "BOB");

    // When:
    final String result = strategy.resolveExistingTopic(sourceName, topicNames);

    // Then:
    assertThat(result, is("Bob"));
  }
}