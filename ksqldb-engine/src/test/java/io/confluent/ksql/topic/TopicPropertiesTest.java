/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.topic;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.topic.TopicProperties.Builder;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

public class TopicPropertiesTest {

  @Test
  public void shouldPreferWithClauseToSourceReplicas() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withWithClause(Optional.of("name"), Optional.empty(), Optional.of((short) 3))
        .withSource(() -> new TopicDescription(
            "",
            false,
            ImmutableList.of(
                new TopicPartitionInfo(
                    0, new Node(0, "", 0), ImmutableList.of(new Node(0, "", 0)), ImmutableList.of()))))
        .build();

    // Then:
    assertThat(properties.getReplicas(), is((short) 3));
    assertThat(properties.getPartitions(), is(1));
  }

  @Test
  public void shouldPreferWithClauseToSourcePartitions() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withWithClause(Optional.of("name"), Optional.of(3), Optional.empty())
        .withSource(() -> new TopicDescription(
            "",
            false,
            ImmutableList.of(
                new TopicPartitionInfo(
                    0, new Node(0, "", 0), ImmutableList.of(new Node(0, "", 0)), ImmutableList.of()))))
        .build();

    // Then:
    assertThat(properties.getReplicas(), is((short) 1));
    assertThat(properties.getPartitions(), is(3));
  }

  @Test
  public void shouldUseNameFromWithClause() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withWithClause(
            Optional.of("name"),
            Optional.of(1),
            Optional.empty()
        )
        .build();

    // Then:
    assertThat(properties.getTopicName(), equalTo("name"));
  }

  @Test
  public void shouldUseNameFromWithClauseWhenNameIsAlsoPresent() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withName("oh no!")
        .withWithClause(
            Optional.of("name"),
            Optional.of(1),
            Optional.empty()
        )
        .build();

    // Then:
    assertThat(properties.getTopicName(), equalTo("name"));
  }

  @Test
  public void shouldUseNameIfNoWIthClause() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withName("name")
        .withWithClause(Optional.empty(), Optional.of(1), Optional.empty())
        .build();

    // Then:
    assertThat(properties.getTopicName(), equalTo("name"));
  }

  @Test
  public void shouldFailIfNoNameSupplied() {
    // When:
    final Exception e = assertThrows(
        NullPointerException.class,
        () -> new TopicProperties.Builder()
            .build()
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Was not supplied with any valid source for topic name!"));
  }

  @Test
  public void shouldFailIfEmptyNameSupplied() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new TopicProperties.Builder()
            .withName("")
            .build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Must have non-empty topic name."));
  }

  @Test
  public void shouldFailIfNoPartitionsSupplied() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new TopicProperties.Builder()
            .withName("name")
            .withWithClause(empty(), empty(), of((short) 1))
            .build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot determine partitions for creating topic"));
  }

  @Test
  public void shouldDefaultIfNoReplicasSupplied() {
    // Given:
    // When:
    final TopicProperties properties = new Builder()
        .withName("name")
        .withWithClause(Optional.empty(), Optional.of(1), Optional.empty())
        .build();

    // Then:
    assertThat(properties.getReplicas(), is(TopicProperties.DEFAULT_REPLICAS));
  }

  @Test
  public void shouldNotMakeRemoteCallIfUnnecessary() {
    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withWithClause(
            Optional.of("name"),
            Optional.of(1),
            Optional.of((short) 1)
        )
        .withSource(() -> {
          throw new RuntimeException();
        })
        .build();

    // Then:
    assertThat(properties.getPartitions(), equalTo(1));
    assertThat(properties.getReplicas(), equalTo((short) 1));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotMakeMultipleRemoteCalls() {
    // Given:
    final Supplier<TopicDescription> source = mock(Supplier.class);
    when(source.get())
        .thenReturn(
            new TopicDescription(
                "",
                false,
                ImmutableList.of(
                    new TopicPartitionInfo(
                        0,
                        null,
                        ImmutableList.of(new Node(1, "", 1)),
                        ImmutableList.of()))))
        .thenThrow();

    // When:
    final TopicProperties properties = new TopicProperties.Builder()
        .withName("name")
        .withSource(source)
        .build();

    // Then:
    assertThat(properties.getPartitions(), equalTo(1));
    assertThat(properties.getReplicas(), equalTo((short) 1));
  }
}
