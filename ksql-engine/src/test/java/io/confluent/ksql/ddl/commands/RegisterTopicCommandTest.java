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
package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RegisterTopicCommandTest {

  private static final String KSQL_TOPIC_NAME = "bob";
  private static final String KAFKA_TOPIC_NAME = "fred";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private CreateSourceProperties properties;
  @Mock
  private KsqlTopic topic;
  @Mock
  private SerdeFactories serdeFactories;
  @Mock
  private KsqlSerdeFactory serdeFactory;
  @Captor
  private ArgumentCaptor<KsqlTopic> topicCaptor;

  private RegisterTopicCommand cmd;

  @Before
  public void setUp() {
    when(properties.getValueFormat()).thenReturn(Format.JSON);
    when(properties.getKafkaTopic()).thenReturn(KAFKA_TOPIC_NAME);
    when(serdeFactories.create(Format.JSON, properties)).thenReturn(serdeFactory);

    cmd = new RegisterTopicCommand(KSQL_TOPIC_NAME, false, properties, serdeFactories);
  }

  @Test
  public void shouldThrowIfAlreadyRegistered() {
    // Given:
    when(metaStore.getTopic(KSQL_TOPIC_NAME)).thenReturn(topic);

    // Then:
    expectedException.expectMessage("A topic with name 'bob' already exists");

    // When:
    cmd.run(metaStore);
  }

  @Test
  public void shouldSetCorrectValueSerdeOnTopic() {
    // When:
    cmd.run(metaStore);

    // Then:
    verify(metaStore).putTopic(topicCaptor.capture());
    assertThat(topicCaptor.getValue().getValueSerdeFactory(), is(sameInstance(serdeFactory)));
  }
}
