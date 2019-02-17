/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.KsqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractCreateStreamCommandTest {

  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", new PrimitiveType(KsqlType.STRING)));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  @Mock
  private AbstractStreamCreateStatement statement;
  @Mock
  private KafkaTopicClient kafkaTopicClient;

  @Before
  public void setUp() {
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);
    when(statement.getName()).thenReturn(QualifiedName.of("bob"));
    when(statement.getProperties()).thenReturn(minValidProps());
    when(kafkaTopicClient.isTopicExists(any())).thenReturn(true);
  }

  @Test
  public void shouldThrowOnNoElements() {
    // Given:
    when(statement.getElements()).thenReturn(Collections.emptyList());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    new TestCmd("look mum, no columns", statement, kafkaTopicClient);
  }

  @Test
  public void shouldNotThrowWhenThereAreElements() {
    // Given:
    when(statement.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    new TestCmd("look mum, columns", statement, kafkaTopicClient);

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfValueFormatNotSupplied() {
    // Given:
    when(statement.getProperties()).thenReturn(propsWithout(DdlConfig.VALUE_FORMAT_PROPERTY));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Topic format(VALUE_FORMAT) should be set in WITH clause.");

    // When:
    new TestCmd("what, no value format?", statement, kafkaTopicClient);
  }

  @Test
  public void shouldThrowIfTopicNameNotSupplied() {
    // Given:
    when(statement.getProperties()).thenReturn(propsWithout(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Corresponding Kafka topic (KAFKA_TOPIC) should be set in WITH clause.");

    // When:
    new TestCmd("what, no value topic?", statement, kafkaTopicClient);
  }

  private static Map<String, Expression> minValidProps() {
    return ImmutableMap.of(
        DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("json"),
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some topic")
    );
  }

  private static Map<String, Expression> propsWithout(final String name) {
    final HashMap<String, Expression> props = new HashMap<>(minValidProps());
    final Expression removed = props.remove(name);
    assertThat("invalid test", removed, is(notNullValue()));
    return ImmutableMap.copyOf(props);
  }

  private static final class TestCmd extends AbstractCreateStreamCommand {

    private TestCmd(
        final String sqlExpression,
        final AbstractStreamCreateStatement statement,
        final KafkaTopicClient kafkaTopicClient
    ) {
      super(sqlExpression, statement, kafkaTopicClient);
    }

    @Override
    public DdlCommandResult run(final MutableMetaStore metaStore) {
      return null;
    }
  }
}