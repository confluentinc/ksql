/*
 * Copyright 2018 Confluent Inc.
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

import static org.easymock.MockType.NICE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class CreateStreamCommandTest {

  @Mock
  private KafkaTopicClient topicClient;
  @Mock(NICE)
  private CreateStream createStreamStatement;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldDefaultToStringKeySerde() {
    // Given:
    givenProperties(propsWith(ImmutableMap.of()));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde, is(instanceOf(Serdes.String().getClass())));
  }

  @Test
  public void shouldExtractSessionWindowType() {
    // Given:
    givenProperties(propsWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("SeSSion"))));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.sessionWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldExtractHoppingWindowType() {
    // Given:
    givenProperties(propsWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("HoPPing"))));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldExtractTumblingWindowType() {
    // Given:
    givenProperties(propsWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("Tumbling"))));

    // When:
    final CreateStreamCommand cmd = createCmd();

    // Then:
    assertThat(cmd.keySerde,
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  @Test
  public void shouldThrowOnUnknownWindowType() {
    // Given:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("WINDOW_TYPE property is not set correctly. "
        + "value: UNKNOWN, validValues: [SESSION, TUMBLING, HOPPING]");

    givenProperties(propsWith(ImmutableMap.of(
        DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral("Unknown"))));

    // When:
    createCmd();
  }

  @Test
  public void shouldThrowOnOldWindowProperty() {
    // Given:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid config variable in the WITH clause: WINDOWED");

    givenProperties(propsWith(ImmutableMap.of(
        "WINDOWED", new BooleanLiteral("true"))));

    // When:
    createCmd();
  }

  private CreateStreamCommand createCmd() {
    return new CreateStreamCommand("some sql", createStreamStatement, topicClient);
  }

  private static Map<String, Expression> propsWith(final Map<String, Expression> props) {
    Map<String, Expression> valid = new HashMap<>(props);
    valid.putIfAbsent(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("Json"));
    valid.putIfAbsent(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some-topic"));
    return valid;
  }

  private void givenProperties(final Map<String, Expression> props) {
    EasyMock.expect(createStreamStatement.getProperties()).andReturn(props).anyTimes();
    EasyMock.expect(createStreamStatement.getName()).andReturn(QualifiedName.of("name")).anyTimes();
    EasyMock.expect(createStreamStatement.getElements()).andReturn(ImmutableList.of());
    EasyMock.replay(createStreamStatement);
  }
}