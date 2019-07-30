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

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyName;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateStreamCommandTest {

  private static final String STREAM_NAME = "s1";
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, "ID", new Type(SqlTypes.BIGINT)),
      new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING))
  );

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private CreateStream createStreamStatement;
  @Mock
  private KsqlConfig ksqlConfig;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

  private CreateStreamCommand cmd;

  @Before
  public void setUp() {
    when(createStreamStatement.getName()).thenReturn(QualifiedName.of(STREAM_NAME));
    when(createStreamStatement.getElements()).thenReturn(SOME_ELEMENTS);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);

    givenPropertiesWith((Collections.emptyMap()));
  }

  @Test
  public void shouldThrowIfAlreadyRegistered() {
    // Given:
    cmd.run(metaStore);

    // Then:
    expectedException.expectMessage("Cannot add stream 's1': A stream with the same name already exists");

    // When:
    cmd.run(metaStore);
  }

  @Test
  public void shouldAddSourceWithKeyField() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        "KEY", new StringLiteral("id")));

    // When:
    cmd.run(metaStore);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName("ID"));
    assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasLegacyName("ID"));
  }

  @Test
  public void shouldAddSourceWithNoKeyField() {
    // When:
    cmd.run(metaStore);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName(Optional.empty()));
    assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasLegacyName(Optional.empty()));
  }

  private static Map<String, Literal> getInitialProps(final Map<String, Literal> props) {
    final Map<String, Literal> allProps = new HashMap<>(props);
    allProps.putIfAbsent(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("Json"));
    allProps.putIfAbsent(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some-topic"));
    return allProps;
  }

  private void givenPropertiesWith(final Map<String, Literal> props) {
    when(createStreamStatement.getProperties()).thenReturn(CreateSourceProperties.from(getInitialProps(props)));

    cmd = new CreateStreamCommand(
        "some sql",
        createStreamStatement,
        ksqlConfig,
        serviceContext
    );
  }
}