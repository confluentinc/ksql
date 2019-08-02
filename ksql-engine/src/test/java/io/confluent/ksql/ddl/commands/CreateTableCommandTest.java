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
import io.confluent.ksql.parser.tree.CreateTable;
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
public class CreateTableCommandTest {

  private static final String TABLE_NAME = "t1";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private CreateTable createTableStatement;
  @Mock
  private KsqlConfig ksqlConfig;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

  @Before
  public void setUp() {
    givenPropertiesWith((Collections.emptyMap()));
    when(createTableStatement.getName()).thenReturn(QualifiedName.of(TABLE_NAME));
    when(createTableStatement.getElements()).thenReturn(TableElements.of(
        new TableElement(Namespace.VALUE, "SOME-KEY", new Type(SqlTypes.STRING))
    ));
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
  }

  @Test
  public void shouldThrowIfAlreadyRegistered() {
    // Given:
    final CreateTableCommand cmd = createCmd();
    cmd.run(metaStore);

    // Then:
    expectedException.expectMessage("Cannot add table 't1': A table with the same name already exists");

    // When:
    cmd.run(metaStore);
  }

  @Test
  public void shouldAddSourceWithKeyField() {
    // Given:
    givenPropertiesWith(ImmutableMap.of(
        "KEY", new StringLiteral("some-key")));
    final CreateTableCommand cmd = createCmd();

    // When:
    cmd.run(metaStore);

    // Then:
    assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName("SOME-KEY"));
    assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasLegacyName("SOME-KEY"));
  }

  @Test
  public void shouldAddSourceWithNoKeyField() {
    // Given:
    final CreateTableCommand cmd = createCmd();

    // When:
    cmd.run(metaStore);

    // Then:
    assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName(Optional.empty()));
    assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasLegacyName(Optional.empty()));
  }


  private CreateTableCommand createCmd() {
    return new CreateTableCommand(
        "some sql",
        createTableStatement,
        ksqlConfig,
        serviceContext);
  }

  private void givenPropertiesWith(final Map<String, Literal> props) {
    final Map<String, Literal> allProps = new HashMap<>(props);
    allProps.putIfAbsent(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("Json"));
    allProps.putIfAbsent(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("some-topic"));
    when(createTableStatement.getProperties()).thenReturn(CreateSourceProperties.from(allProps));
  }
}