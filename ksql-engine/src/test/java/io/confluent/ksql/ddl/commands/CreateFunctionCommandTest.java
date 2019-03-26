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

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateFunctionCommandTest {

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private CreateFunction createFunctionStatement;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());

  @Before
  public void setUp() {
    when(createFunctionStatement.getAuthor()).thenReturn("mitch");
    when(createFunctionStatement.getDescription()).thenReturn("multiply 2 numbers");
    when(createFunctionStatement.getOverview()).thenReturn("multiply 2 numbers");
    when(createFunctionStatement.getVersion()).thenReturn("0.1.0");
    when(createFunctionStatement.getAuthor()).thenReturn("mitch");
    when(createFunctionStatement.shouldReplace()).thenReturn(true);
    when(createFunctionStatement.shouldReplace()).thenReturn(true);
    when(createFunctionStatement.getName()).thenReturn("multiply");
    when(createFunctionStatement.getLanguage()).thenReturn("JAVA");
    when(createFunctionStatement.getReturnType()).thenReturn(
      LogicalSchemas.fromSqlTypeConverter().fromSqlType(PrimitiveType.of(SqlType.INTEGER)));
    when(createFunctionStatement.getScript()).thenReturn("return args[0] * args[1];");
    when(createFunctionStatement.shouldReplace()).thenReturn(true);
    when(createFunctionStatement.getElements()).thenReturn(ImmutableList.of(
        new TableElement("num1", PrimitiveType.of(SqlType.INTEGER)),
        new TableElement("num2", PrimitiveType.of(SqlType.INTEGER))
    ));
    when(topicClient.isTopicExists(any())).thenReturn(false);
  }

  @Test
  public void shouldCreateFunction() {
    // When:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    assertNotNull(metaStore.getFunctionRegistry().getUdfFactory("multiple"));
  }

  @Test
  public void shouldThrowExceptionIfFunctionExists() {
    // Given:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);
  
    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    cmd.run(metaStore);
  }

  private CreateFunctionCommand createCmd() {
    return new CreateFunctionCommand(createFunctionStatement);
  }
}