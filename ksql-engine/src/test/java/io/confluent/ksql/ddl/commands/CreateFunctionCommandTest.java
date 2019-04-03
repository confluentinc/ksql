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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
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
    givenAuthor("bob");
    givenDescription("multiply 2 numbers");
    givenFunctionName("multiply");
    givenLanguage("java");
    givenReturnType(PrimitiveType.of(SqlType.INTEGER));
    givenScript("return X * Y;");
    givenShouldReplace(false);
    givenVersion("0.1.0");
    givenFunctionParamaters(ImmutableList.of(
        new TableElement("X", PrimitiveType.of(SqlType.INTEGER)),
        new TableElement("Y", PrimitiveType.of(SqlType.INTEGER))
    ));
  }

  @Test
  public void shouldCreateFunction() {
    // When:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    assertNotNull(metaStore.getFunctionRegistry().getUdfFactory("multiply"));
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

  @Test
  public void shouldNotThrowExceptionIfReplacing() {
    // Given:
    givenShouldReplace(true);
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    expectedException.expect(KsqlException.class);

    // When:
    cmd.run(metaStore);
  }

  @Test
  public void shouldThrowExceptionIfInvalidLanguage() {
    // Given:
    givenLanguage("python");
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

  private void givenAuthor(String author) {
    when(createFunctionStatement.getAuthor()).thenReturn(author);
  }

  private void givenFunctionParamaters(final ImmutableList<TableElement> elements) {
    when(createFunctionStatement.getElements()).thenReturn(elements);
  }

  private void givenDescription(final String description) {
    when(createFunctionStatement.getDescription()).thenReturn(description);
    when(createFunctionStatement.getOverview()).thenReturn(description);
  }

  private void givenFunctionName(final String functionName) {
    when(createFunctionStatement.getName()).thenReturn(functionName);
  }

  private void givenLanguage(final String language) {
    when(createFunctionStatement.getLanguage()).thenReturn(language);
  }

  private void givenReturnType(final Type returnType) {
    when(createFunctionStatement.getReturnType()).thenReturn(
      LogicalSchemas.fromSqlTypeConverter().fromSqlType(returnType));
  }

  private void givenScript(final String script) {
    when(createFunctionStatement.getScript()).thenReturn(script);
  }

  private void givenShouldReplace(final boolean shouldReplace) {
    when(createFunctionStatement.shouldReplace()).thenReturn(shouldReplace);
  }

  private void givenVersion(final String version) {
    when(createFunctionStatement.getVersion()).thenReturn(version);
  }
}