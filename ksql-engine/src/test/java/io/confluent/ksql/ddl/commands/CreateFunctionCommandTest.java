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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.KudfTester;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.CreateFunction;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateFunctionCommandTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

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
    givenFunctionParameters(ImmutableList.of(
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
    assertNotNull(getUdfFactory("multiply"));
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

    // When:
    // (add function twice, but with replace flag)
    cmd.run(metaStore);
    cmd.run(metaStore);

    // Then:
    // No exception
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

  @Test
  public void shouldRecognizeInlineFunction() {
    // When:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    assertTrue(getUdfFactory("multiply").isInline());
  }

  @Test
  public void shouldDoBasicInlineMath() {
    // When:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    List<Schema> paramTypes = new ArrayList<>();
    paramTypes.add(Schema.OPTIONAL_INT32_SCHEMA);
    paramTypes.add(Schema.OPTIONAL_INT32_SCHEMA);

    Kudf udf = getUdfFactory("multiply").getFunction(paramTypes).newInstance(ksqlConfig);
    assertThat(udf.evaluate(3, 11), is(33));
    assertThat(udf.evaluate(4, 2), is(8));
  }

  @Test
  public void shouldDoBasicStringTransformation() {
    // Given:
    givenDescription("reverse a string");
    givenFunctionName("reverse");
    givenReturnType(PrimitiveType.of(SqlType.STRING));
    givenFunctionParameters(ImmutableList.of(
        new TableElement("SOURCE", PrimitiveType.of(SqlType.STRING))
    ));
    givenScript("return new StringBuilder(SOURCE).reverse().toString();");

    // When:
    final CreateFunctionCommand cmd = createCmd();

    cmd.run(metaStore);

    // Then:
    List<Schema> paramTypes = new ArrayList<>();
    paramTypes.add(Schema.OPTIONAL_STRING_SCHEMA);

    Kudf udf = getUdfFactory("reverse").getFunction(paramTypes).newInstance(ksqlConfig);
    assertThat(udf.evaluate("hello"), is("olleh"));
    assertThat(udf.evaluate("world"), is("dlrow"));
  }

  private UdfFactory getUdfFactory(final String functionName) {
    return metaStore.getFunctionRegistry().getUdfFactory(functionName);
  }

  private CreateFunctionCommand createCmd() {
    return new CreateFunctionCommand(createFunctionStatement);
  }

  private void givenAuthor(String author) {
    when(createFunctionStatement.getAuthor()).thenReturn(author);
  }

  private void givenDescription(final String description) {
    when(createFunctionStatement.getDescription()).thenReturn(description);
    when(createFunctionStatement.getOverview()).thenReturn(description);
  }

  private void givenFunctionName(final String functionName) {
    when(createFunctionStatement.getName()).thenReturn(functionName);
  }

  private void givenFunctionParameters(final ImmutableList<TableElement> elements) {
    when(createFunctionStatement.getElements()).thenReturn(elements);
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