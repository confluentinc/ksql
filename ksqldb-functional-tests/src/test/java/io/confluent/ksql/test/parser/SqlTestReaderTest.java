/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.test.parser.SqlTestReader;
import io.confluent.ksql.test.parser.TestDirective;
import io.confluent.ksql.test.parser.TestDirective.Type;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.test.parser.TestStatement;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SqlTestReaderTest {

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private MetaStore metaStore;
  @Mock
  private CreateStream cs;
  @Mock
  private CreateStreamAsSelect csas;
  @Mock
  private InsertValues iv;

  @Before
  public void setUp() {
    when(engine.getMetaStore()).thenReturn(metaStore);
  }

  @Test
  public void shouldParseBasicTest() {
    // Given:
    final String contents = ""
        + "--@test: test1\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "CREATE STREAM bar AS SELECT * FROM foo;\n"
        + "INSERT INTO foo (id, col1) VALUES (1, 1);\n"
        + "ASSERT VALUES bar (id, col1) VALUES (1, 1);\n"
        ;

    when(engine.prepare(any()))
        .thenReturn((PreparedStatement) PreparedStatement.of("foo", cs))
        .thenReturn(PreparedStatement.of("foo", csas))
        .thenReturn(PreparedStatement.of("foo", iv));

    // When:
    final SqlTestReader reader = new SqlTestReader(contents, engine);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1"))));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getEngineStatement().getStatement(), instanceOf(CreateStream.class));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getEngineStatement().getStatement(), instanceOf(CreateStreamAsSelect.class));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getEngineStatement().getStatement(), instanceOf(InsertValues.class));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getAssertStatement(), instanceOf(AssertValues.class));
    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldReadDirectivesAtEnd() {
    final String contents = ""
        + "--@test: test1\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "--@foo: bar\n";

    when(engine.prepare(any())).thenReturn((PreparedStatement) PreparedStatement.of("foo", cs));

    // When:
    final SqlTestReader reader = new SqlTestReader(contents, engine);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1"))));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getEngineStatement().getStatement(), instanceOf(CreateStream.class));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.UNKNOWN, "bar"))));
    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldIgnoreComments() {
    final String contents = ""
        + "--@test: test1\n"
        + "--foo\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "--bar\n";

    when(engine.prepare(any())).thenReturn((PreparedStatement) PreparedStatement.of("foo", cs));

    // When:
    final SqlTestReader reader = new SqlTestReader(contents, engine);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1"))));
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next().getEngineStatement().getStatement(), instanceOf(CreateStream.class));
    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldThrowOnInvalidStatement() {
    final String contents = ""
        + "CREATE foo;\n";

    // When:
    final SqlTestReader reader = new SqlTestReader(contents, engine);
    final ParsingException parsingException = assertThrows(ParsingException.class, reader::next);

    // Then:
    assertThat(parsingException.getMessage(), is("line 1:8: no viable alternative at input 'CREATE foo'"));
  }
}
