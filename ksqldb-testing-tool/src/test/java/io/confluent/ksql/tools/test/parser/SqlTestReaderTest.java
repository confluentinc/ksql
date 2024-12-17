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

package io.confluent.ksql.tools.test.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.common.base.Charsets;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.tools.test.parser.SqlTestReader;
import io.confluent.ksql.tools.test.parser.TestDirective;
import io.confluent.ksql.tools.test.parser.TestDirective.Type;
import io.confluent.ksql.tools.test.parser.TestStatement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlTestReaderTest {

  @Rule
  public final TemporaryFolder temporaryFolder = KsqlTestFolder.temporaryFolder();

  private static final NodeLocation LOC = new NodeLocation(1, 1);

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

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1", LOC))));

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM foo")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM bar")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("INSERT INTO")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> fail("unexpected statement " + s),
        s -> assertThat(s, instanceOf(AssertValues.class)),
        s -> fail("unexpected statement " + s)
    );
  }

  @Test
  public void shouldHandleMultilineStatements() {
    final String contents = ""
        + "--@test: test1\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');"
        + "CREATE STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='b', value_format='json');";

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1", LOC))));

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM foo")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM bar")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldReadDirectivesAtEnd() {
    final String contents = ""
        + "--@test: test1\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "--@foo: bar\n";

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1", LOC))));

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM foo")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.UNKNOWN, "bar", LOC))));
    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldIgnoreComments() {
    final String contents = ""
        + "--@test: test1\n"
        + "--foo\n"
        + "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "--bar\n";

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1", LOC))));

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM foo")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(false));
  }

  @Test
  public void shouldThrowOnInvalidStatement() {
    final String contents = ""
        + "CREATE foo;\n";

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);
    final ParsingException parsingException = assertThrows(ParsingException.class, reader::next);

    // Then:
    assertThat(parsingException.getMessage(), is("line 1:8: Syntax error at line 1:8"));
    assertThat(parsingException.getUnloggedDetails(), is("line 1:8: no viable alternative at input 'CREATE foo'"));
  }

  @Test
  public void shouldReadRunScript() throws IOException {
    final String fileContents =
        "CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='json');\n"
        + "CREATE STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='b', value_format='json');";
    final Path runScript = Files.write(temporaryFolder.newFile().toPath(), fileContents.getBytes(Charsets.UTF_8));
    final String contents = ""
        + "--@test: test1\n"
        + "RUN SCRIPT '" + runScript.toString() + "';";

    // When:
    final SqlTestReader reader = SqlTestReader.of(contents);

    // Then:
    assertThat(reader.hasNext(), is(true));
    assertThat(reader.next(), is(TestStatement.of(new TestDirective(Type.TEST, "test1", LOC))));

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM foo")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(true));
    reader.next().consume(
        s -> assertThat(s.getMaskedStatementText(), containsString("CREATE STREAM bar")),
        s -> fail("unexpected statement " + s),
        s -> fail("unexpected statement " + s)
    );

    assertThat(reader.hasNext(), is(false));
  }
}
