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

package io.confluent.ksql.cli.console.cmd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.cli.KsqlRequestExecutor;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RunScriptTest {

  private static final String FILE_CONTENT = "some scripts;" + System.lineSeparator() + "more;";
  private static final String WHITE_SPACE = "\t  ";

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlRequestExecutor requestExecutor;
  @Mock
  private Console console;
  private RunScript cmd;
  private StringWriter output;
  private File scriptFile;

  @Before
  public void setUp() throws Exception {
    cmd = new RunScript(console, requestExecutor);

    output = new StringWriter();
    when(console.writer()).thenReturn(new PrintWriter(output));

    scriptFile = TMP.newFile();
    Files.write(scriptFile.toPath(), FILE_CONTENT.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void shouldGetName() {
    assertThat(cmd.getName(), is("run"));
  }

  @Test
  public void shouldGetHelp() {
    // When:
    cmd.printHelp();

    // Then:
    assertThat(output.toString(), is(
        "run <path_to_sql_file>:" + System.lineSeparator()
            + "\tLoad and run the statements in the supplied file." + System.lineSeparator()
            + "\tNote: the file must be UTF-8 encoded." + System.lineSeparator()));
  }

  @Test
  public void shouldExecuteScript() {
    // When:
    cmd.execute(WHITE_SPACE + scriptFile.toString() + WHITE_SPACE);

    // Then:
    verify(requestExecutor).makeKsqlRequest(FILE_CONTENT);
  }

  @Test
  public void shouldHandlePathBeingWrappedInSingleQuotes() {
    // When:
    cmd.execute(WHITE_SPACE + "'" + scriptFile.toString() + "'" + WHITE_SPACE);

    // Then:
    verify(requestExecutor).makeKsqlRequest(FILE_CONTENT);
  }

  @Test
  public void shouldThrowIfFileDoesNotExist() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to read file: you-will-not-find-me");
    expectedException.expectCause(instanceOf(FileNotFoundException.class));

    // When:
    cmd.execute("you-will-not-find-me");
  }

  @Test
  public void shouldThrowIfDirectory() throws Exception {
    // Given:
    final File dir = TMP.newFolder();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to read file: " + dir.toString());
    expectedException.expectCause(hasMessage(containsString("Is a directory")));

    // When:
    cmd.execute(dir.toString());
  }
}