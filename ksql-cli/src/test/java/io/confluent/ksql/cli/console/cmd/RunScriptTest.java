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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.KsqlRequestExecutor;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlRequestExecutor requestExecutor;
  private RunScript cmd;
  private File scriptFile;
  private PrintWriter terminal;

  @Before
  public void setUp() throws Exception {
    terminal = new PrintWriter(new StringWriter());

    cmd = RunScript.create(requestExecutor);

    scriptFile = TMP.newFile();
    Files.write(scriptFile.toPath(), FILE_CONTENT.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void shouldGetName() {
    assertThat(cmd.getName(), is("run script"));
  }

  @Test
  public void shouldGetHelp() {
    assertThat(cmd.getHelpMessage(), is(
        "run script <path_to_sql_file>:" + System.lineSeparator()
            + "\tLoad and run the statements in the supplied file." + System.lineSeparator()
            + "\tNote: the file must be UTF-8 encoded."));
  }

  @Test
  public void shouldThrowIfNoArgSupplied() {
    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Too few parameters");

    // When:
    cmd.execute(ImmutableList.of(), terminal);
  }

  @Test
  public void shouldThrowIfTooManyArgsSupplied() {
    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Too many parameters");

    // When:
    cmd.execute(ImmutableList.of("too", "many"), terminal);
  }

  @Test
  public void shouldExecuteScript() {
    // When:
    cmd.execute(ImmutableList.of(scriptFile.toString()), terminal);

    // Then:
    verify(requestExecutor).makeKsqlRequest(FILE_CONTENT);
  }

  @Test
  public void shouldThrowIfFileDoesNotExist() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to read file: you-will-not-find-me");
    expectedException.expectCause(instanceOf(NoSuchFileException.class));

    // When:
    cmd.execute(ImmutableList.of("you-will-not-find-me"), terminal);
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
    cmd.execute(ImmutableList.of(dir.toString()), terminal);
  }
}