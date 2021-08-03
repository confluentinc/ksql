/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.test.util.KsqlTestFolder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalCommandsFileTest {
  private static final String FILE_NAME = "local_commands_1607381558333.cmds";
  private static final LocalCommand LOCAL_COMMAND1
      = new TransientQueryLocalCommand(
          "_confluent-ksql-default_transient_932097300573686369_1606940079718");

  private static final LocalCommand LOCAL_COMMAND2
      = new TransientQueryLocalCommand(
          "_confluent-ksql-default_transient_123457300573686369_1606940012343");

  @Rule
  public TemporaryFolder commandsDir = KsqlTestFolder.temporaryFolder();

  private LocalCommandsFile localCommandsFile;
  private File internalCommandsFile;

  @Before
  public void setup() throws IOException {
    internalCommandsFile = commandsDir.newFile(FILE_NAME);
    localCommandsFile = LocalCommandsFile.createWriteable(internalCommandsFile);
  }

  @Test
  public void shouldWriteRecord() throws IOException {
    // When
    localCommandsFile.write(LOCAL_COMMAND1);
    localCommandsFile.write(LOCAL_COMMAND2);

    // Then
    final List<String> commands = Files.readAllLines(internalCommandsFile.toPath());
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), is("{\"@type\":\"transient_query\",\"queryApplicationId\":"
        + "\"_confluent-ksql-default_transient_932097300573686369_1606940079718\"}"));
    assertThat(commands.get(1), is("{\"@type\":\"transient_query\",\"queryApplicationId\":"
        + "\"_confluent-ksql-default_transient_123457300573686369_1606940012343\"}"));
  }

  @Test
  public void shouldBeEmptyWhenReadAllCommandsFromEmptyFile() throws IOException {
    // When
    final List<LocalCommand> commands = localCommandsFile.readRecords();

    // Then
    assertThat(commands.size(), is(0));
  }

  @Test
  public void shouldReadCommands() throws IOException {
    // Given
    Files.write(internalCommandsFile.toPath(),
        ("{\"@type\":\"transient_query\",\"queryApplicationId\":"
            + "\"_confluent-ksql-default_transient_932097300573686369_1606940079718\"}\n")
            .getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND);
    Files.write(internalCommandsFile.toPath(),
        ("{\"@type\":\"transient_query\",\"queryApplicationId\":"
            + "\"_confluent-ksql-default_transient_123457300573686369_1606940012343\"}\n")
            .getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND);

    // When
    final List<LocalCommand> commands = localCommandsFile.readRecords();

    // Then
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).getType(), is(TransientQueryLocalCommand.TYPE));
    TransientQueryLocalCommand command = (TransientQueryLocalCommand) commands.get(0);
    assertThat(command.getQueryApplicationId(),
        is("_confluent-ksql-default_transient_932097300573686369_1606940079718"));
    assertThat(commands.get(1).getType(), is(TransientQueryLocalCommand.TYPE));
    command = (TransientQueryLocalCommand) commands.get(1);
    assertThat(command.getQueryApplicationId(),
        is("_confluent-ksql-default_transient_123457300573686369_1606940012343"));
  }
}