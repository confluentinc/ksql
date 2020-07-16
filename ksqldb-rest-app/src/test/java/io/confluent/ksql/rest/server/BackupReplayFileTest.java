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

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class BackupReplayFileTest {
  private static final String KEY_VALUE_SEPARATOR = ":";
  private static final String REPLAY_FILE_NAME = "backup_command_topic_1";

  @Rule
  public TemporaryFolder backupLocation = new TemporaryFolder();

  private BackupReplayFile replayFile;
  private File internalReplayFile;

  @Before
  public void setup() throws IOException {
    internalReplayFile = backupLocation.newFile(REPLAY_FILE_NAME);
    replayFile = new BackupReplayFile(internalReplayFile);
  }

  @Test
  public void shouldGetFilePath() {
    // When
    final String path = replayFile.getPath();

    // Then
    assertThat(path, is(String.format(
        "%s/%s", backupLocation.getRoot().getAbsolutePath(), REPLAY_FILE_NAME)));
  }

  @Test
  public void shouldWriteRecord() throws IOException {
    // Given
    final Pair<CommandId, Command> record = newStreamRecord("stream1");

    // When
    replayFile.write(record.left, record.right);

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\"}"
    ));
  }

  @Test
  public void shouldWriteListOfRecords() throws IOException {
    // Given
    final Pair<CommandId, Command> record1 = newStreamRecord("stream1");
    final Pair<CommandId, Command> record2 = newStreamRecord("stream2");

    // When
    replayFile.write(Arrays.asList(record1, record2));

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\"}"
    ));
    assertThat(commands.get(1), is(
        "\"stream/stream2/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream2 (id INT) WITH (kafka_topic='stream2')\"}"
    ));
  }

  @Test
  public void shouldWriteRecordWithNewLineCharacterInCommand() throws IOException {
    // Given
    final CommandId commandId1 = new CommandId(
        CommandId.Type.STREAM, "stream1", CommandId.Action.CREATE);
    final Command command1 = new Command(
        "CREATE STREAM stream1 (id INT, f\n1 INT) WITH (kafka_topic='topic1)",
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
    );

    // When
    replayFile.write(commandId1, command1);

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":"
            + "\"CREATE STREAM stream1 (id INT, f\\n1 INT) WITH (kafka_topic='topic1)\"}"
    ));
  }

  @Test
  public void shouldBeEmptyWhenReadAllCommandsFromEmptyFile() throws IOException {
    // When
    final List<?> commands = replayFile.readRecords();

    // Then
    assertThat(commands.size(), is(0));
  }

  @Test
  public void shouldReadCommands() throws IOException {
    // Given
    final Pair<CommandId, Command> record1 = newStreamRecord("stream1");
    final Pair<CommandId, Command> record2 = newStreamRecord("stream2");
    Files.write(internalReplayFile.toPath(),
        String.format("%s%s%s%n%s%s%s",
            "\"stream/stream1/create\"",
            KEY_VALUE_SEPARATOR,
            "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\"}",
            "\"stream/stream2/create\"",
            KEY_VALUE_SEPARATOR,
            "{\"statement\":\"CREATE STREAM stream2 (id INT) WITH (kafka_topic='stream2')\"}"
            ).getBytes(StandardCharsets.UTF_8));

    // When
    final List<Pair<CommandId, Command>> commands = replayFile.readRecords();

    // Then
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(record1.left));
    assertThat(commands.get(0).right, is(record1.right));
    assertThat(commands.get(1).left, is(record2.left));
    assertThat(commands.get(1).right, is(record2.right));
  }

  private Pair<CommandId, Command> newStreamRecord(final String streamName) {
    final CommandId commandId = new CommandId(
        CommandId.Type.STREAM, streamName, CommandId.Action.CREATE);
    final Command command = new Command(
        String.format("CREATE STREAM %s (id INT) WITH (kafka_topic='%s')", streamName, streamName),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
    );

    return new Pair<>(commandId, command);
  }
}
