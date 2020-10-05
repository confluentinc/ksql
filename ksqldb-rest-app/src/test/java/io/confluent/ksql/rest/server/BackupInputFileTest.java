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

import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackupInputFileTest {
  private static final String KEY_VALUE_SEPARATOR = ":";
  private static final String BACKUP_FILE_NAME = "backup_command_topic_1";

  @Rule
  public TemporaryFolder backupLocation = new TemporaryFolder();

  private File internalReplayFile;
  private BackupInputFile backupFile;

  @Before
  public void setup() throws IOException {
    internalReplayFile = backupLocation.newFile(BACKUP_FILE_NAME);
    backupFile = new BackupInputFile(internalReplayFile);
  }

  @Test
  public void shouldGetFileAndPaths() {
    // When/Then
    assertThat(backupFile.getFile(), is(internalReplayFile));
    assertThat(backupFile.getPath(), is(internalReplayFile.getPath()));
  }

  @Test
  public void shouldBeEmptyWhenReadAllCommandsFromEmptyFile() throws IOException {
    // When
    final List<?> commands = backupFile.readRecords();

    // Then
    assertThat(commands.size(), is(0));
  }

  @Test
  public void shouldReadAndParseCommandsFromTheFile() throws IOException {
    // Given
    Files.write(internalReplayFile.toPath(),
        String.format("%s%s%s%n%s%s%s",
            "\"stream/stream1/create\"",
            KEY_VALUE_SEPARATOR,
            "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\","
                + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}",
            "\"stream/stream2/create\"",
            KEY_VALUE_SEPARATOR,
            "{\"statement\":\"CREATE STREAM stream2 (id INT) WITH (kafka_topic='stream2')\","
                + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"
        ).getBytes(StandardCharsets.UTF_8));

    // When
    final List<Pair<byte[], byte[]>> commands = backupFile.readRecords();

    // Then
    assertThat(commands.size(), is(2));
    assertThat(new String(commands.get(0).left, StandardCharsets.UTF_8),
        is("\"stream/stream1/create\""));
    assertThat(new String(commands.get(0).right, StandardCharsets.UTF_8),
        is(
        "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\","
        + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"));
    assertThat(new String(commands.get(1).left, StandardCharsets.UTF_8),
        is("\"stream/stream2/create\""));
    assertThat(new String(commands.get(1).right, StandardCharsets.UTF_8),
        is(
        "{\"statement\":\"CREATE STREAM stream2 (id INT) WITH (kafka_topic='stream2')\","
        + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"));
  }
}
