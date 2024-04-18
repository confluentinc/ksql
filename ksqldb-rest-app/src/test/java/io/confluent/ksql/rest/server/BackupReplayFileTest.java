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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.server.BackupReplayFile.Filesystem;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackupReplayFileTest {
  private static final String KEY_VALUE_SEPARATOR = ":";
  private static final String REPLAY_FILE_NAME = "backup_command_topic_1";

  @Rule
  public TemporaryFolder backupLocation = KsqlTestFolder.temporaryFolder();

  private BackupReplayFile replayFile;
  private File internalReplayFile;
  @Mock
  private Filesystem filesystem;

  @Before
  public void setup() throws IOException {
    internalReplayFile = backupLocation.newFile(REPLAY_FILE_NAME);
    replayFile = new BackupReplayFile(internalReplayFile, true, filesystem);
    when(filesystem.outputStream(any(File.class), anyBoolean())).thenAnswer(
        i -> new FileOutputStream((File) i.getArgument(0), i.getArgument(1))
    );
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
    final ConsumerRecord<byte[], byte[]> record = newStreamRecord("stream1");

    // When
    replayFile.write(record);

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\""
            + ",\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"
    ));
  }

  @Test
  public void shouldWriteMultipleRecords() throws IOException {
    // Given
    final ConsumerRecord<byte[], byte[]> record1= newStreamRecord("stream1");
    final ConsumerRecord<byte[], byte[]> record2 = newStreamRecord("stream2");

    // When
    replayFile.write(record1);
    replayFile.write(record2);

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\""
            + ",\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"
    ));
    assertThat(commands.get(1), is(
        "\"stream/stream2/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream2 (id INT) WITH (kafka_topic='stream2')\""
            + ",\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"
    ));
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE")
  private static FileOutputStream mockOutputStream(InvocationOnMock i) throws IOException {
    final FileOutputStream stream
        = new FileOutputStream((File) i.getArgument(0), i.getArgument(1));
    final FileOutputStream spy = Mockito.spy(stream);
    doCallRealMethod().doThrow(new IOException("")).when(spy).write(any(byte[].class));
    return spy;
  }

  @Test
  public void shouldPreserveBackupOnWriteFailure() throws IOException {
    // Given
    final ConsumerRecord<byte[], byte[]> record = newStreamRecord("stream1");
    replayFile.write(record);
    when(filesystem.outputStream(any(), anyBoolean()))
        .thenAnswer(BackupReplayFileTest::mockOutputStream);

    // When/Then:
    try {
      replayFile.write(record);
      Assert.fail("should throw IO exception");
    } catch (final IOException e) {
    }

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":\"CREATE STREAM stream1 (id INT) WITH (kafka_topic='stream1')\""
            + ",\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}"
    ));
  }

  @Test
  public void shouldWriteRecordWithNewLineCharacterInCommand() throws IOException {
    // Given
    final String commandId = buildKey("stream1");
    final String command =
        "{\"statement\":\"CREATE STREAM stream1 (id INT, f\\n1 INT) WITH (kafka_topic='topic1')\"}";

    // When
    replayFile.write(newStreamRecord(commandId, command));

    // Then
    final List<String> commands = Files.readAllLines(internalReplayFile.toPath());
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0), is(
        "\"stream/stream1/create\"" + KEY_VALUE_SEPARATOR
            + "{\"statement\":"
            + "\"CREATE STREAM stream1 (id INT, f\\n1 INT) WITH (kafka_topic='topic1')\"}"
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
    final ConsumerRecord<byte[], byte[]>  record1 = newStreamRecord("stream1");
    final ConsumerRecord<byte[], byte[]>  record2 = newStreamRecord("stream2");
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
    final List<Pair<byte[], byte[]>> commands = replayFile.readRecords();

    // Then
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(record1.key()));
    assertThat(commands.get(0).right, is(record1.value()));
    assertThat(commands.get(1).left, is(record2.key()));
    assertThat(commands.get(1).right, is(record2.value()));
  }

  private ConsumerRecord<byte[], byte[]> newStreamRecord(final String streamName) {
    return newStreamRecord(buildKey(streamName), buildValue(streamName));
  }

  @SuppressWarnings("unchecked")
  private ConsumerRecord<byte[], byte[]> newStreamRecord(final String key, final String value) {
    final ConsumerRecord<byte[], byte[]> consumerRecord = mock(ConsumerRecord.class);

    when(consumerRecord.key()).thenReturn(key.getBytes(StandardCharsets.UTF_8));
    when(consumerRecord.value()).thenReturn(value.getBytes(StandardCharsets.UTF_8));

    return consumerRecord;
  }

  private String buildKey(final String streamName) {
    return String.format("\"stream/%s/create\"", streamName);
  }

  private String buildValue(final String streamName) {
    return String.format("{\"statement\":\"CREATE STREAM %s (id INT) WITH (kafka_topic='%s')\","
            + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}",
        streamName, streamName);
  }
}