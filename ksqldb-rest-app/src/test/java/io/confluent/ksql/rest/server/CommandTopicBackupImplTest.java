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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.rest.server.resources.CommandTopicCorruptionException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandTopicBackupImplTest {
  private static final String COMMAND_TOPIC_NAME = "command_topic";

  private final ConsumerRecord<byte[], byte[]> command1 = newStreamRecord("stream1");
  private final ConsumerRecord<byte[], byte[]> command2 = newStreamRecord("stream2");
  private final ConsumerRecord<byte[], byte[]> command3 = newStreamRecord("stream3");

  @Mock
  private Supplier<Long> ticker;

  @Mock
  private KafkaTopicClient topicClient;

  @Rule
  public TemporaryFolder backupLocation = KsqlTestFolder.temporaryFolder();

  private CommandTopicBackupImpl commandTopicBackup;

  @Before
  public void setup() {
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(true);
    commandTopicBackup = new CommandTopicBackupImpl(
        backupLocation.getRoot().getAbsolutePath(), COMMAND_TOPIC_NAME, ticker, topicClient);
  }

  private ConsumerRecord<byte[], byte[]> newStreamRecord(final String streamName) {
    return newStreamRecord(buildKey(streamName), buildValue(streamName));
  }

  @SuppressWarnings("unchecked")
  private static ConsumerRecord<byte[], byte[]> newStreamRecord(
      final String key,
      final String value
  ) {
    final ConsumerRecord<byte[], byte[]> consumerRecord = mock(ConsumerRecord.class);

    when(consumerRecord.key()).thenReturn(key.getBytes(StandardCharsets.UTF_8));
    when(consumerRecord.value()).thenReturn(value.getBytes(StandardCharsets.UTF_8));

    return consumerRecord;
  }

  private String buildKey(final String streamName) {
    return String.format("\"stream/%s/create\"", streamName);
  }

  private String buildValue(final String streamName) {
    return String.format("{\"statement\":\"CREATE STREAM %s (id INT) WITH (kafka_topic='%s')\"}",
        streamName, streamName);
  }

  @Test
  public void shouldThrowWhenBackupLocationIsNotDirectory() throws IOException {
    // Given
    final File file = backupLocation.newFile();

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> new CommandTopicBackupImpl(file.getAbsolutePath(), COMMAND_TOPIC_NAME, topicClient)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "%s is not a directory.",
        file.getAbsolutePath()
    )));
  }

  @Test
  public void shouldThrowWhenBackupLocationIsNotWritable() throws IOException {
    // Given
    final File file = backupLocation.newFolder();
    Files.setPosixFilePermissions(file.toPath(), PosixFilePermissions.fromString("r-x------"));

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> new CommandTopicBackupImpl(file.getAbsolutePath(), COMMAND_TOPIC_NAME, topicClient)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "The backups directory is not readable/writable for KSQL server: %s",
        file.getAbsolutePath()
    )));
  }

  @Test
  public void shouldThrowWhenBackupLocationIsNotReadable() throws IOException {
    // Given
    final File dir = backupLocation.newFolder();
    Files.setPosixFilePermissions(dir.toPath(), PosixFilePermissions.fromString("-wx------"));

    // When
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> new CommandTopicBackupImpl(dir.getAbsolutePath(), COMMAND_TOPIC_NAME, topicClient)
    );

    // Then
    assertThat(e.getMessage(), containsString(String.format(
        "The backups directory is not readable/writable for KSQL server: %s",
        dir.getAbsolutePath()
    )));
  }


  @Test
  public void shouldCreateBackupLocationWhenDoesNotExist() throws IOException {
    // Given
    final Path dir = Paths.get(backupLocation.newFolder().getAbsolutePath(), "ksql-backups");
    assertThat(Files.exists(dir), is(false));

    // When
    new CommandTopicBackupImpl(dir.toString(), COMMAND_TOPIC_NAME, topicClient);

    // Then
    assertThat(Files.exists(dir), is(true));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotFailIfRecordKeyIsNull() throws IOException {
    // Given
    commandTopicBackup.initialize();
    final ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
    when(record.key()).thenReturn(null);

    // When
    commandTopicBackup.writeRecord(record);

    // Then
    final List<Pair<byte[], byte[]>> commands = commandTopicBackup.getReplayFile().readRecords();
    assertThat(commands.size(), is(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotFailIfRecordValueIsNull() throws IOException {
    // Given
    commandTopicBackup.initialize();
    final ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
    when(record.key()).thenReturn(new byte[]{});
    when(record.value()).thenReturn(null);

    // When
    commandTopicBackup.writeRecord(record);

    // Then
    final List<Pair<byte[], byte[]>> commands = commandTopicBackup.getReplayFile().readRecords();
    assertThat(commands.size(), is(0));
  }

  @Test
  public void shouldNotWriteMigrationCommandToBackup() throws IOException {
    // Given
    commandTopicBackup.initialize();
    final ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
    when(record.key()).thenReturn(InternalTopicSerdes.serializer().serialize("", CommandTopicMigrationUtil.MIGRATION_COMMAND_ID));
    when(record.value()).thenReturn(new byte[]{});

    // When
    commandTopicBackup.writeRecord(record);

    // Then
    final List<Pair<byte[], byte[]>> commands = commandTopicBackup.getReplayFile().readRecords();
    assertThat(commands.size(), is(0));
  }

  @Test
  public void shouldWriteCommandToBackupToReplayFile() throws IOException {
    // Given
    commandTopicBackup.initialize();

    // When
    commandTopicBackup.writeRecord(command1);

    // Then
    final List<Pair<byte[], byte[]>> commands = commandTopicBackup.getReplayFile().readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command1.key()));
    assertThat(commands.get(0).right, is(command1.value()));
  }

  @Test
  public void shouldIgnoreRecordPreviouslyReplayed() throws IOException {
    // Given
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(command1);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(command1);
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    final List<Pair<byte[], byte[]>> commands = currentReplayFile.readRecords();
    assertThat(commands.size(), is(1));
    assertThat(commands.get(0).left, is(command1.key()));
    assertThat(commands.get(0).right, is(command1.value()));
    assertThat(currentReplayFile.getPath(), is(previousReplayFile.getPath()));
  }

  @Test
  public void shouldNotCreateNewReplayFileIfNewRecordsDoNotMatchPreviousBackups() {
    // Given
    commandTopicBackup = new CommandTopicBackupImpl(
        backupLocation.getRoot().getAbsolutePath(), COMMAND_TOPIC_NAME, ticker, topicClient);
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(command1);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    // The write command will conflicts with what's already in the backup file
    try {
      commandTopicBackup.writeRecord(command2);
      assertThat(true, is(false));
    } catch (final CommandTopicCorruptionException e) {
      // This is expected so we do nothing
    }
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    assertThat(currentReplayFile.getPath(), is(previousReplayFile.getPath()));
    assertThat(commandTopicBackup.commandTopicCorruption(), is(true));
    try {
      commandTopicBackup.writeRecord(command2);
      assertThat(true, is(false));
    } catch (final CommandTopicCorruptionException e) {
      // This is expected so we do nothing
    }
  }

  @Test
  public void shouldWritePreviousReplayedRecordsAlreadyChecked() throws IOException {
    // Given
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(command1);
    commandTopicBackup.writeRecord(command2);
    final BackupReplayFile previousReplayFile = commandTopicBackup.getReplayFile();

    // When
    // A 2nd initialize call will open the latest backup and read the previous replayed commands
    commandTopicBackup.initialize();
    // command1 is ignored because it was previously replayed
    commandTopicBackup.writeRecord(command1);
    // The write command will conflicts with what's already in the backup file
    try {
      commandTopicBackup.writeRecord(command3);
      assertThat(true, is(false));
    } catch (final KsqlServerException e) {
      // This is expected so we do nothing
    }
    final BackupReplayFile currentReplayFile = commandTopicBackup.getReplayFile();

    // Then
    List<Pair<byte[], byte[]>> commands = previousReplayFile.readRecords();
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(command1.key()));
    assertThat(commands.get(0).right, is(command1.value()));
    assertThat(commands.get(1).left, is(command2.key()));
    assertThat(commands.get(1).right, is(command2.value()));

    // the backup file should be the same and the contents shouldn't have been modified
    commands = currentReplayFile.readRecords();
    assertThat(commands.size(), is(2));
    assertThat(commands.get(0).left, is(command1.key()));
    assertThat(commands.get(0).right, is(command1.value()));
    assertThat(commands.get(1).left, is(command2.key()));
    assertThat(commands.get(1).right, is(command2.value()));
    assertThat(currentReplayFile.getPath(), is(previousReplayFile.getPath()));
    assertThat(commandTopicBackup.commandTopicCorruption(), is(true));
  }

  @Test
  public void shouldCreateNewReplayFileWhenNoBackupFilesExist() {
    // Given:
    when(ticker.get()).thenReturn(123L);

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_123", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenOneExists() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenTwoExist() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_command_topic_222");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_222", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenLatestReplayFileWhenDifferentCommandTopicNamesExist() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_other_command_topic_222");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void shouldOpenReplayFileAndIgnoreFileWithInvalidTimestamp() throws IOException {
    // Given:
    backupLocation.newFile("backup_command_topic_111");
    backupLocation.newFile("backup_command_topic_222x");

    // When:
    final BackupReplayFile replayFile = commandTopicBackup.openOrCreateReplayFile();

    // Then:
    assertThat(replayFile.getPath(), is(String.format(
        "%s/backup_command_topic_111", backupLocation.getRoot().getAbsolutePath()
    )));
  }

  @Test
  public void deletedCommandTopicMarksCommandBackupAsCorrupted() {
    // Given
    commandTopicBackup.initialize();
    commandTopicBackup.writeRecord(command1);
    commandTopicBackup.writeRecord(command2);

    // When
    // Set command topic as deleted and a second initialize call will
    // detect it is out of sync and will mark command back as corrupted.
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);
    commandTopicBackup.initialize();

    // Then:
    assertThat(commandTopicBackup.commandTopicCorruption(), is(true));
  }
}
