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

package io.confluent.ksql.rest.server.restore;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.BackupInputFile;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.ReservedInternalTopics;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Main command to restore the KSQL command topic.
 */
public class KsqlRestoreCommandTopic {
  private static final Serializer<byte[]> BYTES_SERIALIZER = new ByteArraySerializer();
  private static final int COMMAND_TOPIC_PARTITION = 0;

  private static KsqlConfig loadServerConfig(final File configFile) {
    final Map<String, String> serverProps = PropertiesUtil.loadProperties(configFile);
    return new KsqlConfig(serverProps);
  }

  public static List<Pair<byte[], byte[]>> loadBackup(final File file) throws IOException {
    final BackupInputFile commandTopicBackupFile = new BackupInputFile(file);

    final List<Pair<byte[], byte[]>> records = commandTopicBackupFile.readRecords();
    throwOnInvalidRecords(records);

    return records;
  }

  private static void throwOnInvalidRecords(final List<Pair<byte[], byte[]>> records) {
    int n = 0;

    for (final Pair<byte[], byte[]> record : records) {
      n++;

      try {
        InternalTopicSerdes.deserializer(CommandId.class)
            .deserialize(null, record.getLeft());
      } catch (final Exception e) {
        throw new KsqlException(String.format(
            "Invalid CommandId string (line %d): %s",
            n, new String(record.getLeft(), StandardCharsets.UTF_8), e
        ));
      }

      try {
        InternalTopicSerdes.deserializer(Command.class)
            .deserialize(null, record.getRight());
      } catch (final Exception e) {
        throw new KsqlException(String.format(
            "Invalid Command string (line %d): %s",
            n, new String(record.getRight(), StandardCharsets.UTF_8), e
        ));
      }
    }
  }

  private static void checkFileExists(final File file) throws Exception {
    if (!file.exists()) {
      throw new NoSuchFileException("File does not exist: " + file.getPath());
    }

    if (!file.isFile()) {
      throw new NoSuchFileException("Invalid file: " + file.getPath());
    }

    if (!file.canRead()) {
      throw new Exception("You don't have Read permissions on file: " + file.getPath());
    }
  }

  private static long timer;

  private static void resetTimer() {
    timer = System.currentTimeMillis();
  }

  private static long currentTimer() {
    return System.currentTimeMillis() - timer;
  }

  private static boolean promptQuestion() {
    System.out.println("Restoring the command topic will DELETE your actual metadata.");
    System.out.print("Continue [yes or no] (default: no)? ");

    final Console console = System.console();
    final String decision = console.readLine();

    switch (decision.toLowerCase()) {
      case "yes":
        return true;
      default:
        return false;
    }
  }

  /**
   * Main command to restore the KSQL command topic.
   */
  public static void main(final String[] args) throws Exception {
    final RestoreOptions restoreOptions = RestoreOptions.parse(args);
    if (restoreOptions == null) {
      System.exit(1);
    }

    final File configFile = restoreOptions.getConfigFile();
    final File backupFile = restoreOptions.getBackupFile();

    try {
      checkFileExists(configFile);
      checkFileExists(backupFile);
    } catch (final Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }

    final KsqlConfig serverConfig = loadServerConfig(configFile);
    final KsqlRestoreCommandTopic restoreMetadata = new KsqlRestoreCommandTopic(serverConfig);

    // Stop and ask the user to type 'yes' to continue to warn users about the restore process
    if (!restoreOptions.isAutomaticYes() && !promptQuestion()) {
      System.exit(0);
    }

    System.out.println("Loading backup file ...");
    resetTimer();

    List<Pair<byte[], byte[]>> backupCommands = null;
    try {
      backupCommands = loadBackup(backupFile);
    } catch (final Exception e) {
      System.err.println(String.format(
          "Failed loading backup file.%nError = %s", e.getMessage()));
      System.exit(1);
    }

    System.out.println(String.format(
        "Backup (%d records) loaded in memory in %s ms.", backupCommands.size(), currentTimer()));
    System.out.println();

    System.out.println("Restoring command topic ...");
    resetTimer();

    try {
      restoreMetadata.restore(backupCommands);
    } catch (final Exception e) {
      System.err.println(String.format(
          "Failed restoring command topic.%nError = %s", e.getMessage()));
      System.exit(1);
    }

    System.out.println(String.format(
        "Restore process completed in %d ms.", currentTimer()));
    System.out.println();

    System.out.println("You need to restart the ksqlDB server to re-load the command topic.");
  }

  private final KsqlConfig serverConfig;
  private final String commandTopicName;
  private final KafkaTopicClient topicClient;
  private final Producer<byte[], byte[]> kafkaProducer;

  KsqlRestoreCommandTopic(final KsqlConfig serverConfig) {
    this(
        serverConfig,
        ReservedInternalTopics.commandTopic(serverConfig),
        ServiceContextFactory.create(serverConfig,
            () -> /* no ksql client */ null).getTopicClient(),
        new KafkaProducer<>(
            serverConfig.getProducerClientConfigProps(),
            BYTES_SERIALIZER,
            BYTES_SERIALIZER
        )
    );
  }

  KsqlRestoreCommandTopic(
      final KsqlConfig serverConfig,
      final String commandTopicName,
      final KafkaTopicClient topicClient,
      final Producer<byte[], byte[]> kafkaProducer
  ) {
    this.serverConfig = requireNonNull(serverConfig, "serverConfig");
    this.commandTopicName = requireNonNull(commandTopicName, "commandTopicName");
    this.topicClient = requireNonNull(topicClient, "topicClient");
    this.kafkaProducer = requireNonNull(kafkaProducer, "kafkaProducer");
  }

  public void restore(final List<Pair<byte[], byte[]>> backupCommands) {
    // Delete the command topic
    deleteCommandTopicIfExists();

    // Create the command topic
    KsqlInternalTopicUtils.ensureTopic(commandTopicName, serverConfig, topicClient);

    // Restore the commands
    restoreCommandTopic(backupCommands);
  }

  private void deleteCommandTopicIfExists() {
    if (topicClient.isTopicExists(commandTopicName)) {
      topicClient.deleteTopics(Collections.singletonList(commandTopicName));
      try {
        // Wait a few seconds, otherwise the create topic does not work because it still sees
        // the topic
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        // Don't need to throw an exception in this case
      }
    }
  }

  private void restoreCommandTopic(final List<Pair<byte[], byte[]>> commands) {
    final List<Future<RecordMetadata>> futures = new ArrayList<>(commands.size());

    for (final Pair<byte[], byte[]> command : commands) {
      futures.add(enqueueCommand(command.getLeft(), command.getRight()));
    }

    int i = 0;
    for (final Future<RecordMetadata> future : futures) {
      try {
        future.get();
      } catch (final InterruptedException e) {
        throw new KsqlException("Restore process was interrupted.", e);
      } catch (final Exception e) {
        throw new KsqlException(
            String.format("Failed restoring command (line %d): %s",
                i + 1, new String(commands.get(i).getLeft(), StandardCharsets.UTF_8)), e);
      }

      i++;
    }
  }

  public Future<RecordMetadata> enqueueCommand(final byte[] commandId, final byte[] command) {
    final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
        commandTopicName,
        COMMAND_TOPIC_PARTITION,
        commandId,
        command);

    return kafkaProducer.send(producerRecord);
  }
}
