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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.BackupReplayFile;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.InternalTopicSerdes;
import io.confluent.ksql.rest.server.resources.IncompatibleKsqlCommandVersionException;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryApplicationId;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.json.JSONObject;

/**
 * Main command to restore the KSQL command topic.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlRestoreCommandTopic {
  private static final Serializer<byte[]> BYTES_SERIALIZER = new ByteArraySerializer();
  private static final int COMMAND_TOPIC_PARTITION = 0;

  private static KsqlConfig loadServerConfig(final File configFile) {
    final Map<String, String> serverProps = PropertiesUtil.loadProperties(configFile);
    return new KsqlConfig(serverProps);
  }

  public static List<Pair<byte[], byte[]>> loadBackup(
      final File file,
      final RestoreOptions restoreOptions,
      final KsqlConfig ksqlConfig
  ) throws IOException {
    final BackupReplayFile commandTopicBackupFile = BackupReplayFile.readOnly(file);
    List<Pair<byte[], byte[]>> records = commandTopicBackupFile.readRecords();

    records = checkValidCommands(
        records,
        restoreOptions.isSkipIncompatibleCommands(),
        ksqlConfig);

    return records;
  }

  /**
   * Checks all CommandId and Command pairs to see if they're compatible with the current
   * server version. If skipIncompatibleCommands is true, skip the command and try to clean up 
   * streams state stores and internal topics if the command being skipped is a query.
   * If false, throw an exception when an incomptaible command is detected.
   *
   * @param records a list of CommandId and Command pairs
   * @param skipIncompatibleCommands whether or not to throw an exception on incompatible commands
   * @param ksqlConfig the {@link KsqlConfig} used by the program
   * @return a list of compatible CommandId and Command pairs
   */
  private static List<Pair<byte[], byte[]>> checkValidCommands(
      final List<Pair<byte[], byte[]>> records,
      final boolean skipIncompatibleCommands,
      final KsqlConfig ksqlConfig
  ) {
    int n = 0;
    int numFilteredCommands = 0;
    final List<Pair<byte[], byte[]>> filteredRecords = new ArrayList<>();
    final List<byte[]> incompatibleCommands = new ArrayList<>();

    for (final Pair<byte[], byte[]> record : records) {
      n++;

      try {
        InternalTopicSerdes.deserializer(CommandId.class)
            .deserialize(null, record.getLeft());
      } catch (final Exception e) {
        throw new KsqlException(String.format(
            "Invalid CommandId string (line %d): %s (%s)",
            n, new String(record.getLeft(), StandardCharsets.UTF_8), e.getMessage()
        ));
      }

      try {
        InternalTopicSerdes.deserializer(Command.class)
            .deserialize(null, record.getRight());
      } catch (final SerializationException | IncompatibleKsqlCommandVersionException e) {
        if (skipIncompatibleCommands) {
          incompatibleCommands.add(record.getRight());
          numFilteredCommands++;
          continue;
        } else {
          throw new KsqlException(String.format(
              "Incompatible Command string (line %d): %s (%s)",
              n, new String(record.getLeft(), StandardCharsets.UTF_8), e.getMessage()
          ));
        }
      } catch (final Exception e) {
        throw new KsqlException(String.format(
            "Invalid Command string (line %d): %s (%s)",
            n, new String(record.getRight(), StandardCharsets.UTF_8), e.getMessage()
        ));
      }
      filteredRecords.add(record);
    }

    if (skipIncompatibleCommands) {
      System.out.println(
          String.format(
              "%s incompatible command(s) skipped from backup file.",
              numFilteredCommands));
      incompatibleCommands.forEach(command -> maybeCleanUpQuery(command, ksqlConfig));
    }
    return filteredRecords;
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

    return "yes".equals(decision.toLowerCase());
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
      backupCommands = loadBackup(backupFile, restoreOptions, serverConfig);
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
  private final Supplier<Producer<byte[], byte[]>> kafkaProducerSupplier;

  private static KafkaProducer<byte[], byte[]> transactionalProducer(
      final KsqlConfig serverConfig
  ) {
    final Map<String, Object> transactionalProperties =
        new HashMap<>(serverConfig.getProducerClientConfigProps());

    transactionalProperties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        serverConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
    );

    transactionalProperties.put(
        ProducerConfig.ACKS_CONFIG,
        "all"
    );

    return new KafkaProducer<>(
        transactionalProperties,
        BYTES_SERIALIZER,
        BYTES_SERIALIZER
    );
  }

  KsqlRestoreCommandTopic(final KsqlConfig serverConfig) {
    this(
        serverConfig,
        ReservedInternalTopics.commandTopic(serverConfig),
        ServiceContextFactory.create(serverConfig,
            () -> /* no ksql client */ null).getTopicClient(),
        () -> transactionalProducer(serverConfig)
    );
  }

  @VisibleForTesting
  KsqlRestoreCommandTopic(
      final KsqlConfig serverConfig,
      final String commandTopicName,
      final KafkaTopicClient topicClient,
      final Supplier<Producer<byte[], byte[]>> kafkaProducerSupplier
  ) {
    this.serverConfig = requireNonNull(serverConfig, "serverConfig");
    this.commandTopicName = requireNonNull(commandTopicName, "commandTopicName");
    this.topicClient = requireNonNull(topicClient, "topicClient");
    this.kafkaProducerSupplier = requireNonNull(kafkaProducerSupplier, "kafkaProducerSupplier");
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
    try (Producer<byte[], byte[]> kafkaProducer = createTransactionalProducer()) {
      for (int i = 0; i < commands.size(); i++) {
        final Pair<byte[], byte[]> command = commands.get(i);

        try {
          kafkaProducer.beginTransaction();
          enqueueCommand(kafkaProducer, command.getLeft(), command.getRight());
          kafkaProducer.commitTransaction();
        } catch (final ProducerFencedException
            | OutOfOrderSequenceException
            | AuthorizationException e
        ) {
          // We can't recover from these exceptions, so our only option is close producer and exit.
          // This catch doesn't abortTransaction() since doing that would throw another exception.
          throw new KsqlException(
              String.format("Failed restoring command (line %d): %s",
                  i + 1, new String(commands.get(i).getLeft(), StandardCharsets.UTF_8)), e);
        } catch (final InterruptedException e) {
          kafkaProducer.abortTransaction();
          throw new KsqlException("Restore process was interrupted.", e);
        } catch (final Exception e) {
          kafkaProducer.abortTransaction();
          throw new KsqlException(
              String.format("Failed restoring command (line %d): %s",
                  i + 1, new String(commands.get(i).getLeft(), StandardCharsets.UTF_8)), e);
        }
      }
    }
  }

  private void enqueueCommand(
      final Producer<byte[], byte[]> kafkaProducer,
      final byte[] commandId,
      final byte[] command
  ) throws ExecutionException, InterruptedException {
    final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
        commandTopicName,
        COMMAND_TOPIC_PARTITION,
        commandId,
        command);

    kafkaProducer.send(producerRecord).get();
  }

  private Producer<byte[], byte[]> createTransactionalProducer() {
    try {
      final Producer<byte[], byte[]> kafkaProducer = kafkaProducerSupplier.get();
      kafkaProducer.initTransactions();
      return kafkaProducer;
    } catch (final TimeoutException e) {
      final DefaultErrorMessages errorMessages = new DefaultErrorMessages();
      throw new KsqlException(errorMessages.transactionInitTimeoutErrorMessage(e), e);
    } catch (final Exception e) {
      throw new KsqlException("Failed to initialize topic transactions.", e);
    }
  }

  private static void maybeCleanUpQuery(final byte[] command, final KsqlConfig ksqlConfig) {
    boolean queryIdFound = false;
    final Map<String, Object> streamsProperties =
        new HashMap<>(ksqlConfig.getKsqlStreamConfigProps());
    final JSONObject jsonObject = new JSONObject(new String(command, StandardCharsets.UTF_8));
    if (hasKey(jsonObject, "plan")) {
      final JSONObject plan = jsonObject.getJSONObject("plan");
      if (hasKey(plan, "queryPlan")) {
        final JSONObject queryPlan = plan.getJSONObject("queryPlan");
        final String queryId = queryPlan.getString("queryId");
        streamsProperties.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            QueryApplicationId.build(ksqlConfig, true, new QueryId(queryId)));

        queryIdFound = true;
      }
    }

    // the command contains a query, clean up it's internal state store and also the internal topics
    if (queryIdFound) {
      final StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);
      final String applicationId =
          streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);
      try {
        final Admin admin = new DefaultKafkaClientSupplier()
            .getAdmin(ksqlConfig.getKsqlAdminClientConfigProps());
        final KafkaTopicClient topicClient = new KafkaTopicClientImpl(() -> admin);
        topicClient.deleteInternalTopics(applicationId);

        new StateDirectory(streamsConfig, Time.SYSTEM, true).clean();
        System.out.println(
            String.format(
                "Cleaned up internal state store and internal topics for query %s",
                applicationId));
      } catch (final Exception e) {
        System.out.println(String.format("Failed to clean up query %s ", applicationId));
      }
    }
  }

  private static boolean hasKey(final JSONObject jsonObject, final String key) {
    return jsonObject != null && jsonObject.has(key);
  }
}
