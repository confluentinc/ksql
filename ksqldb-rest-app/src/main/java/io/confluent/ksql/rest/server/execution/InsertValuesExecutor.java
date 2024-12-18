/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.generic.GenericRecordFactory;
import io.confluent.ksql.engine.generic.KsqlGenericRecord;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.serde.protobuf.ProtobufSchemaTranslator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class InsertValuesExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(InsertValuesExecutor.class);
  private static final Duration MAX_SEND_TIMEOUT = Duration.ofSeconds(5);

  private final LongSupplier clock;
  private final boolean canBeDisabledByConfig;
  private final RecordProducer producer;
  private final ValueSerdeFactory valueSerdeFactory;
  private final KeySerdeFactory keySerdeFactory;

  public InsertValuesExecutor() {
    this(true, InsertValuesExecutor::sendRecord);
  }

  public interface RecordProducer {

    void sendRecord(
        ProducerRecord<byte[], byte[]> record,
        ServiceContext serviceContext,
        Map<String, Object> producerProps
    );
  }

  @VisibleForTesting
  public InsertValuesExecutor(
      final boolean canBeDisabledByConfig,
      final RecordProducer producer
  ) {
    this(
        producer,
        canBeDisabledByConfig,
        System::currentTimeMillis,
        new GenericKeySerDe(),
        new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  InsertValuesExecutor(
      final LongSupplier clock,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this(InsertValuesExecutor::sendRecord, true, clock, keySerdeFactory, valueSerdeFactory);
  }

  private InsertValuesExecutor(
      final RecordProducer producer,
      final boolean canBeDisabledByConfig,
      final LongSupplier clock,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this.canBeDisabledByConfig = canBeDisabledByConfig;
    this.producer = Objects.requireNonNull(producer, "producer");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = Objects.requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  @SuppressWarnings("unused") // Part of required API.
  public void execute(
      final ConfiguredStatement<InsertValues> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final InsertValues insertValues = statement.getStatement();
    final MetaStore metaStore = executionContext.getMetaStore();
    final KsqlConfig config = statement.getSessionConfig().getConfig(true);

    final DataSource dataSource = getDataSource(config, metaStore, insertValues);

    validateInsert(insertValues.getColumns(), dataSource);

    final ProducerRecord<byte[], byte[]> record =
        buildRecord(statement, metaStore, dataSource, serviceContext);

    try {
      producer.sendRecord(record, serviceContext, config.getProducerClientConfigProps());
    } catch (final TopicAuthorizationException e) {
      // TopicAuthorizationException does not give much detailed information about why it failed,
      // except which topics are denied. Here we just add the ACL to make the error message
      // consistent with other authorization error messages.
      final Exception rootCause = new KsqlTopicAuthorizationException(
          AclOperation.WRITE,
          e.unauthorizedTopics()
      );

      throw new KsqlException(createInsertFailedExceptionMessage(insertValues), rootCause);
    } catch (final ClusterAuthorizationException e) {
      // ClusterAuthorizationException is thrown when using idempotent producers
      // and either a topic write permission or a cluster-level idempotent write
      // permission (only applicable for broker versions no later than 2.8) is
      // missing. In this case, we include additional context to help the user
      // distinguish this type of failure from other permissions exceptions
      // such as the ones thrown above when TopicAuthorizationException is caught.
      throw new KsqlException(
          createInsertFailedExceptionMessage(insertValues),
          createClusterAuthorizationExceptionRootCause(dataSource)
      );
    } catch (final KafkaException e) {
      if (e.getCause() != null && e.getCause() instanceof ClusterAuthorizationException) {
        // The error message thrown when an idempotent producer is missing permissions
        // is (nondeterministically) inconsistent: it is either a raw ClusterAuthorizationException,
        // as checked for above, or a ClusterAuthorizationException wrapped inside a KafkaException.
        // ksqlDB handles these two the same way, accordingly.
        // See https://issues.apache.org/jira/browse/KAFKA-14138 for more.
        throw new KsqlException(
            createInsertFailedExceptionMessage(insertValues),
            createClusterAuthorizationExceptionRootCause(dataSource)
        );
      } else {
        throw new KsqlException(createInsertFailedExceptionMessage(insertValues), e);
      }
    } catch (final Exception e) {
      throw new KsqlException(createInsertFailedExceptionMessage(insertValues), e);
    }
  }

  private void validateInsert(final List<ColumnName> columns, final DataSource dataSource) {
    final List<String> headerColumns;
    if (columns.isEmpty()) {
      headerColumns = dataSource.getSchema().headers()
          .stream()
          .map(column -> column.name().text())
          .collect(Collectors.toList());
    } else {
      headerColumns = columns.stream()
          .filter(columnName -> dataSource.getSchema().isHeaderColumn(columnName))
          .map(ColumnName::text)
          .collect(Collectors.toList());
    }
    if (!headerColumns.isEmpty()) {
      throw new KsqlException("Cannot insert into HEADER columns: "
          + String.join(", ", headerColumns));
    }
  }

  private static DataSource getDataSource(
      final KsqlConfig ksqlConfig,
      final MetaStore metaStore,
      final InsertValues insertValues
  ) {
    final DataSource dataSource = metaStore.getSource(insertValues.getTarget());
    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream/table: "
          + insertValues.getTarget());
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlException("Cannot insert values into windowed stream/table!");
    }

    final ReservedInternalTopics internalTopics = new ReservedInternalTopics(ksqlConfig);
    if (internalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlException("Cannot insert values into read-only topic: "
          + dataSource.getKafkaTopicName());
    }

    if (dataSource.isSource()) {
      throw new KsqlException(String.format("Cannot insert values into read-only %s: %s",
          dataSource.getDataSourceType().getKsqlType().toLowerCase(), dataSource.getName().text()));
    }

    return dataSource;
  }

  private ProducerRecord<byte[], byte[]> buildRecord(
      final ConfiguredStatement<InsertValues> statement,
      final MetaStore metaStore,
      final DataSource dataSource,
      final ServiceContext serviceContext
  ) {
    throwIfDisabled(statement.getSessionConfig().getConfig(false));

    final InsertValues insertValues = statement.getStatement();
    final KsqlConfig config = statement.getSessionConfig().getConfig(true);

    try {
      final KsqlGenericRecord row = new GenericRecordFactory(config, metaStore, clock).build(
          insertValues.getColumns(),
          insertValues.getValues(),
          dataSource.getSchema(),
          dataSource.getDataSourceType()
      );

      final byte[] key = serializeKey(row.key, dataSource, config, serviceContext);
      final byte[] value = serializeValue(row.value, dataSource, config, serviceContext);

      final String topicName = dataSource.getKafkaTopicName();

      return new ProducerRecord<>(
          topicName,
          null,
          row.ts,
          key,
          value
      );
    } catch (final Exception e) {
      throw new KsqlStatementException(
          createInsertFailedExceptionMessage(insertValues) + " " + e.getMessage(),
          statement.getMaskedStatementText(),
          e);
    }
  }

  private static String createInsertFailedExceptionMessage(final InsertValues insertValues) {
    return "Failed to insert values into '" + insertValues.getTarget().text() + "'.";
  }

  private void throwIfDisabled(final KsqlConfig config) {
    final boolean isEnabled = config.getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED);

    if (canBeDisabledByConfig && !isEnabled) {
      throw new KsqlException("The server has disabled INSERT INTO ... VALUES functionality. "
          + "To enable it, restart your ksqlDB server "
          + "with 'ksql.insert.into.values.enabled'=true");
    }
  }

  private static Exception createClusterAuthorizationExceptionRootCause(
      final DataSource dataSource) {
    return new KsqlTopicAuthorizationException(
        AclOperation.WRITE,
        Collections.singletonList(dataSource.getKafkaTopicName()),
        // Ideally we would forward the message from the ClusterAuthorizationException
        // instead of the hard-coded message below, but until this error message
        // is improved on the Kafka side, e.getMessage() is not helpful.
        // (Today it is just "Cluster authorization failed.")
        "The producer is not authorized to do idempotent sends. "
            + "Check that you have write permissions to the specified topic, "
            + "and disable idempotent sends by setting 'enable.idempotent=false' "
            + " if necessary."
    );
  }

  private byte[] serializeKey(
      final GenericKey keyValue,
      final DataSource dataSource,
      final KsqlConfig config,
      final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getKsqlTopic().getKeyFormat().getFeatures(),
        dataSource.getKsqlTopic().getValueFormat().getFeatures()
    );

    ensureKeySchemasMatch(physicalSchema.keySchema(), dataSource, serviceContext);

    final FormatInfo formatInfo = addSerializerMissingFormatFields(
        dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        dataSource.getKafkaTopicName(),
        true
    );

    try (Serde<GenericKey> keySerde = keySerdeFactory.create(
        formatInfo,
        physicalSchema.keySchema(),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty())
    ) {
      final String topicName = dataSource.getKafkaTopicName();
      try {
        return keySerde
            .serializer()
            .serialize(topicName, keyValue);
      } catch (final Exception e) {
        maybeThrowSchemaRegistryAuthError(
            FormatFactory.fromName(dataSource.getKsqlTopic().getKeyFormat().getFormat()),
            topicName,
            true,
            AclOperation.WRITE,
            e);
        LOG.error("Could not serialize key.", e);
        throw new KsqlException("Could not serialize key", e);
      }
    }
  }

  /**
   * Ensures that the key schema that we generate will be identical
   * to the schema that is registered in schema registry, if it exists.
   * Otherwise, it is possible that we will publish messages with a new
   * schemaID, meaning that logically identical keys might be routed to
   * different partitions.
   */
  private static void ensureKeySchemasMatch(
      final PersistenceSchema keySchema,
      final DataSource dataSource,
      final ServiceContext serviceContext
  ) {
    final KeyFormat keyFormat = dataSource.getKsqlTopic().getKeyFormat();
    final Format format = FormatFactory.fromName(keyFormat.getFormat());
    if (!format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      return;
    }

    final SchemaRegistryClient schemaRegistryClient = serviceContext.getSchemaRegistryClient();

    final FormatInfo formatInfo = addSerializerMissingFormatFields(
        dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        dataSource.getKafkaTopicName(),
        true
    );

    final ParsedSchema schema = format
        .getSchemaTranslator(formatInfo.getProperties())
        .toParsedSchema(keySchema);

    final Optional<ParsedSchema> latest;
    try {
      latest = SchemaRegistryUtil.getLatestParsedSchema(
          schemaRegistryClient,
          dataSource.getKafkaTopicName(),
          true);

    } catch (final KsqlException e) {
      maybeThrowSchemaRegistryAuthError(format, dataSource.getKafkaTopicName(), true,
          AclOperation.READ, e);
      throw new KsqlException("Could not determine that insert values operations is safe; "
          + "operation potentially overrides existing key schema in schema registry.", e);
    }

    if (latest.isPresent() && !latest.get().canonicalString().equals(schema.canonicalString())) {
      final Map<String, String> formatProps = keyFormat.getFormatInfo().getProperties();

      // Hack: skip comparing connect name. See https://github.com/confluentinc/ksql/issues/7211
      // Avro schema are registered in source creation time as well data insertion time.
      // CONNECT_META_DATA_CONFIG is configured to false in Avro Serializer, but it's true in
      // AvroSchemaTranslator. It needs to be true to make ConnectSchema map type work. But
      // enabling it breaks lots of history QTT test which implies backward compatibility issues.
      // So we just bypass the connect name check here.
      if (format instanceof AvroFormat) {
        final SchemaTranslator translator = format.getSchemaTranslator(formatProps);
        translator.configure(ImmutableMap.of(AvroDataConfig.CONNECT_META_DATA_CONFIG, false));
        final ParsedSchema parsedSchema = translator.toParsedSchema(keySchema);
        if (latest.get().canonicalString().equals(parsedSchema.canonicalString())) {
          return;
        }
      } else if (format instanceof ProtobufFormat
          && formatProps.containsKey(ConnectProperties.FULL_SCHEMA_NAME)) {

        // The SR key schema may have multiple schema definitions. The FULL_SCHEMA_NAME is used
        // to specify one definition only. To verify the source key schema matches SR, then we
        // extract the single schema based on the FULL_SCHEMA_NAME and then compare with the
        // source schema.

        final ProtobufSchemaTranslator protoTranslator = new ProtobufSchemaTranslator(
            new ProtobufProperties(formatProps)
        );

        final ParsedSchema extractedSingleSchema = protoTranslator.fromConnectSchema(
            protoTranslator.toConnectSchema(latest.get())
        );

        if (extractedSingleSchema.canonicalString().equals(schema.canonicalString())) {
          return;
        }
      }

      throw new KsqlException("Cannot INSERT VALUES into data source " + dataSource.getName()
          + ". ksqlDB generated schema would overwrite existing key schema."
          + "\n\tExisting Schema: " + latest.get().canonicalString()
          + "\n\tksqlDB Generated: " + schema.canonicalString());
    }
  }

  private byte[] serializeValue(
      final GenericRow row,
      final DataSource dataSource,
      final KsqlConfig config,
      final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getKsqlTopic().getKeyFormat().getFeatures(),
        dataSource.getKsqlTopic().getValueFormat().getFeatures()
    );

    final FormatInfo formatInfo = addSerializerMissingFormatFields(
        dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
        dataSource.getKafkaTopicName(),
        false
    );

    try (Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        formatInfo,
        physicalSchema.valueSchema(),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty())
    ) {
      final String topicName = dataSource.getKafkaTopicName();

      try {
        return valueSerde.serializer().serialize(topicName, row);
      } catch (final Exception e) {
        maybeThrowSchemaRegistryAuthError(
            FormatFactory.fromName(dataSource.getKsqlTopic().getValueFormat().getFormat()),
            topicName,
            false,
            AclOperation.WRITE,
            e);
        LOG.error("Could not serialize value.", e);
        throw new KsqlException("Could not serialize value" + e.getMessage(), e);
      }
    }
  }

  /**
   * Add missing required fields to the passed {@code FormatInfo} and return a
   * new {@code FormatInfo} with the new fields. These fields are required to serialize the
   * key and value schema correctly. For instance, if running INSERT on a stream with a SR schema
   * name different from the default name used in Connect (i.e. ConnectDefault1 for Protobuf).
   * </p>
   * Note: I initially thought of injecting the SCHEMA_FULL_NAME property in the WITH clause
   * when creating the stream, keep that property in the metastore and use it here instead
   * of looking at the SR directly. But this is not compatible with existing streams because
   * they were created previous to this fix. Those previous streams would fail with INSERT.
   * The best option was to dynamically look at the SR schema during an INSERT statement.
   */
  private static FormatInfo addSerializerMissingFormatFields(
      final FormatInfo formatInfo,
      final String topicName,
      final boolean isKey
  ) {
    // Just add missing fields required for serialization SR formats
    final Format format = FormatFactory.fromName(formatInfo.getFormat());
    if (!format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      return formatInfo;
    }

    // If SCHEMA_ID is not specified, then add the SUBJECT_NAME which helps the serializer
    // to identify the schema to fetch from SR but without using the restrictions we
    // have with SCHEMA_ID
    if (!formatInfo.getProperties().containsKey(ConnectProperties.SCHEMA_ID)) {
      final Set<String> supportedProperties = format.getSupportedProperties();

      // Add SUBJECT_NAME only on supported SR formats
      if (supportedProperties.contains(ConnectProperties.SUBJECT_NAME)) {
        final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        propertiesBuilder.putAll(formatInfo.getProperties());

        propertiesBuilder.put(ConnectProperties.SUBJECT_NAME,
            KsqlConstants.getSRSubject(topicName, isKey));

        return FormatInfo.of(formatInfo.getFormat(), propertiesBuilder.build());
      }
    }

    return formatInfo;
  }

  private static void maybeThrowSchemaRegistryAuthError(
      final Format format,
      final String topicName,
      final boolean isKey,
      final AclOperation op,
      final Exception e
  ) {
    if (format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      final Throwable rootCause = ObjectUtils.defaultIfNull(ExceptionUtils.getRootCause(e), e);
      if (rootCause instanceof RestClientException) {
        switch (((RestClientException) rootCause).getStatus()) {
          case HttpStatus.SC_UNAUTHORIZED:
          case HttpStatus.SC_FORBIDDEN:
            throw new KsqlSchemaAuthorizationException(
                op,
                KsqlConstants.getSRSubject(topicName, isKey)
            );
          default:
            break;
        }
      }
    }
  }

  private static void sendRecord(
      final ProducerRecord<byte[], byte[]> record,
      final ServiceContext serviceContext,
      final Map<String, Object> producerProps
  ) {
    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(producerProps);

    final Future<RecordMetadata> producerCallResult;

    try {
      producerCallResult = producer.send(record);
    } finally {
      producer.close(MAX_SEND_TIMEOUT);
    }

    try {
      // Check if the producer failed to write to the topic. This can happen if the
      // ServiceContext does not have write permissions.
      producerCallResult.get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

}