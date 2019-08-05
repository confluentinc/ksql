/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.streams.kstream.KStream;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final KsqlTopic ksqlTopic;
  private final KeyField keyField;
  private final Optional<String> partitionByField;
  private final boolean doCreateInto;
  private final Set<SerdeOption> serdeOptions;
  private final SinkFactory sinkFactory;
  private final Set<Integer> implicitAndKeyFieldIndexes;

  public KsqlStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KeyField keyField,
      final KsqlTopic ksqlTopic,
      final Optional<String> partitionByField,
      final OptionalInt limit,
      final boolean doCreateInto,
      final Set<SerdeOption> serdeOptions
  ) {
    this(
        id,
        source,
        schema,
        timestampExtractionPolicy,
        keyField,
        ksqlTopic,
        partitionByField,
        limit,
        doCreateInto,
        serdeOptions,
        SchemaKStream::new
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  KsqlStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KeyField keyField,
      final KsqlTopic ksqlTopic,
      final Optional<String> partitionByField,
      final OptionalInt limit,
      final boolean doCreateInto,
      final Set<SerdeOption> serdeOptions,
      final SinkFactory<?> sinkFactory
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        id,
        source,
        // KSQL internally copies the implicit and key fields into the value schema.
        // This is done by DataSourceNode
        // Hence, they must be removed again here if they are still in the sink schema.
        // This leads to strange behaviour, but changing it is a breaking change.
        schema.withoutMetaAndKeyFieldsInValue(),
        limit,
        timestampExtractionPolicy
    );

    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.partitionByField = Objects.requireNonNull(partitionByField, "partitionByField");
    this.doCreateInto = doCreateInto;
    this.implicitAndKeyFieldIndexes = implicitAndKeyColumnIndexesInValueSchema(schema);
    this.sinkFactory = requireNonNull(sinkFactory, "sinkFactory");

    validatePartitionByField();
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    final String base = queryIdGenerator.getNextId();
    if (!doCreateInto) {
      return new QueryId("InsertQuery_" + base);
    }
    if (getNodeOutputType().equals(DataSourceType.KTABLE)) {
      return new QueryId("CTAS_" + getId().toString() + "_" + base);
    }
    return new QueryId("CSAS_" + getId().toString() + "_" + base);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(builder);

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId());

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker
    );

    final Serde<GenericRow> outputRowSerde = builder.buildValueSerde(
        getKsqlTopic().getValueFormat().getFormatInfo(),
        PhysicalSchema.from(getSchema(), serdeOptions),
        contextStacker.getQueryContext()
    );

    result.into(
        getKsqlTopic().getKafkaTopicName(),
        outputRowSerde,
        implicitAndKeyFieldIndexes
    );

    return result;
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext.Stacker contextStacker
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final KeyField resultKeyField = KeyField.of(
        schemaKStream.getKeyField().name(),
        getKeyField().legacy()
    );

    final SchemaKStream result = sinkFactory.create(
        schemaKStream.getKstream(),
        schemaKStream.getSchema(),
        schemaKStream.getKeySerde(),
        resultKeyField,
        Collections.singletonList(schemaKStream),
        SchemaKStream.Type.SINK,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );

    if (!partitionByField.isPresent()) {
      return result;
    }

    return result.selectKey(partitionByField.get(), false, contextStacker);
  }

  private void validatePartitionByField() {
    if (!partitionByField.isPresent()) {
      return;
    }

    final String fieldName = partitionByField.get();

    if (getSchema().isMetaField(fieldName) || getSchema().isKeyField(fieldName)) {
      return;
    }

    if (!keyField.name().equals(Optional.of(fieldName))) {
      throw new IllegalArgumentException("keyField must match partition by field");
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Set<Integer> implicitAndKeyColumnIndexesInValueSchema(final LogicalSchema schema) {
    final ConnectSchema valueSchema = schema.valueSchema();

    final Stream<Field> fields = Streams.concat(
        schema.metaFields().stream(),
        schema.keyFields().stream()
    );

    return fields
        .map(Field::name)
        .map(valueSchema::field)
        .filter(Objects::nonNull)
        .map(org.apache.kafka.connect.data.Field::index)
        .collect(Collectors.toSet());
  }

  interface SinkFactory<K> {

    SchemaKStream create(
        KStream<K, GenericRow> kstream,
        LogicalSchema schema,
        KeySerde<K> keySerde,
        KeyField keyField,
        List<SchemaKStream> sourceSchemaKStreams,
        Type type,
        KsqlConfig ksqlConfig,
        FunctionRegistry functionRegistry,
        QueryContext queryContext
    );
  }
}