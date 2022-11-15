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

package io.confluent.ksql.query;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterialization;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;

public final class MaterializationProviderBuilderFactory {
  // Interface used for Function alias
  public interface MaterializationProviderBuilder
      extends Function<KafkaStreams, Optional<MaterializationProvider>> {
  }

  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final KsMaterializationFactory ksMaterializationFactory;
  private final KsqlMaterializationFactory ksqlMaterializationFactory;

  public MaterializationProviderBuilderFactory(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final KsMaterializationFactory ksMaterializationFactory,
      final KsqlMaterializationFactory ksqlMaterializationFactory
  ) {
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.ksMaterializationFactory =
        requireNonNull(ksMaterializationFactory, "ksMaterializationFactory");
    this.ksqlMaterializationFactory =
        requireNonNull(ksqlMaterializationFactory, "ksqlMaterializationFactory");
  }

  public MaterializationProviderBuilder materializationProviderBuilder(
      final MaterializationInfo materializationInfo,
      final PhysicalSchema querySchema,
      final KeyFormat keyFormat,
      final Map<String, Object> streamsProperties,
      final String applicationId
  ) {
    return (kafkaStreams) -> buildMaterializationProvider(
        kafkaStreams,
        materializationInfo,
        querySchema,
        keyFormat,
        streamsProperties,
        applicationId
    );
  }

  private Optional<MaterializationProvider> buildMaterializationProvider(
      final KafkaStreams kafkaStreams,
      final MaterializationInfo materializationInfo,
      final PhysicalSchema schema,
      final KeyFormat keyFormat,
      final Map<String, Object> streamsProperties,
      final String applicationId
  ) {
    final Serializer<Struct> keySerializer = new GenericKeySerDe().create(
        keyFormat.getFormatInfo(),
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    ).serializer();

    final Optional<KsMaterialization> ksMaterialization = ksMaterializationFactory
        .create(
            materializationInfo.stateStoreName(),
            kafkaStreams,
            materializationInfo.getStateStoreSchema(),
            keySerializer,
            keyFormat.getWindowInfo(),
            streamsProperties,
            ksqlConfig,
            applicationId
        );

    return ksMaterialization.map(ksMat -> (queryId, contextStacker) -> ksqlMaterializationFactory
        .create(
            ksMat,
            materializationInfo,
            queryId,
            contextStacker
        ));
  }
}
