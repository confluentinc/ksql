package io.confluent.ksql;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;

public final class KsqlEngineTestUtil {
  private KsqlEngineTestUtil() {
  }

  public static KsqlEngine createKsqlEngine(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore,
      final KsqlConfig initializationKsqlConfig,
      final AdminClient adminClient
  ) {
    return new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        metaStore,
        initializationKsqlConfig,
        adminClient,
        KsqlEngineMetrics::new
    );
  }

  public static KsqlEngine createKsqlEngine(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore,
      final KsqlConfig initializationKsqlConfig,
      final AdminClient adminClient,
      final KsqlEngineMetrics engineMetrics
  ) {
    return new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        metaStore,
        initializationKsqlConfig,
        adminClient,
        ignored -> engineMetrics
    );
  }
}
