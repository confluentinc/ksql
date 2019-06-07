package io.confluent.ksql.structured;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchemaKGroupedTableTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);
  private KTable kTable;
  private KsqlTable ksqlTable;
  private InternalFunctionRegistry functionRegistry;


  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder
        .table(ksqlTable.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String()
            , ksqlTable.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(ksqlTable.getSchema(), new
                KsqlConfig(Collections.emptyMap()), false, new MockSchemaRegistryClientFactory()::get)));

  }

  private SchemaKGroupedTable buildGroupedKTable(final String query, final String...groupByColumns) {
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(query);
    final SchemaKTable initialSchemaKTable = new SchemaKTable(
        logicalPlan.getTheSourceNode().getSchema(), kTable, ksqlTable.getKeyField(), new ArrayList<>(),
        false, SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    final List<Expression> groupByExpressions =
        Arrays.stream(groupByColumns)
            .map(c -> new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("TEST1")), c))
            .collect(Collectors.toList());
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKTable.getSchema(), null, false, () -> null);
    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);
    Assert.assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    return (SchemaKGroupedTable)groupedSchemaKTable;
  }

  @Test
  public void shouldFailwindowedTableAggregation() {
    final SchemaKGroupedTable kGroupedTable = buildGroupedKTable(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final WindowExpression windowExpression = new WindowExpression(
        "window", new TumblingWindowExpression(30, TimeUnit.SECONDS));
    try {
      kGroupedTable.aggregate(
          new KudafInitializer(1),
          Collections.singletonMap(
              0,
              functionRegistry.getAggregate("SUM", Schema.OPTIONAL_INT64_SCHEMA)),
          Collections.singletonMap(0, 0),
          windowExpression,
          new KsqlJsonTopicSerDe().getGenericRowSerde(
              ksqlTable.getSchema(), ksqlConfig, false, () -> null)
      );
      Assert.fail("Should fail to build topology for aggregation with window");
    } catch(final KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo("Windowing not supported for table aggregations."));
    }
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    final SchemaKGroupedTable kGroupedTable = buildGroupedKTable(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    try {
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = new HashMap<>();
      aggValToFunctionMap.put(
          0, functionRegistry.getAggregate("MAX", Schema.OPTIONAL_INT64_SCHEMA));
      aggValToFunctionMap.put(
          1, functionRegistry.getAggregate("MIN", Schema.OPTIONAL_INT64_SCHEMA));
      kGroupedTable.aggregate(
          new KudafInitializer(1),
          aggValToFunctionMap,
          Collections.singletonMap(0, 0),
          null,
          new KsqlJsonTopicSerDe().getGenericRowSerde(
              ksqlTable.getSchema(), ksqlConfig, false, () -> null)
      );
      Assert.fail("Should fail to build topology for aggregation with unsupported function");
    } catch(final KsqlException e) {
      Assert.assertThat(
          e.getMessage(),
          equalTo(
              "The aggregation function(s) (MAX, MIN) cannot be applied to a table."));
    }
  }
}
