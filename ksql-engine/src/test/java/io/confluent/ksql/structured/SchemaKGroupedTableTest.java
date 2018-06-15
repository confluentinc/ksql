package io.confluent.ksql.structured;

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
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

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
    StreamsBuilder builder = new StreamsBuilder();
    kTable = builder
        .table(ksqlTable.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String()
            , ksqlTable.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(ksqlTable.getSchema(), new
                KsqlConfig(Collections.emptyMap()), false, new MockSchemaRegistryClient())));

  }

  private SchemaKGroupedTable buildGroupedKTable(String query, String...groupByColumns) {
    PlanNode logicalPlan = planBuilder.buildLogicalPlan(query);
    SchemaKTable initialSchemaKTable = new SchemaKTable(
        logicalPlan.getTheSourceNode().getSchema(), kTable, ksqlTable.getKeyField(), new ArrayList<>(),
        false, SchemaKStream.Type.SOURCE, functionRegistry, new MockSchemaRegistryClient());
    List<Expression> groupByExpressions =
        Arrays.stream(groupByColumns)
            .map(c -> new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("TEST1")), c))
            .collect(Collectors.toList());
    KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKTable.getSchema(), null, false, null);
    SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        Serdes.String(), rowSerde, groupByExpressions);
    Assert.assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    return (SchemaKGroupedTable)groupedSchemaKTable;
  }

  @Test
  public void shouldFailwindowedTableAggregation() {
    SchemaKGroupedTable kGroupedTable = buildGroupedKTable(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    WindowExpression windowExpression = new WindowExpression(
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
              ksqlTable.getSchema(), ksqlConfig, false, null)
      );
      Assert.fail("Should fail to build topology for aggregation with window");
    } catch(KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo("Windowing not supported for table aggregations."));
    }
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    SchemaKGroupedTable kGroupedTable = buildGroupedKTable(
        "SELECT col0, col1, col2 FROM test1;", "COL1", "COL2");
    InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    try {
      Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = new HashMap<>();
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
              ksqlTable.getSchema(), ksqlConfig, false, null)
      );
      Assert.fail("Should fail to build topology for aggregation with unsupported function");
    } catch(KsqlException e) {
      Assert.assertThat(
          e.getMessage(),
          equalTo(
              "The aggregation function(s) (MAX, MIN) cannot be applied to a table."));
    }
  }
}