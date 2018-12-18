/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
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
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.streams.MaterializedFactory;
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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class SchemaKGroupedTableTest {
  private final static String AGGREGATE_OP_NAME = "AGGREGATE";

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
  private final Schema schema = SchemaBuilder.struct()
      .field("GROUPING_COLUMN", Schema.OPTIONAL_STRING_SCHEMA)
      .field("AGG_VALUE", Schema.OPTIONAL_INT32_SCHEMA)
      .build();
  private final MaterializedFactory materializedFactory = mock(MaterializedFactory.class);
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);

  private KTable kTable;
  private KsqlTable ksqlTable;

  @Before
  public void init() {
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder
        .table(ksqlTable.getKsqlTopic().getKafkaTopicName(), Consumed.with(Serdes.String()
            , ksqlTable.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(ksqlTable.getSchema(), new
                KsqlConfig(Collections.emptyMap()), false, MockSchemaRegistryClient::new)));
  }

  private SchemaKGroupedTable buildSchemaKGroupedTableFromQuery(
      final String query,
      final String...groupByColumns) {
    final PlanNode logicalPlan = planBuilder.buildLogicalPlan(query);
    final SchemaKTable initialSchemaKTable = new SchemaKTable<>(
        logicalPlan.getTheSourceNode().getSchema(), kTable, ksqlTable.getKeyField(), new ArrayList<>(),
        Serdes.String(), SchemaKStream.Type.SOURCE, ksqlConfig, functionRegistry);
    final List<Expression> groupByExpressions =
        Arrays.stream(groupByColumns)
            .map(c -> new DereferenceExpression(new QualifiedNameReference(QualifiedName.of("TEST1")), c))
            .collect(Collectors.toList());
    final KsqlTopicSerDe ksqlTopicSerDe = new KsqlJsonTopicSerDe();
    final Serde<GenericRow> rowSerde = ksqlTopicSerDe.getGenericRowSerde(
        initialSchemaKTable.getSchema(), null, false, () -> null);
    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        rowSerde, groupByExpressions, "GROUP-BY");
    Assert.assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    return (SchemaKGroupedTable)groupedSchemaKTable;
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTableFromQuery(
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
              ksqlTable.getSchema(), ksqlConfig, false, () -> null),
          AGGREGATE_OP_NAME
      );
      Assert.fail("Should fail to build topology for aggregation with window");
    } catch(final KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo("Windowing not supported for table aggregations."));
    }
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTableFromQuery(
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
              ksqlTable.getSchema(), ksqlConfig, false, () -> null),
          AGGREGATE_OP_NAME
      );
      Assert.fail("Should fail to build topology for aggregation with unsupported function");
    } catch(final KsqlException e) {
      Assert.assertThat(
          e.getMessage(),
          equalTo(
              "The aggregation function(s) (MAX, MIN) cannot be applied to a table."));
    }
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable(
      final KGroupedTable kGroupedTable,
      final MaterializedFactory materializedFactory) {
    return new SchemaKGroupedTable(
        schema,
        kGroupedTable,
        schema.fields().get(0),
        Collections.emptyList(),
        ksqlConfig,
        functionRegistry,
        materializedFactory);
  }

  @Test
  public void shouldUseMaterializedFactoryForStateStore() {
    // Given:
    final Serde<GenericRow> valueSerde = mock(Serde.class);
    final Materialized materialized = MaterializedFactory.create(ksqlConfig).create(
        Serdes.String(),
        valueSerde,
        AGGREGATE_OP_NAME);
    expect(
        materializedFactory.create(
            anyObject(Serdes.String().getClass()),
            same(valueSerde),
            eq(AGGREGATE_OP_NAME)))
        .andReturn(materialized);
    final KTable mockKTable = mock(KTable.class);
    expect(
        mockKGroupedTable.aggregate(
            anyObject(),
            anyObject(),
            anyObject(),
            same(materialized)))
        .andReturn(mockKTable);
    replay(materializedFactory, mockKGroupedTable);
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    groupedTable.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        valueSerde,
        AGGREGATE_OP_NAME);

    // Then:
    verify(materializedFactory, mockKGroupedTable);
  }
}
