package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.streams.TableGroupByBuilder.TableKeyValueMapper;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TableGroupByBuilderTest {
  private static final SourceName ALIAS = SourceName.of("SOURCE");
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("PAC"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("MAN"), SqlTypes.STRING)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema.from(SCHEMA, SerdeOption.none());

  private static final List<Expression> GROUPBY_EXPRESSIONS = ImmutableList.of(
      columnReference("PAC"),
      columnReference("MAN")
  );
  private static final QueryContext SOURCE_CONTEXT =
      new QueryContext.Stacker().push("foo").push("source").getQueryContext();
  private static final QueryContext STEP_CONTEXT =
      new QueryContext.Stacker().push("foo").push("groupby").getQueryContext();
  private static final ExecutionStepProperties SOURCE_PROPERTIES =
      new DefaultExecutionStepProperties(SCHEMA, SOURCE_CONTEXT);
  private static final ExecutionStepProperties PROPERTIES = new DefaultExecutionStepProperties(
      SCHEMA,
      STEP_CONTEXT
  );
  private static final Formats FORMATS = Formats.of(
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.JSON)),
      SerdeOption.none()
  );

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private GroupedFactory groupedFactory;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> sourceStep;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Grouped<Struct, GenericRow> grouped;
  @Mock
  private KTable<Struct, GenericRow> sourceTable;
  @Mock
  private KTable<Struct, GenericRow> filteredTable;
  @Mock
  private KGroupedTable<Struct, GenericRow> groupedTable;
  @Captor
  private ArgumentCaptor<TableKeyValueMapper<Struct>> mapperCaptor;
  @Captor
  private ArgumentCaptor<Predicate<Struct, GenericRow>> predicateCaptor;

  private PlanBuilder planBuilder;
  private TableGroupBy<Struct> groupBy;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("qid"));
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(groupedFactory.create(any(), any(KeySerde.class), any())).thenReturn(grouped);
    when(sourceTable.filter(any())).thenReturn(filteredTable);
    when(filteredTable.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedTable);
    when(sourceStep.getProperties()).thenReturn(SOURCE_PROPERTIES);
    when(sourceStep.getSchema()).thenReturn(SCHEMA);
    when(sourceStep.build(any())).thenReturn(
        KTableHolder.unmaterialized(sourceTable, SCHEMA, mock(KeySerdeFactory.class)));
    groupBy = new TableGroupBy<>(
        PROPERTIES,
        sourceStep,
        FORMATS,
        GROUPBY_EXPRESSIONS
    );
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        new StreamsFactories(
            groupedFactory,
            mock(JoinedFactory.class),
            mock(MaterializedFactory.class),
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  @Test
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedTableHolder result = groupBy.build(planBuilder);

    // Then:
    assertThat(result.getGroupedTable(), is(groupedTable));
    verify(sourceTable).filter(any());
    verify(filteredTable).groupBy(mapperCaptor.capture(), same(grouped));
    verifyNoMoreInteractions(filteredTable, sourceTable);
    final GroupByMapper<Struct> mapper = mapperCaptor.getValue().getGroupByMapper();
    assertThat(mapper.getExpressionMetadata(), hasSize(2));
    assertThat(
        mapper.getExpressionMetadata().get(0).getExpression(),
        equalTo(GROUPBY_EXPRESSIONS.get(0))
    );
    assertThat(
        mapper.getExpressionMetadata().get(1).getExpression(),
        equalTo(GROUPBY_EXPRESSIONS.get(1))
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KGroupedTableHolder result = groupBy.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    groupBy.build(planBuilder);

    // Then:
    verify(sourceTable).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = predicateCaptor.getValue();
    assertThat(predicate.test(StructKeyUtil.asStructKey("key"), new GenericRow()), is(true));
    assertThat(predicate.test(StructKeyUtil.asStructKey("key"), null),  is(false));
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    groupBy.build(planBuilder);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    groupBy.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        FORMATS.getKeyFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CONTEXT
    );
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    groupBy.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        FORMATS.getValueFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CONTEXT
    );
  }

  private static Expression columnReference(final String column) {
    return new ColumnReferenceExp(ColumnRef.of(ALIAS, ColumnName.of(column)));
  }
}