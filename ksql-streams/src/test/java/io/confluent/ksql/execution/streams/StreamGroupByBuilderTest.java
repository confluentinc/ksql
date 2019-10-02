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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
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

public class StreamGroupByBuilderTest {
  private static final SourceName ALIAS = SourceName.of("SOURCE");
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("PAC"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("MAN"), SqlTypes.STRING)
      .build()
      .withAlias(SourceName.of(ALIAS.name()))
      .withMetaAndKeyColsInValue();
  private static final PhysicalSchema PHYSICAL_SCHEMA =
      PhysicalSchema.from(SCHEMA, SerdeOption.none());
  private static final List<Expression> GROUP_BY_EXPRESSIONS = ImmutableList.of(
      columnReference("PAC"),
      columnReference("MAN")
  );
  private static final QueryContext SOURCE_CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("foo").push("source").getQueryContext();
  private static final QueryContext STEP_CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("foo").push("groupby").getQueryContext();
  private static final ExecutionStepProperties SOURCE_PROPERTIES
      = new DefaultExecutionStepProperties(SCHEMA, SOURCE_CTX);
  private static final ExecutionStepProperties PROPERTIES = new DefaultExecutionStepProperties(
      SCHEMA,
      STEP_CTX
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
  private ExecutionStep sourceStep;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Grouped<Struct, GenericRow> grouped;
  @Mock
  private KStream<Struct, GenericRow> sourceStream;
  @Mock
  private KStream<Struct, GenericRow> filteredStream;
  @Mock
  private KGroupedStream<Struct, GenericRow> groupedStream;
  @Captor
  private ArgumentCaptor<GroupByMapper<Struct>> mapperCaptor;
  @Captor
  private ArgumentCaptor<Predicate<Struct, GenericRow>> predicateCaptor;

  private PlanBuilder planBuilder;
  private StreamGroupBy<Struct> streamGroupBy;
  private StreamGroupByKey streamGroupByKey;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(groupedFactory.create(any(), any(KeySerde.class), any())).thenReturn(grouped);
    when(sourceStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    when(sourceStream.filter(any())).thenReturn(filteredStream);
    when(filteredStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);
    when(sourceStep.getProperties()).thenReturn(SOURCE_PROPERTIES);
    when(sourceStep.getSchema()).thenReturn(SCHEMA);
    when(sourceStep.build(any())).thenReturn(
        new KStreamHolder<>(sourceStream, mock(KeySerdeFactory.class)));
    streamGroupBy = new StreamGroupBy<>(
        PROPERTIES,
        sourceStep,
        FORMATS,
        GROUP_BY_EXPRESSIONS
    );
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParams.Factory.class),
        new StreamsFactories(
            groupedFactory,
            mock(JoinedFactory.class),
            mock(MaterializedFactory.class)
        )
    );
    streamGroupByKey = new StreamGroupByKey(PROPERTIES, sourceStep, FORMATS);
  }

  @Test
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedStream result = streamGroupBy.build(planBuilder);

    // Then:
    assertThat(result, is(groupedStream));
    verify(sourceStream).filter(any());
    verify(filteredStream).groupBy(mapperCaptor.capture(), same(grouped));
    verifyNoMoreInteractions(filteredStream, sourceStream);
    final GroupByMapper<?> mapper = mapperCaptor.getValue();
    assertThat(mapper.getExpressionMetadata(), hasSize(2));
    assertThat(
        mapper.getExpressionMetadata().get(0).getExpression(),
        equalTo(GROUP_BY_EXPRESSIONS.get(0))
    );
    assertThat(
        mapper.getExpressionMetadata().get(1).getExpression(),
        equalTo(GROUP_BY_EXPRESSIONS.get(1))
    );
  }

  @Test
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    streamGroupBy.build(planBuilder);

    // Then:
    verify(sourceStream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = predicateCaptor.getValue();
    assertThat(predicate.test(StructKeyUtil.asStructKey("foo"), new GenericRow()), is(true));
    assertThat(predicate.test(StructKeyUtil.asStructKey("foo"), null),  is(false));
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    streamGroupBy.build(planBuilder);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    streamGroupBy.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        FORMATS.getKeyFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    streamGroupBy.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        FORMATS.getValueFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldPerformGroupByKeyCorrectly() {
    // When:
    final KGroupedStream result = streamGroupByKey.build(planBuilder);

    // Then:
    assertThat(result, is(groupedStream));
    verify(sourceStream).groupByKey(grouped);
    verifyNoMoreInteractions(sourceStream);
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupByKey() {
    // When:
    streamGroupByKey.build(planBuilder);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupByKey() {
    // When:
    streamGroupByKey.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(
        FORMATS.getKeyFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupByKey() {
    // When:
    streamGroupByKey.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        FORMATS.getValueFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  private static Expression columnReference(final String column) {
    return new ColumnReferenceExp(ColumnRef.of(ALIAS, ColumnName.of(column)));
  }
}