package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.streams.StreamGroupByBuilder.ParamsFactory;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.function.Function;
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

  private static final KeyBuilder STRING_KEY_BUILDER = StructKeyUtil
      .keyBuilder(SystemColumns.ROWKEY_NAME, SqlTypes.STRING);
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("PAC"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("MAN"), SqlTypes.STRING)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final LogicalSchema REKEYED_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumns(SCHEMA.value())
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA =
      PhysicalSchema.from(SCHEMA, SerdeOptions.of());

  private static final PhysicalSchema REKEYED_PHYSICAL_SCHEMA =
      PhysicalSchema.from(REKEYED_SCHEMA, SerdeOptions.of());

  private static final List<Expression> GROUP_BY_EXPRESSIONS = ImmutableList.of(
      columnReference("PAC"),
      columnReference("MAN")
  );

  private static final QueryContext STEP_CTX =
      new QueryContext.Stacker().push("foo").push("groupby").getQueryContext();

  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(
      STEP_CTX
  );

  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.JSON.name()),
      SerdeOptions.of()
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
  private ExecutionStep<KStreamHolder<Struct>> sourceStep;
  @Mock
  private Serde<Struct> keySerde;
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
  private ArgumentCaptor<Predicate<Struct, GenericRow>> predicateCaptor;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KStreamHolder<Struct> streamHolder;
  @Mock
  private ParamsFactory paramsFactory;
  @Mock
  private GroupByParams groupByParams;
  @Mock
  private Function<GenericRow, Struct> mapper;

  private StreamGroupBy<Struct> groupBy;
  private StreamGroupByKey groupByKey;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private StreamGroupByBuilder builder;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(streamHolder.getSchema()).thenReturn(SCHEMA);
    when(streamHolder.getStream()).thenReturn(sourceStream);

    when(paramsFactory.build(any(), any(), any())).thenReturn(groupByParams);

    when(groupByParams.getSchema()).thenReturn(REKEYED_SCHEMA);
    when(groupByParams.getMapper()).thenReturn(mapper);

    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getProcessingLogger(any())).thenReturn(processingLogger);
    when(groupedFactory.create(any(), any(Serde.class), any())).thenReturn(grouped);
    when(sourceStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    when(sourceStream.filter(any())).thenReturn(filteredStream);
    when(filteredStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);

    groupBy = new StreamGroupBy<>(
        PROPERTIES,
        sourceStep,
        FORMATS,
        GROUP_BY_EXPRESSIONS
    );

    groupByKey = new StreamGroupByKey(PROPERTIES, sourceStep, FORMATS);

    builder = new StreamGroupByBuilder(queryBuilder, groupedFactory, paramsFactory);
  }

  @Test
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupBy);

    // Then:
    assertThat(result.getGroupedStream(), is(groupedStream));
    verify(sourceStream).filter(any());
    verify(filteredStream).groupBy(any(), same(grouped));
    verifyNoMoreInteractions(filteredStream, sourceStream);
  }

  @Test
  public void shouldBuildGroupByParamsCorrectly() {
    // When:
    builder.build(streamHolder, groupBy);

    // Then:
    verify(paramsFactory).build(
        eq(SCHEMA),
        any(),
        eq(processingLogger)
    );
  }

  @Test
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    builder.build(streamHolder, groupBy);

    // Then:
    verify(sourceStream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = predicateCaptor.getValue();
    assertThat(predicate.test(STRING_KEY_BUILDER.build("foo"), new GenericRow()), is(true));
    assertThat(predicate.test(STRING_KEY_BUILDER.build("foo"), null), is(false));
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    builder.build(streamHolder, groupBy);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldReturnCorrectSchemaForGroupBy() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupBy);

    // Then:
    assertThat(result.getSchema(), is(REKEYED_SCHEMA));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    builder.build(streamHolder, groupBy);

    // Then:
    verify(queryBuilder).buildKeySerde(
        FORMATS.getKeyFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    builder.build(streamHolder, groupBy);

    // Then:
    verify(queryBuilder).buildValueSerde(
        FORMATS.getValueFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldReturnCorrectSchemaForGroupByKey() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupByKey);

    // Then:
    assertThat(result.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldPerformGroupByKeyCorrectly() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupByKey);

    // Then:
    assertThat(result.getGroupedStream(), is(groupedStream));
    verify(sourceStream).groupByKey(grouped);
    verifyNoMoreInteractions(sourceStream);
  }

  @Test
  public void shouldNotBuildGroupByParamsOnGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(paramsFactory, never()).build(any(), any(), any());
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(queryBuilder).buildKeySerde(
        FORMATS.getKeyFormat(),
        PHYSICAL_SCHEMA,
        STEP_CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(queryBuilder).buildValueSerde(
        FORMATS.getValueFormat(),
        PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  private static Expression columnReference(final String column) {
    return new UnqualifiedColumnReferenceExp(ColumnName.of(column));
  }
}