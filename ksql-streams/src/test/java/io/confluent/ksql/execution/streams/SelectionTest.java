package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.SelectValueMapper.SelectInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SelectionTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("GIRAFFE", SqlTypes.STRING)
      .valueColumn("MANATEE", SqlTypes.INTEGER)
      .valueColumn("RACCOON", SqlTypes.BIGINT)
      .build().withAlias("TEST").withMetaAndKeyColsInValue();

  private static final Expression EXPRESSION1 =
      new QualifiedNameReference(QualifiedName.of("TEST", "GIRAFFE"));

  private static final Expression EXPRESSION2 = new ArithmeticBinaryExpression(
      Operator.ADD,
      new QualifiedNameReference(QualifiedName.of("TEST", "MANATEE")),
      new QualifiedNameReference(QualifiedName.of("TEST", "RACCOON"))
  );
  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
      SelectExpression.of("FOO", EXPRESSION1),
      SelectExpression.of("BAR", EXPRESSION2)
  );

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;
  private final QueryContext queryContext =
      new QueryContext.Stacker(new QueryId("query")).getQueryContext();

  private Selection selection;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(anyString())).thenReturn(processingLogger);
    selection = Selection.of(
        queryContext,
        SCHEMA,
        SELECT_EXPRESSIONS,
        ksqlConfig,
        functionRegistry,
        processingLogContext)
    ;
  }

  @Test
  public void shouldBuildMapperWithCorrectExpressions() {
    // When:
    final SelectValueMapper mapper = selection.getMapper();

    // Then:
    final List<SelectInfo> selectInfos = mapper.getSelects();
    assertThat(
        selectInfos.get(0).evaluator.getExpression(),
        equalTo(EXPRESSION1));
    assertThat(
        selectInfos.get(1).evaluator.getExpression(),
        equalTo(EXPRESSION2));
  }

  @Test
  public void shouldBuildCorrectResultSchema() {
    // When:
    final LogicalSchema resultSchema = selection.getSchema();

    // Then:
    final LogicalSchema expected = new LogicalSchema.Builder()
        .keyColumn("ROWKEY", SqlTypes.STRING)
        .valueColumn("FOO", SqlTypes.STRING)
        .valueColumn("BAR", SqlTypes.BIGINT)
        .build();
    assertThat(resultSchema, equalTo(expected));
  }

  @Test
  public void shouldBuildSelectValueMapperLoggerCorrectly() {
    verify(processingLoggerFactory).getLogger("query.PROJECT");
  }
}