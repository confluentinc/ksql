package io.confluent.ksql.execution;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.evaluator.ExpressionInterpreter;
import io.confluent.ksql.execution.evaluator.Interpreter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class ExpressionEvaluatorParityTest {

  private static final String STREAM_NAME = "ORDERS";
  private static final long ORDER_ID = 10;
  private static final long ROW_TIME = 20000;
  private static final long ORDER_TIME = 100;
  private static final String ITEM_ID = "item_id_0";
  private static final long ITEM_ITEM_ID = 890;
  private static final String ITEM_NAME = "item_name";
  private static final long CATEGORY_ID = 456;
  private static final String CATEGORY_NAME = "cat";
  private static final int ORDER_UNITS = 20;

  private GenericRow ordersRow;

  @Mock
  private ProcessingLogger processingLogger;
  @Captor
  private ArgumentCaptor<ErrorMessage> errorMessageCaptor;

  private MetaStore metaStore;
  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(TestFunctionRegistry.INSTANCE.get());
    ksqlConfig = new KsqlConfig(Collections.emptyMap());

    SqlType itemInfoType = metaStore.getSource(SourceName.of("ORDERS"))
        .getSchema()
        .findColumn(ColumnName.of("ITEMINFO"))
        .get().type();
    final Schema itemInfoTypeSchema = SchemaConverters
        .sqlToConnectConverter()
        .toConnectSchema(itemInfoType);
    final Schema categorySchema = itemInfoTypeSchema.field("CATEGORY").schema();
    Struct itemInfo = new Struct(itemInfoTypeSchema)
        .put("ITEMID", ITEM_ITEM_ID)
        .put("NAME", ITEM_NAME)
        .put("CATEGORY",
            new Struct(categorySchema).put("ID", CATEGORY_ID).put("NAME", CATEGORY_NAME));
    final List<Double> doubleArray = ImmutableList.of(3.5d, 5.25d);
    final Map<String, Double> map = ImmutableMap.of("abc", 6.75d, "def", 9.5d);
    // Note key isn't included first since it's assumed that it's provided as a value
    ordersRow = GenericRow.genericRow(ORDER_ID, ITEM_ID, itemInfo, ORDER_UNITS,
        doubleArray, map, null, ROW_TIME, ORDER_TIME);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void shouldDoComparisons() {
    assertOrders("ORDERID > 9 AND ORDERUNITS < 100", true);
    assertOrders("ORDERTIME <= 100 AND ITEMID + '-blah' = 'item_id_0-blah'", true);
  }

  @Test
  public void shouldDereference() {
    assertOrders("ITEMINFO->NAME", ITEM_NAME);
    assertOrders("ITEMINFO->CATEGORY->NAME", CATEGORY_NAME);
    assertOrders("'a-' + ITEMINFO->CATEGORY->NAME + '-b'", "a-cat-b");
  }

  @Test
  public void shouldDoUdfs() {
    assertOrders("CONCAT('abc-', 'def')", "abc-def");
    assertOrders("SPLIT('a-b-c', '-')", ImmutableList.of("a", "b", "c"));
  }

  @Test
  public void shouldDoArithmetic() {
    assertOrders("1 + 2 + 3 + 4", 10);
    assertOrders("'foo' + 'bar' + 'baz'", "foobarbaz");
    assertOrders("ORDERUNITS % 3", 2);
    assertOrders("ORDERUNITS + 6", 26);
    assertOrders("100 / ORDERUNITS + 6", 11);
    assertOrders("1.23E10 * 1E-1", 1.23E9d);
    assertOrders("1.234567 * 5.678", new BigDecimal("7.009871426"));
    assertOrders("3.4 / 2.0 + 199.4", new BigDecimal("201.100000"));
    assertOrdersError("3.456 / 2.654 + 199.4", "Rounding necessary");
    assertOrdersTypeError("1 + 'a'", "Unsupported arithmetic types");
  }

  @Test
  public void shouldDoCaseExpressions() {
    assertOrders("CASE WHEN ORDERUNITS > 15 THEN 'HIGH' WHEN ORDERUNITS > 10 THEN 'LOW' "
        + "ELSE 'foo' END", "HIGH");
    assertOrders("CASE WHEN ORDERUNITS > 25 THEN 'HIGH' WHEN ORDERUNITS > 10 THEN 'LOW' "
        + "ELSE 'foo' END", "LOW");
    assertOrders("CASE WHEN ORDERUNITS > 25 THEN 'HIGH' WHEN ORDERUNITS > 20 THEN 'LOW' "
        + "ELSE 'foo' END", "foo");
    assertOrders("CASE WHEN ORDERUNITS > 25 THEN 'HIGH' WHEN ORDERUNITS > 20 THEN 'LOW' "
        + "END", null);
  }

  @Test
  public void shouldDoCasts() {
//    assertOrders("CAST(1 as DOUBLE)", 1.0d);
//    assertOrders("CAST(1 as BIGINT)", 1L);
//    assertOrders("CAST(1 as DECIMAL(2,1))", new BigDecimal("1.0"));
//    assertOrders("CAST(1.23E10 as DOUBLE)", 1.23E10d);
//    assertOrders("CAST(1.23E1 as DECIMAL(5,1))", new BigDecimal("12.3"));
//    assertOrders("CAST(1.23E1 as INTEGER)", 12);
//    assertOrders("CAST(1.23E1 as BIGINT)", 12L);
//    assertOrders("CAST(2.345 as DOUBLE)", 2.345E0d);
//    assertOrders("CAST(2.345 as INTEGER)", 2);
//    assertOrders("CAST(2.345 as BIGINT)", 2L);
//    assertOrders("CAST('123' as INTEGER)", 123);
//    assertOrders("CAST('123' as BIGINT)", 123L);
//    assertOrders("CAST('123' as DOUBLE)", 1.23E2);
//    assertOrders("CAST('123' as DECIMAL(5,1))", new BigDecimal("123.0"));
    assertOrdersTypeError("CAST('123' as ARRAY<INTEGER>)",
        "Cast of STRING to ARRAY<INTEGER> is not supported");
  }

  private void assertOrders(
      final String expressionStr,
      final Object result
  ) {
    assertResult(STREAM_NAME, expressionStr, ordersRow, result, Optional.empty(), Optional.empty());
  }

  private void assertOrdersTypeError(
      final String expressionStr,
      final String errorMessage
  ) {
    assertResult(STREAM_NAME, expressionStr, ordersRow, null, Optional.empty(), Optional.of(errorMessage));
  }

  private void assertOrdersError(
      final String expressionStr,
      final String errorMessage
  ) {
    assertResult(STREAM_NAME, expressionStr, ordersRow, null, Optional.of(errorMessage),
        Optional.empty());
  }

  private void assertResult(
      final String streamName,
      final String expressionStr,
      final GenericRow row,
      final Object result,
      final Optional<String> errorEvaluating,
      final Optional<String> typeErrorMessage
  ) {
    Expression expression = getWhereExpression(streamName, expressionStr);

    ColumnReferenceRewriter columnReferenceRewriter = new ColumnReferenceRewriter();
    Expression rewritten
        = ExpressionTreeRewriter.rewriteWith(columnReferenceRewriter::process, expression);

    LogicalSchema schema = metaStore.getSource(SourceName.of(streamName)).getSchema()
        .withPseudoAndKeyColsInValue(false);

    int errorCreating = 0;
    ExpressionMetadata expressionMetadata = null;
    try {
      expressionMetadata = CodeGenRunner.compileExpression(
          rewritten,
          "Test",
          schema,
          ksqlConfig,
          metaStore
      );
    } catch (Exception e) {
      if (typeErrorMessage.isPresent()) {
        assertThat(e.getMessage(), containsString(typeErrorMessage.get()));
        errorCreating++;
      } else {
        throw e;
      }
    }

    ExpressionInterpreter expressionInterpreter = null;
    try {
      expressionInterpreter =
          Interpreter.create(rewritten,  schema, metaStore, ksqlConfig);
    } catch (Exception e) {
      if (typeErrorMessage.isPresent()) {
        assertThat(e.getMessage(), containsString(typeErrorMessage.get()));
        errorCreating++;
      } else {
        throw e;
      }
    }

    if (typeErrorMessage.isPresent()) {
      if (errorCreating == 2) {
        return;
      } else {
        fail("Expected failure from both evaluators");
      }
    }

    Object compiledResult
        = expressionMetadata.evaluate(row, null, processingLogger, () -> "ERROR!!!");
    Object interpretedResult
        = expressionInterpreter.evaluate(row, null, processingLogger, () -> "ERROR!!!");


    assertThat(compiledResult, is(result));
    assertThat(interpretedResult, is(result));

    if (errorEvaluating.isPresent()) {
      verify(processingLogger, times(2)).error(errorMessageCaptor.capture());
      errorMessageCaptor.getAllValues().stream()
          .map(RecordProcessingError.class::cast)
          .forEach(em -> assertThat(em.getException().get().getMessage(),
              containsString(errorEvaluating.get())));
    } else {
      verify(processingLogger, never()).error(any());
    }
    reset(processingLogger);
  }

  private Expression getWhereExpression(final String table, String expression) {
    final Query statement = (Query) KsqlParserTestUtil
        .buildSingleAst("SELECT * FROM " + table + " WHERE " + expression + ";", metaStore)
        .getStatement();

    assertThat(statement.getWhere().isPresent(), is(true));
    return statement.getWhere().get();
  }

  public static final class ColumnReferenceRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    ColumnReferenceRewriter() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context<Void> ctx
    ) {
      return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
    }
  }
}
