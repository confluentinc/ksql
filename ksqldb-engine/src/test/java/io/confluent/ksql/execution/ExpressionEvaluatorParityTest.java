package io.confluent.ksql.execution;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.interpreter.InterpretedExpressionFactory;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
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
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
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
  private static final long ROWPARTITION = 5;
  private static final long ROWOFFSET = 100;
  private static final long ORDER_TIME = 100;
  private static final String ITEM_ID = "item_id_0";
  private static final long ITEM_ITEM_ID = 890;
  private static final String ITEM_NAME = "item_name";
  private static final long CATEGORY_ID = 456;
  private static final String CATEGORY_NAME = "cat";
  private static final int ORDER_UNITS = 20;
  private static final Timestamp TIMESTAMP = new Timestamp(1273881610000L);
  private static final Time TIME = new Time(7205000);
  private static final Date DATE = new Date(864000000);
  private static final ByteBuffer BYTES = ByteBuffer.wrap(new byte[] {123});

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
        doubleArray, map, null, TIMESTAMP, TIME, DATE, BYTES, ROW_TIME, ROWPARTITION, ROWOFFSET, ORDER_TIME);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void shouldDoComparisons() throws Exception {
    assertOrders("ORDERID > 9 AND ORDERUNITS < 100", true);
    assertOrders("ORDERTIME <= 100 AND ITEMID + '-blah' = 'item_id_0-blah'", true);
    assertOrders("ORDERID > 9.5 AND ORDERUNITS < 50.75", true);
    assertOrdersError("ORDERID > ARRAY[0]",
        compileTime("Cannot compare BIGINT to ARRAY<INTEGER>"),
        compileTime("Cannot compare BIGINT to ARRAY<INTEGER>"));
    assertOrders("ARRAY[0,1] = ARRAY[0,1]", true);
    assertOrders("ARRAYCOL = ARRAY[3.5e0, 5.25e0]", true);
    assertOrders("ARRAYCOL = ARRAY[3.5e0, 7.25e0]", false);
    assertOrders("MAPCOL = MAP('abc' := 6.75e0, 'def' := 9.5e0)", true);
    assertOrders("MAPCOL = MAP('abc' := 6.75e0, 'xyz' := 9.5e0)", false);
    assertOrdersError("ARRAYCOL = MAPCOL",
        compileTime("Cannot compare ARRAY<DOUBLE> to MAP<STRING, DOUBLE>"),
        compileTime("Cannot compare ARRAY<DOUBLE> to MAP<STRING, DOUBLE>"));
    assertOrders("TIMESTAMPCOL > DATECOL", true);
    assertOrders("TIMECOL > '03:00:00'", false);
    assertOrders("DATECOL = '1970-01-11'", true);
    assertOrdersError("TIMESTAMPCOL = TIMECOL",
        compileTime("Unexpected comparison to TIME: TIMESTAMP"),
        compileTime("Cannot compare TIMESTAMP to TIME"));
    assertOrders("BYTESCOL = BYTESCOL", true);
  }

  @Test
  public void shouldDoComparisons_null() throws Exception {
    ordersRow = GenericRow.genericRow(null, null, null, null, null, null, null, null, null);
    assertOrdersError("1 = null",
        compileTime("Unexpected error generating code for Test"),
        compileTime("Invalid expression: Comparison with NULL not supported: INTEGER = NULL"));
    assertOrders("ORDERID = 1", false);
    assertOrders("ITEMID > 'a'", false);
  }

  @Test
  public void shouldDereference() throws Exception {
    assertOrders("ITEMINFO->NAME", ITEM_NAME);
    assertOrders("ITEMINFO->CATEGORY->NAME", CATEGORY_NAME);
    assertOrders("'a-' + ITEMINFO->CATEGORY->NAME + '-b'", "a-cat-b");
    // Due to https://github.com/confluentinc/ksql/issues/9136 the next assert cannot be updated.
    // assertOrders("ADDRESS->STREET + 'foo'", null);
  }

  @Test
  public void shouldDoUdfs() throws Exception {
    assertOrders("CONCAT('abc-', 'def')", "abc-def");
    assertOrders("SPLIT('a-b-c', '-')", ImmutableList.of("a", "b", "c"));
    assertOrders("TIMESTAMPADD(SECONDS, 1, '2020-01-01')", new Timestamp(1577836801000L));
    assertOrdersError("SPLIT(123, '2')",
        compileTime("Function 'split' does not accept parameters (INTEGER, STRING)"),
        compileTime("Function 'split' does not accept parameters (INTEGER, STRING)"));
  }

  @Test
  public void shouldDoArithmetic() throws Exception {
    assertOrders("1 + 2 + 3 + 4", 10);
    assertOrders("'foo' + 'bar' + 'baz'", "foobarbaz");
    assertOrders("ORDERUNITS % 3", 2);
    assertOrders("ORDERUNITS + 6", 26);
    assertOrders("100 / ORDERUNITS + 6", 11);
    assertOrders("1.23E10 * 1E-1", 1.23E9d);
    assertOrders("1.234567 * 5.678", new BigDecimal("7.009871426"));
    assertOrders("3.4 / 2.0 + 199.4", new BigDecimal("201.100000"));
    assertOrdersError("3.456 / 2.654 + 199.4",
        evalLogger("Rounding necessary"),
        evalLogger("Rounding necessary"));
    assertOrdersError("1 + 'a'",
        compileTime("Error processing expression"),
        compileTime("Error processing expression"));
  }

  @Test
  public void shouldDoArithmetic_nulls() throws Exception {
    ordersRow = GenericRow.genericRow(null, null, null, null, null, null, null, null, null);

    //The error message coming from the compiler and the interpreter should be the same
    assertOrdersError(
        "1 + null",
        compileTime("Error processing expression"),
        compileTime("Error processing expression")
    );

    assertOrdersError("'a' + null",  compileTime("Error processing expression"),
        compileTime("Error processing expression"));

    assertOrdersError("MAP(1 := 'cat') + null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("Array[1,2,3] + null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("1 - null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("1 * null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("1 / null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("null + null",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("null / 0",  compileTime("Error processing expression"),
            compileTime("Error processing expression"));

    assertOrdersError("1 + ORDERID",  evalLogger(null));
  }

  @Test
  public void shouldDoCaseExpressions() throws Exception {
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
  public void shouldDoCasts() throws Exception {
    assertOrders("CAST(1 as DOUBLE)", 1.0d);
    assertOrders("CAST(1 as BIGINT)", 1L);
    assertOrders("CAST(1 as DECIMAL(2,1))", new BigDecimal("1.0"));
    assertOrders("CAST(1.23E10 as DOUBLE)", 1.23E10d);
    assertOrders("CAST(1.23E1 as DECIMAL(5,1))", new BigDecimal("12.3"));
    assertOrders("CAST(1.23E1 as INTEGER)", 12);
    assertOrders("CAST(1.23E1 as BIGINT)", 12L);
    assertOrders("CAST(2.345 as DOUBLE)", 2.345E0d);
    assertOrders("CAST(2.345 as INTEGER)", 2);
    assertOrders("CAST(2.345 as BIGINT)", 2L);
    assertOrders("CAST('123' as INTEGER)", 123);
    assertOrders("CAST('123' as BIGINT)", 123L);
    assertOrders("CAST('123' as DOUBLE)", 1.23E2);
    assertOrders("CAST('123' as DECIMAL(5,1))", new BigDecimal("123.0"));
    assertOrdersError("CAST('123' as ARRAY<INTEGER>)",
        compileTime("Cast of STRING to ARRAY<INTEGER> is not supported"),
        compileTime("Unsupported cast from STRING to ARRAY<INTEGER>"));
    assertOrders("CAST('true' as BOOLEAN)", true);
    assertOrders("TRUE AND CAST('true' as BOOLEAN)", true);
    assertOrders("CAST('01:00:00.005' as TIME)", new Time(3600005));
    assertOrders("CAST('2002-02-20' as DATE)", new Date(1014163200000L));
    assertOrders("CAST(TIMESTAMPCOL as DATE)", new Date(1273881600000L));
    assertOrders("CAST(TIMESTAMPCOL as TIME)", new Time(10000));
    assertOrdersError("CAST(TIMECOL as TIMESTAMP)",
        compileTime("Cast of TIME to TIMESTAMP is not supported"),
        compileTime("Unsupported cast from TIME to TIMESTAMP"));
    assertOrdersError("CAST(BYTESCOL as STRING)",
        compileTime("Cast of BYTES to STRING is not supported"),
        compileTime("Unsupported cast from BYTES to STRING"));
    assertOrders("CAST(BYTESCOL as BYTES)", BYTES);
  }

  @Test
  public void shouldDoCasts_nulls() throws Exception {
    ordersRow = GenericRow.genericRow(null, null, null, null, null, null, null, null, null);
    assertOrdersError("cast(null as int)",
        compileTime("Unexpected error generating code for expression: CAST(null AS INTEGER)"),
        compileTime("Unexpected error generating code for expression: CAST(null AS INTEGER)"));
    assertOrders("cast(ORDERID as int)", null);
    assertOrders("cast(ITEMID as int)", null);
  }

  @Test
  public void createComplexTypes() throws Exception {
    assertOrders("Array[1,2,3]", ImmutableList.of(1, 2, 3));
    assertOrdersError("Array[1,'a',3]",
        EvaluatorError.compileTime("invalid input syntax for type INTEGER"),
        EvaluatorError.compileTime("invalid input syntax for type INTEGER"));
    assertOrders("MAP(1 := 'a', 2 := 'b')", ImmutableMap.of(1, "a", 2, "b"));
    assertOrdersError("MAP(1 := 'a', 'key' := 'b')",
        EvaluatorError.compileTime("invalid input syntax for type INTEGER"),
        EvaluatorError.compileTime("invalid input syntax for type INTEGER"));
    assertOrders("STRUCT(A := 123, B := 'abc')", new Struct(SchemaBuilder.struct().optional()
        .field("A", SchemaBuilder.int32().optional().build())
        .field("B", SchemaBuilder.string().optional().build())
        .build())
        .put("A", 123)
        .put("B", "abc"));
  }

  @Test
  public void shouldDoNot() throws Exception {
    assertOrders("not true", false);
    assertOrders("not false", true);
    assertOrders("not cast('true' as BOOLEAN)", false);
    assertOrders("not ORDERID > 9 AND ORDERUNITS < 100", false);
    assertOrders("ORDERID > 9 AND not ORDERUNITS < 100", false);
    assertOrders("not (ORDERID > 9 AND ORDERUNITS < 100)", false);
  }

  @Test
  public void shouldDoLambda() throws Exception {
    assertOrders("filter(Array[1,2,3], x => x > 5)", ImmutableList.of());
    assertOrders("filter(Array[1,2,3], x => x > 1)", ImmutableList.of(2, 3));
    assertOrders("filter(Array[Array[1,2,3],Array[4,5,6],Array[7,8,9]], "
        + "x => array_length(filter(x, y => y % 2 = 0)) > 1)",
        ImmutableList.of(ImmutableList.of(4, 5, 6)));
    assertOrders("transform(Array[Array[1,2,3],Array[4,5,6],Array[7,8,9]], "
            + "x => transform(x, y => y % 2 = 0))",
        ImmutableList.of(
            ImmutableList.of(false, true, false),
            ImmutableList.of(true, false, true),
            ImmutableList.of(false, true, false)));
    assertOrders("filter(MAP(1 := 'cat', 2 := 'dog', 3 := 'rat'), (x,y)  => x > 1 "
        + "AND instr(y, 'at') > 0)", ImmutableMap.of(3, "rat"));
    assertOrders("transform(MAP(1 := MAP('a' := 'cat', 'b' := 'rat'),"
        + " 2 := MAP('c' := 'dog', 'd' := 'frog')), (x,y) => x, (x,y) => "
        + "transform(y, (a,b) => a + 'z', (a,b) => 'pet ' + b))",
        ImmutableMap.of(1, ImmutableMap.of("bz", "pet rat", "az", "pet cat"),
            2, ImmutableMap.of("dz", "pet frog", "cz", "pet dog")));
  }

  private void assertOrders(
      final String expressionStr,
      final Object result
  ) throws Exception {
    assertResult(STREAM_NAME, expressionStr, ordersRow, result, Optional.empty(), Optional.empty());
  }

  private void assertOrdersError(
      final String expressionStr,
      final EvaluatorError compilerError
  ) throws Exception {
    assertResult(STREAM_NAME, expressionStr, ordersRow, null, Optional.of(compilerError),
        Optional.empty());
  }

  private void assertOrdersError(
      final String expressionStr,
      final EvaluatorError compilerError,
      final EvaluatorError interpreterError
  ) throws Exception {
    assertResult(STREAM_NAME, expressionStr, ordersRow, null, Optional.of(compilerError),
        Optional.of(interpreterError));
  }

  private void assertResult(
      final String streamName,
      final String expressionStr,
      final GenericRow row,
      final Object result,
      final Optional<EvaluatorError> compilerError,
      final Optional<EvaluatorError> interpreterError
  ) throws Exception {
    Expression expression = getWhereExpression(streamName, expressionStr);

    ColumnReferenceRewriter columnReferenceRewriter = new ColumnReferenceRewriter();
    Expression rewritten
        = ExpressionTreeRewriter.rewriteWith(columnReferenceRewriter::process, expression);

    LogicalSchema schema = metaStore.getSource(SourceName.of(streamName)).getSchema()
        .withPseudoAndKeyColsInValue(false);

    runEvaluator(row,
        () -> CodeGenRunner.compileExpression(
            rewritten,
            "Test",
            schema,
            ksqlConfig,
            metaStore
        ),
        result,
        compilerError);

    runEvaluator(row,
        () -> InterpretedExpressionFactory.create(rewritten,  schema, metaStore, ksqlConfig),
        result,
        interpreterError);
  }

  private void runEvaluator(
      final GenericRow row,
      Callable<ExpressionEvaluator> compile,
      final Object expectedResult,
      final Optional<EvaluatorError> error) throws Exception {
    ExpressionEvaluator expressionEvaluator = null;
    try {
      expressionEvaluator = compile.call();
    } catch (Exception e) {
      if (error.isPresent()
          && error.get().getErrorTime() == ErrorTime.COMPILE_TIME) {
        assertThat(e.getMessage(), containsString(error.get().getMessage()));
        return;
      } else {
        throw e;
      }
    }
    Object result = null;
    try {
      result
          = expressionEvaluator.evaluate(row, null, processingLogger, () -> "ERROR!!!");
    } catch (Exception e) {
      if (error.isPresent()
          && error.get().getErrorTime() == ErrorTime.EVALUATION_TIME) {
        assertThat(e.getMessage(), containsString(error.get().getMessage()));
        return;
      } else {
        throw e;
      }
    }

    if (error.isPresent() && error.get().getErrorTime() == ErrorTime.EVALUATION_LOGGER) {
      verify(processingLogger, times(1)).error(errorMessageCaptor.capture());
      RecordProcessingError processingError
          = ((RecordProcessingError) errorMessageCaptor.getValue());
      if (error.get().getMessage() != null) {
        assertThat(
            "processing error should have exception",
            processingError.getException().isPresent());
        assertThat(processingError.getException().get().getMessage(),
            containsString(error.get().getMessage()));
      }
    } else {
      verify(processingLogger, never()).error(any());
    }
    reset(processingLogger);

    assertThat(result, is(expectedResult));
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
  
  private static class EvaluatorError {

    private final ErrorTime errorTime;
    private final String message;

    public EvaluatorError(final ErrorTime errorTime, final String message) {
      this.errorTime = errorTime;
      this.message = message;
    }

    public static EvaluatorError compileTime(final String message) {
      return new EvaluatorError(ErrorTime.COMPILE_TIME, message);
    }

    public static EvaluatorError evaluation(final String message) {
      return new EvaluatorError(ErrorTime.EVALUATION_TIME, message);
    }

    public static EvaluatorError evalLogger(final String message) {
      return new EvaluatorError(ErrorTime.EVALUATION_LOGGER, message);
    }

    public ErrorTime getErrorTime() {
      return errorTime;
    }

    public String getMessage() {
      return message;
    }
  }

  public enum ErrorTime {
    COMPILE_TIME,
    EVALUATION_TIME,
    EVALUATION_LOGGER
  }

  private static EvaluatorError compileTime(final String message) {
    return EvaluatorError.compileTime(message);
  }

  private static EvaluatorError evaluation(final String message) {
    return EvaluatorError.evaluation(message);
  }

  private static EvaluatorError evalLogger(final String message) {
    return EvaluatorError.evalLogger(message);
  }
}
