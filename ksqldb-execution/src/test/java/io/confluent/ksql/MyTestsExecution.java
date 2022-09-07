package io.confluent.ksql;

import io.confluent.ksql.execution.ImmutabilityTest;
import io.confluent.ksql.execution.codegen.CompiledExpressionTest;
import io.confluent.ksql.execution.codegen.LambdaMappingUtilTest;
import io.confluent.ksql.execution.codegen.SqlToJavaVisitorTest;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccessTest;
import io.confluent.ksql.execution.codegen.helpers.CastEvaluatorTest;
import io.confluent.ksql.execution.codegen.helpers.LambdaUtilTest;
import io.confluent.ksql.execution.codegen.helpers.LikeEvaluatorTest;
import io.confluent.ksql.execution.codegen.helpers.NullSafeTest;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunctionTest;
import io.confluent.ksql.execution.codegen.helpers.SqlTypeCodeGenTest;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommandTest;
import io.confluent.ksql.execution.ddl.commands.KsqlTopicTest;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatterTest;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpressionTest;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpressionTest;
import io.confluent.ksql.execution.expression.tree.BetweenPredicateTest;
import io.confluent.ksql.execution.expression.tree.DoubleLiteralTest;
import io.confluent.ksql.execution.expression.tree.FunctionCallTest;
import io.confluent.ksql.execution.expression.tree.InListExpressionTest;
import io.confluent.ksql.execution.expression.tree.InPredicateTest;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCallTest;
import io.confluent.ksql.execution.expression.tree.LikePredicateTest;
import io.confluent.ksql.execution.expression.tree.WhenClauseTest;
import io.confluent.ksql.execution.function.UdafUtilTest;
import io.confluent.ksql.execution.function.UdfUtilTest;
import io.confluent.ksql.execution.function.udaf.KudafAggregatorTest;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregatorTest;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapperTest;
import io.confluent.ksql.execution.function.udtf.TableFunctionApplierTest;
import io.confluent.ksql.execution.interpreter.InterpretedExpressionTest;
import io.confluent.ksql.execution.plan.ExecutionStepTest;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoinTest;
import io.confluent.ksql.execution.plan.StreamAggregateTest;
import io.confluent.ksql.execution.plan.StreamFilterTest;
import io.confluent.ksql.execution.plan.StreamFlatMapTest;
import io.confluent.ksql.execution.plan.StreamGroupByKeyTest;
import io.confluent.ksql.execution.plan.StreamGroupByTest;
import io.confluent.ksql.execution.plan.StreamGroupByV1Test;
import io.confluent.ksql.execution.plan.StreamSelectKeyTest;
import io.confluent.ksql.execution.plan.StreamSinkTest;
import io.confluent.ksql.execution.plan.StreamSourceTest;
import io.confluent.ksql.execution.plan.StreamTableJoinTest;
import io.confluent.ksql.execution.plan.StreamWindowedAggregateTest;
import io.confluent.ksql.execution.plan.TableAggregateTest;
import io.confluent.ksql.execution.plan.TableFilterTest;
import io.confluent.ksql.execution.plan.TableGroupByTest;
import io.confluent.ksql.execution.plan.TableGroupByV1Test;
import io.confluent.ksql.execution.plan.TableSelectKeyTest;
import io.confluent.ksql.execution.plan.TableSelectTest;
import io.confluent.ksql.execution.plan.TableSinkTest;
import io.confluent.ksql.execution.plan.TableSourceTest;
import io.confluent.ksql.execution.plan.TableSourceV1Test;
import io.confluent.ksql.execution.plan.TableTableJoinTest;
import io.confluent.ksql.execution.plan.WindowedStreamSourceTest;
import io.confluent.ksql.execution.plan.WindowedTableSourceTest;
import io.confluent.ksql.execution.runtime.RuntimeBuildContextTest;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactoryTest;
import io.confluent.ksql.execution.transform.select.SelectValueMapperTest;
import io.confluent.ksql.execution.transform.select.SelectionTest;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicateTest;
import io.confluent.ksql.execution.util.CoercionUtilTest;
import io.confluent.ksql.execution.util.ComparisonUtilTest;
import io.confluent.ksql.execution.util.ExpressionTypeManagerTest;
import io.confluent.ksql.execution.util.FunctionArgumentsUtilTest;
import io.confluent.ksql.execution.util.KeyUtilTest;
import io.confluent.ksql.logging.processing.RecordProcessingErrorTest;
import io.confluent.ksql.schema.ksql.ColumnNamesTest;
import io.confluent.ksql.schema.query.QuerySchemasTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import javax.xml.stream.StreamFilter;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ArrayAccessTest.class,
        CastEvaluatorTest.class,
        LambdaUtilTest.class,
        LikeEvaluatorTest.class,
        NullSafeTest.class,
        SearchedCaseFunctionTest.class,
        SqlTypeCodeGenTest.class,
        CompiledExpressionTest.class,
        LambdaMappingUtilTest.class,
        SqlToJavaVisitorTest.class,
        CreateSourceCommandTest.class,
        KsqlTopicTest.class,
        ExpressionFormatterTest.class,
        ArithmeticBinaryExpressionTest.class,
        ArithmeticUnaryExpressionTest.class,
        BetweenPredicateTest.class,
        DoubleLiteralTest.class,
        FunctionCallTest.class,
        InListExpressionTest.class,
        InPredicateTest.class,
        LambdaFunctionCallTest.class,
        LikePredicateTest.class,
        WhenClauseTest.class,
        KudafAggregatorTest.class,
        KudafUndoAggregatorTest.class,
        KudtfFlatMapperTest.class,
        TableFunctionApplierTest.class,
        UdafUtilTest.class,
        UdfUtilTest.class,
        InterpretedExpressionTest.class,
        ExecutionStepTest.class,
        ForeignKeyTableTableJoinTest.class,
        StreamAggregateTest.class,
        StreamFilterTest.class,
        StreamFlatMapTest.class,
        StreamGroupByKeyTest.class,
        StreamGroupByTest.class,
        StreamGroupByV1Test.class,
        StreamSelectKeyTest.class,
        StreamSinkTest.class,
        StreamSourceTest.class,
        StreamTableJoinTest.class,
        StreamWindowedAggregateTest.class,
        TableAggregateTest.class,
        TableFilterTest.class,
        TableGroupByTest.class,
        TableGroupByV1Test.class,
        TableSelectKeyTest.class,
        TableSelectTest.class,
        TableSinkTest.class,
        TableSourceTest.class,
        TableSourceV1Test.class,
        TableTableJoinTest.class,
        WindowedStreamSourceTest.class,
        WindowedTableSourceTest.class,
        RuntimeBuildContextTest.class,
        SelectionTest.class,
        SelectValueMapperFactoryTest.class,
        SelectValueMapperTest.class,
        SqlPredicateTest.class,
        CoercionUtilTest.class,
        ComparisonUtilTest.class,
        ExpressionTypeManagerTest.class,
        FunctionArgumentsUtilTest.class,
        KeyUtilTest.class,
        ImmutabilityTest.class,
        RecordProcessingErrorTest.class,
        ColumnNamesTest.class,
        QuerySchemasTest.class,
})
public class MyTestsExecution {
}
