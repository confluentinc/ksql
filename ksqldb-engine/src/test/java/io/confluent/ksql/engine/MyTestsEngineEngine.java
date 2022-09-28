package io.confluent.ksql.engine;

import io.confluent.ksql.engine.generic.GenericExpressionResolverTest;
import io.confluent.ksql.engine.generic.GenericRecordFactoryTest;
import io.confluent.ksql.engine.rewrite.AstSanitizerTest;
import io.confluent.ksql.engine.rewrite.DataSourceExtractorTest;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriterTest;
import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestampTest;
import io.confluent.ksql.engine.rewrite.StatementRewriterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        GenericExpressionResolverTest.class,
        GenericRecordFactoryTest.class,
        AstSanitizerTest.class,
        DataSourceExtractorTest.class,
        ExpressionTreeRewriterTest.class,
//        QueryAnonymizerTest.class, todo this test is causing problems.
        StatementRewriteForMagicPseudoTimestampTest.class,
        StatementRewriterTest.class,
        ImmutabilityTest.class,
        KsqlEngineTest.class,
        KsqlPlanV1Test.class,
        OrphanedTransientQueryCleanerTest.class,
        QueryIdUtilTest.class,
        QueryPlanTest.class,
        RuntimeAssignorTest.class,
        TransientQueryCleanupServiceTest.class,
})
public class MyTestsEngineEngine {
}
