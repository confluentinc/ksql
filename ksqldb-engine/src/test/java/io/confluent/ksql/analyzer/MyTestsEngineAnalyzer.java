package io.confluent.ksql.analyzer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AggregateAnalyzerTest.class,
        AnalysisTest.class,
        AnalyzerFunctionalTest.class,
        ColumnReferenceValidatorTest.class,
        FilterTypeValidatorTest.class,
        PullQueryValidatorTest.class,
        PushQueryValidatorTest.class,
        QueryAnalyzerFunctionalTest.class,
        QueryAnalyzerTest.class,
        QueryValidatorUtilTest.class,
        RewrittenAnalysisTest.class,
        SourceSchemasTest.class,
})
public class MyTestsEngineAnalyzer {
}
