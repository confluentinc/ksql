package io.confluent.ksql;

import io.confluent.ksql.test.driver.AssertExecutorMetaTest;
import io.confluent.ksql.test.driver.KsqlTesterTest;
import io.confluent.ksql.test.driver.TestDriverPipelineTest;
import io.confluent.ksql.test.functional.ImmutabilityTest;
import io.confluent.ksql.test.model.KeyFormatNodeTest;
import io.confluent.ksql.test.model.KsqlVersionTest;
import io.confluent.ksql.test.model.PostConditionsNodeTest;
import io.confluent.ksql.test.model.RecordNodeTest;
import io.confluent.ksql.test.model.SchemaNodeTest;
import io.confluent.ksql.test.model.SourceNodeTest;
import io.confluent.ksql.test.parser.SqlTestReaderTest;
import io.confluent.ksql.test.parser.TestDirectiveTest;
import io.confluent.ksql.test.planned.PlannedTestGeneratorTest;
import io.confluent.ksql.test.planned.PlannedTestRewriterTest;
import io.confluent.ksql.test.planned.PlannedTestsUpToDateTest;
import io.confluent.ksql.test.planned.TestCasePlanLoaderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

// todo issue loading resources for some of the tests
@RunWith(Suite.class)
@Suite.SuiteClasses({
        AssertExecutorMetaTest.class,
//        KsqlTesterTest.class,
        TestDriverPipelineTest.class,
        ImmutabilityTest.class,
        KeyFormatNodeTest.class,
        KsqlVersionTest.class,
        PostConditionsNodeTest.class,
        RecordNodeTest.class,
        SchemaNodeTest.class,
        SourceNodeTest.class,
        SqlTestReaderTest.class,
        TestDirectiveTest.class,
        PlannedTestGeneratorTest.class,
        PlannedTestRewriterTest.class,
        PlannedTestsUpToDateTest.class,
//        TestCasePlanLoaderTest.class,
        // todo add more tests
})
public class MyTestsFunctionalTests {
}
