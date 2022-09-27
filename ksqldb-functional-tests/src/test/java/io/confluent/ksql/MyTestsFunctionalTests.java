package io.confluent.ksql;

import io.confluent.ksql.test.QueryAnonymizerTest;
import io.confluent.ksql.test.QueryTranslationTest;
import io.confluent.ksql.test.SchemaTranslationTest;
import io.confluent.ksql.test.SchemaTranslationWithSchemaIdTest;
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
import io.confluent.ksql.test.rest.RestQueryTranslationTest;
import io.confluent.ksql.test.rest.RestTestExecutorTest;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSchemaSerdeSupplierTest;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplierTest;
import io.confluent.ksql.test.serde.kafka.KafkaSerdeSupplierTest;
import io.confluent.ksql.test.tools.ExpectedRecordComparatorTest;
import io.confluent.ksql.test.tools.HistoricalTestingFunctionalTest;
import io.confluent.ksql.test.tools.KsqlTestingToolTest;
import io.confluent.ksql.test.tools.RecordTest;
import io.confluent.ksql.test.tools.StubKafkaServiceTest;
import io.confluent.ksql.test.tools.TestExecutorTest;
import io.confluent.ksql.test.tools.TestExecutorUtilTest;
import io.confluent.ksql.test.tools.TestJsonMapperTest;
import io.confluent.ksql.test.tools.command.TestOptionsParserTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

// todo issue loading resources for some of the tests
@RunWith(Suite.class)
@Suite.SuiteClasses({
        AssertExecutorMetaTest.class,
        KsqlTesterTest.class,
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
        TestCasePlanLoaderTest.class,
        RestQueryTranslationTest.class,
        RestTestExecutorTest.class,
        ValueSpecJsonSchemaSerdeSupplierTest.class,
        ValueSpecJsonSerdeSupplierTest.class,
        KafkaSerdeSupplierTest.class,
        TestOptionsParserTest.class,
        ExpectedRecordComparatorTest.class,
        HistoricalTestingFunctionalTest.class,
        KsqlTestingToolTest.class,
        RecordTest.class,
        StubKafkaServiceTest.class,
        TestExecutorTest.class,
        TestExecutorUtilTest.class,
        TestJsonMapperTest.class,
        QueryAnonymizerTest.class,
        QueryTranslationTest.class,
        SchemaTranslationTest.class,
        SchemaTranslationWithSchemaIdTest.class,
})
public class MyTestsFunctionalTests {
}
