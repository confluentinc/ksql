package io.confluent.ksql.test.tools;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FakeInsertValuesExecutorTest {
  private ServiceContext serviceContext;
  private KsqlEngine ksqlEngine;
  private KsqlConfig ksqlConfig;
  private TestCase testCase;
  private FakeKafkaService fakeKafkaService;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    final QttTestFile qttTestFile = JsonMapper.INSTANCE.mapper
            .readValue(new File("src/test/resources/testing_tool_tests.json"), QttTestFile.class);
    final TestCaseNode testCaseNode = qttTestFile.tests.get(0);
    testCase = testCaseNode.buildTests(
            new File("src/test/resources/testing_tool_tests.json").toPath(),
            TestFunctionRegistry.INSTANCE.get()
    ).get(0);

    serviceContext = TestExecutor.getServiceContext();
    ksqlEngine = TestExecutor.getKsqlEngine(serviceContext);
    ksqlConfig = new KsqlConfig(TestExecutor.getConfigs(Collections.emptyMap()));
    fakeKafkaService = FakeKafkaService.create();

    final Topic sourceTopic = new Topic("test_topic", Optional.empty(), new StringSerdeSupplier(), 1, 1);
    fakeKafkaService.createTopic(sourceTopic);
    TestExecutorUtil.buildStreamsTopologyTestDrivers(
            testCase,
            serviceContext,
            ksqlEngine,
            ksqlConfig,
            fakeKafkaService
    );
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldExecuteDelimited() {
    // Given:
    List<Expression> values = new ArrayList<>();
    values.add(new IntegerLiteral(1034));
    values.add(new StringLiteral("k"));
    values.add(new DoubleLiteral("45.3"));
    InsertValues insertValues = new InsertValues(QualifiedName.of("TEST"), ImmutableList.of(), values);

    // When:
    FakeInsertValuesExecutor.execute(ksqlEngine, ksqlConfig, fakeKafkaService, insertValues);

    // Then:
    assertThat(fakeKafkaService.readRecords("test_topic").get(0).getTestRecord().value, equalTo("1034,k,45.3"));
  }
}