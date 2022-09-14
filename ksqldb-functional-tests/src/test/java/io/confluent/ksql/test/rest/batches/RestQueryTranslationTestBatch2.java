package io.confluent.ksql.test.rest.batches;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.rest.RestQueryTranslationTest;
import io.confluent.ksql.test.rest.RestTestCase;
import io.confluent.ksql.test.util.ThreadTestUtil.ThreadSnapshot;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class RestQueryTranslationTestBatch2 extends RestQueryTranslationTest {

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP = createTestApp(TEST_HARNESS);
  @ClassRule
  public static final RuleChain CHAIN = createRuleChain(TEST_HARNESS, REST_APP);
  private static final AtomicReference<ThreadSnapshot> STARTING_THREADS = new AtomicReference<>();

  public RestQueryTranslationTestBatch2(String name,
      RestTestCase testCase) {
    super(name, testCase, REST_APP, TEST_HARNESS, STARTING_THREADS);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return data(3, 2);
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    super.shouldBuildAndExecuteQueries();
  }
}
