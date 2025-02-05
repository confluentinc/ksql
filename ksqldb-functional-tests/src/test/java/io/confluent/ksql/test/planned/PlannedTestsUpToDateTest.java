/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.planned;

import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.test.QueryTranslationTest;
import io.confluent.ksql.test.tools.TestCase;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test that ensures that each QTT test case that should be tested from a physical
 * plan has the latest physical plan written to the local filesystem.
 */
@RunWith(Parameterized.class)
public class PlannedTestsUpToDateTest {

  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();

  private final TestCase testCase;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return QueryTranslationTest.findTestCases()
        .filter(PlannedTestUtils::isPlannedTestCase)
        .filter(PlannedTestUtils::isIncluded)
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toList());
  }

  /**
   * Test to check for qtt cases that require a new plan to be generated/persisted.
   *
   * <p>If this test fails it means there is a QTT test for which there is no query plan,
   * or the query plan has changed.
   *
   * <p>If you add a new QTT test you should run
   * {@link PlannedTestGeneratorTest#manuallyGeneratePlans()}
   * to generate a plan for your new test(s) and check that in with your new test(s).
   *
   * <p>If you've made a change that means existing QTT tests are now generating different query
   * plans then you will hopefully know if this was intention or not.  If not intentional, fix
   * your changes so that they don't result in a new query plan.  If intentional, run
   * {@link PlannedTestGeneratorTest#manuallyGeneratePlans()} to generate new query plans and check
   * them in with your change.
   *
   * <p>Note: Running {@link PlannedTestGeneratorTest#manuallyGeneratePlans()} may create <i>new</i>
   * query plans. It will not change existing ones. To maintain backwards compatibility KSQL needs
   * to support both the new and old plans.
   *
   * @param name unused - included just so the test has a name
   * @param testCase test case to check for requiring plan generation
   */
  @SuppressWarnings("unused") // `name` is used to name the test.
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PlannedTestsUpToDateTest(final String name, final TestCase testCase) {
    this.testCase = Objects.requireNonNull(testCase);
  }

  @Test
  public void shouldHaveLatestPlans() {
    final Optional<TestCasePlan> latest = TestCasePlanLoader.latestForTestCase(testCase);
    final TestCasePlan current = TestCasePlanLoader.currentForTestCase(testCase, true);
    assertThat(
        "Current query plan differs from latest for: " + testCase.getName() + ". "
            + System.lineSeparator()
            + "location: " + testCase.getTestLocation()
            + System.lineSeparator()
            + "Please re-generate QTT plans by running temporarily commenting out the @Ignore and"
            + "then running PlannedTestGeneratorTest.manuallyGeneratePlans()."
            + System.lineSeparator()
            + "See `ksqldb-functional-tests/README.md` for more info.",
        current, isLatestPlan(latest)
    );
  }

  private static TypeSafeMatcher<TestCasePlan> isLatestPlan(final Optional<TestCasePlan> latest) {
    return new TypeSafeMatcher<TestCasePlan>() {
      @Override
      protected boolean matchesSafely(final TestCasePlan current) {
        return PlannedTestUtils.isSamePlan(latest, current);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(
            latest.map(PlannedTestsUpToDateTest::planText).orElse("no saved plan"));
      }
    };
  }

  private static String planText(final TestCasePlan plan) {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter()
          .writeValueAsString(plan.getPlanNode().getPlan());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
