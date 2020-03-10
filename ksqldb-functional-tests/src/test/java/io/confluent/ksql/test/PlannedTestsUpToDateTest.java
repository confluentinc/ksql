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

package io.confluent.ksql.test;

import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.test.planned.TestCasePlan;
import io.confluent.ksql.test.planned.TestCasePlanLoader;
import io.confluent.ksql.test.planned.PlannedTestUtils;
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
  private static final ObjectMapper MAPPER = PlanJsonMapper.create();

  private final TestCase testCase;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return QueryTranslationTest.findTestCases()
        .filter(PlannedTestUtils::isPlannedTestCase)
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toList());
  }

  /**
   * Test to check for qtt cases that require a new plan to be generated/persisted
   *
   * @param name unused - included just so the test has a name
   * @param testCase test case to check for requiring plan generation
   */
  public PlannedTestsUpToDateTest(final String name, final TestCase testCase) {
    this.testCase = Objects.requireNonNull(testCase);
  }

  @Test
  public void shouldHaveLatestPlans() {
    final Optional<TestCasePlan> latest = TestCasePlanLoader.latestForTestCase(testCase);
    final TestCasePlan current = TestCasePlanLoader.currentForTestCase(testCase);
    assertThat(
        String.format(
            "Current query plan differs from latest for: %s. Please re-generate QTT plans."
                + " See `ksql-functional-tests/README.md` for more info.",
            testCase.getName()
        ),
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
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(plan.getPlan());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
