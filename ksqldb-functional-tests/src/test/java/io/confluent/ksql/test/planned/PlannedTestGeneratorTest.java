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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.test.QueryTranslationTest;
import org.junit.Ignore;
import org.junit.Test;

public class PlannedTestGeneratorTest {

  /**
   * Run this test to generate new query plans for the {@link QueryTranslationTest} test cases.
   *
   * <p>NB: You'll need to temporarily comment out the {@code @Ignore} annotation to run the test,
   * but make sure you put it back afterward!
   *
   * <p>Ensure only the test plans you expected have changed, then check the new query plans in
   * with your change. Otherwise, {@link PlannedTestsUpToDateTest} fill fail if there are missing
   * or changed query plans.
   */
  @Ignore("Comment me out to regenerate the historic plans")
  @Test
  public void manuallyGeneratePlans() {
    PlannedTestGenerator.generatePlans(QueryTranslationTest.findTestCases()
        .filter(PlannedTestUtils::isIncluded), true);
  }

  @Ignore("Comment me out to regenerate the historic plans without validation")
  @Test
  public void manuallyGeneratePlansSkipCurrentTestValidation() {
    PlannedTestGenerator.generatePlans(QueryTranslationTest.findTestCases()
        .filter(PlannedTestUtils::isIncluded), false);
  }

  @Test
  public void shouldNotCheckInThisClassWithTheAboveTestEnabled() throws Exception {
    final Ignore ignoreAnnotation = this.getClass()
        .getMethod("manuallyGeneratePlans")
        .getAnnotation(Ignore.class);

    assertThat(
        "Ensure you add back the @Ignore annotation above before committing your change.",
        ignoreAnnotation,
        is(notNullValue())
    );
  }
}
