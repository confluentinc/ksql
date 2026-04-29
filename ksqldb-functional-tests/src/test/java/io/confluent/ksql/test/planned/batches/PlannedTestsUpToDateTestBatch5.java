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

package io.confluent.ksql.test.planned.batches;

import io.confluent.ksql.test.planned.PlannedTestsUpToDateTest;
import io.confluent.ksql.test.tools.TestCase;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PlannedTestsUpToDateTestBatch5 extends PlannedTestsUpToDateTest {

  public PlannedTestsUpToDateTestBatch5(final String name, final TestCase testCase) {
    super(name, testCase);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return PlannedTestsUpToDateTest.data(8, 5);
  }

  @Test
  public void shouldHaveLatestPlans() {
    super.shouldHaveLatestPlans();
  }
}
