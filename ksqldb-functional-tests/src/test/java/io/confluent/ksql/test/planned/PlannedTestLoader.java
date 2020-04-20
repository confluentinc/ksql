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

import io.confluent.ksql.test.loader.TestLoader;
import io.confluent.ksql.test.tools.VersionedTest;
import java.util.List;
import java.util.stream.Stream;

/**
 * Loads test cases that include physical plan for any QTT test case that should be tested against a
 * saved physical plan (according to {@link PlannedTestUtils#isPlannedTestCase})
 */
public final class PlannedTestLoader {

  private PlannedTestLoader() {
  }

  public static Stream<VersionedTest> load() {
    final List<String> whiteList = TestLoader.getWhiteList();

    return TestCasePlanLoader.load(whiteList)
        .map(PlannedTestUtils::buildPlannedTestCase);
  }
}
