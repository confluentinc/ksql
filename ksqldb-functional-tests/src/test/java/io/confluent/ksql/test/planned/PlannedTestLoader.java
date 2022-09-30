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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.tools.test.TestLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Loads test cases that include physical plan for any QTT test case that should be tested against a
 * saved physical plan (according to {@link PlannedTestUtils#isPlannedTestCase})
 */
public final class PlannedTestLoader {

  private final TestCasePlanLoader planLoader;
  private final Predicate<Path> predicate;

  public PlannedTestLoader() {
    this(new TestCasePlanLoader(), defaultPredicate());
  }

  @VisibleForTesting
  public PlannedTestLoader(
      final TestCasePlanLoader planLoader,
      final Predicate<Path> predicate
  ) {
    this.planLoader = requireNonNull(planLoader, "planLoader");
    this.predicate = requireNonNull(predicate, "predicate");
  }

  public Stream<TestCase> loadTests() {
    return planLoader.load(predicate)
        .filter(t -> t.getSpecNode().getTestCase().isEnabled())
        .map(PlannedTestUtils::buildPlannedTestCase);
  }

  public static Stream<TestCase> load() {
    return new PlannedTestLoader().loadTests();
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Predicate<Path> defaultPredicate() {
    final List<String> whiteList = TestLoader.getWhiteList();
    if (whiteList.isEmpty()) {
      return path -> true;
    }

    return whiteList.stream()
            .map(com.google.common.io.Files::getNameWithoutExtension)
            .map(item -> (Predicate<Path>) path -> path.getFileName().toString().startsWith(item + "_-_"))
            .reduce(path -> false, Predicate::or);
  }
}
