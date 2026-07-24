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

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tool for rewriting planned test cases
 *
 * If, after the running the re-write you want to revert changes to some subset of files, e.g. all
 * the {@code plan.json} files. Then you can run {@code git checkout '*plan.json'}.
 */
public class PlannedTestRewriter {

  private static final Logger LOG = LogManager.getLogger(PlannedTestRewriter.class);

  private final Function<TestCasePlan, TestCasePlan> rewriter;

  public static final Function<TestCasePlan, TestCasePlan> FULL
      = TestCasePlanLoader::rebuild;

  public PlannedTestRewriter(final Function<TestCasePlan, TestCasePlan> rewriter) {
    this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
  }

  public void rewriteTestCasePlans(final Stream<TestCasePlan> testPlans) {
    testPlans
        .forEach(this::rewriteTestCasePlan);
  }

  private void rewriteTestCasePlan(final TestCasePlan original) {
    LOG.info("Rewriting "
        + original.getSpecNode().getTestCase().name()
        + " - " + original.getSpecNode().getVersion());

    final TestCasePlan rewritten = rewriter.apply(original);

    TestCasePlanWriter.writeTestCasePlan(rewritten);
  }
}
