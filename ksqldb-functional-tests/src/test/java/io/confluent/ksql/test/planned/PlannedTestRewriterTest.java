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

import io.confluent.ksql.test.QueryTranslationTest;
import org.junit.Ignore;
import org.junit.Test;

public class PlannedTestRewriterTest {

  /**
   * Re-write ALL existing test plans.
   *
   * <p>You almost certainly do NOT want to do this as historical plans should be IMMUTABLE.
   * The only time this is really valid is if you have fixed a bug in the testing framework
   * and now need to correct bad historic test data.
   */
  @Test
  @Ignore
  public void rewritePlans() {
    new PlannedTestRewriter(PlannedTestRewriter.FULL)
        .rewriteTestCases(QueryTranslationTest.findTestCases());
  }
}
