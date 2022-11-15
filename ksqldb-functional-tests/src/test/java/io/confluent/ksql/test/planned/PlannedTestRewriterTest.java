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

import org.junit.Ignore;
import org.junit.Test;

public class PlannedTestRewriterTest {

  /**
   * Re-write ALL existing test plans.
   *
   * <p>NB: You'll need to temporarily comment out the {@code @Ignore} annotation to run the test,
   * but make sure you put it back afterwards!
   *
   * <p>You almost certainly do NOT want to do this as historical plans should be IMMUTABLE.
   * The only time this is really valid is if you have fixed a bug in the testing framework
   * and now need to correct bad historic test data.
   */
  @Ignore("Comment me out to rewrite the historic plans")
  @Test
  public void rewritePlans() {
    new PlannedTestRewriter(PlannedTestRewriter.FULL)
        .rewriteTestCasePlans(new TestCasePlanLoader().all());
  }

  @Test
  public void shouldNotCheckInThisClassWithTheAboveTestEnabled() throws Exception {
    final Ignore ignoreAnnotation = this.getClass()
        .getMethod("rewritePlans")
        .getAnnotation(Ignore.class);

    assertThat(
        "Ensure you add back the @Ignore annotation above before committing your change.",
        ignoreAnnotation,
        is(notNullValue())
    );
  }
}
