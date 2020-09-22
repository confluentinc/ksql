/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.parser.OutputRefinement;
import org.junit.Test;

public class RefinementInfoTest {
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(RefinementInfo.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            RefinementInfo.of(OutputRefinement.FINAL),
            RefinementInfo.of(OutputRefinement.FINAL)
        )
        .addEqualityGroup(
            RefinementInfo.of(OutputRefinement.CHANGES)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final RefinementInfo refinementInfo = RefinementInfo.of(OutputRefinement.FINAL);

    // When:
    final String result = refinementInfo.toString();

    // Then:
    assertThat(result, containsString("FINAL"));
  }
}