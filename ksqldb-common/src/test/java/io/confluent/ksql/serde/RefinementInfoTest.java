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
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;

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
            RefinementInfo.of(Optional.of(OutputRefinement.FINAL)),
            RefinementInfo.of(Optional.of(OutputRefinement.FINAL))
        )
        .addEqualityGroup(
            RefinementInfo.of(Optional.of(OutputRefinement.CHANGES)),
            RefinementInfo.of(Optional.of(OutputRefinement.CHANGES))
        )
        .addEqualityGroup(
            RefinementInfo.of(Optional.empty()),
            RefinementInfo.of(Optional.empty())
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final RefinementInfo refinementInfo = RefinementInfo.of(Optional.of(OutputRefinement.FINAL));

    // When:
    final String result = refinementInfo.toString();

    // Then:
    assertThat(result, containsString("FINAL"));
  }

  @Test
  public void shouldGetOutputRefinement() {
    // Given:
    final RefinementInfo refinementInfo = RefinementInfo.of(Optional.of(OutputRefinement.FINAL));

    // When:
    final OutputRefinement result = refinementInfo.getOutputRefinement().get();

    // Then:
    assertThat(result, is(OutputRefinement.FINAL));
  }
}