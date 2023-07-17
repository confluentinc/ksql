/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AssertValuesTest {

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private InsertValues insertA;
  @Mock
  private InsertValues insertB;

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new AssertValues(Optional.empty(), insertA),
            new AssertValues(Optional.of(new NodeLocation(1, 1)), insertA))
        .addEqualityGroup(
            new AssertValues(Optional.empty(), insertB))
        .testEquals();
  }

}