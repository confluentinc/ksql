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

package io.confluent.ksql.analyzer;

import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.tree.ResultMaterialization;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ContinuousQueryValidatorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Analysis analysis;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new ContinuousQueryValidator();
  }

  @Test
  public void shouldThrowOnContinuousQueryThatIsFinal() {
    // Given:
    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.FINAL);

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Continuous queries don't support `EMIT FINAL`.");

    // When:
    validator.validate(analysis);
  }
}