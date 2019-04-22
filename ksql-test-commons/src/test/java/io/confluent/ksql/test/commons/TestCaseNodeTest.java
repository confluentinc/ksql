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

package io.confluent.ksql.test.commons;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.util.KsqlException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestCaseNodeTest {

  @Mock
  private FunctionRegistry functionRegistry;

  @Test
  public void shouldBuildTestsCorrectly() {
    // Given:
    final TestCaseNode testCaseNode = new TestCaseNode(
        "test",
        null,
        null,
        null,
        null,
        ImmutableList.of("Test"),
        null,
        new ExpectedExceptionNode(KsqlException.class.getCanonicalName(), ""),
        new PostConditionsNode(Collections.emptyList())
    );

    // When:
    final List<TestCase> testCases = testCaseNode.buildTests(
        Paths.get("resources/project-filter.json"), functionRegistry);

    // Then:
    assertThat(testCases.size(), is(1));
    assertThat(testCases.get(0).statements().size(), is(1));
    assertThat(testCases.get(0).statements().get(0), equalTo("Test"));

  }

}