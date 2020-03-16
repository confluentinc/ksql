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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.server.TemporaryEngine;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DescribeFunctionExecutorTest {

  @Rule public TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldDescribeUDF() {
    // When:
    final FunctionDescriptionList functionList = (FunctionDescriptionList)
        CustomExecutors.DESCRIBE_FUNCTION.execute(
            engine.configure("DESCRIBE FUNCTION CONCAT;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList, new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(final FunctionDescriptionList item) {
        return functionList.getName().equals("CONCAT")
            && functionList.getType().equals(FunctionType.SCALAR);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(functionList.getName());
      }
    });
  }

  @Test
  public void shouldDescribeUDAF() {
    // When:
    final FunctionDescriptionList functionList = (FunctionDescriptionList)
        CustomExecutors.DESCRIBE_FUNCTION.execute(
            engine.configure("DESCRIBE FUNCTION MAX;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList, new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(final FunctionDescriptionList item) {
        return functionList.getName().equals("MAX")
            && functionList.getType().equals(FunctionType.AGGREGATE);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(functionList.getName());
      }
    });
  }

  @Test
  public void shouldDescribeUDTF() {
    // When:
    final FunctionDescriptionList functionList = (FunctionDescriptionList)
        CustomExecutors.DESCRIBE_FUNCTION.execute(
            engine.configure("DESCRIBE FUNCTION TEST_UDTF1;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList, new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(final FunctionDescriptionList item) {
        return functionList.getName().equals("TEST_UDTF1")
            && functionList.getType().equals(FunctionType.TABLE);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(functionList.getName());
      }
    });
  }

}
