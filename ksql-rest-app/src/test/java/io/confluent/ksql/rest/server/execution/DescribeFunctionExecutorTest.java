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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
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
    final List<? extends KsqlEntity> result = CustomExecutors.DESCRIBE_FUNCTION.execute(
        engine.configure("DESCRIBE FUNCTION CONCAT;"),
        ImmutableMap.of(),
        engine.getEngine(),
        engine.getServiceContext()
    );

    // Then:
    assertThat(result, contains(instanceOf(FunctionDescriptionList.class)));
    assertThat((FunctionDescriptionList) result.get(0),
        functionList("CONCAT", FunctionType.SCALAR));
  }

  @Test
  public void shouldDescribeUDAF() {
    // When:
    final List<? extends KsqlEntity> result = CustomExecutors.DESCRIBE_FUNCTION.execute(
            engine.configure("DESCRIBE FUNCTION MAX;"),
            ImmutableMap.of(),
            engine.getEngine(),
            engine.getServiceContext()
    );

    // Then:
    assertThat(result, contains(instanceOf(FunctionDescriptionList.class)));
    assertThat((FunctionDescriptionList) result.get(0),
        functionList("MAX", FunctionType.AGGREGATE));
  }

  @Test
  public void shouldDescribeUDTF() {
    // When:
    final List<? extends KsqlEntity> result = CustomExecutors.DESCRIBE_FUNCTION.execute(
            engine.configure("DESCRIBE FUNCTION TEST_UDTF1;"),
            ImmutableMap.of(),
            engine.getEngine(),
            engine.getServiceContext()
    );

    // Then:
    assertThat(result, contains(instanceOf(FunctionDescriptionList.class)));
    assertThat((FunctionDescriptionList) result.get(0),
        functionList("TEST_UDTF1", FunctionType.TABLE));
  }

  private static Matcher<FunctionDescriptionList> functionList(
      final String name,
      final FunctionType type
  ) {
    return new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(FunctionDescriptionList item) {
        return item.getName().equals(name)
            && item.getType().equals(type);
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("with name: ").appendValue(name)
            .appendText("and type: ").appendValue(type);
      }
    };
  }
}
