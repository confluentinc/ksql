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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.server.TemporaryEngine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListFunctionsExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldListFunctions() {
    // When:
    final FunctionNameList functionList = (FunctionNameList) CustomExecutors.LIST_FUNCTIONS.execute(
        engine.configure("LIST FUNCTIONS;"),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList.getFunctions(), hasItems(
        new SimpleFunctionInfo("EXTRACTJSONFIELD", FunctionType.scalar),
        new SimpleFunctionInfo("ARRAYCONTAINS", FunctionType.scalar),
        new SimpleFunctionInfo("CONCAT", FunctionType.scalar),
        new SimpleFunctionInfo("TOPK", FunctionType.aggregate),
        new SimpleFunctionInfo("MAX", FunctionType.aggregate)));

    assertThat("shouldn't contain internal functions", functionList.getFunctions(),
        not(hasItem(new SimpleFunctionInfo("FETCH_FIELD_FROM_STRUCT", FunctionType.scalar))));
  }


}
