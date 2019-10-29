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

package io.confluent.ksql.execution.function.udtf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.GenericRow;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KudtfFlatMapperTest {

  @Test
  public void shouldFlatMapOneFunction() {
    // Given:
    TableFunctionApplier applier = createApplier(Arrays.asList(10, 10, 10));
    List<TableFunctionApplier> appliers = Arrays.asList(applier);
    KudtfFlatMapper flatMapper = new KudtfFlatMapper(appliers);
    GenericRow row = new GenericRow(1, 2, 3);

    // When:
    Iterable<GenericRow> iterable = flatMapper.apply(row);

    // Then:
    Iterator<GenericRow> iter = iterable.iterator();
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10)));
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10)));
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10)));
    assertThat(iter.hasNext(), is(false));
  }

  @Test
  public void shouldZipTwoFunctions() {
    // Given:
    TableFunctionApplier applier1 = createApplier(Arrays.asList(10, 10, 10));
    TableFunctionApplier applier2 = createApplier(Arrays.asList(20, 20));
    List<TableFunctionApplier> appliers = Arrays.asList(applier1, applier2);
    KudtfFlatMapper flatMapper = new KudtfFlatMapper(appliers);
    GenericRow row = new GenericRow(1, 2, 3);

    // When:
    Iterable<GenericRow> iterable = flatMapper.apply(row);

    // Then:
    Iterator<GenericRow> iter = iterable.iterator();
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10, 20)));
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10, 20)));
    assertThat(iter.next().getColumns(), is(Arrays.asList(1, 2, 3, 10, null)));
    assertThat(iter.hasNext(), is(false));
  }

  private <T> TableFunctionApplier createApplier(List<T> list) {
    TableFunctionApplier applier = mock(TableFunctionApplier.class);
    Mockito.doReturn(list).when(applier).apply(any());
    return applier;
  }

}