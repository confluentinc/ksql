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

package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.FieldMatchers;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class DataSourceMatchersTest {

  @Mock
  private DataSource<?> dataSource;

  @Test
  public void shouldMatchSourceName() {
    // Given:
    when(dataSource.getName()).thenReturn("foo");

    // Then:
    assertThat(dataSource, DataSourceMatchers.hasName("foo"));
  }

  @Test
  public void shouldNotMatchSourceName() {
    // Given:
    when(dataSource.getName()).thenReturn("not-foo");

    // Then:
    assertThat(dataSource, not(DataSourceMatchers.hasName("foo")));
  }

  @Test
  public void shouldMatchFieldName() {
    // Given:
    final Field field = Field.of("foo", SqlTypes.STRING);

    // Then:
    assertThat(field, FieldMatchers.hasFullName("foo"));
  }

  @Test
  public void shouldNotMatchFieldName() {
    // Given:
    final Field field = Field.of("not-foo", SqlTypes.STRING);

    // Then:
    assertThat(field, not(FieldMatchers.hasFullName("foo")));
  }
}