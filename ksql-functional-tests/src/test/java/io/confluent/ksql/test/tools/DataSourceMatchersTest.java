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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.FieldMatchers;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceMatchersTest {

  @Mock
  private DataSource dataSource;

  @Test
  @SuppressWarnings("unchecked")
  public void shouldGetMatcherForName() {
    // Given:
    when(dataSource.getName()).thenReturn("foo");

    // When:
    final Matcher matcher = DataSourceMatchers.hasName("foo");

    // Then:
    assertThat(matcher, instanceOf(FeatureMatcher.class));
    final FeatureMatcher<DataSource<?>, String> featureMatcher = (FeatureMatcher<DataSource<?>, String>) matcher;
    assertTrue(featureMatcher.matches(dataSource));
  }


  @Test
  public void shouldGetCorrectTypeSafeDiagnosingMatcher() {

    // Given:

    // When:
    final Matcher matcher = DataSourceMatchers.OptionalMatchers.of(FieldMatchers.hasName("foo"));

    // Then:
    assertThat(matcher, instanceOf(TypeSafeDiagnosingMatcher.class));
    final TypeSafeDiagnosingMatcher typeSafeDiagnosingMatcher = (TypeSafeDiagnosingMatcher) matcher;
    assertTrue(typeSafeDiagnosingMatcher.matches(Optional.of(new Field("foo", 0, Schema.OPTIONAL_INT32_SCHEMA))));


  }
}