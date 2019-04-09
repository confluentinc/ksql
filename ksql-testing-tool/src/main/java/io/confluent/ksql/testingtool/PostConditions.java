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

package io.confluent.ksql.testingtool;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;

@SuppressWarnings("unchecked")
public class PostConditions {

  static final PostConditions NONE = new PostConditions(hasItems(anything()));

  final Matcher<Iterable<StructuredDataSource<?>>> sourcesMatcher;

  PostConditions(
      final Matcher<Iterable<StructuredDataSource<?>>> sourcesMatcher
  ) {
    this.sourcesMatcher = Objects.requireNonNull(sourcesMatcher, "sourcesMatcher");
  }

  public void verify(final MetaStore metaStore) {
    final Collection<StructuredDataSource<?>> values = metaStore
        .getAllStructuredDataSources()
        .values();

    final String text = values.stream()
        .map(s -> s.getDataSourceType() + ":" + s.getName()
            + ", key:" + s.getKeyField()
            + ", value:" + s.getSchema())
        .collect(Collectors.joining(System.lineSeparator()));

    assertThat("metastore sources after the statements have run:"
        + System.lineSeparator() + text, values, sourcesMatcher);
  }
}