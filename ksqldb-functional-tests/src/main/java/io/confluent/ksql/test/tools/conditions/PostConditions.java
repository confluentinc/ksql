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

package io.confluent.ksql.test.tools.conditions;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.test.model.PostConditionsNode;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;

@SuppressWarnings("unchecked")
public class PostConditions {

  public static final String MATCH_NOTHING = "(?!)";

  public static final PostConditions NONE = new PostConditions(
      hasItems(anything()),
      Pattern.compile(MATCH_NOTHING),
      new PostConditionsNode(ImmutableList.of(), Optional.empty())
  );

  private final Matcher<Iterable<DataSource>> sourcesMatcher;
  private final Pattern topicBlackList;
  private final PostConditionsNode sourceNode;

  public PostConditions(
      final Matcher<Iterable<DataSource>> sourcesMatcher,
      final Pattern topicBlackList,
      final PostConditionsNode sourceNode
  ) {
    this.sourcesMatcher = requireNonNull(sourcesMatcher, "sourcesMatcher");
    this.topicBlackList = requireNonNull(topicBlackList, "topicBlackList");
    this.sourceNode = requireNonNull(sourceNode, "sourceNode");
  }

  public void verify(
      final MetaStore metaStore,
      final Collection<String> topicNames
  ) {
    verifyMetaStore(metaStore);
    verifyTopics(topicNames);
  }

  public PostConditionsNode asNode() {
    if (this == NONE) {
      return null;
    }
    return sourceNode;
  }

  private void verifyMetaStore(final MetaStore metaStore) {
    final Collection<DataSource> values = metaStore
        .getAllDataSources()
        .values();

    final String text = values.stream()
        .map(s -> s.getDataSourceType() + ":" + s.getName().text()
            + ", key:" + s.getKeyField().ref()
            + ", value:" + s.getSchema()
            + ", keyFormat:" + s.getKsqlTopic().getKeyFormat()
        )
        .collect(Collectors.joining(System.lineSeparator()));

    assertThat("metastore sources after the statements have run:"
        + System.lineSeparator() + text, values, sourcesMatcher);
  }

  private void verifyTopics(final Collection<String> topicNames) {
    final Set<String> blackListed = topicNames.stream()
        .filter(topicBlackList.asPredicate())
        .collect(Collectors.toSet());

    assertThat("blacklisted topics found", blackListed, is(empty()));
  }
}
