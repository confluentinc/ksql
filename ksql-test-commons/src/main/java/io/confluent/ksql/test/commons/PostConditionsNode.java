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

import static org.hamcrest.Matchers.hasItems;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import java.util.List;
import org.hamcrest.Matcher;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostConditionsNode {

  private final List<SourceNode> sources;

  PostConditionsNode(@JsonProperty("sources") final List<SourceNode> sources) {
    this.sources = sources == null ? ImmutableList.of() : ImmutableList.copyOf(sources);
  }

  @SuppressWarnings("unchecked")
  PostConditions build() {
    final Matcher<StructuredDataSource<?>>[] matchers = sources.stream()
        .map(SourceNode::build)
        .toArray(Matcher[]::new);

    final Matcher<Iterable<StructuredDataSource<?>>> sourcesMatcher = hasItems(matchers);
    return new PostConditions(sourcesMatcher);
  }
}