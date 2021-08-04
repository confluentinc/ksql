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

package io.confluent.ksql.test.model;

import static org.hamcrest.Matchers.hasItems;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.util.List;
import java.util.regex.Pattern;
import org.hamcrest.Matcher;

// CHECKSTYLE_RULES.OFF: ArrayTypeStyle
// suppress due to https://github.com/checkstyle/checkstyle/issues/10215
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PostConditionsNode {

  private final List<SourceNode> sources;
  private final TopicsNode topics;

  public PostConditionsNode(
      @JsonProperty("sources") final List<SourceNode> sources,
      @JsonProperty("topics") final TopicsNode topics
  ) {
    this.sources = sources == null ? ImmutableList.of() : ImmutableList.copyOf(sources);
    this.topics = topics == null ? new TopicsNode(PostConditions.MATCH_NOTHING) : topics;
  }

  @SuppressWarnings("unchecked")
  public PostConditions build() {
    final Matcher<DataSource>[] matchers = sources.stream()
        .map(SourceNode::build)
        .toArray(Matcher[]::new);

    final Matcher<Iterable<DataSource>> sourcesMatcher = hasItems(matchers);
    final Pattern blackListPattern = topics.build();

    return new PostConditions(sourcesMatcher, blackListPattern);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class TopicsNode {

    private final String blackList;

    public TopicsNode(
        @JsonProperty("blacklist") final String blackList
    ) {
      this.blackList = blackList == null ? "" : blackList;
    }

    Pattern build() {
      try {
        return Pattern.compile(blackList);
      } catch (final Exception e) {
        throw new InvalidFieldException("blacklist", "not valid regex", e);
      }
    }
  }
}