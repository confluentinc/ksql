/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class AssertTopic extends Statement {

  private final String topic;
  private final ImmutableMap<String, Literal> config;
  private final Optional<WindowTimeClause> timeout;
  private final boolean exists;

  public AssertTopic(
      final Optional<NodeLocation> location,
      final String topic,
      final Map<String, Literal> config,
      final Optional<WindowTimeClause> timeout,
      final boolean exists
  ) {
    super(location);
    this.topic = Objects.requireNonNull(topic, "topic");
    this.config = ImmutableMap.copyOf(Objects.requireNonNull(config, "config"));
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.exists = exists;
  }

  public String getTopic() {
    return topic;
  }

  public Map<String, Literal> getConfig() {
    return config;
  }

  public Optional<WindowTimeClause> getTimeout() {
    return timeout;
  }

  public boolean checkExists() {
    return exists;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AssertTopic that = (AssertTopic) o;
    return topic.equals(that.topic)
        && Objects.equals(config, that.config)
        && timeout.equals(that.timeout)
        && exists == that.exists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, config, timeout);
  }

  @Override
  public String toString() {
    return "AssertTopic{"
        + "topic=" + topic
        + ",config=" + config
        + ",timeout=" + timeout
        + ",exists=" + exists
        + '}';
  }
}
