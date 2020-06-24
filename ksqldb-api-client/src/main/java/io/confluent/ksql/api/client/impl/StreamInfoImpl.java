/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.StreamInfo;
import java.util.Objects;

public class StreamInfoImpl implements StreamInfo {

  private final String name;
  private final String topicName;
  private final String format;

  StreamInfoImpl(final String name, final String topicName, final String format) {
    this.name = Objects.requireNonNull(name);
    this.topicName = Objects.requireNonNull(topicName);
    this.format = Objects.requireNonNull(format);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getTopic() {
    return topicName;
  }

  @Override
  public String getFormat() {
    return format;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamInfoImpl that = (StreamInfoImpl) o;
    return name.equals(that.name)
        && topicName.equals(that.topicName)
        && format.equals(that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, topicName, format);
  }

  @Override
  public String toString() {
    return "StreamInfo{"
        + "name='" + name + '\''
        + ", topicName='" + topicName + '\''
        + ", format='" + format + '\''
        + '}';
  }
}
