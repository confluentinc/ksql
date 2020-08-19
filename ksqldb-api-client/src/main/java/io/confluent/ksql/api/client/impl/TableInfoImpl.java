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

import io.confluent.ksql.api.client.TableInfo;
import java.util.Objects;

public class TableInfoImpl implements TableInfo {

  private final String name;
  private final String topicName;
  private final String format;
  private final boolean isWindowed;

  TableInfoImpl(
      final String name,
      final String topicName,
      final String format,
      final boolean isWindowed
  ) {
    this.name = Objects.requireNonNull(name);
    this.topicName = Objects.requireNonNull(topicName);
    this.format = Objects.requireNonNull(format);
    this.isWindowed = isWindowed;
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
  public boolean isWindowed() {
    return isWindowed;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableInfoImpl tableInfo = (TableInfoImpl) o;
    return isWindowed == tableInfo.isWindowed
        && name.equals(tableInfo.name)
        && topicName.equals(tableInfo.topicName)
        && format.equals(tableInfo.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, topicName, format, isWindowed);
  }

  @Override
  public String toString() {
    return "TableInfo{"
        + "name='" + name + '\''
        + ", topicName='" + topicName + '\''
        + ", format='" + format + '\''
        + ", isWindowed=" + isWindowed
        + '}';
  }
}
