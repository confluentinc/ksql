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

import io.confluent.ksql.api.client.QueryInfo;
import java.util.Objects;
import java.util.Optional;

public class QueryInfoImpl implements QueryInfo {

  private final QueryType queryType;
  private final String id;
  private final String sql;
  private final Optional<String> sinkName;
  private final Optional<String> sinkTopicName;

  QueryInfoImpl(
      final QueryType queryType,
      final String id,
      final String sql,
      final Optional<String> sinkName,
      final Optional<String> sinkTopicName
  ) {
    this.queryType = Objects.requireNonNull(queryType);
    this.id = Objects.requireNonNull(id);
    this.sql = Objects.requireNonNull(sql);
    this.sinkName = Objects.requireNonNull(sinkName);
    this.sinkTopicName = Objects.requireNonNull(sinkTopicName);
  }

  @Override
  public QueryType getQueryType() {
    return queryType;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public Optional<String> getSink() {
    return sinkName;
  }

  @Override
  public Optional<String> getSinkTopic() {
    return sinkTopicName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryInfoImpl queryInfo = (QueryInfoImpl) o;
    return queryType == queryInfo.queryType
        && id.equals(queryInfo.id)
        && sql.equals(queryInfo.sql)
        && sinkName.equals(queryInfo.sinkName)
        && sinkTopicName.equals(queryInfo.sinkTopicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryType, id, sql, sinkName, sinkTopicName);
  }

  @Override
  public String toString() {
    return "QueryInfo{"
        + "queryType=" + queryType
        + ", id='" + id + '\''
        + ", sql='" + sql + '\''
        + ", sinkName=" + sinkName
        + ", sinkTopicName=" + sinkTopicName
        + '}';
  }
}
