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

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class SourceDescriptionImplTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql"),
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "other_name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "other_type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("other_f", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("INTEGER"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "other_topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "other_keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "other_valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.emptyList(),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.emptyList(),
                Optional.empty(),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.of("timestamp"),
                Optional.empty(),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.of("window"),
                "sql")
        )
        .addEqualityGroup(
            new SourceDescriptionImpl(
                "name",
                "type",
                Collections.singletonList(new FieldInfoImpl("f1", new ColumnTypeImpl("STRING"), false)),
                "topic",
                "keyFormat",
                "valueFormat",
                Collections.singletonList(new QueryInfoImpl(QueryType.PUSH, "q_id", "q_sql",
                    Optional.empty(), Optional.empty())),
                Collections.singletonList(new QueryInfoImpl(QueryType.PERSISTENT, "q2_id", "q2_sql",
                    Optional.of("name"), Optional.of("topic"))),
                Optional.empty(),
                Optional.empty(),
                "other_sql")
        )
        .testEquals();
  }

}