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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.junit.Test;

public class TableRowsTest {

  private static final ObjectMapper MAPPER;
  private static final String SOME_SQL = "some SQL";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();

  private static final QueryId QUERY_ID = new QueryId("bob");

  private static final List<?> A_VALUE =
      ImmutableList.of("key value", 10.1D, "some text");

  static {
    MAPPER = new ObjectMapper();
    MAPPER.registerModule(new Jdk8Module());
    MAPPER.registerModule(new KsqlTypesSerializationModule());
    MAPPER.registerModule(new KsqlTypesDeserializationModule());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnRowWindowTypeMismatch() {
    new TableRows(
        SOME_SQL,
        QUERY_ID,
        LOGICAL_SCHEMA,
        ImmutableList.of(ImmutableList.of("too", "few"))
    );
  }
}