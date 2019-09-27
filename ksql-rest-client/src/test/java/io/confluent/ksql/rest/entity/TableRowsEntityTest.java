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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class TableRowsEntityTest {

  private static final ObjectMapper MAPPER;
  private static final String SOME_SQL = "some SQL";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .noImplicitColumns()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();


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
    new TableRowsEntity(
        SOME_SQL,
        LOGICAL_SCHEMA,
        ImmutableList.of(ImmutableList.of("too", "few"))
    );
  }

  @Test
  public void shouldSerializeEntity() throws Exception {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        LOGICAL_SCHEMA,
        ImmutableList.of(A_VALUE)
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, is("{"
        + "\"@type\":\"rows\","
        + "\"statementText\":\"some SQL\","
        + "\"schema\":\"`ROWKEY` STRING KEY, `v0` DOUBLE, `v1` STRING\","
        + "\"rows\":["
        + "[\"key value\",10.1,\"some text\"]"
        + "],"
        + "\"warnings\":[]}"));

    // When:
    final KsqlEntity result = MAPPER.readValue(json, KsqlEntity.class);

    // Then:
    assertThat(result, is(entity));
  }

  @Test
  public void shouldSerializeNullElements() throws Exception {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        LOGICAL_SCHEMA,
        ImmutableList.of(Arrays.asList(null, 10.1D, null))
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, containsString("[null,10.1,null]"));

    // When:
    final KsqlEntity result = MAPPER.readValue(json, KsqlEntity.class);

    // Then:
    assertThat(result, is(entity));
  }
}