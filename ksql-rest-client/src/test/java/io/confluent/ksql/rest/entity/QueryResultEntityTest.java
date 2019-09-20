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
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.rest.client.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.rest.entity.QueryResultEntity.ResultRow;
import io.confluent.ksql.rest.entity.QueryResultEntity.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.Test;

public class QueryResultEntityTest {

  private static final ObjectMapper MAPPER;
  private static final String SOME_SQL = "some SQL";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn("ROWKEY", SqlTypes.STRING)
      .valueColumn("v0", SqlTypes.DOUBLE)
      .valueColumn("v1", SqlTypes.STRING)
      .build();

  private static final Optional<Window> SESSION_WINDOW = Optional
      .of(new Window(12_234, OptionalLong.of(43_234)));

  private static final Optional<Window> TIME_WINDOW = Optional
      .of(new Window(12_234, OptionalLong.empty()));

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
    new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.TUMBLING),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(SESSION_WINDOW, A_VALUE))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnRowWindowTypeIfNoWindowTypeSupplied() {
    new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(SESSION_WINDOW, A_VALUE))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoRowWindowIfWindowTypeSupplied() {
    new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.TUMBLING),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(Optional.empty(), A_VALUE))
    );
  }

  @Test
  public void shouldNotThrowOnSessionRows() {
    new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.SESSION),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(SESSION_WINDOW, A_VALUE))
    );
  }

  @Test
  public void shouldNotThrowOnHoppingRows() {
    new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.HOPPING),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(TIME_WINDOW, A_VALUE))
    );
  }

  @Test
  public void shouldNotThrowOnTumblingRows() {
    new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.TUMBLING),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(TIME_WINDOW, A_VALUE))
    );
  }

  @Test
  public void shouldSerializeEntity() throws Exception {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.SESSION),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(SESSION_WINDOW, A_VALUE))
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, is("{"
        + "\"@type\":\"rows\","
        + "\"statementText\":\"some SQL\","
        + "\"windowType\":\"SESSION\","
        + "\"schema\":\"`ROWKEY` STRING KEY, `v0` DOUBLE, `v1` STRING\","
        + "\"rows\":["
        + "{"
        + "\"window\":{\"start\":12234,\"end\":43234},"
        + "\"values\":[\"key value\",10.1,\"some text\"]"
        + "}"
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
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.SESSION),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(SESSION_WINDOW, Arrays.asList(null, 10.1D, null)))
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, containsString("\"values\":[null,10.1,null]"));

    // When:
    final KsqlEntity result = MAPPER.readValue(json, KsqlEntity.class);

    // Then:
    assertThat(result, is(entity));
  }

  @Test
  public void shouldSerializeRowWithNoWindow() throws Exception {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(Optional.empty(), A_VALUE))
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, containsString("\"window\":null"));

    // When:
    final KsqlEntity result = MAPPER.readValue(json, KsqlEntity.class);

    // Then:
    assertThat(result, is(entity));
  }

  @Test
  public void shouldSerializeRowWithTimeWindow() throws Exception {
    // Given:

    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.HOPPING),
        LOGICAL_SCHEMA,
        ImmutableList.of(ResultRow.of(TIME_WINDOW, A_VALUE))
    );

    // When:
    final String json = MAPPER.writeValueAsString(entity);

    // Then:
    assertThat(json, containsString("\"window\":{\"start\":12234,\"end\":null}"));

    // When:
    final KsqlEntity result = MAPPER.readValue(json, KsqlEntity.class);

    // Then:
    assertThat(result, is(entity));
  }
}