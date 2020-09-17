/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser.json;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.junit.Test;

public class KsqlTypesSerdeModuleTest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .registerModule(new KsqlTypesDeserializationModule())
      .registerModule(new KsqlTypesSerializationModule());

  @Test
  public void shouldSerDeSqlPrimitiveTypes() throws JsonProcessingException {
    // Given:
    final SqlType[] types = new SqlType[]{
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.STRING
    };

    for (final SqlType type : types) {
      // When:
      final SqlType out = MAPPER.readValue(MAPPER.writeValueAsString(type), SqlType.class);

      // Then
      assertThat(out, is(type));
    }
  }

  @Test
  public void shouldSerDeSqlArrayTypes() throws JsonProcessingException {
    // Given:
    final SqlType[] types = new SqlType[]{
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.STRING
    };

    for (final SqlType type : types) {
      // When:
      final SqlType out = MAPPER.readValue(MAPPER.writeValueAsString(SqlArray.of(type)), SqlType.class);

      // Then
      assertThat(out, is(SqlArray.of(type)));
    }
  }

  @Test
  public void shouldSerDeSqlMapTypes() throws JsonProcessingException {
    // Given:
    final List<SqlType> types = ImmutableList.of(
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.STRING
    );

    for (final SqlType keyType : types) {
      for (final SqlType valueType : Lists.reverse(types)) {
        // When:
        final String serialized = MAPPER.writeValueAsString(SqlMap.of(keyType, valueType));
        final SqlType out = MAPPER.readValue(serialized, SqlType.class);

        // Then
        assertThat(out, is(SqlMap.of(keyType, valueType)));
      }
    }
  }

  @Test
  public void shouldSerDeStructType() throws JsonProcessingException {
    // Given:
    SqlStruct struct = SqlStruct.builder().field("foo", SqlArray.of(SqlTypes.STRING)).build();

    // When:
    final SqlType out = MAPPER.readValue(MAPPER.writeValueAsString(struct), SqlType.class);

    // Then:
    assertThat(out, is(struct));
  }
}