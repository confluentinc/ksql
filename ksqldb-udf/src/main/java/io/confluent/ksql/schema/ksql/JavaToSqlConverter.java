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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.utils.SchemaException;
import io.confluent.ksql.types.KsqlStruct;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Version of JavaToSqlConverter with {@link KsqlStruct} linked in.
 */
class JavaToSqlConverter implements JavaToSqlTypeConverter {

  private static final BiMap<Class<?>, SqlBaseType> JAVA_TO_SQL
      = ImmutableBiMap.<Class<?>, SqlBaseType>builder()
      .put(Boolean.class, SqlBaseType.BOOLEAN)
      .put(Integer.class, SqlBaseType.INTEGER)
      .put(Long.class, SqlBaseType.BIGINT)
      .put(Double.class, SqlBaseType.DOUBLE)
      .put(String.class, SqlBaseType.STRING)
      .put(BigDecimal.class, SqlBaseType.DECIMAL)
      .put(List.class, SqlBaseType.ARRAY)
      .put(Map.class, SqlBaseType.MAP)
      .put(KsqlStruct.class, SqlBaseType.STRUCT)
      .put(Timestamp.class, SqlBaseType.TIMESTAMP)
      .build();

  @Override
  public SqlBaseType toSqlType(final Class<?> javaType) {
    return JAVA_TO_SQL.entrySet().stream()
        .filter(e -> e.getKey().isAssignableFrom(javaType))
        .map(Entry::getValue)
        .findAny()
        .orElseThrow(() -> new SchemaException("Unexpected java type: " + javaType));
  }
}
