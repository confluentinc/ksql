/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.json;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;

public final class KsqlTypesSerializationModule extends SimpleModule {

  public KsqlTypesSerializationModule() {
    addSerializer(LogicalSchema.class, new LogicalSchemaSerializer());
    addSerializer(SqlType.class, new SqlTypeSchemaSerializer());
  }
}
