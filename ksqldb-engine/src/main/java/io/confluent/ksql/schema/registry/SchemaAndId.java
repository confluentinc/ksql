/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.schema.registry;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public class SchemaAndId {

  private ParsedSchema schema;
  private Integer id;

  public SchemaAndId(final ParsedSchema schema, final int id) {
    this.schema = schema;
    this.id = id;
  }

  public ParsedSchema getSchema() {
    return schema;
  }

  public Integer getId() {
    return id;
  }

}