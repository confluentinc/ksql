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

package io.confluent.ksql.properties.with;

import org.apache.kafka.common.config.ConfigDef;

/**
 * 'With Clause' properties for 'INSERT INTO' statements.
 */
public final class InsertIntoConfigs {
  public static final String QUERY_ID_PROPERTY = "ID";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          QUERY_ID_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          ConfigDef.Importance.LOW,
          "Custom query ID to use for INSERT INTO queries"
      );

  public static final ConfigMetaData CONFIG_METADATA = ConfigMetaData.of(CONFIG_DEF);

  private InsertIntoConfigs() {
  }
}
