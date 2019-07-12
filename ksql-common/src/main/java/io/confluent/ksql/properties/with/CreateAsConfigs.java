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

package io.confluent.ksql.properties.with;

import org.apache.kafka.common.config.ConfigDef;

/**
 * 'With Clause' properties for 'CREATE AS' statements.
 */
public final class CreateAsConfigs {

  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    CommonCreateConfigs.addToConfigDef(CONFIG_DEF, false, false);
  }

  public static final ConfigMetaData CONFIG_METADATA = ConfigMetaData.of(CONFIG_DEF);

  private CreateAsConfigs() {
  }
}
