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

import static io.confluent.ksql.configdef.ConfigValidators.enumValues;

import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.DurationParser;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * 'With Clause' properties for 'CREATE' statements.
 */
public final class CreateConfigs {

  public static final String WINDOW_TYPE_PROPERTY = "WINDOW_TYPE";
  public static final String WINDOW_SIZE_PROPERTY = "WINDOW_SIZE";
  public static final String SOURCE_CONNECTOR = "SOURCE_CONNECTOR";
  public static final String SOURCED_BY_CONNECTOR_PROPERTY = "SOURCED_BY_CONNECTOR";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          WINDOW_TYPE_PROPERTY,
          ConfigDef.Type.STRING,
          null,
          enumValues(WindowType.class),
          Importance.LOW,
          "If the data is windowed, i.e. was created using KSQL using a query that "
              + "contains a ``WINDOW`` clause, then the property can be used to provide the "
              + "window type. Valid values are SESSION, HOPPING or TUMBLING."
      ).define(
          WINDOW_SIZE_PROPERTY,
          Type.STRING,
          null,
          ConfigValidators.nullsAllowed(ConfigValidators.parses(DurationParser::parse)),
          Importance.LOW,
          "If the data is windowed, i.e., was created using KSQL via a query that "
              + "contains a ``WINDOW`` clause and the window is a HOPPING or TUMBLING window, "
              + "then the property should be used to provide the window size, "
              + "for example: '20 SECONDS'."
      ).define(
          SOURCE_CONNECTOR,
          Type.STRING,
          null,
          Importance.LOW,
          "Indicates that this source was created by a connector with the given name. This "
              + "is useful for understanding which sources map to which connectors and will "
              + "be automatically populated for connectors."
      ).define(
          SOURCED_BY_CONNECTOR_PROPERTY,
          Type.STRING,
          null,
          Importance.LOW,
          "Expresses the dataflow between connectors and the topics they source."
      );

  static {
    CommonCreateConfigs.addToConfigDef(CONFIG_DEF, true);
  }

  public static final ConfigMetaData CONFIG_METADATA = ConfigMetaData.of(CONFIG_DEF);

  private CreateConfigs() {
  }
}
