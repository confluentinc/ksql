/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.cli.console;

import io.confluent.ksql.configdef.ConfigValidators;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

public class CliConfig extends AbstractConfig {

  public static final String WRAP_CONFIG = "WRAP";
  public static final String COLUMN_WIDTH_CONFIG = "COLUMN-WIDTH";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          WRAP_CONFIG,
          Type.STRING,
          OnOff.ON.name(),
          ConfigValidators.enumValues(OnOff.class),
          Importance.MEDIUM,
          "A value of 'OFF' will clip lines to ensure that query results do not exceed the "
              + "terminal width (i.e. each row will appear on a single line)."
      )
      .define(
          COLUMN_WIDTH_CONFIG,
          Type.INT,
          0,
          ConfigValidators.zeroOrPositive(),
          Importance.MEDIUM,
          "The width in characters of each column in tabular output. A value of '0' indicates "
              + "column width should be based on terminal width and number of columns."
      );

  public CliConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }

  public CliConfig with(final String property, final Object value) {
    if (!CONFIG_DEF.names().contains(property.toUpperCase())) {
      throw new ConfigException(
          "Undefined property: " + property + ". Valid properties are: " + CONFIG_DEF.names());
    }

    final Map<String, Object> originals = new HashMap<>(originals());
    originals.put(property.toUpperCase(), value);
    return new CliConfig(originals);
  }

  @SuppressWarnings("unused") // used in validation
  public enum OnOff {
    ON, OFF
  }

}
