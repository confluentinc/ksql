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

package io.confluent.ksql.configdef;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Wrapper around ConfigDef to disable modification.
 */
public final class UnmodifiableConfigDef extends ConfigDef {

  public static UnmodifiableConfigDef of(final ConfigDef configDef) {
    return new UnmodifiableConfigDef(configDef);
  }

  private UnmodifiableConfigDef(final ConfigDef configDef) {
    super(configDef);
  }

  @Override
  public Map<String, ConfigKey> configKeys() {
    return Collections.unmodifiableMap(super.configKeys());
  }

  @Override
  public List<String> groups() {
    return Collections.unmodifiableList(super.groups());
  }

  @Override
  public ConfigDef define(final ConfigKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConfigDef withClientSslSupport() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConfigDef withClientSaslSupport() {
    throw new UnsupportedOperationException();
  }
}
