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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

public final class RocksDBConfigSetterHandler {

  private RocksDBConfigSetterHandler() {
  }

  public static void maybeConfigureRocksDBConfigSetter(final KsqlConfig ksqlConfig) {
    final Map<String, Object> streamsProps = ksqlConfig.getKsqlStreamConfigProps();
    final Class<?> clazz =
        (Class) streamsProps.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

    if (clazz != null && org.apache.kafka.common.Configurable.class.isAssignableFrom(clazz)) {
      try {
        ((org.apache.kafka.common.Configurable) Utils.newInstance(clazz))
            .configure(ksqlConfig.originals());
      } catch (final Exception e) {
        throw new ConfigException(
            "Failed to configure Configurable RocksDBConfigSetter. "
                + StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG + ": " + clazz.getName(),
            e);
      }
    }
  }
}
