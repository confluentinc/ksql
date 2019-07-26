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

package io.confluent.ksql.plugins.rocksdb;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

/**
 * Invoked via reflection by KsqlRestApplicationTest.java
 */
public class TestRocksDBConfigSetterWithoutConfigure implements RocksDBConfigSetter {

  @Override
  public void setConfig(
      final String storeName,
      final Options options,
      final Map<String, Object> configs) {
    // do nothing
  }

  @Override
  public void close(final String storeName, final Options options) {
  }
}