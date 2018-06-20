/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.test.TestUtils;

import io.confluent.ksql.metastore.MetaStore;

public class UdfLoaderUtil {

  public static void load(final MetaStore metaStore) {
    new UdfLoader(metaStore,
        TestUtils.tempDirectory(),
        UdfLoaderUtil.class.getClassLoader(),
        value -> false, new UdfCompiler(), new Metrics(), true, false)
        .load();
  }
}
