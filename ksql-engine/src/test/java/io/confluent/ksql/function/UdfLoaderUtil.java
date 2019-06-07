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

import io.confluent.ksql.metastore.MetaStore;
import java.util.Optional;
import org.apache.kafka.test.TestUtils;

public final class UdfLoaderUtil {
  private UdfLoaderUtil() {}

  public static MetaStore load(final MetaStore metaStore) {
    new UdfLoader(metaStore,
        TestUtils.tempDirectory(),
        UdfLoaderUtil.class.getClassLoader(),
        value -> false, new UdfCompiler(Optional.empty()), Optional.empty(), true)
        .load();

    return metaStore;
  }
}
