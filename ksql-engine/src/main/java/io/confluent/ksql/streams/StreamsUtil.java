/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.streams;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;

public final class StreamsUtil {
  private StreamsUtil() {
  }

  public static boolean useProvidedName(final KsqlConfig ksqlConfig) {
    return Objects.equals(
        ksqlConfig.getString(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS),
        KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON
    );
  }
}
