/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.streams;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;

final class StreamsUtil {
  private StreamsUtil() {
  }

  static boolean useProvidedName(final KsqlConfig ksqlConfig) {
    return Objects.equals(
        ksqlConfig.getString(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS),
        KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON
    );
  }
}
