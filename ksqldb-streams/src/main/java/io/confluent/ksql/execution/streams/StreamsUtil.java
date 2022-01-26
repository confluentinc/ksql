/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.context.QueryContext;
import java.util.Collections;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;

public final class StreamsUtil {
  public static final StreamsMetadataImpl NOT_AVAILABLE = new StreamsMetadataImpl(
      HostInfo.unavailable(),
      Collections.emptySet(),
      Collections.emptySet(),
      Collections.emptySet(),
      Collections.emptySet()
  );

  private StreamsUtil() {
  }

  public static String buildOpName(final QueryContext opContext) {
    return String.join("-", opContext.getContext());
  }
}
