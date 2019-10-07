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

package io.confluent.ksql.execution.streams.materialization;

import java.net.URI;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

/**
 * Type used to locate on which KSQL node materialized data is stored.
 *
 * <p>Data stored in materialized stores can be spread across KSQL nodes. This type can be used to
 * determine which KSQL server stores a specific key.
 */
public interface Locator {

  /**
   * Locate which KSQL node stores the supplied {@code key}.
   *
   * <p>Implementations are free to return {@link Optional#empty()} if the location is not known at
   * this time.
   *
   * @param key the required key.
   * @return the owning node, if known.
   */
  Optional<KsqlNode> locate(Struct key);


  interface KsqlNode {

    /**
     * @return {@code true} if this is the local node, i.e. the KSQL instance handling the call.
     */
    boolean isLocal();

    /**
     * @return The base URI of the node, including protocol, host and port.
     */
    URI location();
  }
}
