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

package io.confluent.ksql.serde;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

@Immutable
public interface KeySerde<K> extends Serde<K> {

  /**
   * @return {@code true} if this serde is configured for a {@link Windowed} key.
   */
  boolean isWindowed();

  /**
   * Create a new instance, the same as this, except bound to a new key schema.
   *
   * @param keySchema the new key's schema.
   * @return the new instance
   */
  KeySerde<Struct> rebind(PersistenceSchema keySchema);

  /**
   * Create a new instance, the same as this, but now windowed.
   *
   * @param window the info about the window
   * @return the new instance.
   */
  KeySerde<Windowed<Struct>> rebind(WindowInfo window);
}
