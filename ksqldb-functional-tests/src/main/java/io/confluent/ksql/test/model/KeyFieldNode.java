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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.test.model.matchers.MetaStoreMatchers.KeyFieldMatchers;
import java.util.Optional;
import org.hamcrest.Matcher;

public final class KeyFieldNode {

  private static final KeyFieldNode NONE = new KeyFieldNode(Optional.empty());

  @JsonValue
  private final Optional<String> name;

  @SuppressWarnings("WeakerAccess") // Invoked via reflection
  @JsonCreator
  public KeyFieldNode(
      final Optional<String> name
  ) {
    this.name = requireNonNull(name, "name");
  }

  /**
   * @return a node that explicitly checks that no key field is set.
   */
  public static KeyFieldNode none() {
    return NONE;
  }

  Matcher<KeyField> build() {
    return KeyFieldMatchers.hasName(name);
  }
}