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
import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.test.serde.KeyFieldDeserializer;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.hamcrest.Matcher;

@JsonDeserialize(using = KeyFieldDeserializer.class)
public class KeyFieldNode {

  public static final Optional<String> EXCLUDE_NAME = Optional
      .of("explicit check that name is not set");

  public static final Optional<SqlType> EXCLUDE_SCHEMA = Optional.of(
      SqlTypes
          .struct()
          .field("explicit check that schema is not set", SqlTypes.BIGINT)
          .build());

  private final Optional<String> name;
  private final Optional<String> legacyName;
  private final Optional<SqlType> legacySchema;

  public KeyFieldNode(
      final Optional<String> name,
      final Optional<String> legacyName,
      final Optional<SqlType> legacySchema
  ) {
    this.name = requireNonNull(name, "name");
    this.legacyName = requireNonNull(legacyName, "legacyName");
    this.legacySchema = requireNonNull(legacySchema, "legacySchema");
  }

  @SuppressWarnings("unchecked")
  Matcher<KeyField> build() {
    final Matcher<KeyField> nameMatcher = name.equals(EXCLUDE_NAME)
        ? null
        : KeyFieldMatchers.hasName(name);

    final Matcher<KeyField> legacyNameMatcher = legacyName.equals(EXCLUDE_NAME)
        ? null
        : KeyFieldMatchers.hasLegacyName(legacyName);

    final Matcher<KeyField> legacySchemaMatcher = legacySchema.equals(EXCLUDE_SCHEMA)
        ? null
        : KeyFieldMatchers.hasLegacyType(legacySchema);

    final Matcher<KeyField>[] matchers = Stream
        .of(nameMatcher, legacyNameMatcher, legacySchemaMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }
}