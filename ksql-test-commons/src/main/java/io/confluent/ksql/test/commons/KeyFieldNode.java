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

package io.confluent.ksql.test.commons;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Matcher;

@JsonDeserialize(using = KeyFieldDeserializer.class)
public class KeyFieldNode {

  static final Optional<String> EXCLUDE_NAME = Optional.of("explicit check that name is not set");
  static final Optional<Schema> EXCLUDE_SCHEMA = Optional.of(
      new SchemaBuilder(Type.STRING).name("explicit check that schema is not set").build());

  private final Optional<String> name;
  private final Optional<String> legacyName;
  private final Optional<Schema> legacySchema;

  KeyFieldNode(
      final Optional<String> name,
      final Optional<String> legacyName,
      final Optional<Schema> legacySchema
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
        : KeyFieldMatchers.hasLegacySchema(legacySchema);

    final Matcher[] matchers = Stream
        .of(nameMatcher, legacyNameMatcher, legacySchemaMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }
}