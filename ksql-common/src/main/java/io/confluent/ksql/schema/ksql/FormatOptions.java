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

package io.confluent.ksql.schema.ksql;

import java.util.Objects;
import java.util.function.Function;

public final class FormatOptions {

  private final Function<String, String> fieldNameEscaper;

  public static FormatOptions none() {
    return new FormatOptions(Function.identity());
  }

  public static FormatOptions of(final Function<String, String> fieldNameEscaper) {
    return new FormatOptions(fieldNameEscaper);
  }

  private FormatOptions(final Function<String, String> fieldNameEscaper) {
    this.fieldNameEscaper = Objects.requireNonNull(fieldNameEscaper, "fieldNameEscaper");
  }

  public String escapeFieldName(final String word) {
    return fieldNameEscaper.apply(word);
  }
}
