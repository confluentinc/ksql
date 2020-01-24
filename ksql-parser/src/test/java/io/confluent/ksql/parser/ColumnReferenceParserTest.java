/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import org.junit.Test;

public class ColumnReferenceParserTest {

  @Test
  public void shouldParseUnquotedIdentifier() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("foo");

    // Then:
    assertThat(result.name(), is(ColumnName.of("FOO")));
  }

  @Test
  public void shouldParseQuotedIdentifier() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("`foo`");

    // Then:
    assertThat(result.name(), is(ColumnName.of("foo")));
  }
}