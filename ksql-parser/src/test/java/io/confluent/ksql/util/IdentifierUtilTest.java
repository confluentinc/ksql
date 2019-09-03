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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

public class IdentifierUtilTest {

  @Test
  public void shouldNeedBackQuotes() {
    // Given:
    final String[] identifiers = new String[]{
        "SELECT",   // reserved word
        "@ID",      // invalid character
        "FOO.BAR",  // with a dot
    };

    // Then:
    for (final String identifier : identifiers) {
      assertThat("Expected quotes for " + identifier, IdentifierUtil.needsQuotes(identifier));
    }
  }

  @Test
  public void shouldNotNeedBackQuotes() {
    // Given:
    final String[] identifiers = new String[]{
        "FOO",      // nothing special
        "TABLES",   // in vocabulary but non-reserved
        "`SELECT`"  // already has back quotes
    };

    // Then:
    for (final String identifier : identifiers) {
      assertThat("Expected no quotes for " + identifier, !IdentifierUtil.needsQuotes(identifier));
    }
  }

  @Test
  public void shouldWrapInBackQuotes() {
    assertThat(IdentifierUtil.escape("SELECT"), is("`SELECT`"));
  }

  @Test
  public void shouldNotWrapInBackQuotes() {
    assertThat(IdentifierUtil.escape("FOO"), is("FOO"));
  }

}