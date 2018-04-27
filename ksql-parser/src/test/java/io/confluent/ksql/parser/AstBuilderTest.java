/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.parser;

import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class AstBuilderTest {
  @Test
  public void shouldGetIdentifierTextInUpperCase() {
    // Given:
    final SqlBaseParser.IdentifierContext context = mock(SqlBaseParser.IdentifierContext.class);
    expect(context.getText()).andReturn("Some_Column");
    replay(context);

    // When:
    final String identifier = AstBuilder.getIdentifierText(context);

    // Then:
    assertThat(identifier, is("SOME_COLUMN"));
  }

  @Test
  public void shouldGetQuotedIdentifierTextInSuppliedCase() {
    // Given:
    final SqlBaseParser.IdentifierContext context =
        mock(SqlBaseParser.QuotedIdentifierAlternativeContext.class);
    expect(context.getText()).andReturn("\"Some_Column\"");
    replay(context);

    // When:
    final String identifier = AstBuilder.getIdentifierText(context);

    // Then:
    assertThat(identifier, is("Some_Column"));
  }

  @Test
  public void shouldGetBackQuotedIdentifierTextInSuppliedCase() {
    // Given:
    final SqlBaseParser.IdentifierContext context =
        mock(SqlBaseParser.BackQuotedIdentifierContext.class);
    expect(context.getText()).andReturn("`Some_Column`");
    replay(context);

    // When:
    final String identifier = AstBuilder.getIdentifierText(context);

    // Then:
    assertThat(identifier, is("Some_Column"));
  }
}