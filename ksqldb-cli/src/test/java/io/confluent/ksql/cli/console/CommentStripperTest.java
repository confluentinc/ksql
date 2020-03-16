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

package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import org.junit.Test;

public class CommentStripperTest {

  @Test
  public void shouldReturnLineWithoutCommentAsIs() {
    // Given:
    final String line = "no comment here";

    // Then:
    assertThat(CommentStripper.strip(line), is(sameInstance(line)));
  }

  @Test
  public void shouldReturnLineWithCommentInSingleQuotesAsIs() {
    // Given:
    final String line = "no comment here '-- even this is not a comment'...";

    // Then:
    assertThat(CommentStripper.strip(line), is(sameInstance(line)));
  }

  @Test
  public void shouldReturnLineWithCommentInBackQuotesAsIs() {
    // Given:
    final String line = "no comment here `-- even this is not a comment`...";

    // Then:
    assertThat(CommentStripper.strip(line), is(sameInstance(line)));
  }

  @Test
  public void shouldReturnLineWithCommentInDoubleQuotesAsIs() {
    // Given:
    final String line = "no comment here \"-- even this is not a comment\"...";

    // Then:
    assertThat(CommentStripper.strip(line), is(sameInstance(line)));
  }

  @Test
  public void shouldReturnLineWithUnClosedQuotesAsIs() {
    // Given:
    final String line = "no comment here ` -- even this is not a comment";

    // Then:
    assertThat(CommentStripper.strip(line), is(sameInstance(line)));
  }

  @Test
  public void shouldStripComment() {
    // Given:
    final String line = "some line -- this is a comment";

    // Then:
    assertThat(CommentStripper.strip(line), is("some line"));
  }

  @Test
  public void shouldStripDoubleComment() {
    // Given:
    final String line = "some line -- this is a comment -- with other dashes";

    // Then:
    assertThat(CommentStripper.strip(line), is("some line"));
  }

  @Test
  public void shouldStripCommentFromStatementContainingQuoteCharactersInStrings() {
    // Given:
    final String line = "\"````````\" '\"\"\"\"\"\"' '`````' -- this is a comment -- with other dashes";

    // Then:
    assertThat(CommentStripper.strip(line), is("\"````````\" '\"\"\"\"\"\"' '`````'"));
  }

  @Test
  public void shouldCorrectHandleEscapedSingleQuotes() {
    // Given:
    final String line = "'this isn''t a comment -- the first quote isn''t closed' -- comment";
    final String line2 = "'''this isn''t a comment -- the first quote isn''t closed' -- comment";

    // Then:
    assertThat(CommentStripper.strip(line), is("'this isn''t a comment -- the first quote isn''t closed'"));
    assertThat(CommentStripper.strip(line2), is("'''this isn''t a comment -- the first quote isn''t closed'"));
  }

  @Test
  public void shouldCorrectHandleEscapedDoubleQuotes() {
    // Given:
    final String line = "\"this isn''t a comment -- the first quote isn''t closed\" -- comment";
    final String line2 = "\"\"\"this isn''t a comment -- the first quote isn''t closed\" -- comment";

    // Then:
    assertThat(CommentStripper.strip(line), is("\"this isn''t a comment -- the first quote isn''t closed\""));
    assertThat(CommentStripper.strip(line2), is("\"\"\"this isn''t a comment -- the first quote isn''t closed\""));
  }

  @Test
  public void shouldHandleMultiLine() {
    // Given:
    final String line = "some multi-line\n"
        + "statement";

    // Then:
    assertThat(CommentStripper.strip(line), is("some multi-line\nstatement"));
  }

  @Test
  public void shouldTerminateCommentAtNewLine() {
    // Given:
    final String line = "some multi-line -- this is a comment\n"
        + "statement";

    // Then:
    assertThat(CommentStripper.strip(line), is("some multi-line\nstatement"));
  }
}