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
package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import java.nio.ByteBuffer;

public class SplitTest {
  private final static Split splitUdf = new Split();

  private final static ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[]{});
  private final static ByteBuffer DOT_BYTES = ByteBuffer.wrap(new byte[]{'.'});
  private final static ByteBuffer X_DASH_Y_BYTES = ByteBuffer.wrap(new byte[]{'x', '-', 'y'});

  @Test
  public void shouldReturnNullOnAnyNullParametersOnSplitString() {
    assertThat(splitUdf.split(null, ""), is(nullValue()));
    assertThat(splitUdf.split("", null), is(nullValue()));
    assertThat(splitUdf.split((String) null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnAnyNullParametersOnSplitBytes() {
    assertThat(splitUdf.split(null, EMPTY_BYTES), is(nullValue()));
    assertThat(splitUdf.split(EMPTY_BYTES, null), is(nullValue()));
    assertThat(splitUdf.split((ByteBuffer) null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnOriginalStringOnNotFoundDelimiter() {
    assertThat(splitUdf.split("", "."), contains(""));
    assertThat(splitUdf.split("x-y", "."), contains("x-y"));
  }

  @Test
  public void shouldReturnOriginalBytesOnNotFoundDelimiter() {
    assertThat(splitUdf.split(EMPTY_BYTES, DOT_BYTES), contains(EMPTY_BYTES));
    assertThat(splitUdf.split(X_DASH_Y_BYTES, DOT_BYTES), contains(X_DASH_Y_BYTES));
  }

  @Test
  public void shouldSplitAllCharactersByGivenAnEmptyDelimiter() {
    assertThat(splitUdf.split("", ""), contains(""));
    assertThat(splitUdf.split("x-y", ""), contains("x", "-", "y"));
  }

  @Test
  public void shouldSplitAllBytesByGivenAnEmptyDelimiter() {
    final ByteBuffer xBytes = ByteBuffer.wrap(new byte[]{'x'});
    final ByteBuffer dashBytes = ByteBuffer.wrap(new byte[]{'-'});
    final ByteBuffer yBytes = ByteBuffer.wrap(new byte[]{'y'});

    assertThat(splitUdf.split(EMPTY_BYTES, EMPTY_BYTES), contains(EMPTY_BYTES));
    assertThat(splitUdf.split(X_DASH_Y_BYTES, EMPTY_BYTES), contains(xBytes, dashBytes, yBytes));
  }

  @Test
  public void shouldSplitStringByGivenDelimiter() {
    assertThat(splitUdf.split("x-y", "-"), contains("x", "y"));
    assertThat(splitUdf.split("x-y", "x"), contains("", "-y"));
    assertThat(splitUdf.split("x-y", "y"), contains("x-", ""));
    assertThat(splitUdf.split("a.b.c.d", "."), contains("a", "b", "c", "d"));

  }

  @Test
  public void shouldSplitBytesByGivenDelimiter() {
    assertThat(
        splitUdf.split(
            X_DASH_Y_BYTES,
            ByteBuffer.wrap(new byte[]{'-'})),
        contains(
            ByteBuffer.wrap(new byte[]{'x'}),
            ByteBuffer.wrap(new byte[]{'y'})));

    assertThat(
        splitUdf.split(
            X_DASH_Y_BYTES,
            ByteBuffer.wrap(new byte[]{'x'})),
        contains(ByteBuffer.wrap(new byte[]{}),
            ByteBuffer.wrap(new byte[]{'-','y'})));

    assertThat(
        splitUdf.split(
            X_DASH_Y_BYTES,
            ByteBuffer.wrap(new byte[]{'y'})),
        contains(
            ByteBuffer.wrap(new byte[]{'x', '-'}),
            ByteBuffer.wrap(new byte[]{})));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'a', '.', 'b', '.', 'c', '.', 'd'}),
            ByteBuffer.wrap(new byte[]{'.'})),
        contains(
            ByteBuffer.wrap(new byte[]{'a'}),
            ByteBuffer.wrap(new byte[]{'b'}),
            ByteBuffer.wrap(new byte[]{'c'}),
            ByteBuffer.wrap(new byte[]{'d'})));
  }

  @Test
  public void shouldSplitBytesByGivenMultipleBytesDelimiter() {
    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'a', '-', '-', 'b'}),
            ByteBuffer.wrap(new byte[]{'-', '-'})),
        contains(
            ByteBuffer.wrap(new byte[]{'a'}),
            ByteBuffer.wrap(new byte[]{'b'})));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'$', '-', 'a', '-', '-', 'b'}),
            ByteBuffer.wrap(new byte[]{'$', '-'})),
        contains(
            EMPTY_BYTES,
            ByteBuffer.wrap(new byte[]{'a', '-', '-', 'b'})));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'a', '-', '-', 'b', '$', '-'}),
            ByteBuffer.wrap(new byte[]{'$', '-'})),
        contains(
            ByteBuffer.wrap(new byte[]{'a', '-', '-', 'b'}),
            EMPTY_BYTES));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterStringIsFoundAtTheBeginningOrEnd() {
    assertThat(splitUdf.split("$A", "$"), contains("", "A"));
    assertThat(splitUdf.split("$A$B", "$"), contains("", "A", "B"));
    assertThat(splitUdf.split("A$", "$"), contains("A", ""));
    assertThat(splitUdf.split("A$B$", "$"), contains("A", "B", ""));
    assertThat(splitUdf.split("$A$B$", "$"), contains("", "A", "B", ""));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterBytesIsFoundAtTheBeginningOrEnd() {
    final ByteBuffer aBytes = ByteBuffer.wrap(new byte[]{'A'});
    final ByteBuffer bBytes = ByteBuffer.wrap(new byte[]{'B'});
    final ByteBuffer dollarBytes = ByteBuffer.wrap(new byte[]{'$'});

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'$','A'}),
            dollarBytes),
        contains(EMPTY_BYTES, aBytes));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'$','A','$','B'}),
            dollarBytes),
        contains(EMPTY_BYTES, aBytes, bBytes));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'A','$'}),
            dollarBytes),
        contains(aBytes, EMPTY_BYTES));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'A','$','B','$'}),
            dollarBytes),
        contains(aBytes, bBytes, EMPTY_BYTES));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'$','A','$','B','$'}),
            dollarBytes),
        contains(EMPTY_BYTES, aBytes, bBytes, EMPTY_BYTES));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterStringIsFoundInContiguousPositions() {
    assertThat(splitUdf.split("A||A", "|"), contains("A", "", "A"));
    assertThat(splitUdf.split("z||A||z", "|"), contains("z", "", "A", "", "z"));
    assertThat(splitUdf.split("||A||A", "|"), contains("", "", "A", "", "A"));
    assertThat(splitUdf.split("A||A||", "|"), contains("A", "", "A", "", ""));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterBytesIsFoundInContiguousPositions() {
    final ByteBuffer aBytes = ByteBuffer.wrap(new byte[]{'A'});
    final ByteBuffer zBytes = ByteBuffer.wrap(new byte[]{'z'});
    final ByteBuffer pipeBytes = ByteBuffer.wrap(new byte[]{'|'});

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'A','|','|','A'}),
            pipeBytes),
        contains(aBytes, EMPTY_BYTES, aBytes));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'z','|','|','A','|','|','z'}),
            pipeBytes),
        contains(zBytes, EMPTY_BYTES, aBytes, EMPTY_BYTES, zBytes));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'|', '|', 'A','|','|','A'}),
            pipeBytes),
        contains(EMPTY_BYTES, EMPTY_BYTES, aBytes, EMPTY_BYTES, aBytes));

    assertThat(
        splitUdf.split(
            ByteBuffer.wrap(new byte[]{'A','|','|','A','|','|'}),
            pipeBytes),
        contains(aBytes, EMPTY_BYTES, aBytes, EMPTY_BYTES, EMPTY_BYTES));
  }
}
