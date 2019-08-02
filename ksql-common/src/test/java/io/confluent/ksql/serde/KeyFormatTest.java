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

import static io.confluent.ksql.model.WindowType.HOPPING;
import static io.confluent.ksql.model.WindowType.SESSION;
import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.DELIMITED;
import static io.confluent.ksql.serde.Format.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class KeyFormatTest {

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(FormatInfo.class, mock(FormatInfo.class))
        .setDefault(WindowInfo.class, mock(WindowInfo.class))
        .testAllPublicStaticMethods(KeyFormat.class);
  }

  @Test
  public void shouldImplementEquals() {

    final FormatInfo format1 = FormatInfo.of(AVRO, Optional.empty());
    final FormatInfo format2 = FormatInfo.of(JSON, Optional.empty());

    final WindowInfo window1 = WindowInfo.of(SESSION, Optional.empty());
    final WindowInfo window2 = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(1000)));

    new EqualsTester()
        .addEqualityGroup(
            KeyFormat.nonWindowed(format1),
            KeyFormat.nonWindowed(format1)
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(format2)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, window1),
            KeyFormat.windowed(format1, window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format2, window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, window2)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final FormatInfo formatInfo = FormatInfo.of(AVRO, Optional.of("something"));
    final WindowInfo windowInfo = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(10101)));

    final KeyFormat keyFormat = KeyFormat.windowed(formatInfo, windowInfo);

    // When:
    final String result = keyFormat.toString();

    // Then:
    assertThat(result, containsString(formatInfo.toString()));
    assertThat(result, containsString(windowInfo.toString()));
  }

  @Test
  public void shouldGetFormat() {
    // Given:
    final FormatInfo format = FormatInfo.of(DELIMITED, Optional.empty());
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format);

    // When:
    final Format result = keyFormat.getFormat();

    // Then:
    assertThat(result, is(format.getFormat()));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final FormatInfo format = FormatInfo.of(AVRO, Optional.of("something"));
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format);

    // When:
    final FormatInfo result = keyFormat.getFormatInfo();

    // Then:
    assertThat(result, is(format));
  }

  @Test
  public void shouldHandleNoneWindowedFunctionsForNonWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(JSON, Optional.empty()));

    // Then:
    assertThat(keyFormat.isWindowed(), is(false));
    assertThat(keyFormat.getWindowType(), is(Optional.empty()));
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleWindowedFunctionsForWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(JSON),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)))
    );

    // Then:
    assertThat(keyFormat.isWindowed(), is(true));
    assertThat(keyFormat.getWindowType(), is(Optional.of(HOPPING)));
    assertThat(keyFormat.getWindowSize(), is(Optional.of(Duration.ofMinutes(4))));
  }

  @Test
  public void shouldHandleWindowedWithAvroSchemaName() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(AVRO, Optional.of("something")),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)))
    );

    // Then:
    assertThat(keyFormat.getFormatInfo(), is(FormatInfo.of(AVRO, Optional.of("something"))));
  }

  @Test
  public void shouldHandleWindowedWithOutSize() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(DELIMITED),
        WindowInfo.of(SESSION, Optional.empty())
    );

    // Then:
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }
}