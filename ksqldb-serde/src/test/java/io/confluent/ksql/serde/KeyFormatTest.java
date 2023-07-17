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
import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.DELIMITED;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.serde.connect.ConnectProperties;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class KeyFormatTest {

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(FormatInfo.class, mock(FormatInfo.class))
        .setDefault(WindowInfo.class, mock(WindowInfo.class))
        .setDefault(SerdeFeatures.class, mock(SerdeFeatures.class))
        .testAllPublicStaticMethods(KeyFormat.class);
  }

  @Test
  public void shouldImplementEquals() {

    final FormatInfo format1 = FormatInfo.of(AVRO.name());
    final FormatInfo format2 = FormatInfo.of(JSON.name());

    final WindowInfo window1 = WindowInfo.of(SESSION, Optional.empty(), Optional.empty());
    final WindowInfo window2 = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(1000)), Optional.empty());

    new EqualsTester()
        .addEqualityGroup(
            KeyFormat.nonWindowed(format1, SerdeFeatures.of()),
            KeyFormat.nonWindowed(format1, SerdeFeatures.of())
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(format2, SerdeFeatures.of())
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(format1, SerdeFeatures.of(UNWRAP_SINGLES))
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, SerdeFeatures.of(), window1),
            KeyFormat.windowed(format1, SerdeFeatures.of(), window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format2, SerdeFeatures.of(), window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format2, SerdeFeatures.of(WRAP_SINGLES), window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, SerdeFeatures.of(), window2)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final FormatInfo formatInfo = FormatInfo.of(AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "something"));
    final WindowInfo windowInfo = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(10101)), Optional.empty());

    final KeyFormat keyFormat = KeyFormat.windowed(formatInfo, SerdeFeatures.of(WRAP_SINGLES), windowInfo);

    // When:
    final String result = keyFormat.toString();

    // Then:
    assertThat(result, containsString(formatInfo.toString()));
    assertThat(result, containsString(WRAP_SINGLES.toString()));
    assertThat(result, containsString(windowInfo.toString()));
  }

  @Test
  public void shouldGetFormatName() {
    // Given:
    final FormatInfo format = FormatInfo.of(DELIMITED.name());
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format, SerdeFeatures.of());

    // When:
    final String result = keyFormat.getFormat();

    // Then:
    assertThat(result, is(DELIMITED.name()));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final FormatInfo format = FormatInfo.of(AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "something"));
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format, SerdeFeatures.of());

    // When:
    final FormatInfo result = keyFormat.getFormatInfo();

    // Then:
    assertThat(result, is(format));
  }

  @Test
  public void shouldGetSereFeatures() {
    // Given:
    final FormatInfo format = FormatInfo.of(DELIMITED.name());
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format, SerdeFeatures.of(UNWRAP_SINGLES));

    // When:
    final SerdeFeatures result = keyFormat.getFeatures();

    // Then:
    assertThat(result, is(SerdeFeatures.of(UNWRAP_SINGLES)));
  }

  @Test
  public void shouldHandleNonWindowedFunctionsForNonWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(JSON.name()), SerdeFeatures.of());

    // Then:
    assertThat(keyFormat.isWindowed(), is(false));
    assertThat(keyFormat.getWindowType(), is(Optional.empty()));
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleWindowedFunctionsForWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(JSON.name()),
        SerdeFeatures.of(),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)), Optional.empty())
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
        FormatInfo.of(AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "something")),
        SerdeFeatures.of(),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)), Optional.empty())
    );

    // Then:
    assertThat(keyFormat.getFormatInfo(), is(FormatInfo.of(AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "something"))));
  }

  @Test
  public void shouldHandleWindowedWithOutSize() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(DELIMITED.name()),
        SerdeFeatures.of(),
        WindowInfo.of(SESSION, Optional.empty(), Optional.empty())
    );

    // Then:
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }
}