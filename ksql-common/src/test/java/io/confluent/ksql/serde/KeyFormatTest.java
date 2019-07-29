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
import static io.confluent.ksql.model.WindowType.TUMBLING;
import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.DELIMITED;
import static io.confluent.ksql.serde.Format.JSON;
import static io.confluent.ksql.serde.Format.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class KeyFormatTest {

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(KeyFormat.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            KeyFormat.nonWindowed(AVRO, Optional.of("this")),
            KeyFormat.nonWindowed(AVRO, Optional.of("this"))
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(AVRO),
            KeyFormat.nonWindowed(AVRO, Optional.empty())
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(JSON)
        )
        .addEqualityGroup(
            KeyFormat.windowed(AVRO, Optional.of("that"), SESSION, Optional.empty()),
            KeyFormat.windowed(AVRO, Optional.of("that"), SESSION, Optional.empty())
        )
        .addEqualityGroup(
            KeyFormat.windowed(AVRO, SESSION, Optional.empty()),
            KeyFormat.windowed(AVRO, Optional.empty(), SESSION, Optional.empty())
        )
        .addEqualityGroup(
            KeyFormat.windowed(KAFKA, SESSION, Optional.empty())
        )
        .addEqualityGroup(
            KeyFormat.windowed(AVRO, HOPPING, Optional.of(Duration.ofHours(1))),
            KeyFormat.windowed(AVRO, HOPPING, Optional.of(Duration.ofHours(1)))
        )
        .addEqualityGroup(
            KeyFormat.windowed(AVRO, TUMBLING, Optional.of(Duration.ofHours(1)))
        )
        .addEqualityGroup(
            KeyFormat.windowed(AVRO, TUMBLING, Optional.of(Duration.ofHours(2)))
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        AVRO,
        Optional.of("something"),
        HOPPING,
        Optional.of(Duration.ofMillis(10101))
    );

    // When:
    final String result = keyFormat.toString();

    // Then:
    assertThat(result, containsString("AVRO"));
    assertThat(result, containsString("something"));
    assertThat(result, containsString("HOPPING"));
    assertThat(result, containsString("10101"));
  }

  @Test
  public void shouldGetFormat() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(
        DELIMITED
    );

    // When:
    final Format result = keyFormat.getFormat();

    // Then:
    assertThat(result, is(DELIMITED));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(
        AVRO,
        Optional.of("something")
    );

    // When:
    final FormatInfo result = keyFormat.getFormatInfo();

    // Then:
    assertThat(result, is(FormatInfo.of(AVRO, Optional.of("something"))));
  }

  @Test
  public void shouldHandleNoneWindowedFunctionsForNonWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(JSON);

    // Then:
    assertThat(keyFormat.isWindowed(), is(false));
    assertThat(keyFormat.getWindowType(), is(Optional.empty()));
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleWindowedFunctionsForWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        JSON,
        HOPPING,
        Optional.of(Duration.ofMinutes(4))
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
        AVRO,
        Optional.of("something"),
        HOPPING,
        Optional.of(Duration.ofMinutes(4))
    );

    // Then:
    assertThat(keyFormat.getFormatInfo(), is(FormatInfo.of(AVRO, Optional.of("something"))));
  }

  @Test
  public void shouldHandleWindowedWithOutSize() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        DELIMITED,
        SESSION,
        Optional.empty()
    );

    // Then:
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }
}