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

import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.DELIMITED;
import static io.confluent.ksql.serde.Format.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.junit.Test;

public class ValueFormatTest {

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(ValueFormat.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            ValueFormat.of(AVRO, Optional.of("this")),
            ValueFormat.of(AVRO, Optional.of("this"))
        )
        .addEqualityGroup(
            ValueFormat.of(AVRO),
            ValueFormat.of(AVRO, Optional.empty())
        )
        .addEqualityGroup(
            ValueFormat.of(JSON)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(
        AVRO,
        Optional.of("something")
    );

    // When:
    final String result = valueFormat.toString();

    // Then:
    assertThat(result, containsString("AVRO"));
    assertThat(result, containsString("something"));
  }

  @Test
  public void shouldGetFormat() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(
        DELIMITED
    );

    // When:
    final Format result = valueFormat.getFormat();

    // Then:
    assertThat(result, is(DELIMITED));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(
        AVRO,
        Optional.of("something")
    );

    // When:
    final FormatInfo result = valueFormat.getFormatInfo();

    // Then:
    assertThat(result, is(FormatInfo.of(AVRO, Optional.of("something"))));
  }
}