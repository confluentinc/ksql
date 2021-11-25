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

import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.serde.connect.ConnectProperties;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class ValueFormatTest {

  private static final FormatInfo FORMAT_INFO =
      FormatInfo.of(
          AVRO.name(),
          ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "something")
      );

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(SerdeFeatures.class, SerdeFeatures.of())
        .setDefault(FormatInfo.class, FormatInfo.of("AVRO"))
        .testAllPublicStaticMethods(ValueFormat.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            ValueFormat.of(FORMAT_INFO, SerdeFeatures.of()),
            ValueFormat.of(FORMAT_INFO, SerdeFeatures.of())
        )
        .addEqualityGroup(
            ValueFormat.of(FormatInfo.of(JSON.name()), SerdeFeatures.of())
        )
        .addEqualityGroup(
            ValueFormat.of(FORMAT_INFO, SerdeFeatures.of(WRAP_SINGLES))
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO, SerdeFeatures.of(WRAP_SINGLES));

    // When:
    final String result = valueFormat.toString();

    // Then:
    assertThat(result, containsString(FORMAT_INFO.toString()));
    assertThat(result, containsString(WRAP_SINGLES.toString()));
  }

  @Test
  public void shouldGetFormatName() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO, SerdeFeatures.of());

    // When:
    final String result = valueFormat.getFormat();

    // Then:
    assertThat(result, is(FORMAT_INFO.getFormat()));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO, SerdeFeatures.of());

    // When:
    final FormatInfo result = valueFormat.getFormatInfo();

    // Then:
    assertThat(result, is(FORMAT_INFO));
  }

  @Test
  public void shouldGetSereFeatures() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO, SerdeFeatures.of(UNWRAP_SINGLES));

    // When:
    final SerdeFeatures result = valueFormat.getFeatures();

    // Then:
    assertThat(result, is(SerdeFeatures.of(UNWRAP_SINGLES)));
  }
}