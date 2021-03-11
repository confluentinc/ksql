/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class KsqlVersionTest {

  @Test
  public void shouldOnlyCompareMajorMinorVersionsInSame() {
    assertThat(new KsqlVersion("5.1.1").same(new KsqlVersion("5.1.0")), is(true));
    assertThat(new KsqlVersion("5.1.1").same(new KsqlVersion("5.1.1")), is(true));
    assertThat(new KsqlVersion("5.1.1").same(new KsqlVersion("5.1.2")), is(true));

    assertThat(new KsqlVersion("5.1.0").same(new KsqlVersion("5.0.0")), is(false));
    assertThat(new KsqlVersion("5.1.0").same(new KsqlVersion("5.2.0")), is(false));
    assertThat(new KsqlVersion("5.1.0").same(new KsqlVersion("6.1.0")), is(false));
    assertThat(new KsqlVersion("5.1.0").same(new KsqlVersion("6.1.0")), is(false));
  }

  @Test
  public void shouldCompareCpVersionToStandaloneVersionInSame() {
    // known mappings
    assertThat(new KsqlVersion("6.0.").same(new KsqlVersion("0.10.")), is(true));
    assertThat(new KsqlVersion("0.10.").same(new KsqlVersion("6.0.")), is(true));

    assertThat(new KsqlVersion("6.1.").same(new KsqlVersion("0.14.")), is(true));
    assertThat(new KsqlVersion("0.14.").same(new KsqlVersion("6.1.")), is(true));

    assertThat(new KsqlVersion("6.0.").same(new KsqlVersion("0.14.")), is(false));
    assertThat(new KsqlVersion("0.10.").same(new KsqlVersion("6.1.")), is(false));

    // unknown mappings
    assertThat(new KsqlVersion("5.0.").same(new KsqlVersion("0.10.")), is(false));
    assertThat(new KsqlVersion("6.0.").same(new KsqlVersion("0.11.")), is(false));
    assertThat(new KsqlVersion("6.2.").same(new KsqlVersion("0.17.")), is(false));
  }

  @Test
  public void shouldOnlyCompareMajorMinorVersionsInIsAtLeast() {
    assertThat(new KsqlVersion("5.1.0").isAtLeast(new KsqlVersion("5.1.1")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("5.1.0")), is(true));

    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("5.2.0")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("5.2.1")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("5.2.2")), is(false));
    assertThat(new KsqlVersion("5.2.1").isAtLeast(new KsqlVersion("5.1.0")), is(true));
    assertThat(new KsqlVersion("5.2.1").isAtLeast(new KsqlVersion("5.1.1")), is(true));
    assertThat(new KsqlVersion("5.2.1").isAtLeast(new KsqlVersion("5.1.2")), is(true));

    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.0.0")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.0.1")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.0.2")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.1.0")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.1.1")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.1.2")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.2.0")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.2.1")), is(false));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("6.2.2")), is(false));

    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.0.0")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.0.1")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.0.2")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.1.0")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.1.1")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.1.2")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.2.0")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.2.1")), is(true));
    assertThat(new KsqlVersion("5.1.1").isAtLeast(new KsqlVersion("4.2.2")), is(true));
  }

  @Test
  public void shouldCompareCpVersionToStandaloneVersionIsAtLeast() {
    // known mappings
    assertThat(new KsqlVersion("6.0.").isAtLeast(new KsqlVersion("0.10.")), is(true));
    assertThat(new KsqlVersion("0.10.").isAtLeast(new KsqlVersion("6.0.")), is(true));

    assertThat(new KsqlVersion("6.1.").isAtLeast(new KsqlVersion("0.10.")), is(true));
    assertThat(new KsqlVersion("6.1.").isAtLeast(new KsqlVersion("0.14.")), is(true));
    assertThat(new KsqlVersion("0.14.").isAtLeast(new KsqlVersion("6.0.")), is(true));
    assertThat(new KsqlVersion("0.14.").isAtLeast(new KsqlVersion("6.1.")), is(true));

    assertThat(new KsqlVersion("6.0.").isAtLeast(new KsqlVersion("0.14.")), is(false));
    assertThat(new KsqlVersion("0.10.").isAtLeast(new KsqlVersion("6.1.")), is(false));

    // unknown mappings
    assertThat(new KsqlVersion("6.2.").isAtLeast(new KsqlVersion("0.10.")), is(false));
    assertThat(new KsqlVersion("0.10.").isAtLeast(new KsqlVersion("6.2.")), is(false));
  }
}
