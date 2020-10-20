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

package io.confluent.ksql.test.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import io.confluent.ksql.model.SemanticVersion;
import org.junit.Test;

public class KsqlVersionTest {

  @Test
  public void shouldParseMajorMinor() {
    // When:
    final KsqlVersion result = KsqlVersion.parse("5.4");

    // Then:
    assertThat(result.getName(), is("5.4"));
    assertThat(result.getVersion(), is(SemanticVersion.of(5, 4, 0)));
  }

  @Test
  public void shouldParseMajorMinorSnapshot() {
    // When:
    final KsqlVersion result = KsqlVersion.parse("5.4-SNAPSHOT");

    // Then:
    assertThat(result.getName(), is("5.4-SNAPSHOT"));
    assertThat(result.getVersion(), is(SemanticVersion.of(5, 4, 0)));
  }

  @Test
  public void shouldParseMajorMinorPoint() {
    // When:
    final KsqlVersion result = KsqlVersion.parse("5.4.1");

    // Then:
    assertThat(result.getName(), is("5.4.1"));
    assertThat(result.getVersion(), is(SemanticVersion.of(5, 4, 1)));
  }

  @Test
  public void shouldParseMajorMinorPointSnapshot() {
    // When:
    final KsqlVersion result = KsqlVersion.parse("5.4.1-SNAPSHOT");

    // Then:
    assertThat(result.getName(), is("5.4.1-SNAPSHOT"));
    assertThat(result.getVersion(), is(SemanticVersion.of(5, 4, 1)));
  }

  @Test
  public void shouldParseNanoVersions() {
    // When:
    final KsqlVersion result = KsqlVersion.parse("5.4.1-0");

    // Then:
    assertThat(result.getName(), is("5.4.1-0"));
    assertThat(result.getVersion(), is(SemanticVersion.of(5, 4, 1)));
  }

  @Test
  public void shouldCompareUsingTimestamps() {
    // Given:
    final KsqlVersion v1 = KsqlVersion.parse("5.4.1").withTimestamp(123);
    final KsqlVersion v2 = KsqlVersion.parse("5.4.1").withTimestamp(456);

    // Then:
    assertThat(v1, lessThan(v2));
  }

  @Test
  public void shouldTreatNoTimestampAsHigher() {
    // Given:
    final KsqlVersion v1 = KsqlVersion.parse("5.4.1").withTimestamp(123);
    final KsqlVersion v2 = KsqlVersion.parse("5.4.1");

    // Then:
    assertThat(v1, lessThan(v2));
  }

  @Test
  public void shouldCompareVersionBeforeTimestamp() {
    // Given:
    final KsqlVersion v1 = KsqlVersion.parse("5.4.1").withTimestamp(456);
    final KsqlVersion v2 = KsqlVersion.parse("5.4.2").withTimestamp(123);

    // Then:
    assertThat(v1, lessThan(v2));
  }
}
