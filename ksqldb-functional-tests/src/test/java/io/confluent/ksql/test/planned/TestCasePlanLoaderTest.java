/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.planned;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.test.model.KsqlVersion;
import org.junit.Test;

public class TestCasePlanLoaderTest {

  @SuppressWarnings("unused")
  @Test
  public void shouldParseVersionInPomFile() {
    // When:
    final KsqlVersion version = TestCasePlanLoader.getFormattedVersionFromPomFile();

    // Then: (did not throw)
    assertThat(version, is(notNullValue()));
  }
}