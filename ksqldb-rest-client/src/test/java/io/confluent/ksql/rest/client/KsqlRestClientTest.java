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

package io.confluent.ksql.rest.client;

import io.confluent.ksql.properties.LocalProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestClientTest {

  @Mock
  private KsqlClient client;
  @Mock
  private LocalProperties localProps;

  @Test
  public void shouldThrowOnInvalidServerAddress() {
    // Then:
    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        () -> new KsqlRestClient(client, "timbuktu", localProps)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The supplied serverAddress is invalid: timbuktu"));
  }
}