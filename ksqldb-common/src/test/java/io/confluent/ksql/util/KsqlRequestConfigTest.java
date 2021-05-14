/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import org.junit.Test;

public class KsqlRequestConfigTest {
  
  @Test
  public void shouldUseDefaultValuesWhenConfigNotPresent() {
    final KsqlRequestConfig requestConfig = new KsqlRequestConfig(Collections.emptyMap());
    assertThat(requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST), is(false));
    assertThat(requestConfig.getBoolean(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING), is(false));
  }
}
