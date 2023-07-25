/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

import com.google.common.collect.ImmutableList;

import java.util.List;
import org.junit.Test;

public class JsonPathTokenizerTest {

  @Test
  public void testJsonPathTokenizer() {
    final JsonPathTokenizer jsonPathTokenizer = new JsonPathTokenizer("$.logs[0].cloud.region");
    final List<String> tokenList = ImmutableList.copyOf(jsonPathTokenizer);
    assertThat(tokenList.size(), is(equalTo(4)));
    assertThat(tokenList.get(0), is(equalTo("logs")));

    assertThat(tokenList.get(1), is(equalTo("0")));
    assertThat(tokenList.get(2), is(equalTo("cloud")));
    assertThat(tokenList.get(3), is(equalTo("region")));
  }

  @Test
  public void shouldToStringWithCarrot() {
    assertThat(new JsonPathTokenizer("$.logs[0].cloud.region").toString(),
        is("$‸.logs[0].cloud.region"));
  }
}
