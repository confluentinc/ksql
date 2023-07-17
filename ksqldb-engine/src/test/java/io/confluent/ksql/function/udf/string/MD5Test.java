/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MD5Test {

  private MD5 udf;

  @Before
  public void setUp() {
    udf = new MD5();
  }

  @Test
  public void shouldReturnNullForNull() {
    assertThat(udf.md5(null), is(nullValue()));
  }

  @Test
  public void shouldReturnHexString() {
    assertThat(udf.md5("one"), is("f97c5d29941bfb1b2fdab0874906ab82"));
    assertThat(udf.md5("two"), is("b8a9f715dbb64fd5c56e7783c6820a61"));
    assertThat(udf.md5("three"), is("35d6d33467aae9a2e3dccb4b6b027878"));
    assertThat(udf.md5(""), is("d41d8cd98f00b204e9800998ecf8427e"));
    assertThat(udf.md5(" "), is("7215ee9c7d9dc229d2921a40e899ec5f"));

    // Sanity check some strange Unicode characters
    assertThat(udf.md5("★☀☺"), is("670884f26d7be1298886c2e9d7c248a4"));

    // Now generate random input strings and compare results to DigestUtils
    for (int i = 0; i < 1000; i++) {
      String uuid = UUID.randomUUID().toString();
      assertThat(udf.md5(uuid), is(DigestUtils.md5Hex(uuid)));
    }
  }
}

