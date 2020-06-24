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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class MaskTest {
  private final Mask udf = new Mask();

  @Test
  public void shouldApplyAllDefaultTypeMasks() {
    final String result = udf.mask("AbCd#$123xy Z");
    assertThat(result, is("XxXx--nnnxx-X"));
  }

  @Test
  public void shouldApplyAllExplicitTypeMasks() {
    final String result = udf.mask("AbCd#$123xy Z", "Q", "q", "9", "@");
    assertThat(result, is("QqQq@@999qq@Q"));
  }

  @Test
  public void shouldMaskNothingIfNullMasks() {
    final String result = udf.mask("AbCd#$123xy Z", null, null, null, null);
    assertThat(result, is("AbCd#$123xy Z"));
  }

  @Test
  public void shouldMaskOnlySpecifiedCharTypes() {
    final String result = udf.mask("AbCd#$123xy Z", null, "q", null, "=");
    assertThat(result, is("AqCq==123qq=Z"));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    final String result = udf.mask(null);
    assertThat(result, is(nullValue()));
 }

}
