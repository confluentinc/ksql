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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class FieldNameTest {

  private FieldName fieldName;
  private FieldName fieldNameNoSource;

  @Before
  public void setUp() {
    fieldName = FieldName.of(Optional.of("someSource"), "someName");
    fieldNameNoSource = FieldName.of(Optional.empty(), "someName");
  }

  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .testAllPublicStaticMethods(FieldName.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            FieldName.of(Optional.of("someSource"), "someName"),
            FieldName.of(Optional.of("someSource"), "someName")
        )
        .addEqualityGroup(
            FieldName.of(Optional.empty(), "someName".toUpperCase())
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnLeadingWhiteSpaceOfSource() {
    FieldName.of(Optional.of("  source_with_leading_ws"), "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnLeadingWhiteSpaceOfNameWhenNoSource() {
    FieldName.of(Optional.empty(), "  name_with_leading_ws");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnLeadingWhiteSpaceOfNameWhenSource() {
    FieldName.of(Optional.of("source"), "  name_with_leading_ws");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnTrailingWhiteSpaceOfSource() {
    FieldName.of(Optional.of("source_with_leading_ws  "), "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnTrailingWhiteSpaceOfNameWhenNoSource() {
    FieldName.of(Optional.empty(), "name_with_leading_ws  ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnTrailingWhiteSpaceOfNameWhenSource() {
    FieldName.of(Optional.of("source"), "name_with_leading_ws  ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptySource() {
    FieldName.of(Optional.of(""), "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptyName() {
    FieldName.of(Optional.empty(), "");
  }

  @Test
  public void shouldReturnSource() {
    assertThat(fieldName.source(), is(Optional.of("someSource")));
  }

  @Test
  public void shouldReturnName() {
    assertThat(fieldName.name(), is("someName"));
  }

  @Test
  public void shouldReturnFullName() {
    assertThat(fieldName.fullName(), is("someSource.someName"));
  }

  @Test
  public void shouldReturnFullNameNoSource() {
    assertThat(fieldNameNoSource.fullName(), is("someName"));
  }

  @Test
  public void shouldToString() {
    assertThat(fieldName.toString(), is("`someSource`.`someName`"));
    assertThat(fieldNameNoSource.toString(), is("`someName`"));
  }

  @Test
  public void shouldToStringWithReservedWords() {
    // Given:
    final FormatOptions options = FormatOptions.of(
        identifier -> identifier.equals("reserved")
            || identifier.equals("word")
            || identifier.equals("reserved.name")
    );

    // Then:
    assertThat(FieldName.of("not-reserved").toString(options),
        is("not-reserved"));

    assertThat(FieldName.of("reserved").toString(options),
        is("`reserved`"));

    assertThat(FieldName.of("reserved", "word").toString(options),
        is("`reserved`.`word`"));

    assertThat(FieldName.of("source", "word").toString(options),
        is("source.`word`"));
  }
}